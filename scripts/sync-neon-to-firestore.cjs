#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────────────────────
// Bem na Mosca — NEON → FIRESTORE PRICE SYNC
// ──────────────────────────────────────────────────────────────────────────────
// Syncs the current-price snapshot (`offers` table) from each pharmacy
// chain's Neon Postgres database into Firestore, keyed by barcode (EAN).
//
// COST STRATEGY (why this stays cheap at 500k+ products):
//   Firestore bills every read AND write. Instead of reading Firestore to
//   find out "does this barcode/store combo already exist, and did the
//   price change?", this script keeps that answer in a PUBLIC GITHUB REPO
//   (the "mirror" — see scripts/lib/syncMirror.cjs) which costs nothing to
//   read or write. Only rows that are NEW or CHANGED vs. the mirror ever
//   touch Firestore, and the total Firestore write count per run is capped
//   at sync-config.maxWritesPerRun (default 15,000 — safely under the
//   20,000/day free-tier write limit, leaving room for normal app traffic).
//
//   500k+ products can't be pulled in one run: each store's `offers` table
//   is paginated with a per-store cursor persisted in the mirror
//   (state/cursor.json), so the FIRST full load spreads naturally across
//   many runs/days. Once a store's cursor completes a full pass, the next
//   run starts over from the top — by then most rows are unchanged
//   (0 Firestore writes), and only real price changes get synced.
//
// BARCODE VALIDATION: barcode is the app's product-matching key across
// every store. A row is only synced if its EAN is all-digits, within
// sync-config.barcode.{minLength,maxLength}, and not all zeros — anything
// else is skipped and counted in the run report.
//
// USAGE:
//   node scripts/sync-neon-to-firestore.cjs                # dry-run, all stores
//   node scripts/sync-neon-to-firestore.cjs --apply         # write for real
//   node scripts/sync-neon-to-firestore.cjs --store drogaleste --apply
//   node scripts/sync-neon-to-firestore.cjs --apply --max-writes 500  # override cap
//
// Local/emulator use: scripts/sync-neon-to-firestore-local.cjs sets
// FIRESTORE_EMULATOR_HOST first and spawns this file — same pattern as
// import-csv-local.cjs.
// ──────────────────────────────────────────────────────────────────────────────
'use strict';

const fs = require('fs');
const path = require('path');
const { Client } = require('pg');
const syncConfig = require('./sync-config.cjs');
const { SyncMirror } = require('./lib/syncMirror.cjs');
const core = require('./lib/neonSyncCore.cjs');

const ROOT = path.resolve(__dirname, '..');

// ── CLI args ─────────────────────────────────────────────────────────────────
function parseArgs(argv) {
  const args = { apply: false, store: null, maxWrites: null, serviceAccount: null, projectId: null, ignoreGuard: false };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === '--apply') args.apply = true;
    if (token === '--dry-run') args.apply = false;
    if (token === '--store') args.store = argv[++i];
    if (token === '--max-writes') args.maxWrites = Number(argv[++i]);
    if (token === '--service-account') args.serviceAccount = argv[++i];
    if (token === '--project-id') args.projectId = argv[++i];
    // Bypass the dynamic write-budget check (today's remaining Firestore
    // write headroom) and use --max-writes/config maxWritesPerRun as-is.
    if (token === '--ignore-guard') args.ignoreGuard = true;
  }
  return args;
}
const args = parseArgs(process.argv.slice(2));

// ── .env.local (manual parse — matches import-csv.cjs / setup scripts) ───────
const envPath = path.join(ROOT, '.env.local');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith('#')) continue;
    const eq = t.indexOf('=');
    if (eq === -1) continue;
    const k = t.slice(0, eq).trim();
    const v = t.slice(eq + 1).trim();
    if (!(k in process.env)) process.env[k] = v;
  }
}

const isEmulatorRun = !!process.env.FIRESTORE_EMULATOR_HOST;
const MAX_WRITES = Number.isFinite(args.maxWrites) && args.maxWrites > 0
  ? args.maxWrites
  : syncConfig.maxWritesPerRun;

// ── Barcode validation / doc-ids / Firestore plumbing — shared with
// sync-neon-high-value.cjs via scripts/lib/neonSyncCore.cjs ─────────────────
function normalizeBarcode(value) { return core.normalizeBarcode(value); }
function isValidBarcode(barcode) { return core.isValidBarcode(barcode, syncConfig.barcode); }
function buildProductDocId(barcode) { return core.buildProductDocId(barcode); }
function buildStoreDocId(storeSlug) { return core.buildStoreDocId(storeSlug); }
function effectivePrice(row) { return core.effectivePrice(row); }
function round2(n) { return core.round2(n); }
function initFirestore() { return core.initFirestore(args, isEmulatorRun); }
function createWriteBuffer(db, batchSize) { return core.createWriteBuffer(db, batchSize); }

// ── Per-store sync ───────────────────────────────────────────────────────────
async function syncStore(db, mirror, writer, storeConfig, budget, report) {
  const url = process.env[storeConfig.envVar];
  if (!url) {
    report.skippedStores.push(`${storeConfig.slug} (no ${storeConfig.envVar})`);
    return;
  }

  const client = new Client({ connectionString: url, ssl: { rejectUnauthorized: false } });
  await client.connect();

  try {
    const cursor = mirror.getCursor(storeConfig.slug);
    // queryCursor drives THIS run's Postgres pagination (advances per row,
    // regardless of outcome). confirmedBarcode is what gets PERSISTED as
    // next run's starting point — it only advances once a row's outcome is
    // fully settled (invalid/unchanged/written), so a row that hits the
    // write-budget wall mid-page is revisited next run instead of being
    // silently skipped forever.
    let queryCursor = cursor.lastBarcode || '';
    let confirmedBarcode = cursor.lastBarcode || '';
    const totalSyncedBeforeRun = cursor.totalSynced || 0;
    let storeScanned = 0;
    let storeChanged = 0;
    let budgetHit = false;
    const storeId = buildStoreDocId(storeConfig.slug);
    let storeDocEnsured = false;

    for (;;) {
      if (budget.writesUsed >= budget.maxWrites) break;

      const { rows } = await client.query(
        `select product_id, product_name, brand, ean, regular_price, promo_price,
                is_discounted, is_available, unit, product_url, image_url, updated_at
         from offers
         where is_available = true
           and ean is not null and ean <> ''
           and ean > $1
         order by ean asc
         limit $2`,
        [queryCursor, syncConfig.pageSizePerStorePerRun]
      );

      if (rows.length === 0) {
        // Full pass complete — next run starts over (freshness check).
        if (args.apply) {
          mirror.setCursor(storeConfig.slug, { lastBarcode: '', totalSynced: totalSyncedBeforeRun, completedFullPass: true });
        }
        break;
      }

      // Batch min/max lookup from price_history for this page's barcodes.
      const eans = [...new Set(rows.map((r) => normalizeBarcode(r.ean)).filter(isValidBarcode))];
      const minMaxByEan = new Map();
      if (eans.length > 0) {
        const { rows: aggRows } = await client.query(
          `select ean,
                  min(case when is_discounted and promo_price > 0 then promo_price else regular_price end) as min_price,
                  max(case when is_discounted and promo_price > 0 then promo_price else regular_price end) as max_price
           from price_history
           where ean = any($1) and store_id = $2
           group by ean`,
          [eans, storeConfig.slug]
        );
        aggRows.forEach((r) => minMaxByEan.set(r.ean, { min: Number(r.min_price), max: Number(r.max_price) }));
      }

      for (const row of rows) {
        storeScanned += 1;
        queryCursor = row.ean;

        const barcode = normalizeBarcode(row.ean);
        if (!isValidBarcode(barcode)) {
          report.invalidBarcodes += 1;
          confirmedBarcode = row.ean;
          continue;
        }

        const price = effectivePrice(row);
        if (price === null) {
          report.invalidPrice += 1;
          confirmedBarcode = row.ean;
          continue;
        }

        const agg = minMaxByEan.get(row.ean);
        const min = agg && Number.isFinite(agg.min) ? round2(Math.min(agg.min, price)) : round2(price);
        const max = agg && Number.isFinite(agg.max) ? round2(Math.max(agg.max, price)) : round2(price);
        const priceRounded = round2(price);

        // storeConfig.slug already separates stores via the mirror's folder
        // layout (mirror/<slug>/...) — the record key itself must be the
        // PLAIN barcode, or every entry would shard by the (constant) store
        // prefix instead of spreading across shard files by barcode.
        const previous = mirror.get(storeConfig.slug, barcode);
        const unchanged = previous
          && previous.price === priceRounded
          && previous.min === min
          && previous.max === max;
        if (unchanged) { confirmedBarcode = row.ean; continue; }

        if (budget.writesUsed >= budget.maxWrites) {
          // This row's price change was NOT synced — leave confirmedBarcode
          // (and therefore the persisted cursor) pointing before it, so
          // the next run picks it up instead of skipping it forever.
          budgetHit = true;
          break;
        }

        const isNewProduct = !previous;
        const nowIso = new Date().toISOString();

        if (args.apply) {
          if (!storeDocEnsured) {
            core.ensureStoreDoc(writer, storeId, storeConfig);
            budget.writesUsed += 1;
            storeDocEnsured = true;
          }

          // Product + StoreRecentPriceEntry + PriceEntry triplet: this sync
          // does the denormalization onPriceEntryCreated would normally do,
          // since that trigger is intentionally skipped (_bulk:true) to
          // avoid its own extra reads+writes on top of this run's budget.
          core.writePriceTriplet(writer, { storeId, storeConfig, barcode, row, priceRounded, min, max, isNewProduct });
          budget.writesUsed += 3;

          await writer.flushIfFull();

          // Only persist the new state once it is actually written to
          // Firestore — a dry-run must leave the mirror and cursor
          // untouched, or it would silently "consume" real progress and
          // the next --apply run would skip rows that were never synced.
          mirror.set(storeConfig.slug, barcode, {
            price: priceRounded, min, max,
            productName: String(row.product_name || '').trim(),
            updatedAt: nowIso,
          });
        } else {
          budget.writesUsed += 3; // dry-run estimate: Product + StoreRecentPriceEntry + PriceEntry
        }

        confirmedBarcode = row.ean;
        storeChanged += 1;
      }

      if (args.apply) {
        // storeScanned = rows actually iterated this run (stops early on a
        // budget-hit break), not the raw page size fetched from Postgres —
        // pageSizePerStorePerRun can over-fetch relative to a tiny write
        // budget, and totalSynced should reflect real progress.
        mirror.setCursor(storeConfig.slug, {
          lastBarcode: confirmedBarcode,
          totalSynced: totalSyncedBeforeRun + storeScanned,
          completedFullPass: false,
        });
      }

      if (budgetHit) break;

      if (rows.length < syncConfig.pageSizePerStorePerRun) {
        // Reached the end of the table on this page.
        if (args.apply) {
          mirror.setCursor(storeConfig.slug, { lastBarcode: '', totalSynced: totalSyncedBeforeRun + storeScanned, completedFullPass: true });
        }
        break;
      }
    }

    report.perStore.push({ slug: storeConfig.slug, scanned: storeScanned, changed: storeChanged });
  } finally {
    await client.end().catch(() => {});
  }
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`\n=== Neon → Firestore sync (${isEmulatorRun ? 'EMULATOR' : 'PRODUCTION'} | ${args.apply ? 'APPLY' : 'DRY-RUN'}) ===`);

  const db = initFirestore();

  // Cap the requested budget at a safe fraction of today's REMAINING write
  // headroom (SystemHealth/firestore-free-tier-guard, kept fresh hourly by
  // firestoreFreeTierGuardScheduler) — never just the static config value.
  let effectiveMaxWrites = MAX_WRITES;
  if (args.apply && !args.ignoreGuard) {
    const dyn = await core.computeDynamicWriteBudget(db, MAX_WRITES, syncConfig.dynamicBudgetSafetyPercent);
    if (dyn.guardAvailable) {
      effectiveMaxWrites = dyn.budget;
      console.log(`Today's writes so far: ${dyn.used} / ${dyn.limit} (checked ${dyn.checkedAt || 'unknown time'})`);
      console.log(`Safe remaining (${syncConfig.dynamicBudgetSafetyPercent}% of ${dyn.remaining} left): ${dyn.safeRemaining}`);
      console.log(`Write budget this run: ${effectiveMaxWrites} (requested ${MAX_WRITES})`);
    } else {
      console.log(`Write budget this run: ${effectiveMaxWrites} (guard data unavailable — using requested/configured value as-is)`);
    }
  } else {
    console.log(`Write budget this run: ${effectiveMaxWrites}${args.ignoreGuard ? ' (--ignore-guard: dynamic check skipped)' : ''}`);
  }

  const mirror = new SyncMirror(path.join(ROOT, syncConfig.mirror.localPath));
  const writer = createWriteBuffer(db, syncConfig.writeBatchSize);

  const report = { perStore: [], skippedStores: [], invalidBarcodes: 0, invalidPrice: 0 };
  const targetStores = args.store
    ? syncConfig.stores.filter((s) => s.slug === args.store)
    : syncConfig.stores;

  if (targetStores.length === 0) {
    throw new Error(`No store config matches "${args.store}". Known slugs: ${syncConfig.stores.map((s) => s.slug).join(', ')}`);
  }

  // Fair share, not first-come-first-served: a single fixed-order shared
  // budget would let the first store in the list (drogaleste) exhaust the
  // whole run on its own backlog for many runs in a row, starving stores
  // later in the array of any sync progress for weeks. Each store with
  // configured credentials gets an EQUAL slice of MAX_WRITES this run —
  // every store makes some progress every day, none monopolizes the cap.
  const activeStores = targetStores.filter((s) => !!process.env[s.envVar]);
  for (const storeConfig of targetStores) {
    if (!process.env[storeConfig.envVar]) {
      report.skippedStores.push(`${storeConfig.slug} (no ${storeConfig.envVar})`);
    }
  }
  const perStoreWrites = activeStores.length > 0 ? Math.max(1, Math.floor(effectiveMaxWrites / activeStores.length)) : 0;

  let globalWritesUsed = 0;
  for (const storeConfig of activeStores) {
    console.log(`\n-- ${storeConfig.displayName} (${storeConfig.slug}) — budget ${perStoreWrites} --`);
    const storeBudget = { writesUsed: 0, maxWrites: perStoreWrites };
    await syncStore(db, mirror, writer, storeConfig, storeBudget, report);
    globalWritesUsed += storeBudget.writesUsed;
  }

  let gitResult = { pushed: false, reason: 'dry-run — mirror not touched' };
  if (args.apply) {
    await writer.flushRemaining();
    mirror.flush();
    gitResult = mirror.commitAndPush(`sync: ${new Date().toISOString().slice(0, 10)} — ${globalWritesUsed} writes`);
  }

  console.log('\n=== Summary ===');
  for (const s of report.perStore) {
    console.log(`  ${s.slug.padEnd(20)} scanned=${s.scanned}  changed=${s.changed}`);
  }
  if (report.skippedStores.length > 0) {
    console.log(`  Skipped: ${report.skippedStores.join(', ')}`);
  }
  console.log(`  Invalid barcodes skipped: ${report.invalidBarcodes}`);
  console.log(`  Invalid/missing price skipped: ${report.invalidPrice}`);
  console.log(`  Per-store budget this run: ${perStoreWrites} (${effectiveMaxWrites} / ${activeStores.length} active stores)`);
  console.log(`  Firestore writes ${args.apply ? 'performed' : 'estimated'}: ${globalWritesUsed} / ${effectiveMaxWrites}`);
  console.log(`  Mirror commit/push: ${gitResult.pushed ? 'OK' : `skipped (${gitResult.reason})`}`);
  if (!args.apply) {
    console.log('\n  Dry-run only — re-run with --apply to write to Firestore and update the mirror.');
  }
}

main().catch((err) => {
  console.error('Sync failed:', err?.message || err);
  process.exit(1);
});

#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────────────────────
// Bem na Mosca — NEON → FIRESTORE HIGH-VALUE CROSS-STORE SYNC
// ──────────────────────────────────────────────────────────────────────────────
// sync-neon-to-firestore.cjs walks each store's `offers` table independently,
// oldest-cursor-first — a barcode that's expensive in one store can take
// weeks to reach in another. This script instead finds every barcode that is
// EXPENSIVE ANYWHERE (>= sync-config.MIN_VALUE) and cross-checks it against
// EVERY configured store immediately, so a shopper comparing a pricey item
// gets the full cross-store picture right away instead of waiting for the
// slow cursor to catch up.
//
// WHY NOT JUST FILTER `price >= MIN_VALUE` AND SYNC THAT:
//   The same barcode can be R$100 at Drogasil and R$80 (discounted) at
//   Drogaraia. Filtering >= MIN_VALUE per store and syncing only those rows
//   would sync the R$100 offer but silently miss the R$80 one entirely — the
//   product would look artificially expensive everywhere. So the price
//   filter is only used to DISCOVER which barcodes matter; once a barcode is
//   flagged, every store is queried for it with NO price filter, to capture
//   genuinely lower prices too.
//
// TWO-PHASE APPROACH:
//   Phase 1 (discovery) — query every active store for
//     effective_price >= MIN_VALUE, collect the union of barcodes found.
//   Phase 2 (cross-store fetch) — split that union into two priority tiers
//     so the smaller, higher-value tier (>= MIN_VALUE * 1.5) is processed —
//     and, if the write budget runs out, WRITTEN — before the larger,
//     merely-above-threshold tier. For every barcode in a tier, query ALL
//     active stores for that exact barcode (no price filter) and diff
//     against the mirror same as the main sync.
//
// This is a supplementary, stateless spotlight sync — it has no cursor of
// its own (nothing to persist between runs; phase 1 just re-discovers fresh
// each time) and shares the mirror's per-(store,barcode) price cache with
// sync-neon-to-firestore.cjs, so a barcode caught up by either script won't
// be re-written as "changed" by the other.
//
// USAGE:
//   node scripts/sync-neon-high-value.cjs                       # dry-run
//   node scripts/sync-neon-high-value.cjs --apply
//   node scripts/sync-neon-high-value.cjs --apply --min-value 100000  # override
//   node scripts/sync-neon-high-value.cjs --apply --barcode 7891234567890  # skip
//     discovery, cross-check exactly this one barcode against every store
//   node scripts/sync-neon-high-value.cjs --apply --ignore-guard  # skip the
//     dynamic write-budget check, use --max-writes/config as-is
//
// DYNAMIC WRITE BUDGET: on --apply, the requested budget (--max-writes or
// sync-config.maxWritesPerRun) is capped at sync-config.dynamicBudgetSafetyPercent
// of today's REMAINING Firestore write headroom (20,000/day free tier minus
// writes already used today, per SystemHealth/firestore-free-tier-guard —
// see scripts/lib/neonSyncCore.cjs's computeDynamicWriteBudget). This can
// only shrink the budget, never grow it past what was requested.
//
// Local/emulator use: scripts/sync-neon-high-value-local.cjs sets
// FIRESTORE_EMULATOR_HOST first and spawns this file.
// ──────────────────────────────────────────────────────────────────────────────
'use strict';

const fs = require('fs');
const path = require('path');
const { Client } = require('pg');
const syncConfig = require('./sync-config.cjs');
const { SyncMirror } = require('./lib/syncMirror.cjs');
const core = require('./lib/neonSyncCore.cjs');

const ROOT = path.resolve(__dirname, '..');

// Barcodes discovered at >= MIN_VALUE * PRIORITY_MULTIPLIER are processed
// (and, under a tight write budget, written) before the rest — fewer of them
// exist, so this tier finishes fast and the highest-value mismatches never
// get starved by a big low-priority backlog.
const PRIORITY_MULTIPLIER = 1.5;

// Cross-store lookups use `ean = ANY($1)` — chunked defensively so a very
// broad MIN_VALUE never sends an unbounded query parameter array.
const CROSS_STORE_CHUNK_SIZE = 500;

// ── CLI args ─────────────────────────────────────────────────────────────────
function parseArgs(argv) {
  const args = { apply: false, minValue: null, maxWrites: null, serviceAccount: null, projectId: null, barcode: null, ignoreGuard: false };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === '--apply') args.apply = true;
    if (token === '--dry-run') args.apply = false;
    if (token === '--min-value') args.minValue = Number(argv[++i]);
    if (token === '--max-writes') args.maxWrites = Number(argv[++i]);
    if (token === '--service-account') args.serviceAccount = argv[++i];
    if (token === '--project-id') args.projectId = argv[++i];
    // Skip discovery entirely and cross-check exactly one barcode against
    // every active store — for a controlled single-product smoke test
    // instead of a full MIN_VALUE scan.
    if (token === '--barcode') args.barcode = argv[++i];
    // Bypass the dynamic write-budget check (today's remaining Firestore
    // write headroom) and use --max-writes/config maxWritesPerRun as-is.
    if (token === '--ignore-guard') args.ignoreGuard = true;
  }
  return args;
}
const args = parseArgs(process.argv.slice(2));

// ── .env.local (manual parse — matches sync-neon-to-firestore.cjs) ───────────
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
const MIN_VALUE = Number.isFinite(args.minValue) && args.minValue > 0
  ? args.minValue
  : syncConfig.MIN_VALUE;

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// ── Phase 1: discover barcodes priced >= MIN_VALUE anywhere ─────────────────
async function discoverHighValueBarcodes(clients, activeStores, report) {
  const maxPriceByBarcode = new Map();

  for (const storeConfig of activeStores) {
    const client = clients.get(storeConfig.slug);
    const { rows } = await client.query(
      `select ean, (case when is_discounted and promo_price > 0 then promo_price else regular_price end) as eff_price
       from offers
       where is_available = true
         and ean is not null and ean <> ''
         and (case when is_discounted and promo_price > 0 then promo_price else regular_price end) >= $1`,
      [MIN_VALUE]
    );

    let storeDiscovered = 0;
    for (const row of rows) {
      const barcode = core.normalizeBarcode(row.ean);
      if (!core.isValidBarcode(barcode, syncConfig.barcode)) continue;
      const price = Number(row.eff_price);
      if (!Number.isFinite(price) || price <= 0) continue;
      storeDiscovered += 1;
      const prevMax = maxPriceByBarcode.get(barcode) || 0;
      if (price > prevMax) maxPriceByBarcode.set(barcode, price);
    }
    report.discoveryByStore.push({ slug: storeConfig.slug, found: storeDiscovered });
  }

  return maxPriceByBarcode;
}

// ── Phase 2: for a list of barcodes, fetch + diff + write from every store ──
async function crossCheckBarcodes(db, mirror, writer, clients, activeStores, barcodes, budget, report, tierLabel) {
  for (const barcodeGroup of chunk(barcodes, CROSS_STORE_CHUNK_SIZE)) {
    for (const storeConfig of activeStores) {
      if (budget.writesUsed >= budget.maxWrites) return;

      const client = clients.get(storeConfig.slug);
      const { rows } = await client.query(
        `select product_id, product_name, brand, ean, regular_price, promo_price,
                is_discounted, is_available, unit, product_url, image_url, updated_at
         from offers
         where is_available = true and ean = any($1)`,
        [barcodeGroup]
      );
      if (rows.length === 0) continue;

      const eans = [...new Set(rows.map((r) => core.normalizeBarcode(r.ean)).filter((b) => core.isValidBarcode(b, syncConfig.barcode)))];
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

      const storeId = core.buildStoreDocId(storeConfig.slug);
      let storeDocEnsured = false;
      let storeScanned = 0;
      let storeChanged = 0;

      for (const row of rows) {
        if (budget.writesUsed >= budget.maxWrites) break;
        storeScanned += 1;

        const barcode = core.normalizeBarcode(row.ean);
        if (!core.isValidBarcode(barcode, syncConfig.barcode)) { report.invalidBarcodes += 1; continue; }

        const price = core.effectivePrice(row);
        if (price === null) { report.invalidPrice += 1; continue; }

        const agg = minMaxByEan.get(row.ean);
        const min = agg && Number.isFinite(agg.min) ? core.round2(Math.min(agg.min, price)) : core.round2(price);
        const max = agg && Number.isFinite(agg.max) ? core.round2(Math.max(agg.max, price)) : core.round2(price);
        const priceRounded = core.round2(price);

        const previous = mirror.get(storeConfig.slug, barcode);
        const unchanged = previous
          && previous.price === priceRounded
          && previous.min === min
          && previous.max === max;
        if (unchanged) continue;

        const isNewProduct = !previous;
        const nowIso = new Date().toISOString();

        if (args.apply) {
          if (!storeDocEnsured) {
            core.ensureStoreDoc(writer, storeId, storeConfig);
            budget.writesUsed += 1;
            storeDocEnsured = true;
          }
          core.writePriceTriplet(writer, { storeId, storeConfig, barcode, row, priceRounded, min, max, isNewProduct });
          budget.writesUsed += 3;
          await writer.flushIfFull();

          mirror.set(storeConfig.slug, barcode, {
            price: priceRounded, min, max,
            productName: String(row.product_name || '').trim(),
            updatedAt: nowIso,
          });
        } else {
          budget.writesUsed += 3; // dry-run estimate: Product + StoreRecentPriceEntry + PriceEntry
        }

        storeChanged += 1;
      }

      if (storeScanned > 0) {
        report.crossCheckByStore.push({ slug: storeConfig.slug, tier: tierLabel, scanned: storeScanned, changed: storeChanged });
      }
    }
  }
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`\n=== Neon → Firestore HIGH-VALUE cross-store sync (${isEmulatorRun ? 'EMULATOR' : 'PRODUCTION'} | ${args.apply ? 'APPLY' : 'DRY-RUN'}) ===`);
  console.log(`MIN_VALUE this run: ${MIN_VALUE} (priority tier: >= ${MIN_VALUE * PRIORITY_MULTIPLIER})`);

  const db = core.initFirestore(args, isEmulatorRun);

  // Cap the requested budget at a safe fraction of today's REMAINING write
  // headroom (SystemHealth/firestore-free-tier-guard, kept fresh hourly by
  // firestoreFreeTierGuardScheduler) — never just the static config value.
  // Skipped entirely for --dry-run (nothing gets written anyway) or
  // --ignore-guard (explicit opt-out for a controlled test).
  let effectiveMaxWrites = MAX_WRITES;
  if (args.apply && !args.ignoreGuard) {
    const dyn = await core.computeDynamicWriteBudget(db, MAX_WRITES, syncConfig.dynamicBudgetSafetyPercent);
    if (dyn.guardAvailable) {
      effectiveMaxWrites = dyn.budget;
      const estProducts = Math.floor(effectiveMaxWrites / 3);
      console.log(`Today's writes so far: ${dyn.used} / ${dyn.limit} (checked ${dyn.checkedAt || 'unknown time'})`);
      console.log(`Safe remaining (${syncConfig.dynamicBudgetSafetyPercent}% of ${dyn.remaining} left): ${dyn.safeRemaining}`);
      console.log(`Write budget this run: ${effectiveMaxWrites} (requested ${MAX_WRITES}) — ~${estProducts} product(s) at ~3 writes each`);
    } else {
      console.log(`Write budget this run: ${effectiveMaxWrites} (guard data unavailable — using requested/configured value as-is)`);
    }
  } else {
    console.log(`Write budget this run: ${effectiveMaxWrites}${args.ignoreGuard ? ' (--ignore-guard: dynamic check skipped)' : ''}`);
  }

  const mirror = new SyncMirror(path.join(ROOT, syncConfig.mirror.localPath));
  const writer = core.createWriteBuffer(db, syncConfig.writeBatchSize);

  const report = { discoveryByStore: [], crossCheckByStore: [], skippedStores: [], invalidBarcodes: 0, invalidPrice: 0 };
  const activeStores = syncConfig.stores.filter((s) => !!process.env[s.envVar]);
  for (const storeConfig of syncConfig.stores) {
    if (!process.env[storeConfig.envVar]) report.skippedStores.push(`${storeConfig.slug} (no ${storeConfig.envVar})`);
  }

  if (activeStores.length === 0) {
    throw new Error('No stores have a configured Neon connection string.');
  }

  const clients = new Map();
  for (const storeConfig of activeStores) {
    const client = new Client({ connectionString: process.env[storeConfig.envVar], ssl: { rejectUnauthorized: false } });
    await client.connect();
    clients.set(storeConfig.slug, client);
  }

  try {
    const budget = { writesUsed: 0, maxWrites: effectiveMaxWrites };

    if (args.barcode) {
      // Manual single-product mode: skip discovery, cross-check exactly this
      // barcode against every active store.
      const barcode = core.normalizeBarcode(args.barcode);
      if (!core.isValidBarcode(barcode, syncConfig.barcode)) {
        throw new Error(`--barcode "${args.barcode}" is not a valid EAN (${syncConfig.barcode.minLength}-${syncConfig.barcode.maxLength} digits, not all zeros).`);
      }
      console.log(`\n-- Manual mode: cross-checking barcode ${barcode} against ${activeStores.length} store(s) --`);
      await crossCheckBarcodes(db, mirror, writer, clients, activeStores, [barcode], budget, report, 'manual');
    } else {
      console.log('\n-- Phase 1: discovery --');
      const maxPriceByBarcode = await discoverHighValueBarcodes(clients, activeStores, report);
      for (const s of report.discoveryByStore) {
        console.log(`  ${s.slug.padEnd(20)} found=${s.found}`);
      }

      const priorityThreshold = MIN_VALUE * PRIORITY_MULTIPLIER;
      const highTier = [];
      const normalTier = [];
      for (const [barcode, maxPrice] of maxPriceByBarcode) {
        (maxPrice >= priorityThreshold ? highTier : normalTier).push([barcode, maxPrice]);
      }
      highTier.sort((a, b) => b[1] - a[1]);
      normalTier.sort((a, b) => b[1] - a[1]);
      console.log(`\nDiscovered ${maxPriceByBarcode.size} unique high-value barcode(s): ${highTier.length} priority (>= ${priorityThreshold}), ${normalTier.length} standard (>= ${MIN_VALUE})`);

      console.log('\n-- Phase 2: cross-store fetch (priority tier first) --');
      await crossCheckBarcodes(db, mirror, writer, clients, activeStores, highTier.map((x) => x[0]), budget, report, 'priority');
      if (budget.writesUsed < budget.maxWrites) {
        await crossCheckBarcodes(db, mirror, writer, clients, activeStores, normalTier.map((x) => x[0]), budget, report, 'standard');
      } else {
        console.log('  Write budget exhausted by priority tier — standard tier deferred to next run.');
      }
    }

    let gitResult = { pushed: false, reason: 'dry-run — mirror not touched' };
    if (args.apply) {
      await writer.flushRemaining();
      mirror.flush();
      gitResult = mirror.commitAndPush(`sync-high-value: ${new Date().toISOString().slice(0, 10)} — ${budget.writesUsed} writes`);
    }

    console.log('\n=== Summary ===');
    for (const s of report.crossCheckByStore) {
      console.log(`  ${s.slug.padEnd(20)} [${s.tier}] scanned=${s.scanned}  changed=${s.changed}`);
    }
    if (report.skippedStores.length > 0) {
      console.log(`  Skipped: ${report.skippedStores.join(', ')}`);
    }
    console.log(`  Invalid barcodes skipped: ${report.invalidBarcodes}`);
    console.log(`  Invalid/missing price skipped: ${report.invalidPrice}`);
    console.log(`  Firestore writes ${args.apply ? 'performed' : 'estimated'}: ${budget.writesUsed} / ${effectiveMaxWrites}`);
    console.log(`  Mirror commit/push: ${gitResult.pushed ? 'OK' : `skipped (${gitResult.reason})`}`);
    if (!args.apply) {
      console.log('\n  Dry-run only — re-run with --apply to write to Firestore and update the mirror.');
    }
  } finally {
    for (const client of clients.values()) await client.end().catch(() => {});
  }
}

main().catch((err) => {
  console.error('High-value sync failed:', err?.message || err);
  process.exit(1);
});

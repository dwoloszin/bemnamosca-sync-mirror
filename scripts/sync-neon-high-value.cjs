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

// A barcode only counts as high-value if its top price isn't an obvious
// data error. Measured 2026-07: 370 of 4433 qualifying barcodes (8.3%) had a
// max >= 10x their min across stores — e.g. a R$52 toy listed at R$2900 by
// one store, or R$17.99 vs R$1962 across 26 listings. Those single bad rows
// dragged cheap products into the high-value catalog. Compared against the
// MEDIAN (robust to outliers, unlike the mean) and only when there are enough
// listings to judge; genuine price spread between pharmacies is nowhere near
// this factor, so legitimate expensive drugs are unaffected.
const OUTLIER_MAX_OVER_MEDIAN = Number.parseFloat(process.env.OUTLIER_MAX_OVER_MEDIAN || '10') || 10;
const OUTLIER_MIN_LISTINGS = 3;

// How far a row's updated_at may lag its OWN store's newest updated_at before
// we stop trusting its `is_available` flag. Scrape cycles run every 3-8h, so
// 48h means 6-16 missed refreshes for that row while the store kept updating —
// conclusively abandoned, not a blip. See the query comment for why this is
// measured against the store's max rather than wall-clock time.
const STALE_ROW_LAG_HOURS = Number.parseFloat(process.env.STALE_ROW_LAG_HOURS || '48') || 48;

function median(nums) {
  const a = [...nums].sort((x, y) => x - y);
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

// ── Neon connection manager ──────────────────────────────────────────────────
//
// Neon computes suspend, restart and occasionally drop sockets. `pg` surfaces
// that as an ASYNCHRONOUS 'error' event on the Client — not as a rejection
// from whatever query is in flight — and an EventEmitter with no 'error'
// listener makes Node throw, which kills the process outright.
//
// That is exactly what took down the 06:30 UTC runs on 2026-07-29 and
// 2026-07-31 (`Unhandled 'error' event: Connection terminated unexpectedly`,
// 24s and 7s into the run). All 34 connections were opened up front and held
// open for the whole run, so while one store was being queried the other 33
// sat idle — and a single idle socket dying aborted every store's sync before
// one write had landed, losing the mirror commit and the resume cursor too.
//
// This manager closes both holes:
//   * every Client gets an 'error' listener, so a dropped socket degrades to
//     one store being skipped instead of crashing the run;
//   * connections are opened on demand and released the moment a store is
//     done, shrinking the idle window from minutes to a single query;
//   * a query that dies with the socket is retried once on a fresh connection,
//     which is all a transient Neon blip needs.
function createStoreConnections(report) {
  const live = new Map();   // slug → connected Client
  const dead = new Set();   // slug → unreachable, don't keep paying the timeout
  let everConnected = false;

  // One line per store in the summary, no matter how many chunks hit it.
  const recordFailure = (slug, stage, message) => {
    if (!report.failedStores.some((s) => s.startsWith(`${slug} `))) {
      report.failedStores.push(`${slug} (${stage}: ${message})`);
    }
  };

  const release = async (slug) => {
    const client = live.get(slug);
    live.delete(slug);
    if (client) await client.end().catch(() => {});
  };

  const open = async (storeConfig) => {
    // core.createNeonClient attaches the 'error' listener whose absence used
    // to crash the run; the onDrop callback just forgets the dead client so
    // the next acquire() opens a fresh one.
    const client = core.createNeonClient(
      process.env[storeConfig.envVar],
      storeConfig.slug,
      () => { if (live.get(storeConfig.slug) === client) live.delete(storeConfig.slug); },
    );
    await client.connect();
    everConnected = true;
    live.set(storeConfig.slug, client);
    return client;
  };

  const acquire = async (storeConfig) => {
    const slug = storeConfig.slug;
    if (dead.has(slug)) return null;
    if (live.has(slug)) return live.get(slug);
    try {
      return await open(storeConfig);
    } catch (err) {
      await release(slug);
      // A second attempt costs one more handshake and rescues the common case
      // of a compute that was cold when we first knocked.
      try {
        return await open(storeConfig);
      } catch (retryErr) {
        const detail = core.describeError(retryErr);
        console.warn(`  ${slug}: connection failed — ${detail}`);
        recordFailure(slug, 'connect', detail);
        dead.add(slug);
        return null;
      }
    }
  };

  const query = async (storeConfig, sql, params) => {
    const slug = storeConfig.slug;
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      const client = await acquire(storeConfig);
      if (!client) throw new Error('no usable connection');
      try {
        return await client.query(sql, params);
      } catch (err) {
        await release(slug);
        if (attempt === 2 || !core.isConnectionError(err)) throw err;
        console.warn(`  ${slug}: lost the connection mid-query (${core.describeError(err)}) — reconnecting`);
      }
    }
    throw new Error('unreachable');
  };

  return {
    acquire,
    query,
    release,
    recordFailure,
    get everConnected() { return everConnected; },
    async closeAll() {
      for (const slug of [...live.keys()]) await release(slug);
    },
  };
}

// ── Phase 1: discover barcodes priced >= MIN_VALUE anywhere ─────────────────
async function discoverHighValueBarcodes(conns, activeStores, report) {
  const maxPriceByBarcode = new Map();

  for (const storeConfig of activeStores) {
    // Isolate per-store failures: one chain's DB being unreachable, mid-
    // migration, or missing the `offers` table must not abort the whole run
    // and stall price updates for every other chain.
    let rows;
    try {
      ({ rows } = await conns.query(
        storeConfig,
        `select ean, (${core.EFFECTIVE_PRICE_SQL}) as eff_price
         from offers
         where is_available = true
           and ean is not null and ean <> ''
           and (${core.EFFECTIVE_PRICE_SQL}) >= $1`,
        [MIN_VALUE]
      ));
    } catch (err) {
      console.warn(`  ${storeConfig.slug}: discovery failed — ${err.message}`);
      conns.recordFailure(storeConfig.slug, 'discovery', err.message);
      continue;
    } finally {
      // Discovery touches each store exactly once — hand the socket back
      // immediately rather than letting it idle through every other store.
      await conns.release(storeConfig.slug);
    }

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
//
// RESUME CURSOR: this used to be stateless, processing the tier price-descending
// from index 0 every run. Once the write budget was exhausted near the top of
// the list (which it is on every run while there's a backlog), the remaining
// barcodes were never reached — they were starved indefinitely, not merely
// delayed. Measured 2026-07: 4063 qualifying products, only 643 in Firestore.
// Now each tier keeps a cursor in the mirror state and the next run continues
// where the last one stopped, wrapping around after a full pass. Ordering
// within a tier is by barcode (stable across runs, so the cursor is meaningful);
// price priority is still expressed by the tier split itself (>=1500 first).
async function crossCheckBarcodes(db, mirror, writer, conns, activeStores, barcodes, budget, report, tierLabel) {
  const cursorKey = `__highvalue_${tierLabel}__`;
  const lastBarcode = String((mirror.getCursor(cursorKey) || {}).lastBarcode || '');
  const sorted = [...barcodes].sort();
  const resumeAt = lastBarcode ? sorted.findIndex((b) => b > lastBarcode) : 0;
  const ordered = resumeAt > 0
    ? [...sorted.slice(resumeAt), ...sorted.slice(0, resumeAt)]
    : sorted;

  let lastCompleted = lastBarcode;
  let wrapped = false;
  const saveCursor = (value, completedFullPass) => {
    if (!args.apply) return;
    mirror.setCursor(cursorKey, { lastBarcode: value, completedFullPass, updatedAt: new Date().toISOString() });
  };

  const perStoreStats = new Map(); // slug -> { scanned, changed }
  const bumpStat = (slug, field) => {
    if (!perStoreStats.has(slug)) perStoreStats.set(slug, { scanned: 0, created: 0, updated: 0 });
    perStoreStats.get(slug)[field] += 1;
  };
  const storeDocEnsured = new Set();

  let outOfBudget = false;
  for (const barcodeGroup of chunk(ordered, CROSS_STORE_CHUNK_SIZE)) {
    if (outOfBudget) break; // don't run Phase A queries we can't act on
    // Phase A — collect this chunk's rows from EVERY store first (2 queries per
    // store), grouped by barcode. Processing barcode-major (rather than
    // store-major) is what lets the cursor advance one barcode at a time: a
    // chunk of 500 barcodes x 19 stores could need ~28k writes against a 1k
    // budget, so a chunk-boundary cursor would never advance at all.
    const byBarcode = new Map();
    for (const storeConfig of activeStores) {
      // Same per-store isolation as discovery: skip a failing chain for this
      // chunk instead of aborting the run for every chain.
      let rows;
      const minMaxByEan = new Map();
      try {
        // Defence-in-depth on availability. `is_available = true` is the
        // primary signal and the scrapers maintain it well (measured: every
        // available row is refreshed within 24h, and they already flag tens of
        // thousands as unavailable). This second condition only catches a row
        // the scraper left flagged available but stopped refreshing — i.e. if
        // that upstream behaviour ever regresses.
        //
        // Crucially it compares against the STORE'S OWN newest updated_at, not
        // wall-clock time. If a whole scraper dies, every row ages together so
        // the lag stays ~0 and nothing is excluded — whereas a wall-clock rule
        // would mark that entire chain unavailable and let the prune delete its
        // whole catalog from Firestore. Today this excludes 0 rows.
        ({ rows } = await conns.query(
          storeConfig,
          `select product_id, product_name, brand, ean, regular_price, promo_price,
                  is_discounted, is_available, unit, product_url, image_url, updated_at
           from offers
           where is_available = true
             and ean = any($1)
             and updated_at >= (select max(updated_at) from offers) - ($2::text || ' hours')::interval`,
          [barcodeGroup, String(STALE_ROW_LAG_HOURS)]
        ));
        if (rows.length === 0) continue;

        const eans = [...new Set(rows.map((r) => core.normalizeBarcode(r.ean)).filter((b) => core.isValidBarcode(b, syncConfig.barcode)))];
        if (eans.length > 0) {
          // The 30-day min/max must only count prices a customer could
          // actually have paid, so unavailable history is excluded (measured:
          // 25% of qualidoc's price_history rows are is_available=false).
          //
          // Wrapped in its OWN try/catch: a min/max failure must degrade to
          // "no range" (the caller falls back to the current price) rather
          // than abort the whole store for this chunk, which would silently
          // stop syncing its prices.
          try {
            const { rows: aggRows } = await conns.query(
              storeConfig,
              `select ean,
                      min(${core.EFFECTIVE_PRICE_SQL}) as min_price,
                      max(${core.EFFECTIVE_PRICE_SQL}) as max_price
               from price_history
               where ean = any($1) and store_id = $2 and is_available = true
               group by ean`,
              [eans, storeConfig.slug]
            );
            aggRows.forEach((r) => minMaxByEan.set(r.ean, { min: Number(r.min_price), max: Number(r.max_price) }));
          } catch (histErr) {
            console.warn(`  ${storeConfig.slug}: price_history min/max unavailable — ${histErr.message}`);
          }
        }
      } catch (err) {
        console.warn(`  ${storeConfig.slug}: cross-check failed — ${err.message}`);
        conns.recordFailure(storeConfig.slug, 'cross-check', err.message);
        continue;
      } finally {
        // Both of this store's queries are done for this chunk. Releasing here
        // means a store's socket is only ever open while it is being used, so
        // there is nothing sitting idle for Neon to reap mid-run.
        await conns.release(storeConfig.slug);
      }

      // A store can list the same EAN on several rows (measured: 109k such
      // rows across all chains) — variants, or scraper duplicates. Keep only
      // the CHEAPEST per (store, barcode): it's the price the customer can
      // actually pay, and it makes the write deterministic instead of
      // "whichever row happened to come last".
      const cheapestForStore = new Map();
      for (const row of rows) {
        const barcode = core.normalizeBarcode(row.ean);
        if (!core.isValidBarcode(barcode, syncConfig.barcode)) { report.invalidBarcodes += 1; continue; }
        const price = core.effectivePrice(row);
        if (price === null) continue;
        const held = cheapestForStore.get(barcode);
        if (!held || price < held.price) cheapestForStore.set(barcode, { price, row });
      }
      for (const [barcode, { row }] of cheapestForStore) {
        if (!byBarcode.has(barcode)) byBarcode.set(barcode, []);
        byBarcode.get(barcode).push({ storeConfig, row, agg: minMaxByEan.get(row.ean) });
      }
    }

    // Phase B — walk the chunk in barcode order, writing every store's price
    // for a barcode before moving on. Budget is checked per barcode, so the
    // cursor always stops on a fully-completed barcode.
    //
    // BACKLOG PRIORITY: pass 'new' writes only (store, product) pairs Firestore
    // has never seen; pass 'update' then spends whatever budget is LEFT on
    // price changes to already-synced pairs. Measured 2026-07: refreshing
    // existing pairs needs ~1242 writes/pass, more than one run's whole budget,
    // so without this split price churn would keep starving the ~10k-pair
    // backlog. Self-balancing: while the backlog is large the 'new' pass
    // consumes the budget and 'update' rarely runs; once the backlog clears the
    // 'new' pass writes nothing and updates get the full budget. The cursor
    // advances on the 'new' pass, so coverage is still guaranteed.
    for (const phase of ['new', 'update']) {
      if (outOfBudget) break;
      for (const barcode of barcodeGroup) {
      if (budget.writesUsed >= budget.maxWrites) {
        // Persist where we stopped so the next run picks up from here
        // instead of restarting at the top of the tier.
        if (phase === 'new') {
          saveCursor(lastCompleted, false);
          report.resumeFrom = report.resumeFrom || {};
          report.resumeFrom[tierLabel] = lastCompleted || '(start)';
        }
        outOfBudget = true;
        break;
      }

      const entries = byBarcode.get(barcode) || [];

      // Re-validate the high-value threshold against the DEDUPED prices.
      // Phase 1 discovery sees raw rows, so a store's duplicate row with a
      // bogus price (e.g. a R$52 toy also listed at R$2900) can qualify a
      // barcode that isn't actually high-value. Once we keep only the
      // cheapest row per store, that inflated price is gone — so if nothing
      // reaches MIN_VALUE any more, the product doesn't belong in the catalog.
      //
      // Applies to BOTH phases on purpose. Skipping only 'new' would leave
      // already-synced sub-threshold products being re-priced forever, which
      // keeps bumping their date_recorded — and orphanDataCleanupScheduler
      // deletes on staleness, so they'd never age out and never be removed.
      // (prune-discontinued can't help either: these products are still
      // is_available=true in Neon, just cheap.) Freezing them here lets the
      // 14-day orphan cleanup retire the ones already in Firestore, so the
      // catalog self-heals instead of needing a manual purge.
      if (entries.length > 0) {
        const realMax = Math.max(...entries.map((e) => core.effectivePrice(e.row) ?? 0));
        if (realMax < MIN_VALUE) {
          if (phase === 'new') {
            report.belowThresholdSkipped = (report.belowThresholdSkipped || 0) + 1;
            lastCompleted = barcode;
          }
          continue;
        }
      }

      // Reject barcodes pulled in by an obviously wrong price. All stores'
      // prices for this barcode are already in hand, so this costs no extra
      // query. Skipping here means no Firestore write at all for the barcode.
      if (entries.length >= OUTLIER_MIN_LISTINGS) {
        const prices = entries.map((e) => core.effectivePrice(e.row)).filter((p) => p !== null && p > 0);
        if (prices.length >= OUTLIER_MIN_LISTINGS) {
          const med = median(prices);
          const mx = Math.max(...prices);
          if (med > 0 && mx / med >= OUTLIER_MAX_OVER_MEDIAN) {
            if (phase === 'new') {
              report.outlierSkipped = (report.outlierSkipped || 0) + 1;
              if (!report.outlierExamples) report.outlierExamples = [];
              if (report.outlierExamples.length < 5) {
                report.outlierExamples.push(`${barcode} (max ${mx} vs median ${med})`);
              }
              lastCompleted = barcode;
            }
            continue;
          }
        }
      }

      for (const { storeConfig, row, agg } of entries) {
        if (phase === 'new') bumpStat(storeConfig.slug, 'scanned');

        const price = core.effectivePrice(row);
        if (price === null) { report.invalidPrice += 1; continue; }

        const min = agg && Number.isFinite(agg.min) ? core.round2(Math.min(agg.min, price)) : core.round2(price);
        const max = agg && Number.isFinite(agg.max) ? core.round2(Math.max(agg.max, price)) : core.round2(price);
        const priceRounded = core.round2(price);

        // Write ONLY when the current price changed. min/max (the 30-day
        // range) is purely informative — a product still at R$1000 whose
        // range shifted from 8000-12000 to 7000-13000 changes nothing the
        // user acts on, so spending 3 writes on it would starve the backlog.
        // The mirror is intentionally left untouched in that case too, so we
        // don't churn git with range-only diffs; whenever the price does
        // change we write the freshly-queried min/max along with it.
        const previous = mirror.get(storeConfig.slug, barcode);
        if (previous && previous.price === priceRounded) continue;

        const isNewProduct = !previous;
        // Backlog priority: capture never-seen (store, product) pairs first —
        // a newly listed product is invisible to users until it's written —
        // and only spend leftover budget on re-pricing pairs we already have.
        if (phase === 'new' && !isNewProduct) continue;
        if (phase === 'update' && isNewProduct) continue;

        const storeId = core.buildStoreDocId(storeConfig.slug);

        if (args.apply) {
          if (!storeDocEnsured.has(storeConfig.slug)) {
            core.ensureStoreDoc(writer, storeId, storeConfig);
            budget.writesUsed += 1;
            storeDocEnsured.add(storeConfig.slug);
          }
          core.writePriceTriplet(writer, { storeId, storeConfig, barcode, row, priceRounded, min, max, isNewProduct });
          budget.writesUsed += 3;
          await writer.flushIfFull();

          mirror.set(storeConfig.slug, barcode, {
            price: priceRounded, min, max,
            productName: String(row.product_name || '').trim(),
            updatedAt: new Date().toISOString(),
          });
        } else {
          budget.writesUsed += 3; // dry-run estimate: Product + StoreRecentPriceEntry + PriceEntry
        }

        bumpStat(storeConfig.slug, phase === 'new' ? 'created' : 'updated');
      }

      // Barcode fully handled across all stores for this phase. Only the
      // 'new' pass owns the cursor — the 'update' pass is opportunistic
      // (leftover budget) and must not move the coverage pointer.
      if (phase === 'new') {
        lastCompleted = barcode;
        if (lastBarcode && barcode >= lastBarcode) wrapped = true;
      }
      }
    }
  }

  for (const [slug, st] of perStoreStats) report.crossCheckByStore.push({ slug, tier: tierLabel, ...st });

  // Only when we actually walked the whole tier within budget is the pass
  // complete — reset the cursor so the next run starts over and picks up
  // refreshed prices. (If the budget ran out, the cursor was already saved at
  // the stopping point and must not be cleared here.)
  if (!outOfBudget) {
    saveCursor('', true);
    report.fullPass = report.fullPass || {};
    report.fullPass[tierLabel] = wrapped ? 'completed (wrapped)' : 'completed';
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

  const report = { discoveryByStore: [], crossCheckByStore: [], skippedStores: [], failedStores: [], invalidBarcodes: 0, invalidPrice: 0 };
  const activeStores = syncConfig.stores.filter((s) => !!process.env[s.envVar]);
  for (const storeConfig of syncConfig.stores) {
    if (!process.env[storeConfig.envVar]) report.skippedStores.push(`${storeConfig.slug} (no ${storeConfig.envVar})`);
  }

  if (activeStores.length === 0) {
    throw new Error('No stores have a configured Neon connection string.');
  }

  // With no write budget there is nothing this run can act on, and discovery
  // is not free: it opens 34 Neon connections and full-scans every store's
  // offers table. On 2026-07-31 the budget was 0 all day (the Firestore
  // incident had consumed the daily write allowance) and three runs paid that
  // cost to write nothing. Stop before touching Neon at all.
  if (args.apply && effectiveMaxWrites <= 0) {
    console.log('\nWrite budget is 0 for this run — skipping discovery and cross-check entirely.');
    console.log('Nothing was read from Neon and nothing was written to Firestore.');
    console.log('The budget returns once the daily Firestore write quota resets (07:00 UTC).');
    return;
  }

  // Connections are opened on demand by the manager and released as soon as a
  // store's query is done — see createStoreConnections for why holding all 34
  // open for the whole run used to make one dropped socket fatal.
  const conns = createStoreConnections(report);

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
      await crossCheckBarcodes(db, mirror, writer, conns, activeStores, [barcode], budget, report, 'manual');
    } else {
      console.log('\n-- Phase 1: discovery --');
      const maxPriceByBarcode = await discoverHighValueBarcodes(conns, activeStores, report);
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
      await crossCheckBarcodes(db, mirror, writer, conns, activeStores, highTier.map((x) => x[0]), budget, report, 'priority');
      if (budget.writesUsed < budget.maxWrites) {
        await crossCheckBarcodes(db, mirror, writer, conns, activeStores, normalTier.map((x) => x[0]), budget, report, 'standard');
      } else {
        console.log('  Write budget exhausted by priority tier — standard tier deferred to next run.');
      }
    }

    // Individual stores failing is survivable and reported; EVERY store failing
    // means the run learned nothing, and must not be reported as a success.
    // (This replaces the old up-front connect-them-all preflight — with lazy
    // connections there is no earlier point at which this is knowable.)
    if (!conns.everConnected) throw new Error('No store databases could be reached.');

    // Record data freshness for EVERY active store (not just ones with price
    // changes this run) so the freshness monitor can detect a dead scraper.
    if (args.apply) {
      for (const storeConfig of activeStores) {
        const client = await conns.acquire(storeConfig);
        if (!client) continue; // unreachable this run — already in failedStores
        try {
          await core.recordStoreFreshness(writer, client, storeConfig);
        } finally {
          await conns.release(storeConfig.slug);
        }
      }
    }

    let gitResult = { pushed: false, reason: 'dry-run — mirror not touched' };
    if (args.apply) {
      await writer.flushRemaining();
      if (isEmulatorRun) {
        // Emulator (:local) runs must not commit/push the shared production
        // mirror — see the same guard in sync-neon-to-firestore.cjs.
        gitResult = { pushed: false, reason: 'emulator run — production mirror not touched' };
      } else {
        mirror.flush();
        gitResult = mirror.commitAndPush(`sync-high-value: ${new Date().toISOString().slice(0, 10)} — ${budget.writesUsed} writes`);
      }
    }

    console.log('\n=== Summary ===');
    for (const s of report.crossCheckByStore) {
      console.log(`  ${s.slug.padEnd(20)} [${s.tier}] scanned=${s.scanned}  new=${s.created ?? 0}  repriced=${s.updated ?? 0}`);
    }
    if (report.skippedStores.length > 0) {
      console.log(`  Skipped: ${report.skippedStores.join(', ')}`);
    }
    if (report.failedStores.length > 0) {
      console.log(`  FAILED (isolated, run continued): ${report.failedStores.join('; ')}`);
    }
    console.log(`  Invalid barcodes skipped: ${report.invalidBarcodes}`);
    if (report.belowThresholdSkipped) {
      console.log(`  Below MIN_VALUE after de-duplication (not created): ${report.belowThresholdSkipped}`);
    }
    if (report.outlierSkipped) {
      console.log(`  Outlier prices rejected (>=${OUTLIER_MAX_OVER_MEDIAN}x median, likely bad data): ${report.outlierSkipped}`);
      (report.outlierExamples || []).forEach((e) => console.log(`    e.g. ${e}`));
    }
    console.log(`  Invalid/missing price skipped: ${report.invalidPrice}`);
    console.log(`  Firestore writes ${args.apply ? 'performed' : 'estimated'}: ${budget.writesUsed} / ${effectiveMaxWrites}`);
    if (report.fullPass) {
      for (const [tier, state] of Object.entries(report.fullPass)) console.log(`  Tier "${tier}": full pass ${state}`);
    }
    if (report.resumeFrom) {
      for (const [tier, at] of Object.entries(report.resumeFrom)) console.log(`  Tier "${tier}": budget hit — next run resumes after barcode ${at}`);
    }
    console.log(`  Mirror commit/push: ${gitResult.pushed ? 'OK' : `skipped (${gitResult.reason})`}`);
    if (!args.apply) {
      console.log('\n  Dry-run only — re-run with --apply to write to Firestore and update the mirror.');
    }
  } finally {
    await conns.closeAll();
  }
}

main().catch((err) => {
  console.error('High-value sync failed:', err?.message || err);
  process.exit(1);
});

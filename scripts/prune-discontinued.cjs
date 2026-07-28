#!/usr/bin/env node
'use strict';
// ──────────────────────────────────────────────────────────
// Bem na Mosca — prune discontinued products
// ──────────────────────────────────────────────────────────
// A product that disappears from a store's scraper (Neon offers.is_available
// goes false / the row vanishes) otherwise keeps its Firestore price docs
// forever, so the app keeps showing a price for something that no longer
// exists. This routine removes them — but only after a GRACE period of
// continuous unavailability, so a temporary stockout isn't deleted.
//
// It runs in the SYNC/mirror environment because it needs BOTH Neon (to ask
// "is this still available?") and the mirror (to remember WHEN each barcode
// first went missing, for free — Cloud Functions have neither). The mirror
// is also updated so pruned barcodes can't be re-written by a later sync.
//
// Per store, per run:
//   1. Ask Neon which of the store's mirror-tracked barcodes are available.
//   2. Available  → clear its `missingSince` (it's back / never left).
//      Missing    → stamp `missingSince = now` (mirror only, no Firestore cost).
//   3. Missing longer than GRACE_DAYS → delete its StoreRecentPriceEntry +
//      PriceEntry for that store, and drop it from the mirror. If the product
//      is then gone from EVERY store, delete the Product doc too.
//
// Firestore DELETES are capped against the day's remaining delete quota
// (dynamic budget, same guard the sync trusts) so this can never blow the
// 20k/day free-tier delete limit; leftover work resumes next run.
//
// Usage:
//   node scripts/prune-discontinued.cjs                 # DRY-RUN (default, safe)
//   node scripts/prune-discontinued.cjs --apply         # actually delete
//   flags: --grace-days N  --max-deletes N  --ignore-guard
//          --service-account <path>  --project-id <id>
// ──────────────────────────────────────────────────────────

const path = require('path');
const { Client } = require('pg');
const syncConfig = require('./sync-config.cjs');
const core = require('./lib/neonSyncCore.cjs');
const { SyncMirror } = require('./lib/syncMirror.cjs');

const ROOT = path.resolve(__dirname, '..');

// Local runs read secrets from .env.local; CI sets them in the workflow env
// (dotenv may not be installed there, and never overrides existing vars).
try { require('dotenv').config({ path: path.resolve(ROOT, '.env.local') }); } catch { /* CI: env already set */ }
// Days a product must be continuously unavailable before its price entry is
// deleted. Lowered 7 -> 3 because 7 days of showing a product a customer
// cannot actually buy is worse for them than briefly dropping one that comes
// back: scrape cycles run every 3-8h, so 3 days is 9-24 consecutive
// unavailable observations — conclusive, not a blip. A returning product is
// simply re-created on the next sync (3 writes), and a scraper OUTAGE cannot
// trigger false deletions because stale flags keep their last value, so
// previously-available rows are never stamped as missing.
const DEFAULT_GRACE_DAYS = 3;
const DEFAULT_MAX_DELETES = 10000;   // hard ceiling before the dynamic cap
const SAFETY_PERCENT = 75;           // matches the sync's write-budget safety margin
const NEON_BATCH = 5000;             // barcodes per Neon `= any($1)` query

function parseArgs(argv) {
  const args = {
    apply: false, graceDays: null, maxDeletes: null,
    serviceAccount: null, projectId: null, ignoreGuard: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const t = argv[i];
    if (t === '--apply') args.apply = true;
    else if (t === '--dry-run') args.apply = false;
    else if (t === '--ignore-guard') args.ignoreGuard = true;
    else if (t === '--grace-days' && argv[i + 1]) args.graceDays = Number(argv[++i]);
    else if (t === '--max-deletes' && argv[i + 1]) args.maxDeletes = Number(argv[++i]);
    else if (t === '--service-account' && argv[i + 1]) args.serviceAccount = path.resolve(ROOT, argv[++i]);
    else if (t === '--project-id' && argv[i + 1]) args.projectId = argv[++i];
  }
  return args;
}

const args = parseArgs(process.argv.slice(2));
const isEmulatorRun = !!process.env.FIRESTORE_EMULATOR_HOST;
const GRACE_DAYS = Number.isFinite(args.graceDays) && args.graceDays > 0
  ? args.graceDays
  : (Number.parseFloat(process.env.PRUNE_GRACE_DAYS) || DEFAULT_GRACE_DAYS);
const REQUESTED_MAX_DELETES = Number.isFinite(args.maxDeletes) && args.maxDeletes > 0
  ? args.maxDeletes
  : DEFAULT_MAX_DELETES;

// Delete budget from the free-tier guard's delete usage (reuses the sync's
// generic budget math). Fails OPEN to the requested max when guard data is
// missing — but the daily schedule runs right after quota reset, so headroom
// is normally full anyway.
async function computeDeleteBudget(db) {
  if (args.ignoreGuard) return { budget: REQUESTED_MAX_DELETES, guardAvailable: false };
  try {
    const snap = await db.collection('SystemHealth').doc('firestore-free-tier-guard').get();
    const data = snap.exists ? (snap.data() || {}) : {};
    const used = Number(data.usage && data.usage.deletes);
    const limit = Number(data.limits && data.limits.deletes);
    if (Number.isFinite(used) && Number.isFinite(limit) && limit > 0) {
      const r = core.calculateDynamicWriteBudget({
        writesUsed: used, writeLimit: limit, requestedMax: REQUESTED_MAX_DELETES, safetyPercent: SAFETY_PERCENT,
      });
      return { budget: r.budget, guardAvailable: true, used, limit };
    }
  } catch { /* fall through */ }
  return { budget: REQUESTED_MAX_DELETES, guardAvailable: false };
}

async function run() {
  const db = core.initFirestore(args, isEmulatorRun);
  const mirror = new SyncMirror(path.join(ROOT, syncConfig.mirror.localPath));
  const nowMs = Date.now();
  const graceMs = GRACE_DAYS * 24 * 60 * 60 * 1000;

  const { budget: maxDeletes, guardAvailable } = await computeDeleteBudget(db);
  let deletesUsed = 0;

  console.log(`\n=== Prune discontinued (${isEmulatorRun ? 'EMULATOR' : 'PRODUCTION'} | ${args.apply ? 'APPLY' : 'DRY-RUN'}) ===`);
  console.log(`Grace: ${GRACE_DAYS}d | Delete budget: ${maxDeletes}${guardAvailable ? '' : ' (guard unavailable — requested max)'}`);

  const activeStores = syncConfig.stores.filter((s) => !!process.env[s.envVar]);
  const report = { perStore: [], entriesDeleted: 0, productsDeleted: 0, newlyMissing: 0, cleared: 0, budgetHit: false };
  const prunedProductIds = new Set();

  for (const storeConfig of activeStores) {
    const slug = storeConfig.slug;
    const tracked = mirror.listBarcodes(slug);
    if (tracked.length === 0) continue;

    // Which of the tracked barcodes are still available at this store?
    const availableSet = new Set();
    const client = new Client({ connectionString: process.env[storeConfig.envVar], ssl: { rejectUnauthorized: false } });
    try {
      await client.connect();
      for (let i = 0; i < tracked.length; i += NEON_BATCH) {
        const { rows } = await client.query(
          'select ean from offers where is_available = true and ean = any($1)',
          [tracked.slice(i, i + NEON_BATCH)]
        );
        rows.forEach((r) => availableSet.add(String(r.ean)));
      }
    } catch (err) {
      console.warn(`  ${slug}: Neon query failed, skipping store this run: ${err.message}`);
      continue;
    } finally {
      await client.end().catch(() => {});
    }

    const storeId = core.buildStoreDocId(slug);
    const storeRecentKey = core.buildStoreRecentKey(storeId);
    let newlyMissing = 0, cleared = 0, deletedHere = 0;

    for (const barcode of tracked) {
      const rec = mirror.get(slug, barcode) || {};

      if (availableSet.has(barcode)) {
        if (rec.missingSince) { delete rec.missingSince; mirror.set(slug, barcode, rec); cleared++; }
        continue;
      }
      // Missing from Neon.
      if (!rec.missingSince) {
        rec.missingSince = new Date(nowMs).toISOString();
        mirror.set(slug, barcode, rec);
        newlyMissing++;
        continue;
      }
      if ((nowMs - Date.parse(rec.missingSince)) <= graceMs) continue; // still inside grace

      // Past grace → prune this (product, store), budget permitting.
      if (deletesUsed >= maxDeletes) { report.budgetHit = true; break; }
      const productId = core.buildProductDocId(barcode);
      const recentId = core.buildRecentDocId(productId, storeRecentKey);

      if (args.apply) {
        if (recentId) await db.collection('StoreRecentPriceEntry').doc(recentId).delete().catch(() => {});
        const peSnap = await db.collection('PriceEntry')
          .where('product_id', '==', productId).where('store_id', '==', storeId).get();
        for (const d of peSnap.docs) {
          if (deletesUsed >= maxDeletes) { report.budgetHit = true; break; }
          await d.ref.delete().catch(() => {});
          deletesUsed++;
        }
      }
      deletesUsed++; // the StoreRecentPriceEntry delete
      report.entriesDeleted++;
      mirror.delete(slug, barcode);
      prunedProductIds.add(productId);
      deletedHere++;
    }

    report.newlyMissing += newlyMissing;
    report.cleared += cleared;
    report.perStore.push({ slug, tracked: tracked.length, newlyMissing, cleared, deleted: deletedHere });
    if (report.budgetHit) break;
  }

  // Whole-product cleanup: a product pruned from a store may still exist at
  // others. Delete the Product doc only when NO StoreRecentPriceEntry remains.
  for (const productId of prunedProductIds) {
    if (deletesUsed >= maxDeletes) { report.budgetHit = true; break; }
    const remaining = await db.collection('StoreRecentPriceEntry').where('product_id', '==', productId).limit(1).get();
    if (!remaining.empty) continue;
    if (args.apply) {
      await db.collection('Product').doc(productId).delete().catch(() => {});
      const peSnap = await db.collection('PriceEntry').where('product_id', '==', productId).get();
      for (const d of peSnap.docs) { await d.ref.delete().catch(() => {}); }
    }
    deletesUsed++;
    report.productsDeleted++;
  }

  // Persist mirror changes (missingSince stamps + pruned barcodes).
  if (args.apply) {
    mirror.flush();
    if (!isEmulatorRun) {
      const res = mirror.commitAndPush(`prune: ${report.entriesDeleted} store-entries, ${report.productsDeleted} products (grace ${GRACE_DAYS}d)`);
      console.log(`Mirror commit/push: ${res.pushed ? 'OK' : `skipped (${res.reason})`}`);
    } else {
      console.log('Mirror: flushed locally (emulator run — not pushed)');
    }
  }

  console.log('\n=== Summary ===');
  for (const s of report.perStore) {
    if (s.deleted || s.newlyMissing || s.cleared) {
      console.log(`  ${s.slug.padEnd(20)} tracked=${s.tracked} newlyMissing=${s.newlyMissing} cleared=${s.cleared} deleted=${s.deleted}`);
    }
  }
  console.log(`  Store-entries ${args.apply ? 'deleted' : 'eligible'}: ${report.entriesDeleted}`);
  console.log(`  Whole products ${args.apply ? 'deleted' : 'eligible'}: ${report.productsDeleted}`);
  console.log(`  Newly-missing stamped: ${report.newlyMissing} | recovered/cleared: ${report.cleared}`);
  console.log(`  Firestore deletes ${args.apply ? 'performed' : 'estimated'}: ${deletesUsed} / ${maxDeletes}${report.budgetHit ? ' (budget hit — resumes next run)' : ''}`);
  if (!args.apply) console.log('\n  Dry-run only — re-run with --apply to delete and update the mirror.');
}

run().catch((err) => {
  console.error('\nPrune failed:', err?.stack || err?.message || err);
  process.exit(1);
});

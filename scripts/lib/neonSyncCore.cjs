// ──────────────────────────────────────────────────────────
// Bem na Mosca — shared Neon → Firestore sync helpers
// ──────────────────────────────────────────────────────────
// Barcode validation, doc-id schemes, and the Product/StoreRecentPriceEntry/
// PriceEntry write triplet are identical between the full-catalog cursor
// sync (sync-neon-to-firestore.cjs) and the high-value cross-store sync
// (sync-neon-high-value.cjs) — kept here once so both stay in lockstep with
// functions/index.js's doc-id scheme instead of drifting apart.
'use strict';
const { computeHighValueSavings, collectFromMirror } = require('./highValueSavings.cjs');

const fs = require('fs');
const crypto = require('crypto');
const admin = require('firebase-admin');
const { Client } = require('pg');

const STORE_RECENT_PRICE_COLLECTION = 'StoreRecentPriceEntry';

// ── Neon client factory ──────────────────────────────────────────────────────
//
// Never construct `new Client(...)` directly in a sync script. Neon computes
// suspend, restart and occasionally drop sockets, and `pg` reports that as an
// ASYNCHRONOUS 'error' event on the Client — not as a rejection from the query
// in flight. A Client with no 'error' listener makes Node throw, killing the
// whole process: that is what aborted the 06:30 UTC high-value sync runs on
// 2026-07-29 and 2026-07-31 ("Unhandled 'error' event: Connection terminated
// unexpectedly") before a single write had landed.
//
// The listener here turns that into a warning. The caller's existing
// try/catch around its queries then handles the dead connection normally —
// skip the store, or reconnect — instead of the run dying.
const NEON_CONNECT_TIMEOUT_MS = Number.parseInt(process.env.NEON_CONNECT_TIMEOUT_MS || '20000', 10) || 20000;
const NEON_QUERY_TIMEOUT_MS = Number.parseInt(process.env.NEON_QUERY_TIMEOUT_MS || '180000', 10) || 180000;

function createNeonClient(connectionString, label, onDrop) {
  const client = new Client({
    connectionString,
    ssl: { rejectUnauthorized: false },
    keepAlive: true,                                  // stop idle sockets being reaped silently
    connectionTimeoutMillis: NEON_CONNECT_TIMEOUT_MS,
    query_timeout: NEON_QUERY_TIMEOUT_MS,
  });
  client.on('error', (err) => {
    console.warn(`  ${label}: connection dropped — ${describeError(err)}`);
    if (typeof onDrop === 'function') {
      try { onDrop(err, client); } catch { /* a drop handler must never throw */ }
    }
    client.end().catch(() => {});
  });
  return client;
}

// Node's happy-eyeballs connect rejects with an AggregateError whose own
// `message` is EMPTY — the real causes sit in `.errors`. Logging `err.message`
// therefore produced bare lines like "drogasil: connection failed — " in the
// 2026-07-31 15:18 UTC run, which said nothing about why all 34 stores failed.
function describeError(err) {
  if (!err) return 'unknown error';
  const own = String(err.message || '').trim();
  const nested = Array.isArray(err.errors)
    ? err.errors.map((e) => String(e?.message || e || '').trim()).filter(Boolean)
    : [];
  if (nested.length) {
    // De-duplicate: happy-eyeballs usually reports the same failure per address.
    const unique = [...new Set(nested)];
    return own ? `${own} (${unique.join('; ')})` : unique.join('; ');
  }
  return own || `${err.name || 'Error'} (no message)`;
}

// Errors that mean "this socket is gone" rather than "this SQL is wrong".
// Only these are worth reconnecting for; a bad query would just fail twice.
function isConnectionError(err) {
  const msg = String(err?.message || '');
  return /connection terminated|connection closed|server closed the connection|terminating connection|socket hang up|read ECONNRESET|Client has encountered a connection error|Connection ended/i.test(msg)
    || ['ECONNRESET', 'EPIPE', 'ETIMEDOUT', 'ENOTFOUND', 'ECONNREFUSED', '57P01', '57P02', '57P03'].includes(String(err?.code || ''));
}

// Attribution for everything the automatic price sync writes. Without these
// fields the UI's user-tag falls back to "Anonymous", which reads as untrusted
// user-submitted data when it's actually our own verified pipeline.
//
// The id matches PRICE_DROP_SKIP_ACTOR_IDS' default in functions/index.js
// ('bemnamosc4'), which is the actor the backend already treats as the bulk
// importer — so it stays exempt from the per-user daily quota. Override with
// SYNC_ACTOR_ID / SYNC_ACTOR_NAME if that id ever changes; keep it in step
// with PRICE_DROP_SKIP_ACTOR_IDS or synced writes would start counting against
// a real user's quota.
const SYNC_ACTOR_ID = String(process.env.SYNC_ACTOR_ID || 'bemnamosc4').trim();
const SYNC_ACTOR_NAME = String(process.env.SYNC_ACTOR_NAME || 'BemnaMosc4').trim();

function normalizeBarcode(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/e\+?\d+$/i.test(raw)) {
    const n = Number(raw.replace(',', '.'));
    if (Number.isFinite(n) && n > 0) return Math.round(n).toString();
  }
  if (/^\d+(\.0+)?$/.test(raw)) return raw.split('.')[0];
  return raw.replace(/\D/g, '');
}

function isValidBarcode(barcode, barcodeConfig) {
  const { minLength, maxLength } = barcodeConfig;
  if (!barcode) return false;
  if (!/^\d+$/.test(barcode)) return false;
  if (barcode.length < minLength || barcode.length > maxLength) return false;
  if (/^0+$/.test(barcode)) return false; // all-zero is not a real EAN
  return true;
}

function buildProductDocId(barcode) {
  return `product_${barcode}`;
}
function buildStoreDocId(storeSlug) {
  return `store_pharmacy_${storeSlug}`;
}
function buildPriceEntryDocId(productId, storeId, dateRecorded) {
  const key = `${productId}|${storeId}|${dateRecorded}`;
  return `price_${crypto.createHash('sha1').update(key).digest('hex').slice(0, 24)}`;
}

// StoreRecentPriceEntry doc-id/key scheme — CJS copy of getStoreRecentKey()/
// buildRecentDocId() in functions/index.js. Matching these exactly means
// sync scripts land on the SAME document any organic (non-sync) price write
// would use, instead of a second, differently-keyed doc for the same
// product+store.
function buildStoreRecentKey(storeId) {
  return `id:${storeId}`;
}
function buildRecentDocId(productId, storeRecentKey) {
  const safeProd = String(productId || '').trim().replace(/[^a-zA-Z0-9_.-]/g, '_').slice(0, 200);
  const safeStore = String(storeRecentKey || '').trim().replace(/[^a-zA-Z0-9_.-]/g, '_').slice(0, 200);
  if (!safeProd || !safeStore) return null;
  return `${safeProd}__${safeStore}`;
}

// The price a customer actually pays: the promo price whenever it's a real,
// LOWER price than the regular one, else the regular price.
//
// We deliberately do NOT require `is_discounted`. Audited across all 19 Neon
// DBs (2026-07): most scrapers leave that flag NULL while still populating
// promo_price — ~111k available offers across 10 chains — so gating on the
// flag silently served the HIGHER regular price (avg ~20-24% too high) for
// every one of them. The inverse also exists (drogasil/drogaraia set the flag
// but leave promo_price empty), which the promo>0 guard already handles.
// Requiring promo < regular keeps bad data (promo above regular) harmless.
function effectivePrice(row) {
  const promo = Number(row.promo_price);
  const regular = Number(row.regular_price);
  const promoValid = Number.isFinite(promo) && promo > 0;
  const regularValid = Number.isFinite(regular) && regular > 0;
  if (promoValid && (!regularValid || promo < regular)) return promo;
  return regularValid ? regular : null;
}

// SQL mirror of effectivePrice(), for the price_history min/max aggregates.
// Kept here as the single source of truth so both sync scripts stay in step
// with the JS logic above — they previously had the same is_discounted bug,
// which froze price_min_30d/price_max_30d (min==max for ~80% of products at
// some stores) and made the price thermometer useless.
const EFFECTIVE_PRICE_SQL = `case
    when promo_price > 0 and (regular_price is null or regular_price <= 0 or promo_price < regular_price)
      then promo_price
    when regular_price > 0 then regular_price
    else null
  end`;

// CJS copy of src/lib/searchText.js — keep in sync.
// The sync creates essentially the whole catalogue, so anything it does not
// write here simply does not exist: name_search was 0/3942 in production
// because only import-csv and the client ever set it.
function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

const SEARCH_TOKEN_MIN_LENGTH = 2;
const SEARCH_TOKEN_MAX_COUNT = 30;

function buildSearchTokens(value) {
  const normalized = normalizeSearchText(value);
  if (!normalized) return [];
  const seen = new Set();
  for (const word of normalized.split(' ')) {
    if (word.length < SEARCH_TOKEN_MIN_LENGTH) continue;
    seen.add(word);
    if (seen.size >= SEARCH_TOKEN_MAX_COUNT) break;
  }
  return [...seen];
}

function round2(n) {
  return Math.round(Number(n) * 100) / 100;
}

function initFirestore(args, isEmulatorRun) {
  if (admin.apps.length > 0) return admin.firestore();

  if (isEmulatorRun) {
    admin.initializeApp({ projectId: args.projectId || process.env.VITE_FIREBASE_PROJECT_ID || 'bemnamosca' });
    return admin.firestore();
  }
  if (args.serviceAccount) {
    const svc = JSON.parse(fs.readFileSync(args.serviceAccount, 'utf8'));
    admin.initializeApp({ credential: admin.credential.cert(svc), projectId: args.projectId || svc.project_id });
    return admin.firestore();
  }
  if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    throw new Error('Set GOOGLE_APPLICATION_CREDENTIALS or use --service-account <path> (or run via the *-local.cjs wrapper for the emulator).');
  }
  admin.initializeApp({
    credential: admin.credential.applicationDefault(),
    ...(args.projectId ? { projectId: args.projectId } : {}),
  });
  return admin.firestore();
}

function createWriteBuffer(db, batchSize) {
  let batch = db.batch();
  let pending = 0;
  let totalWrites = 0;
  const flushes = [];

  return {
    upsert(collection, id, data) {
      batch.set(db.collection(collection).doc(id), data, { merge: true });
      pending += 1;
      totalWrites += 1;
    },
    async flushIfFull() {
      if (pending < batchSize) return;
      flushes.push(batch.commit());
      batch = db.batch();
      pending = 0;
    },
    async flushRemaining() {
      if (pending > 0) flushes.push(batch.commit());
      await Promise.all(flushes);
    },
    get totalWrites() { return totalWrites; },
  };
}

// Writes the Product + StoreRecentPriceEntry + PriceEntry triplet for one
// changed (barcode, store) price, mirroring what onPriceEntryCreated would
// denormalize — done here instead so the trigger can be skipped via
// _bulk:true (see functions/index.js) and avoid double reads/writes on a
// bulk sync.
function writePriceTriplet(writer, { storeId, storeConfig, barcode, row, priceRounded, min, max, isNewProduct }) {
  const nowIso = new Date().toISOString();
  const dateOnly = nowIso.slice(0, 10);
  const productId = buildProductDocId(barcode);
  const productUrl = String(row.product_url || '').trim();
  const productName = String(row.product_name || '').trim();

  const resolvedName = productName || `Product ${barcode}`;

  writer.upsert('Product', productId, {
    ...(isNewProduct ? {
      barcode,
      name: resolvedName,
      brand: String(row.brand || '').trim(),
      name_lower: productName.toLowerCase(),
      image_url: String(row.image_url || '').trim(),
      created_date: nowIso,
      created_by: SYNC_ACTOR_ID,
      created_by_name: SYNC_ACTOR_NAME,
    } : {}),
    // Written on EVERY pass, not just for new products. This document is
    // already being upserted for the price, so the search fields ride along at
    // no extra write cost — which is also how the existing catalogue gets
    // backfilled: each product picks them up the next time its price moves.
    name_search: normalizeSearchText(resolvedName),
    name_tokens: buildSearchTokens(resolvedName),
    // No store identity: Product is world-readable, so this named one of the
    // stores selling the item to every visitor. Price and date stay — they are
    // what the free tier is meant to show.
    price_summary: {
      latest_price: priceRounded,
      latest_date: nowIso,
    },
    price_min_30d: min,
    price_max_30d: max,
    updated_date: nowIso,
    updated_by: SYNC_ACTOR_ID,
    updated_by_name: SYNC_ACTOR_NAME,
    source: 'neon_sync',
  });

  const storeRecentKey = buildStoreRecentKey(storeId);
  const recentId = buildRecentDocId(productId, storeRecentKey);
  writer.upsert(STORE_RECENT_PRICE_COLLECTION, recentId, {
    product_id: productId,
    store_id: storeId,
    store_name: storeConfig.displayName,
    store_recent_key: storeRecentKey,
    price: priceRounded,
    quantity: 1,
    notes: `Min:${min} Max:${max}`,
    product_url: productUrl || null,
    date_recorded: dateOnly,
    recent_sort_date: nowIso,
    updated_date: nowIso,
    created_by: SYNC_ACTOR_ID,
    created_by_name: SYNC_ACTOR_NAME,
    updated_by: SYNC_ACTOR_ID,
    updated_by_name: SYNC_ACTOR_NAME,
    source: 'neon_sync',
  });

  const priceEntryId = buildPriceEntryDocId(productId, storeId, dateOnly);
  writer.upsert('PriceEntry', priceEntryId, {
    product_id: productId,
    store_id: storeId,
    store_name: storeConfig.displayName,
    price: priceRounded,
    quantity: 1,
    notes: `Min:${min} Max:${max}`,
    product_url: productUrl || null,
    date_recorded: dateOnly,
    created_date: nowIso,
    updated_date: nowIso,
    created_by: SYNC_ACTOR_ID,
    created_by_name: SYNC_ACTOR_NAME,
    updated_by: SYNC_ACTOR_ID,
    updated_by_name: SYNC_ACTOR_NAME,
    source: 'neon_sync',
    _bulk: true,
  });

  return productId;
}

function ensureStoreDoc(writer, storeId, storeConfig) {
  writer.upsert('Store', storeId, {
    name: storeConfig.displayName,
    type: 'pharmacy_online',
    source: 'neon_sync',
    updated_date: new Date().toISOString(),
  });
}

// Record per-store data freshness so the freshness monitor can detect a dead
// scraper. last_data_at = the newest updated_at in the store's Neon offers —
// it FREEZES when the scraper stops even though the sync keeps running and
// prices "look" present. This is distinct from PriceEntry.date_recorded, which
// only advances when a price CHANGES (a legitimately stable store would look
// stale by that metric). Cost: one aggregate query per store + one Firestore
// write. Runs regardless of the write budget — cheap and safety-critical.
async function recordStoreFreshness(writer, client, storeConfig) {
  let lastDataAt = null;
  try {
    const { rows } = await client.query(
      'select max(updated_at) as last_data_at from offers where is_available = true'
    );
    if (rows[0] && rows[0].last_data_at) {
      lastDataAt = new Date(rows[0].last_data_at).toISOString();
    }
  } catch (err) {
    // Non-fatal — a freshness-probe failure must never abort the price sync.
    console.warn(`[recordStoreFreshness] ${storeConfig.slug}: ${err.message}`);
    return;
  }
  // Include the store identity fields too. ensureStoreDoc() only runs when a
  // price is actually written, so a store with no price changes this run would
  // otherwise get last_data_at but NO `source` — which made it invisible to
  // the freshness monitor and unprotected by the isSyncedCatalogDoc() rule.
  writer.upsert('Store', buildStoreDocId(storeConfig.slug), {
    name: storeConfig.displayName,
    type: 'pharmacy_online',
    source: 'neon_sync',
    last_data_at: lastDataAt,
    last_synced_at: new Date().toISOString(),
  });
}

// Pure math — no Firestore access — so this is directly unit-testable.
// Caps requestedMax at a SAFE fraction of whatever write headroom is left
// today, so a sync run can never be the thing that pushes the project over
// the free-tier write limit. safetyPercent < 100 leaves room for staleness
// in the cached usage snapshot (see fetchGuardUsage) and for organic app
// traffic writing the rest of the day.
function calculateDynamicWriteBudget({ writesUsed, writeLimit, requestedMax, safetyPercent }) {
  const used = Number(writesUsed) || 0;
  const limit = Number(writeLimit) > 0 ? Number(writeLimit) : 20000;
  const safety = Math.max(0, Math.min(100, Number(safetyPercent))) / 100;

  const remaining = Math.max(0, limit - used);
  const safeRemaining = Math.floor(remaining * safety);
  const budget = Math.max(0, Math.min(Number(requestedMax) || 0, safeRemaining));

  return { budget, used, limit, remaining, safeRemaining };
}

// How old a guard snapshot may be and still be trusted. The scheduler refreshes
// hourly, so anything past two hours means the guard itself has stopped — and a
// dead guard must not be able to veto the sync indefinitely. On 2026-08-01 the
// guard stopped writing at 07:20 holding a miscounted over-limit reading, and
// every sync run for the rest of the day read that frozen snapshot and gave
// itself a zero budget.
const GUARD_SNAPSHOT_MAX_AGE_MS = 2 * 60 * 60 * 1000;

// Reads the same SystemHealth/firestore-free-tier-guard doc the app's own
// compaction job trusts (kept fresh by firestoreFreeTierGuardScheduler,
// which runs hourly). Fails OPEN — a missing/unreadable doc or a stale
// snapshot should never block a sync run; it just means this extra safety
// layer is skipped for that run and the configured/requested budget is
// used as-is.
async function fetchGuardUsage(db, { now = Date.now(), maxAgeMs = GUARD_SNAPSHOT_MAX_AGE_MS } = {}) {
  try {
    const snap = await db.collection('SystemHealth').doc('firestore-free-tier-guard').get();
    if (!snap.exists) return null;
    const data = snap.data() || {};
    const writesUsed = Number(data.usage?.writes);
    const writeLimit = Number(data.limits?.writes);
    if (!Number.isFinite(writesUsed) || !Number.isFinite(writeLimit) || writeLimit <= 0) return null;

    // Fail open on a snapshot too old to describe the current quota day.
    const checkedAtMs = Date.parse(String(data.checkedAt || ''));
    if (!Number.isFinite(checkedAtMs)) return null;
    if (now - checkedAtMs > maxAgeMs) return null;

    return { writesUsed, writeLimit, checkedAt: data.checkedAt || null };
  } catch {
    return null;
  }
}

// Combines the two: fetch today's usage, then compute a safe budget. See
// calculateDynamicWriteBudget for the fallback-to-requestedMax behavior
// when guard data isn't available.
async function computeDynamicWriteBudget(db, requestedMax, safetyPercent) {
  const guard = await fetchGuardUsage(db);
  if (!guard) {
    return { budget: requestedMax, used: null, limit: null, remaining: null, safeRemaining: null, checkedAt: null, guardAvailable: false };
  }
  const result = calculateDynamicWriteBudget({
    writesUsed: guard.writesUsed,
    writeLimit: guard.writeLimit,
    requestedMax,
    safetyPercent,
  });
  return { ...result, checkedAt: guard.checkedAt, guardAvailable: true };
}


// ────────────────────────────────────────────────────────────
// "Where comparing saves the most" — the Home shop window.
//
// Computed from the MIRROR, which already holds the last known price per
// (store, barcode) for every store and persists across runs. So this costs
// zero Firestore reads: the sync is the only process that ever needed the
// cross-store view, and it has it in memory anyway.
//
// One write per run. The document names no store — only how many agree —
// because which shop sells it at that price is the subscription.
// ────────────────────────────────────────────────────────────
const HIGHLIGHTS_DOC = 'CatalogueHighlights/current';

async function writeHighValueHighlights(db, mirror, storeSlugs, { apply = false, limit = 3 } = {}) {
  const byBarcode = collectFromMirror(mirror, storeSlugs);
  const items = computeHighValueSavings(byBarcode, { limit });

  const payload = {
    items: items.map((it) => ({
      product_id: buildProductDocId(it.barcode),
      barcode: it.barcode,
      name: it.name,
      store_count: it.store_count,
      min_price: it.min_price,
      max_price: it.max_price,
      saving_amount: it.saving_amount,
      saving_percent: it.saving_percent,
    })),
    candidates_considered: byBarcode.size,
    updated_at: new Date().toISOString(),
  };

  if (!apply) return { ...payload, written: false };

  const [collection, docId] = HIGHLIGHTS_DOC.split('/');
  await db.collection(collection).doc(docId).set(payload);
  return { ...payload, written: true };
}

module.exports = {
  writeHighValueHighlights,
  HIGHLIGHTS_DOC,
  STORE_RECENT_PRICE_COLLECTION,
  createNeonClient,
  isConnectionError,
  describeError,
  normalizeBarcode,
  isValidBarcode,
  buildProductDocId,
  buildStoreDocId,
  buildPriceEntryDocId,
  buildStoreRecentKey,
  buildRecentDocId,
  effectivePrice,
  EFFECTIVE_PRICE_SQL,
  round2,
  initFirestore,
  createWriteBuffer,
  writePriceTriplet,
  ensureStoreDoc,
  recordStoreFreshness,
  normalizeSearchText,
  buildSearchTokens,
  calculateDynamicWriteBudget,
  fetchGuardUsage,
  computeDynamicWriteBudget,
  GUARD_SNAPSHOT_MAX_AGE_MS,
};

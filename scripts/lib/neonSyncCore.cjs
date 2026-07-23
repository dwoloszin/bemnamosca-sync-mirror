// ──────────────────────────────────────────────────────────
// Bem na Mosca — shared Neon → Firestore sync helpers
// ──────────────────────────────────────────────────────────
// Barcode validation, doc-id schemes, and the Product/StoreRecentPriceEntry/
// PriceEntry write triplet are identical between the full-catalog cursor
// sync (sync-neon-to-firestore.cjs) and the high-value cross-store sync
// (sync-neon-high-value.cjs) — kept here once so both stay in lockstep with
// functions/index.js's doc-id scheme instead of drifting apart.
'use strict';

const fs = require('fs');
const crypto = require('crypto');
const admin = require('firebase-admin');

const STORE_RECENT_PRICE_COLLECTION = 'StoreRecentPriceEntry';

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

function effectivePrice(row) {
  const promo = Number(row.promo_price);
  const regular = Number(row.regular_price);
  if (row.is_discounted && Number.isFinite(promo) && promo > 0) return promo;
  return Number.isFinite(regular) && regular > 0 ? regular : null;
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

  writer.upsert('Product', productId, {
    ...(isNewProduct ? {
      barcode,
      name: productName || `Product ${barcode}`,
      brand: String(row.brand || '').trim(),
      name_lower: productName.toLowerCase(),
      image_url: String(row.image_url || '').trim(),
      created_date: nowIso,
    } : {}),
    price_summary: {
      latest_price: priceRounded,
      latest_date: nowIso,
      latest_store_name: storeConfig.displayName,
      latest_store_id: storeId,
    },
    price_min_30d: min,
    price_max_30d: max,
    updated_date: nowIso,
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

// Reads the same SystemHealth/firestore-free-tier-guard doc the app's own
// compaction job trusts (kept fresh by firestoreFreeTierGuardScheduler,
// which runs hourly). Fails OPEN — a missing/unreadable doc or a stale
// snapshot should never block a sync run; it just means this extra safety
// layer is skipped for that run and the configured/requested budget is
// used as-is.
async function fetchGuardUsage(db) {
  try {
    const snap = await db.collection('SystemHealth').doc('firestore-free-tier-guard').get();
    if (!snap.exists) return null;
    const data = snap.data() || {};
    const writesUsed = Number(data.usage?.writes);
    const writeLimit = Number(data.limits?.writes);
    if (!Number.isFinite(writesUsed) || !Number.isFinite(writeLimit) || writeLimit <= 0) return null;
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

module.exports = {
  STORE_RECENT_PRICE_COLLECTION,
  normalizeBarcode,
  isValidBarcode,
  buildProductDocId,
  buildStoreDocId,
  buildPriceEntryDocId,
  buildStoreRecentKey,
  buildRecentDocId,
  effectivePrice,
  round2,
  initFirestore,
  createWriteBuffer,
  writePriceTriplet,
  ensureStoreDoc,
  calculateDynamicWriteBudget,
  fetchGuardUsage,
  computeDynamicWriteBudget,
};

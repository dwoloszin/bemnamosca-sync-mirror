'use strict';

// ──────────────────────────────────────────────────────────────────────────────
// "Where comparing saves the most" — the Home page's shop window.
//
// Built from the sync mirror, which already holds the last known price per
// (store, barcode) for all 40 stores and persists across runs. That means this
// costs ZERO Firestore reads: the data is already in memory when the sync runs.
//
// The filter is corroboration between stores, not a price threshold. A junk
// price appears at one store; a real one appears at several. That is what
// keeps a R$ 9.999.876 nappy out without also throwing away Spinraza, which
// genuinely costs half a million reais.
//
// The headline is the saving in REAIS, not the percent. "Economize R$ 12.165"
// lands where "23%" does not, and for specialty medicine the absolute figure
// is the whole point.
// ──────────────────────────────────────────────────────────────────────────────

const DEFAULTS = {
  minStores: 3,
  limit: 3,
  // Applies only when there are exactly 3 prices and trimming is not possible.
  // Three independent stores rarely agree on a wrong number, so a loose bound
  // is enough to catch the sentinel values (28 vs 9,999,876 is 357,138x).
  maxRatioWhenUntrimmed: 3,
  // Below this the "saving" is noise rather than news.
  minSavingAmount: 1,
};

/**
 * Trim the single lowest and single highest price.
 *
 * Only when there are at least 4, because trimming 3 leaves one value and a
 * range needs two. That is the whole reason for the untrimmed branch below:
 * the minimum of 3 stores exists so rare medicines — which no more than a
 * handful of pharmacies carry — are not excluded by definition.
 */
function trimExtremes(sortedPrices) {
  if (sortedPrices.length >= 4) {
    return { prices: sortedPrices.slice(1, -1), trimmed: true };
  }
  return { prices: sortedPrices, trimmed: false };
}

/**
 * @param {Map<string, Array<{price:number, productName?:string, storeSlug?:string}>>} byBarcode
 * @param {object} [options]
 * @returns {Array<object>} ranked, richest saving first
 */
function computeHighValueSavings(byBarcode, options = {}) {
  const opt = { ...DEFAULTS, ...options };
  const out = [];

  for (const [barcode, rawEntries] of byBarcode) {
    const entries = (rawEntries || []).filter((e) => Number.isFinite(Number(e?.price)) && Number(e.price) > 0);

    // Distinct STORES, not distinct rows: the same shop listing an item twice
    // is one opinion, not two.
    const byStore = new Map();
    for (const e of entries) {
      const slug = String(e.storeSlug || '').trim();
      if (!slug || byStore.has(slug)) continue;
      byStore.set(slug, e);
    }
    if (byStore.size < opt.minStores) continue;

    const chosen = [...byStore.values()];
    const sorted = chosen.map((e) => Number(e.price)).sort((a, b) => a - b);

    const { prices, trimmed } = trimExtremes(sorted);
    if (prices.length < 2) continue;

    const min = prices[0];
    const max = prices[prices.length - 1];

    // Untrimmed means one wild value would BE the range, so a plausibility
    // bound stands in for the trimming that could not happen.
    if (!trimmed && max > min * opt.maxRatioWhenUntrimmed) continue;

    const savingAmount = max - min;
    if (savingAmount < opt.minSavingAmount) continue;

    const name = chosen.map((e) => String(e.productName || '').trim()).find(Boolean) || '';

    out.push({
      barcode,
      name,
      store_count: byStore.size,
      min_price: Number(min.toFixed(2)),
      max_price: Number(max.toFixed(2)),
      saving_amount: Number(savingAmount.toFixed(2)),
      saving_percent: Number(((savingAmount / max) * 100).toFixed(1)),
      trimmed,
    });
  }

  // Ties broken by store count: more corroboration is the better story, and it
  // also keeps the output stable between runs.
  out.sort((a, b) => (b.saving_amount - a.saving_amount) || (b.store_count - a.store_count));
  return out.slice(0, opt.limit);
}

/**
 * Walk the mirror into the shape computeHighValueSavings wants.
 *
 * Reads only local files — the mirror is the sync's read side precisely so
 * Firestore never has to be.
 */
function collectFromMirror(mirror, storeSlugs) {
  const byBarcode = new Map();
  for (const slug of storeSlugs) {
    for (const barcode of mirror.listBarcodes(slug)) {
      const rec = mirror.get(slug, barcode);
      if (!rec) continue;
      if (!byBarcode.has(barcode)) byBarcode.set(barcode, []);
      byBarcode.get(barcode).push({
        storeSlug: slug,
        price: Number(rec.price),
        productName: rec.productName,
      });
    }
  }
  return byBarcode;
}

module.exports = { computeHighValueSavings, collectFromMirror, trimExtremes, DEFAULTS };

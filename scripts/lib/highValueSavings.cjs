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
  // Bound on the FINAL range, trimmed or not. It used to apply only to the
  // untrimmed 3-price case, so a bimodal spread sailed through: the same EAN
  // sold fractionated ("Unidade", R$49,90) and by the box ("30 Un", R$990)
  // trims to [49.9 … 990] and headlines as "economize R$940 (95%)" — the
  // silver vitrine was ALL such items, because the real big-ticket savings
  // sit above that tier's cap. Measured legitimate spreads on specialty
  // medicine run ≤1.7x (Xospata 37%, Lumakras 39.9%); package mixes run
  // 9–35x. 3x keeps every real story and kills every package mix seen.
  maxRatio: 3,
  // Below this the "saving" is noise rather than news.
  minSavingAmount: 1,
};

// ── Package-count reading, for the second line of defence ────────────────
//
// The names themselves say when one store sells the unit and another the
// box: "Bolsa Urostomia … - Unidade" vs "… - 30 Un", "(caixa com 12
// unidades)" vs "(unidade)". If two stores' names carry DIFFERENT counts,
// the prices are not comparable and the barcode has no place in a savings
// headline — whatever the ratio.
//
// Deliberately conservative: only counts the names actually state are
// compared; a name that names none abstains (a box name that omits its
// count must not be read as 1). Counts above 999 are ignored — they are
// catalogue/reference codes ("Coloplast 28706 UN"), not pack sizes.
const COUNT_PATTERNS = [
  /caixa\s+com\s+(\d+)/i,
  /\bcx\s*(\d+)\b/i,
  /(\d+)\s*(?:un\b|und\b|unid\b|unidades?\b)/i,
  /(\d+)\s*(?:comprimidos?\b|cprs?\b|c[aá]psulas?\b|caps\b|sach[eê]s?\b|ampolas?\b|flaconetes?\b)/i,
];
// A bare "unidade"/"un" with no number means ONE — but only when no
// numbered count was found in the same name.
const BARE_UNIT_RE = /\b(?:un|unid|unidade)\b/i;

function parsePackCount(name) {
  const text = String(name || '');
  if (!text) return null;
  for (const re of COUNT_PATTERNS) {
    const m = text.match(re);
    if (m) {
      const n = Number(m[1]);
      if (Number.isFinite(n) && n >= 1 && n <= 999) return n;
    }
  }
  if (BARE_UNIT_RE.test(text)) return 1;
  return null;
}

function hasPackMix(entries) {
  const counts = new Set();
  for (const e of entries) {
    const n = parsePackCount(e.productName);
    if (n != null) counts.add(n);
    if (counts.size > 1) return true;
  }
  return false;
}

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
    let entries = (rawEntries || []).filter((e) => Number.isFinite(Number(e?.price)) && Number(e.price) > 0);
    // A capped tier's shop window shows only offers inside its slice — the
    // min, max and saving are then all numbers that tier can act on. The
    // corroboration minimum applies WITHIN the slice, so an item whose cheap
    // offers are real but whose expensive ones sit outside still qualifies.
    if (Number.isFinite(Number(opt.maxPrice)) && opt.maxPrice > 0) {
      entries = entries.filter((e) => Number(e.price) <= Number(opt.maxPrice));
    }

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

    // Stores naming different pack sizes are quoting different things.
    if (hasPackMix(chosen)) continue;

    const sorted = chosen.map((e) => Number(e.price)).sort((a, b) => a - b);

    const { prices, trimmed } = trimExtremes(sorted);
    if (prices.length < 2) continue;

    const min = prices[0];
    const max = prices[prices.length - 1];

    // The plausibility bound applies to the final range ALWAYS — trimming
    // removes one wild value per side, never a bimodal cluster.
    if (max > min * opt.maxRatio) continue;

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

module.exports = { computeHighValueSavings, collectFromMirror, trimExtremes, parsePackCount, hasPackMix, DEFAULTS };

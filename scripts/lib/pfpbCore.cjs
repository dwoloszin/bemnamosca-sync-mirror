'use strict';

// ── Farmácia Popular (PFPB) → Firestore documents ───────────────────────────
//
// Pure shaping of what the FARMACIA_POPULAR project keeps in its Neon database
// (DATABASE_URL_PFPB) into the few documents the app reads. Nothing here
// touches a network; scripts/pfpb-sync-to-firestore.cjs does the I/O.
//
// Why item-centric and not a flag on the catalogue: the price catalogue only
// holds products above MIN_VALUE, and the free list is cheap generics — of
// ~3,000 matched EANs about 200 exist as Product documents (3 of 41 items,
// measured 2026-11-05). So the 41 items are the unit; the EAN index only adds
// a seal where a catalogue product happens to match.
//
// Documents:
//   PfpbCatalog/elenco   the 41 items, each with its most-carried products
//   PfpbCatalog/eans     EAN → item ids (+ a short name for search)
//   PfpbCity/{cityKey}   every ACTIVE accredited pharmacy of one city — the
//                        fallback while most pharmacies have no coordinates
//   PfpbCell/{geohash4}  the pharmacies WITH coordinates, by ~39×20 km cell
//
// Any accredited pharmacy dispenses the whole list (the chain is irrelevant),
// so pharmacy documents carry no medicine data at all.

const crypto = require('crypto');

const GH_BASE32 = '0123456789bcdefghjkmnpqrstuvwxyz';
const CELL_PRECISION = 4;
const EXAMPLES_PER_ITEM = 8;
const NAME_MAX = 80;

/** Standard geohash. Must stay identical to functions/lib/pfpb.js. */
function geohashEncode(lat, lng, precision = CELL_PRECISION) {
  const latR = [-90, 90];
  const lngR = [-180, 180];
  let out = '';
  let bit = 0;
  let ch = 0;
  let even = true;
  while (out.length < precision) {
    if (even) {
      const mid = (lngR[0] + lngR[1]) / 2;
      if (lng > mid) { ch = (ch << 1) | 1; lngR[0] = mid; } else { ch <<= 1; lngR[1] = mid; }
    } else {
      const mid = (latR[0] + latR[1]) / 2;
      if (lat > mid) { ch = (ch << 1) | 1; latR[0] = mid; } else { ch <<= 1; latR[1] = mid; }
    }
    even = !even;
    bit += 1;
    if (bit === 5) { out += GH_BASE32[ch]; bit = 0; ch = 0; }
  }
  return out;
}

/** "SP" + "São José dos Campos" → "SP__sao-jose-dos-campos". Mirrored in functions/lib/pfpb.js. */
function cityKey(uf, municipio) {
  const slug = String(municipio || '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  const state = String(uf || '').trim().toUpperCase();
  return state && slug ? `${state}__${slug}` : '';
}

function num(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clean(value, max = 160) {
  const s = String(value ?? '').replace(/\s+/g, ' ').trim();
  return s ? s.slice(0, max) : null;
}

/**
 * One pharmacy as stored: short keys, nulls dropped (21k of them ride in
 * these documents). gq = how far to trust the pin: 'exact' for a rooftop
 * Google match, 'approx' for anything coarser (CEP centroid, interpolated).
 */
function compactPharmacy(row) {
  const lat = num(row.latitude);
  const lng = num(row.longitude);
  const hasGeo = lat !== null && lng !== null && Math.abs(lat) <= 90 && Math.abs(lng) <= 180;
  const precise = String(row.geo_fonte || '') === 'google'
    && String(row.geo_precisao || '') === 'ROOFTOP'
    && row.geo_parcial !== true;
  const out = {
    c: String(row.cnpj || '').replace(/\D/g, ''),
    n: clean(row.nome_fantasia) || clean(row.razao_social) || 'Farmácia',
    a: clean(row.endereco),
    b: clean(row.bairro, 80),
    cep: clean(String(row.cep || '').replace(/\D/g, ''), 8),
    t: clean(String(row.telefone || '').replace(/\D/g, ''), 13),
    r: clean(row.rede, 40),
  };
  if (hasGeo) {
    out.lat = Math.round(lat * 1e6) / 1e6;
    out.lng = Math.round(lng * 1e6) / 1e6;
    out.gq = precise ? 'exact' : 'approx';
  }
  for (const k of Object.keys(out)) if (out[k] === null) delete out[k];
  return out;
}

function byNameThenAddress(a, b) {
  return String(a.b || '').localeCompare(String(b.b || ''), 'pt')
    || String(a.n).localeCompare(String(b.n), 'pt')
    || String(a.c).localeCompare(String(b.c));
}

/**
 * All documents for one sync, keyed by path ("PfpbCity/SP__campinas").
 * `normalizeBarcode` comes from the caller so EANs match Product.barcode
 * exactly the way the price sync writes it.
 */
function buildPfpbDocs({ elenco = [], produtos = [], farmacias = [], normalizeBarcode = (b) => String(b || '').replace(/\D/g, '') }) {
  const docs = new Map();

  // ── Items and their most-carried products ──────────────────────────────
  const byItemEan = new Map(); // pfpb_id → Map(ean → agg)
  const eanIndex = new Map();  // ean → { ids:Set, name }
  for (const p of produtos) {
    const id = String(p.pfpb_id || '').trim();
    const ean = normalizeBarcode(p.barcode);
    if (!id || !ean) continue;
    if (!byItemEan.has(id)) byItemEan.set(id, new Map());
    const m = byItemEan.get(id);
    if (!m.has(ean)) m.set(ean, { ean, name: clean(p.product_name, NAME_MAX), via: String(p.match_via || 'ingredient'), stores: new Set(), min: null });
    const agg = m.get(ean);
    if (p.store_name) agg.stores.add(String(p.store_name));
    const price = num(p.price);
    if (price !== null && price > 0 && p.is_available !== false && (agg.min === null || price < agg.min)) agg.min = price;
    if (!eanIndex.has(ean)) eanIndex.set(ean, { ids: new Set(), name: agg.name });
    eanIndex.get(ean).ids.add(id);
  }

  const items = elenco
    .map((e) => {
      const id = String(e.pfpb_id || '').trim();
      const aggs = [...(byItemEan.get(id)?.values() || [])];
      const prices = aggs.map((a) => a.min).filter((v) => v !== null);
      const examples = aggs
        .sort((a, b) => b.stores.size - a.stores.size || String(a.name).localeCompare(String(b.name)))
        .slice(0, EXAMPLES_PER_ITEM)
        .map((a) => ({ ean: a.ean, name: a.name, via: a.via, stores: a.stores.size, ...(a.min !== null ? { min: a.min } : {}) }));
      return {
        id,
        indicacao: clean(e.indicacao, 80),
        principio_ativo: clean(e.principio_ativo, 160),
        free: e.gratuito !== false,
        eans: aggs.length,
        examples,
        ...(prices.length ? { min_price: Math.min(...prices) } : {}),
      };
    })
    .filter((it) => it.id)
    .sort((a, b) => String(a.indicacao).localeCompare(String(b.indicacao), 'pt') || a.id.localeCompare(b.id));

  const version = elenco.map((e) => String(e.elenco_versao || '')).filter(Boolean).sort().pop() || null;
  docs.set('PfpbCatalog/elenco', { version, items });

  const map = {};
  for (const [ean, v] of [...eanIndex].sort(([a], [b]) => a.localeCompare(b))) {
    map[ean] = { i: [...v.ids].sort(), ...(v.name ? { n: v.name } : {}) };
  }
  docs.set('PfpbCatalog/eans', { map });

  // ── Pharmacies by city, and the geocoded ones by cell ──────────────────
  const cities = new Map();
  const cells = new Map();
  for (const row of farmacias) {
    const status = String(row.situacao_cadastral || '').trim().toUpperCase();
    if (status && status !== 'ATIVA') continue;
    const key = cityKey(row.uf, row.municipio);
    const ph = compactPharmacy(row);
    if (!key || !ph.c) continue;
    if (!cities.has(key)) {
      cities.set(key, {
        uf: String(row.uf || '').trim().toUpperCase(),
        municipio: clean(row.municipio, 80),
        ibge: clean(row.cod_municipio_ibge, 7),
        pharmacies: [],
      });
    }
    cities.get(key).pharmacies.push(ph);
    if (ph.lat !== undefined) {
      const gh = geohashEncode(ph.lat, ph.lng, CELL_PRECISION);
      if (!cells.has(gh)) cells.set(gh, []);
      cells.get(gh).push({ ...ph, k: key, m: cities.get(key).municipio, uf: cities.get(key).uf });
    }
  }
  for (const [key, city] of [...cities].sort(([a], [b]) => a.localeCompare(b))) {
    city.pharmacies.sort(byNameThenAddress);
    const geocoded = city.pharmacies.filter((p) => p.lat !== undefined).length;
    if (!city.ibge) delete city.ibge;
    docs.set(`PfpbCity/${key}`, { ...city, count: city.pharmacies.length, geocoded });
  }
  for (const [gh, list] of [...cells].sort(([a], [b]) => a.localeCompare(b))) {
    docs.set(`PfpbCell/${gh}`, { pharmacies: list.sort((a, b) => a.c.localeCompare(b.c)) });
  }

  return docs;
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    return Object.keys(value).sort().reduce((acc, k) => { acc[k] = canonical(value[k]); return acc; }, {});
  }
  return value;
}

function stableHash(value) {
  return crypto.createHash('sha1').update(JSON.stringify(canonical(value))).digest('hex').slice(0, 16);
}

/**
 * What a run must change, in the order it should change it: the catalogue
 * first, then cells, then cities biggest-first (the most people per write),
 * deletions last. Paths whose hash matches the manifest are skipped.
 */
function planPfpbWrites(docs, manifestHashes = {}) {
  const priority = (p) => (p.startsWith('PfpbCatalog/') ? 0 : p.startsWith('PfpbCell/') ? 1 : 2);
  const sets = [];
  for (const [path, data] of docs) {
    const hash = stableHash(data);
    if (manifestHashes[path] !== hash) sets.push({ path, hash, data, size: Array.isArray(data.pharmacies) ? data.pharmacies.length : 0 });
  }
  sets.sort((a, b) => priority(a.path) - priority(b.path) || b.size - a.size || a.path.localeCompare(b.path));
  const deletes = Object.keys(manifestHashes).filter((p) => !docs.has(p)).sort();
  return { sets, deletes };
}

module.exports = {
  CELL_PRECISION,
  geohashEncode,
  cityKey,
  compactPharmacy,
  buildPfpbDocs,
  stableHash,
  planPfpbWrites,
};

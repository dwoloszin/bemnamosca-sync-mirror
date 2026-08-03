// ──────────────────────────────────────────────────────────
// Bem na Mosca — Neon → Firestore sync configuration
// ──────────────────────────────────────────────────────────
// Tunable knobs for scripts/sync-neon-to-firestore.cjs. Edit and re-run —
// no code changes needed to raise/lower the sync volume.
'use strict';

module.exports = {
  // Hard ceiling on actual Firestore WRITE operations per run (Product +
  // StoreRecentPriceEntry + PriceEntry documents combined, across all
  // stores). Firestore's free tier allows 20,000 writes/day; this stays
  // safely under it, leaving headroom for normal app traffic the same day.
  // Raise/lower freely — nothing else in the sync depends on this number.
  //
  // 2000 x 6 scheduled runs = 12,000/day = 60% of the 20k free-tier write
  // quota, leaving ~8k/day for app traffic and the other scheduled jobs
  // (~100/day). Raised from 1000 to halve the initial-backlog ETA (~10k
  // (store,product) pairs ≈ 30k writes: ~5 days at 1000, ~2.5 at 2000).
  // Going much higher has diminishing returns: dynamicBudgetSafetyPercent
  // below caps each run at 75% of the day's REMAINING headroom, so once
  // cumulative usage nears 15k the later runs get throttled anyway.
  maxWritesPerRun: 2000,

  // How many `offers` rows to pull per store, per run, before diffing
  // against the mirror. Kept well above maxWritesPerRun because most rows
  // on a repeat pass are unchanged (0 writes) — this just bounds the Neon
  // query size, not the Firestore write count.
  pageSizePerStorePerRun: 4000,

  // Firestore batch.commit() chunk size (Firestore's hard limit is 500).
  writeBatchSize: 400,

  // A barcode must be all digits, this many characters, and not all
  // zeros to be treated as a valid product key. Anything else is
  // skipped and counted in the run report — barcode is the app's
  // primary matching key across stores, so a bad one is never synced.
  barcode: {
    minLength: 8,
    maxLength: 14,
  },

  // Threshold (same currency units as offers.regular_price/promo_price —
  // Brazilian reais, e.g. 1000 = R$1.000,00) used by
  // sync-neon-high-value.cjs to find expensive products worth cross-checking
  // across every store, instead of relying on the full-catalog cursor sync
  // (which walks stores independently and can take weeks to reach a given
  // barcode). See that script's header for the two-phase discovery/cross-
  // store approach. Lower = more products qualify = more read/write pressure
  // (still capped by maxWritesPerRun and the dynamic budget below).
  MIN_VALUE: 1000,

  // Caps each run's write budget at this percent of TODAY'S REMAINING write
  // headroom (20,000/day free-tier limit minus writes already used today,
  // per the SystemHealth/firestore-free-tier-guard doc the app's own guard
  // scheduler keeps updated hourly) — never just the static maxWritesPerRun
  // above. E.g. at 75%, if 2,752 writes are already used today, the dynamic
  // ceiling is floor((20000-2752) * 0.75) = 12,936, and the run's actual
  // budget is min(maxWritesPerRun, that ceiling). The 25% left out covers
  // staleness in the hourly snapshot plus organic app traffic the rest of
  // the day. Matches the existing PRICE_COMPACTION_GUARD_SAFETY_PERCENT
  // convention in functions/index.js (also defaults to 75).
  dynamicBudgetSafetyPercent: 75,

  // Public GitHub repo used as the historical sync mirror: it stores the
  // last-synced (price, min, max) per barcode per store so repeat runs
  // can diff WITHOUT reading Firestore. Owner/token are reused from the
  // existing VITE_GITHUB_OWNER / VITE_GITHUB_TOKEN in .env.local.
  mirror: {
    repo: 'bemnamosca-sync-mirror',
    branch: 'main',
    localPath: '.', // gitignored working copy of the mirror repo
  },

  // One Neon Postgres database per pharmacy chain (each scraped on its own
  // schedule — see the scraper cron table in the repo notes). `envVar` names
  // the .env.local variable holding that store's connection string. Stores
  // whose env var is unset/empty are skipped automatically — safe to list a
  // chain before its credentials exist. The Firestore sync itself runs on the
  // schedule in bemnamosca-sync-mirror/.github/workflows/sync-neon.yml,
  // independent of each store's scrape cadence.
  stores: [
    // Large chains
    { slug: 'drogaleste', displayName: 'Drogaleste', envVar: 'NEON_DATABASE_URL_DROGALESTE' },
    { slug: 'drogasil', displayName: 'Drogasil', envVar: 'NEON_DATABASE_URL_DROGASIL' },
    { slug: 'drogaraia', displayName: 'Droga Raia', envVar: 'NEON_DATABASE_URL_DROGARAIA' },
    { slug: 'drogariasaopaulo', displayName: 'Drogaria São Paulo', envVar: 'NEON_DATABASE_URL_DROGARIASAOPAULO' },
    { slug: 'ultrafarma', displayName: 'Ultrafarma', envVar: 'NEON_DATABASE_URL_ULTRAFARMA' },
    { slug: 'paguemenos', displayName: 'Pague Menos', envVar: 'NEON_DATABASE_URL_PAGUEMENOS' },
    { slug: 'farmais', displayName: 'Farmais', envVar: 'NEON_DATABASE_URL_FARMAIS' },
    { slug: 'panvel', displayName: 'Panvel', envVar: 'NEON_DATABASE_URL_PANVEL' },
    { slug: 'farmaciasapp', displayName: 'Farmácias APP', envVar: 'NEON_DATABASE_URL_FARMACIASAPP' },
    { slug: 'farmaconde', displayName: 'Farmaconde', envVar: 'NEON_DATABASE_URL_FARMACONDE' },
    { slug: 'pacheco', displayName: 'Drogaria Pacheco', envVar: 'NEON_DATABASE_URL_PACHECO' },

    // Smaller / regional chains
    { slug: 'eualiria', displayName: 'Eu Aliria', envVar: 'NEON_DATABASE_URL_EUALIRIA' },
    { slug: 'agillemed', displayName: 'Agille Med', envVar: 'NEON_DATABASE_URL_AGILLEMED' },
    { slug: 'novamed', displayName: 'Novamed', envVar: 'NEON_DATABASE_URL_NOVAMED' },
    { slug: 'pharmed', displayName: 'Pharmed', envVar: 'NEON_DATABASE_URL_PHARMED' },
    { slug: 'justmedicamentos', displayName: 'Just Medicamentos', envVar: 'NEON_DATABASE_URL_JUSTMEDICAMENTOS' },
    { slug: 'ghfarma', displayName: 'GH Farma', envVar: 'NEON_DATABASE_URL_GHFARMA' },
    { slug: 'levitta', displayName: 'Levitta', envVar: 'NEON_DATABASE_URL_LEVITTA' },
    { slug: 'dinamica', displayName: 'Dinâmica', envVar: 'NEON_DATABASE_URL_DINAMICA' },
    { slug: 'facilita', displayName: 'Facilita', envVar: 'NEON_DATABASE_URL_FACILITA' },
    { slug: 'mevofarma', displayName: 'Mevofarma', envVar: 'NEON_DATABASE_URL_MEVOFARMA' },
    { slug: 'singular', displayName: 'Singular', envVar: 'NEON_DATABASE_URL_SINGULAR' },
    { slug: 'mundial', displayName: 'Mundial', envVar: 'NEON_DATABASE_URL_MUNDIAL' },
    { slug: 'remed', displayName: 'Remed', envVar: 'NEON_DATABASE_URL_REMED' },
    { slug: 'fast', displayName: 'Fast', envVar: 'NEON_DATABASE_URL_FAST' },
    { slug: 'progoods', displayName: 'ProGoods', envVar: 'NEON_DATABASE_URL_PROGOODS' },
    { slug: 'hera', displayName: 'Hera', envVar: 'NEON_DATABASE_URL_HERA' },
    { slug: 'campea', displayName: 'Campeã', envVar: 'NEON_DATABASE_URL_CAMPEA' },
    { slug: 'integral', displayName: 'Integral', envVar: 'NEON_DATABASE_URL_INTEGRAL' },
    { slug: 'qualidoc', displayName: 'Qualidoc', envVar: 'NEON_DATABASE_URL_QUALIDOC' },
    { slug: 'alianza', displayName: 'Alianza', envVar: 'NEON_DATABASE_URL_ALIANZA' },

    // Oncology-focused
    { slug: 'oncoexpresso', displayName: 'Onco Expresso', envVar: 'NEON_DATABASE_URL_ONCOEXPRESSO' },
    { slug: 'oncohealthmedicamentos', displayName: 'Onco Health Medicamentos', envVar: 'NEON_DATABASE_URL_ONCOHEALTHMEDICAMENTOS' },
    { slug: 'lj_oncoexpress', displayName: 'LJ Onco Express', envVar: 'NEON_DATABASE_URL_LJ_ONCOEXPRESS' },

    // Added 2026-08-03. All six connect and carry fresh scrapes; verified
    // before registering. displayName is what the app shows on the store
    // card — adjust if a chain's public name differs.
    { slug: 'farmsaopaulo', displayName: 'Farma São Paulo', envVar: 'NEON_DATABASE_URL_FARMSAOPAULO' },
    { slug: 'nissei', displayName: 'Farmácias Nissei', envVar: 'NEON_DATABASE_URL_NISSEI' },
    { slug: 'drogal', displayName: 'Drogal', envVar: 'NEON_DATABASE_URL_DROGAL' },
    { slug: 'veracruz', displayName: 'Farmácia Vera Cruz', envVar: 'NEON_DATABASE_URL_VERACRUZ' },
    { slug: 'farmagerty', displayName: 'Farma Gerty', envVar: 'NEON_DATABASE_URL_FARMAGERTY' },
    { slug: 'sampharma', displayName: 'Sampharma', envVar: 'NEON_DATABASE_URL_SAMPHARMA' },
  ],
};

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
  maxWritesPerRun: 1000,

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

  // One Neon Postgres database per pharmacy chain (each scraped daily).
  // `envVar` names the .env.local variable holding that store's
  // connection string. Stores whose env var is unset/empty are skipped
  // automatically — safe to list a chain before its credentials exist.
  stores: [
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
  ],
};

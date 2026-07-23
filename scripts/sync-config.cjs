// ──────────────────────────────────────────────────────────
// Bem na Mosca — Neon → Firestore sync configuration
// ──────────────────────────────────────────────────────────
// This is the copy that RUNS the scheduled/manual GitHub Action from this
// repo (bemnamosca-sync-mirror is public, so Actions minutes are free —
// see README.md). It must stay in sync with the source of truth in the
// main app repo: bemnamosca_db/scripts/sync-config.cjs. Local/emulator
// testing still happens from bemnamosca_db; only the production automation
// runs from here.
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
  // Brazilian reais, e.g. 100000 = R$100.000,00) used by
  // sync-neon-high-value.cjs to find expensive products worth cross-checking
  // across every store, instead of relying on the full-catalog cursor sync
  // (which walks stores independently and can take weeks to reach a given
  // barcode). See that script's header for the two-phase discovery/cross-
  // store approach.
  MIN_VALUE: 100000,

  // This script runs FROM the mirror repo's own checkout in CI (or from a
  // local clone of it), so the mirror IS the current working directory —
  // no separate clone step needed. localPath: '.' means SyncMirror reads
  // and writes mirror/ and state/ right here at the repo root.
  mirror: {
    repo: 'bemnamosca-sync-mirror',
    branch: 'main',
    localPath: '.',
  },

  // One Neon Postgres database per pharmacy chain (each scraped daily).
  // `envVar` names the environment variable holding that store's
  // connection string (GitHub Actions secret in CI, .env.local locally).
  // Stores whose env var is unset/empty are skipped automatically — safe
  // to list a chain before its credentials exist.
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

# bemnamosca-sync-mirror

Runs the Bem na Mosca Neon → Firestore price sync on a schedule
(`.github/workflows/sync-neon.yml`) **and** stores its historical mirror
data — both live here, in this public repo, instead of the private
`bemnamosca_db` app repo, so GitHub Actions minutes are free no matter how
often it runs.

## Layout

- `scripts/` — copy of `bemnamosca_db/scripts/sync-neon-*.cjs` and their
  shared libs. If the sync logic changes in the main app repo, copy the
  updated files here too (local/emulator testing still happens from
  `bemnamosca_db`; this copy is what production actually runs).
- `mirror/<storeSlug>/<shard>.json` — last-synced (price, min, max) per
  barcode per store, so repeat runs can detect what changed WITHOUT
  reading Firestore (reads and writes there count toward billing; this
  repo is free git storage).
- `state/cursor.json` — per-store full-catalog pagination progress.

Do not edit `mirror/` or `state/` files by hand — they are overwritten on
every sync run.

## Secrets required (repo Settings → Secrets and variables → Actions)

- `NEON_DATABASE_URL_<CHAIN>` — one per pharmacy chain (see
  `scripts/sync-config.cjs` for the list of slugs/env var names)
- `FIREBASE_SERVICE_ACCOUNT_JSON` — service account JSON for the
  `bemnamosca` Firebase project
- `VITE_FIREBASE_PROJECT_ID` — Firebase project id (`bemnamosca`)

No GitHub token/PAT secret is needed here: the workflow pushes mirror
updates back to this same repo using the default `GITHUB_TOKEN` (via
`permissions: contents: write`).

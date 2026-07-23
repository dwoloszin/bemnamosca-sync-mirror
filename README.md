# bemnamosca-sync-mirror

Runs the Bem na Mosca Neon → Firestore price sync on a schedule
(`.github/workflows/sync-neon.yml`) **and** stores its historical mirror
data — both live here, in this public repo, instead of the private
`bemnamosca_db` app repo, so GitHub Actions minutes are free no matter how
often it runs.

## High-value only (by design)

The scheduled (17:00 UTC daily) and manual runs always use
`scripts/sync-neon-high-value.cjs`, never the full-catalog cursor script
(`sync-neon-to-firestore.cjs`). Every run:

1. Discovers every barcode priced `>= MIN_VALUE` (`scripts/sync-config.cjs`)
   in ANY store.
2. Cross-checks that exact barcode against EVERY store (no price filter on
   the cross-check), so a genuine discount at another store is never missed
   just because it falls under `MIN_VALUE` there.

`MIN_VALUE` is meant to be lowered over time — each run re-reads it fresh
from `scripts/sync-config.cjs`, so no code change is needed to widen
coverage, just edit that number (and let the CI gate in `bemnamosca_db`
promote the change — see `push-sync-to-mirror.cjs`).

The full-catalog script still exists here and in `bemnamosca_db` for a
manual/local run (`npm run sync:neon:apply`) if you ever want the rest of
the catalog synced too — it's just not part of the automatic schedule.

## Layout

- `scripts/` — copy of `bemnamosca_db/scripts/sync-neon-*.cjs` and their
  shared libs. If the sync logic changes in the main app repo, copy the
  updated files here too (local/emulator testing still happens from
  `bemnamosca_db`; this copy is what production actually runs).
- `mirror/<storeSlug>/<shard>.json` — last-synced (price, min, max) per
  barcode per store, so repeat runs can detect what changed WITHOUT
  reading Firestore (reads and writes there count toward billing; this
  repo is free git storage).
- `state/cursor.json` — per-store full-catalog pagination progress (only
  advances if you run the full-catalog script manually; unused by the
  automatic high-value-only schedule).

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

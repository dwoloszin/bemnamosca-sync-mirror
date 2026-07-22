# bemnamosca-sync-mirror

Auto-generated historical mirror for the Bem na Mosca Neon → Firestore price
sync (`scripts/sync-neon-to-firestore.cjs` in the main app repo).

Stores the LAST-SYNCED price/min/max per barcode per pharmacy chain, so
repeat sync runs can detect what changed WITHOUT reading Firestore (reads
and writes there count toward billing; this repo is free git storage).

Do not edit these files by hand — they are overwritten on every sync run.

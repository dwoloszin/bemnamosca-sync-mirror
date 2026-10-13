#!/usr/bin/env node
/**
 * users-backup-to-neon.cjs — off-GCP backup of the irreplaceable user data.
 *
 * Copies the four collections that cannot be reconstructed from Neon or the
 * scrapers — User, Subscription, Transaction, ProductUnlocks — into the
 * dedicated backup Postgres (NEON_USERS_BACKUP_URL, a Neon project SEPARATE
 * from the 62 store databases so the sync lifeline's compute is untouched).
 *
 * Read-budget strategy (the whole point of this design):
 *   - FIRST run (empty backup_state): full snapshot — every doc, once.
 *   - DAILY runs: incremental — tail each collection's timestamp fields
 *     (cursor kept in Neon, 10-min overlap for clock skew). A quiet day
 *     costs ~1 read per cursor query, not one per document.
 *   - MONTHLY (--full): full re-read that also PURGES rows whose Firestore
 *     doc is gone — deletion propagates to the backup within a month (LGPD).
 *   This script NEVER writes to Firestore.
 *
 * Cursor fields per collection (why two for some: the field a write bumps):
 *   User          updated_date / created_date   (ISO strings, base44 client)
 *   Subscription  updatedAt                     (server Timestamp, all writes)
 *   Transaction   createdAt / updatedAt         (create vs status flips —
 *                 every status flip bumps updatedAt as of 2026-10-13)
 *   ProductUnlocks updated_at                   (ISO string, buildGrantPatch)
 * Docs missing a cursor field are invisible to range queries — that is what
 * the bootstrap full snapshot and the monthly --full sweep are for.
 *
 * Neon schema (created on demand):
 *   backup_state(key PK, cursor, run_at)      one row per collection+field
 *   backup_<collection>(doc_id PK, payload jsonb, synced_at)
 *
 * Usage (mirror repo workflow users-backup.yml, or local):
 *   node scripts/users-backup-to-neon.cjs --dry-run
 *   node scripts/users-backup-to-neon.cjs --apply [--full]
 *     --service-account ./service-account.ci.json --project-id bemnamosca
 */

const path = require('path');
const { createNeonClient, initFirestore, describeError } = require(path.join(__dirname, 'lib', 'neonSyncCore.cjs'));

const admin = require('firebase-admin');

// ── CLI ───────────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const APPLY = argv.includes('--apply');
const FULL = argv.includes('--full');
const argValue = (flag) => {
  const i = argv.indexOf(flag);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : null;
};
const args = {
  serviceAccount: argValue('--service-account'),
  projectId: argValue('--project-id'),
};
// Brake: a run that tries to read more than this aborts before burning the
// guard's headroom — raise it deliberately, never by accident.
const MAX_DOCS = Number(argValue('--max-docs') || 60000);
const PAGE_SIZE = 500;
const OVERLAP_MS = 10 * 60 * 1000;
const UPSERT_CHUNK = 100;

const BACKUP_URL = process.env.NEON_USERS_BACKUP_URL;
if (!BACKUP_URL) {
  console.error('NEON_USERS_BACKUP_URL is not set — refusing to run.');
  process.exit(1);
}

// type: how the cursor value is written in Firestore. 'iso' compares as a
// string; 'ts' needs a Timestamp for the range query.
const COLLECTIONS = [
  { name: 'User', table: 'backup_user', cursors: [
    { field: 'updated_date', type: 'iso' },
    { field: 'created_date', type: 'iso' },
  ] },
  { name: 'Subscription', table: 'backup_subscription', cursors: [
    { field: 'updatedAt', type: 'ts' },
  ] },
  { name: 'Transaction', table: 'backup_transaction', cursors: [
    { field: 'createdAt', type: 'ts' },
    { field: 'updatedAt', type: 'ts' },
  ] },
  { name: 'ProductUnlocks', table: 'backup_productunlocks', cursors: [
    { field: 'updated_at', type: 'iso' },
  ] },
];

// ── Serialization ────────────────────────────────────────────────────────────
// Firestore Timestamps (and anything date-like) become ISO strings so the
// JSONB payload is self-describing and restorable without the admin SDK.
function plainify(value) {
  if (value === null || value === undefined) return value;
  if (typeof value.toDate === 'function') return value.toDate().toISOString();
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(plainify);
  if (typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = plainify(v);
    return out;
  }
  return value;
}

function cursorValueOf(doc, cursor) {
  const raw = doc[cursor.field];
  if (raw === null || raw === undefined) return null;
  if (typeof raw.toDate === 'function') return raw.toDate().toISOString();
  if (typeof raw === 'string') return raw;
  return null;
}

// ── Neon side ────────────────────────────────────────────────────────────────
async function ensureSchema(client) {
  await client.query(`CREATE TABLE IF NOT EXISTS backup_state (
    key text PRIMARY KEY,
    cursor text NOT NULL,
    run_at timestamptz NOT NULL DEFAULT now()
  )`);
  for (const spec of COLLECTIONS) {
    await client.query(`CREATE TABLE IF NOT EXISTS ${spec.table} (
      doc_id text PRIMARY KEY,
      payload jsonb NOT NULL,
      synced_at timestamptz NOT NULL DEFAULT now()
    )`);
  }
}

async function readCursor(client, key) {
  const res = await client.query('SELECT cursor FROM backup_state WHERE key = $1', [key]);
  return res.rows[0]?.cursor || null;
}

async function writeCursor(client, key, cursor) {
  await client.query(
    `INSERT INTO backup_state (key, cursor, run_at) VALUES ($1, $2, now())
     ON CONFLICT (key) DO UPDATE SET cursor = EXCLUDED.cursor, run_at = now()`,
    [key, cursor],
  );
}

async function upsertDocs(client, table, docs) {
  for (let i = 0; i < docs.length; i += UPSERT_CHUNK) {
    const chunk = docs.slice(i, i + UPSERT_CHUNK);
    const params = [];
    const tuples = chunk.map((d, j) => {
      params.push(d.id, JSON.stringify(plainify(d.data)));
      return `($${j * 2 + 1}, $${j * 2 + 2}::jsonb, now())`;
    });
    await client.query(
      `INSERT INTO ${table} (doc_id, payload, synced_at) VALUES ${tuples.join(',')}
       ON CONFLICT (doc_id) DO UPDATE SET payload = EXCLUDED.payload, synced_at = now()`,
      params,
    );
  }
}

// ── Firestore side ───────────────────────────────────────────────────────────
let totalReads = 0;
function countReads(n) {
  totalReads += n;
  if (totalReads > MAX_DOCS) {
    throw new Error(`read brake: run exceeded --max-docs=${MAX_DOCS} — aborting before it costs more.`);
  }
}

async function fullSnapshot(db, spec) {
  const docs = new Map();
  let last = null;
  for (;;) {
    let q = db.collection(spec.name).orderBy(admin.firestore.FieldPath.documentId()).limit(PAGE_SIZE);
    if (last) q = q.startAfter(last);
    const snap = await q.get();
    countReads(Math.max(snap.size, 1));
    if (snap.empty) break;
    snap.forEach((d) => docs.set(d.id, d.data()));
    last = snap.docs[snap.docs.length - 1];
    if (snap.size < PAGE_SIZE) break;
  }
  return docs;
}

async function incrementalChanges(db, client, spec) {
  const docs = new Map();
  const newCursors = {};
  for (const cursor of spec.cursors) {
    const key = `${spec.name}:${cursor.field}`;
    const stored = await readCursor(client, key);
    if (!stored) return null; // no cursor yet → caller falls back to full
    const sinceIso = new Date(Date.parse(stored) - OVERLAP_MS).toISOString();
    const since = cursor.type === 'ts' ? admin.firestore.Timestamp.fromDate(new Date(sinceIso)) : sinceIso;

    let maxSeen = stored;
    let last = null;
    for (;;) {
      let q = db.collection(spec.name).where(cursor.field, '>', since).orderBy(cursor.field).limit(PAGE_SIZE);
      if (last) q = q.startAfter(last);
      const snap = await q.get();
      countReads(Math.max(snap.size, 1));
      if (snap.empty) break;
      snap.forEach((d) => {
        const data = d.data();
        docs.set(d.id, data);
        const v = cursorValueOf(data, cursor);
        if (v && v > maxSeen) maxSeen = v;
      });
      last = snap.docs[snap.docs.length - 1];
      if (snap.size < PAGE_SIZE) break;
    }
    newCursors[key] = maxSeen;
  }
  return { docs, newCursors };
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  const isEmulatorRun = Boolean(process.env.FIRESTORE_EMULATOR_HOST);
  if (isEmulatorRun) console.log(`users-backup: FIRESTORE EMULATOR at ${process.env.FIRESTORE_EMULATOR_HOST}`);
  const db = initFirestore(args, isEmulatorRun);
  const client = createNeonClient(BACKUP_URL, 'users-backup');
  await client.connect();
  console.log(`users-backup: ${APPLY ? 'APPLY' : 'DRY-RUN'}${FULL ? ' FULL' : ''} — max-docs ${MAX_DOCS}`);

  try {
    await ensureSchema(client);

    for (const spec of COLLECTIONS) {
      const label = spec.name.padEnd(14);
      let incremental = null;
      if (!FULL) incremental = await incrementalChanges(db, client, spec);

      if (incremental) {
        const { docs, newCursors } = incremental;
        console.log(`  ${label} incremental: ${docs.size} changed doc(s)`);
        if (APPLY && docs.size > 0) {
          await upsertDocs(client, spec.table, [...docs].map(([id, data]) => ({ id, data })));
        }
        if (APPLY) {
          for (const [key, cur] of Object.entries(newCursors)) await writeCursor(client, key, cur);
        }
        continue;
      }

      // Full snapshot: bootstrap (no cursor yet) or --full reconciliation.
      const docs = await fullSnapshot(db, spec);
      console.log(`  ${label} full snapshot: ${docs.size} doc(s)`);
      if (APPLY) {
        if (docs.size > 0) {
          await upsertDocs(client, spec.table, [...docs].map(([id, data]) => ({ id, data })));
        }
        // Purge rows whose Firestore doc no longer exists — this is how a
        // user's account deletion reaches the backup (LGPD), monthly at most.
        const purged = await client.query(
          `DELETE FROM ${spec.table} WHERE NOT (doc_id = ANY($1::text[]))`,
          [[...docs.keys()]],
        );
        if (purged.rowCount > 0) console.log(`  ${label} purged ${purged.rowCount} deleted doc(s)`);
        // Cursors restart from the newest value actually seen.
        for (const cursor of spec.cursors) {
          let maxSeen = new Date(0).toISOString();
          for (const data of docs.values()) {
            const v = cursorValueOf(data, cursor);
            if (v && v > maxSeen) maxSeen = v;
          }
          await writeCursor(client, `${spec.name}:${cursor.field}`, maxSeen);
        }
      }
    }

    console.log(`users-backup done — ${totalReads} Firestore read(s) this run.`);
  } finally {
    try { await client.end(); } catch { /* already closed */ }
  }
}

main().catch((err) => {
  console.error('users-backup failed:', describeError ? describeError(err) : (err?.message || err));
  process.exit(1);
});

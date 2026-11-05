#!/usr/bin/env node
/**
 * pfpb-sync-to-firestore.cjs — Farmácia Popular data into the app.
 *
 * Reads the FARMACIA_POPULAR project's Neon database (NEON_DATABASE_URL_PFPB:
 * pfpb_elenco, pfpb_produtos, pfpb_farmacias) in ONE connection, three
 * sequential queries, and writes the documents shaped by lib/pfpbCore.cjs.
 *
 * Write-frugal: a manifest (PfpbCatalog/manifest) holds a hash per document,
 * so a weekly run writes only what changed — typically the cities whose
 * pharmacies gained coordinates that week. --max-writes (default 2500, the
 * price sync's own per-run cap) brakes the first load (~4,800 documents),
 * which therefore completes over two runs; whatever a run leaves out keeps
 * its old hash and is picked up by the next run.
 *
 * Runs from the mirror repo (pfpb-sync.yml), Mondays after the PFPB
 * project's own weekly jobs. Dry-run unless --apply.
 *
 *   node scripts/pfpb-sync-to-firestore.cjs [--apply] [--max-writes N]
 *     --service-account ./service-account.ci.json --project-id bemnamosca
 */

const path = require('path');
const { createNeonClient, initFirestore, normalizeBarcode, describeError } = require(path.join(__dirname, 'lib', 'neonSyncCore.cjs'));
const { buildPfpbDocs, planPfpbWrites } = require(path.join(__dirname, 'lib', 'pfpbCore.cjs'));

const argv = process.argv.slice(2);
const APPLY = argv.includes('--apply');
const argValue = (flag) => {
  const i = argv.indexOf(flag);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : null;
};
const args = { serviceAccount: argValue('--service-account'), projectId: argValue('--project-id') };
const MAX_WRITES = Math.max(1, Number(argValue('--max-writes') || 2500));
const BATCH_SIZE = 400;
const MANIFEST_PATH = 'PfpbCatalog/manifest';

const PFPB_URL = process.env.NEON_DATABASE_URL_PFPB;
if (!PFPB_URL) {
  console.error('NEON_DATABASE_URL_PFPB is not set — refusing to run.');
  process.exit(1);
}

async function readNeon() {
  const client = createNeonClient(PFPB_URL, 'pfpb');
  await client.connect();
  try {
    const elenco = (await client.query(
      'SELECT pfpb_id, indicacao, principio_ativo, gratuito, elenco_versao FROM pfpb_elenco',
    )).rows;
    const produtos = (await client.query(
      `SELECT pfpb_id, match_via, barcode, product_name, store_name, price, is_available
         FROM pfpb_produtos WHERE barcode IS NOT NULL`,
    )).rows;
    const farmacias = (await client.query(
      `SELECT cnpj, razao_social, nome_fantasia, rede, endereco, bairro, municipio, uf,
              cod_municipio_ibge, cep, telefone, latitude, longitude, geo_fonte,
              geo_precisao, geo_parcial, situacao_cadastral
         FROM pfpb_farmacias`,
    )).rows;
    return { elenco, produtos, farmacias };
  } finally {
    try { await client.end(); } catch { /* already closed */ }
  }
}

async function main() {
  const isEmulatorRun = Boolean(process.env.FIRESTORE_EMULATOR_HOST);
  const db = initFirestore(args, isEmulatorRun);
  console.log(`pfpb-sync: ${APPLY ? 'APPLY' : 'DRY-RUN'} — max-writes ${MAX_WRITES}${isEmulatorRun ? ' (emulator)' : ''}`);

  const rows = await readNeon();
  console.log(`  neon: ${rows.elenco.length} items, ${rows.produtos.length} product rows, ${rows.farmacias.length} pharmacies`);

  const docs = buildPfpbDocs({ ...rows, normalizeBarcode });
  const cities = [...docs.keys()].filter((p) => p.startsWith('PfpbCity/')).length;
  const cells = [...docs.keys()].filter((p) => p.startsWith('PfpbCell/')).length;
  const geocoded = [...docs].filter(([p]) => p.startsWith('PfpbCity/')).reduce((s, [, d]) => s + d.geocoded, 0);
  const active = [...docs].filter(([p]) => p.startsWith('PfpbCity/')).reduce((s, [, d]) => s + d.count, 0);
  console.log(`  shaped: ${cities} cities, ${cells} cells, ${active} active pharmacies (${geocoded} with coordinates)`);

  const manifestRef = db.doc(MANIFEST_PATH);
  const manifestSnap = await manifestRef.get();
  const manifest = manifestSnap.exists ? (manifestSnap.data() || {}) : {};
  const hashes = { ...(manifest.hashes || {}) };

  const { sets, deletes } = planPfpbWrites(docs, hashes);
  console.log(`  changes: ${sets.length} to write, ${deletes.length} to delete`);

  // The manifest write counts against the brake too.
  const budget = MAX_WRITES - 1;
  const ops = [
    ...sets.map((s) => ({ kind: 'set', ...s })),
    ...deletes.map((p) => ({ kind: 'delete', path: p })),
  ].slice(0, budget);
  const pending = sets.length + deletes.length - ops.length;

  if (!APPLY) {
    console.log(`  dry-run: would apply ${ops.length} op(s)${pending ? `, leaving ${pending} for the next run` : ''}`);
    return;
  }

  for (let i = 0; i < ops.length; i += BATCH_SIZE) {
    const batch = db.batch();
    for (const op of ops.slice(i, i + BATCH_SIZE)) {
      if (op.kind === 'set') batch.set(db.doc(op.path), op.data);
      else batch.delete(db.doc(op.path));
    }
    await batch.commit();
    for (const op of ops.slice(i, i + BATCH_SIZE)) {
      if (op.kind === 'set') hashes[op.path] = op.hash;
      else delete hashes[op.path];
    }
  }

  await manifestRef.set({
    hashes,
    updated_at: new Date().toISOString(),
    stats: { items: rows.elenco.length, eans: Object.keys(docs.get('PfpbCatalog/eans').map).length, cities, cells, active, geocoded },
    pending,
  });
  console.log(`pfpb-sync done — ${ops.length} op(s) applied${pending ? `, ${pending} left for the next run` : ', fully in sync'}.`);
}

main().catch((err) => {
  console.error('pfpb-sync failed:', describeError ? describeError(err) : (err?.message || err));
  process.exit(1);
});

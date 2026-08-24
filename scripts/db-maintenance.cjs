// ── Neon housekeeping: keep forty small databases fast ─────────────────────
//
// The offers tables accumulate dead rows (is_available=false) far beyond the
// live catalogue — enough that any full-table aggregate grinds past the
// statement timeout on the free-tier computes, measured 2026-08-24 when a
// simple per-band COUNT timed out on every store while index walks flew.
// price_history grows without bound by design.
//
// MODE=report  (default): catalog stats only — table sizes, row estimates,
//   the history date column — zero scans, instant.
// MODE=apply: per store, sequentially:
//   1. delete offers rows dead longer than DEAD_GRACE_DAYS (default 30) —
//      the scraper upserts by ean, so a product that returns simply
//      reappears; the grace keeps recent churn visible to the prune's
//      "came back" check;
//   2. delete price_history older than HISTORY_RETENTION_DAYS (default 180)
//      — the aggregates the app uses read min/max from this table, so the
//      window becomes their horizon, which is closer to the intended "30d"
//      semantics than all-time was;
//   3. VACUUM ANALYZE both tables — the part that actually restores speed.
// Deletes run in ctid batches so no statement outlives the timeout and no
// transaction holds the table.
const { createNeonClient } = require('./lib/neonSyncCore.cjs');
const syncConfig = require('./sync-config.cjs');

const MODE = String(process.env.MODE || 'report').toLowerCase();
const DEAD_GRACE_DAYS = Number(process.env.DEAD_GRACE_DAYS || 30);
const HISTORY_RETENTION_DAYS = Number(process.env.HISTORY_RETENTION_DAYS || 180);
const BATCH = 5000;

const DATE_CANDIDATES = ['scraped_at', 'recorded_at', 'created_at', 'updated_at', 'date', 'captured_at'];

async function tableStats(client) {
  const { rows } = await client.query(
    "SELECT c.relname AS table, c.reltuples::bigint AS est_rows, " +
    "pg_total_relation_size(c.oid) AS bytes " +
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
    "WHERE n.nspname = 'public' AND c.relname IN ('offers', 'price_history') AND c.relkind = 'r'");
  return rows;
}

async function historyDateColumn(client) {
  const { rows } = await client.query(
    "SELECT column_name FROM information_schema.columns " +
    "WHERE table_schema = 'public' AND table_name = 'price_history'");
  const cols = rows.map((r) => r.column_name);
  return DATE_CANDIDATES.find((c) => cols.includes(c)) || null;
}

async function batchedDelete(client, table, whereSql, params) {
  let total = 0;
  for (;;) {
    const { rowCount } = await client.query(
      'DELETE FROM ' + table + ' WHERE ctid = ANY(ARRAY(' +
      'SELECT ctid FROM ' + table + ' WHERE ' + whereSql + ' LIMIT ' + BATCH + '))', params);
    total += rowCount;
    if (rowCount < BATCH) return total;
  }
}

(async () => {
  const stores = syncConfig.stores.filter((s) => process.env[s.envVar]);
  const failures = [];
  let grandBytes = 0;
  let grandDeadOffers = 0;
  let grandOldHistory = 0;

  for (const store of stores) {
    const client = createNeonClient(process.env[store.envVar], store.slug);
    try {
      // node-postgres does not connect on first query — it queues the query
      // for ever and the timeout fires. Every working script connects first.
      await client.connect();
      const stats = await tableStats(client);
      const offers = stats.find((s) => s.table === 'offers') || {};
      const history = stats.find((s) => s.table === 'price_history') || {};
      const dateCol = await historyDateColumn(client);
      grandBytes += Number(offers.bytes || 0) + Number(history.bytes || 0);

      if (MODE === 'apply') {
        const dead = await batchedDelete(client, 'offers',
          "is_available = false AND updated_at < now() - ($1 || ' days')::interval", [String(DEAD_GRACE_DAYS)]);
        let oldHist = 0;
        if (dateCol) {
          oldHist = await batchedDelete(client, 'price_history',
          dateCol + " < now() - ($1 || ' days')::interval", [String(HISTORY_RETENTION_DAYS)]);
        }
        await client.query('VACUUM (ANALYZE) offers');
        await client.query('VACUUM (ANALYZE) price_history');
        grandDeadOffers += dead;
        grandOldHistory += oldHist;
        console.log(store.slug.padEnd(24) +
          ' offers ~' + offers.est_rows + ' rows ' + (offers.bytes / 1048576).toFixed(1) + 'MB' +
          ' | history ~' + history.est_rows + ' rows ' + (history.bytes / 1048576).toFixed(1) + 'MB (' + (dateCol || 'sem coluna de data') + ')' +
          ' | APAGADOS offers:' + dead + ' history:' + oldHist + ' | vacuum ok');
      } else {
        console.log(store.slug.padEnd(24) +
          ' offers ~' + String(offers.est_rows).padStart(8) + ' rows ' + (offers.bytes / 1048576).toFixed(1).padStart(8) + 'MB' +
          ' | history ~' + String(history.est_rows).padStart(9) + ' rows ' + (history.bytes / 1048576).toFixed(1).padStart(8) + 'MB' +
          ' | data: ' + (dateCol || '?'));
      }
    } catch (e) {
      failures.push(store.slug);
      console.log(store.slug.padEnd(24) + ' ERRO: ' + String(e.message).slice(0, 70));
    } finally {
      try { await client.end(); } catch { /* best-effort */ }
    }
  }
  console.log('====');
  console.log('TOTAL armazenado: ' + (grandBytes / 1073741824).toFixed(2) + ' GiB em ' + stores.length + ' bancos');
  if (MODE === 'apply') console.log('TOTAL apagado — offers mortos: ' + grandDeadOffers + ' | history >' + HISTORY_RETENTION_DAYS + 'd: ' + grandOldHistory);
  console.log('falhas: ' + (failures.length ? failures.join(',') : 'nenhuma'));
  if (failures.length > stores.length / 2) process.exit(1);
})();

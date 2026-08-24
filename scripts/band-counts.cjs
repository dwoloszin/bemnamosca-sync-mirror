// One-off census: how many available offers sit in each price band, per
// store and in total. Runs HERE because this environment talks to all forty
// Neon databases in minutes — the same connectivity the sync uses — while
// ad-hoc queries from a home machine time out. Read-only; touches nothing.
const { createNeonClient, EFFECTIVE_PRICE_SQL } = require('./lib/neonSyncCore.cjs');
const syncConfig = require('./sync-config.cjs');

(async () => {
  const stores = syncConfig.stores.filter((s) => process.env[s.envVar]);
  const totals = { b500: 0, b300: 0, b200: 0, b100: 0, b0: 0 };
  const failures = [];
  for (const store of stores) {
    const client = createNeonClient(process.env[store.envVar], store.slug);
    try {
      const r = await client.query(`
        SELECT
          count(*) FILTER (WHERE eff >= 500)               AS b500,
          count(*) FILTER (WHERE eff >= 300 AND eff < 500) AS b300,
          count(*) FILTER (WHERE eff >= 200 AND eff < 300) AS b200,
          count(*) FILTER (WHERE eff >= 100 AND eff < 200) AS b100,
          count(*) FILTER (WHERE eff > 0    AND eff < 100) AS b0
        FROM (SELECT (${EFFECTIVE_PRICE_SQL}) AS eff FROM offers WHERE is_available = true) x`);
      const row = r.rows[0];
      for (const k of Object.keys(totals)) totals[k] += Number(row[k]) || 0;
      console.log(`${store.slug.padEnd(24)} >=500:${String(row.b500).padStart(6)}  300-500:${String(row.b300).padStart(6)}  200-300:${String(row.b200).padStart(6)}  100-200:${String(row.b100).padStart(6)}  <100:${String(row.b0).padStart(7)}`);
    } catch (e) {
      failures.push(store.slug);
      console.log(`${store.slug.padEnd(24)} ERRO: ${String(e.message).slice(0, 60)}`);
    } finally {
      try { await client.end(); } catch { /* best-effort */ }
    }
  }
  console.log('====');
  console.log(`TOTAIS pares(loja,produto)  >=500: ${totals.b500} | 300-500: ${totals.b300} | 200-300: ${totals.b200} | 100-200: ${totals.b100} | <100: ${totals.b0}`);
  console.log(`falhas: ${failures.length ? failures.join(',') : 'nenhuma'}`);
  if (failures.length > stores.length / 2) process.exit(1);
})();

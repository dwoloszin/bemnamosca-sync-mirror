// Census by the sync's own footsteps: the aggregate version timed out on
// every store — the offers tables carry far more dead rows than live ones,
// and a full-scan COUNT grinds for minutes on the small computes. The sync
// itself walks all forty stores in six minutes, paging live rows by the ean
// index; this does exactly that walk and tallies the price bands in JS.
const { createNeonClient, effectivePrice } = require('./lib/neonSyncCore.cjs');
const syncConfig = require('./sync-config.cjs');

const PAGE = 4000;

(async () => {
  const stores = syncConfig.stores.filter((s) => process.env[s.envVar]);
  const totals = { b500: 0, b300: 0, b200: 0, b100: 0, b0: 0 };
  const failures = [];
  for (const store of stores) {
    const client = createNeonClient(process.env[store.envVar], store.slug);
    const bands = { b500: 0, b300: 0, b200: 0, b100: 0, b0: 0 };
    try {
      await client.connect();
      let cursor = '';
      for (;;) {
        const { rows } = await client.query(
          `select ean, regular_price, promo_price, is_discounted
           from offers
           where is_available = true and ean is not null and ean <> '' and ean > $1
           order by ean asc limit $2`,
          [cursor, PAGE],
        );
        if (rows.length === 0) break;
        for (const row of rows) {
          const eff = Number(effectivePrice(row));
          if (!Number.isFinite(eff) || eff <= 0) continue;
          if (eff >= 500) bands.b500 += 1;
          else if (eff >= 300) bands.b300 += 1;
          else if (eff >= 200) bands.b200 += 1;
          else if (eff >= 100) bands.b100 += 1;
          else bands.b0 += 1;
        }
        cursor = rows[rows.length - 1].ean;
        if (rows.length < PAGE) break;
      }
      for (const k of Object.keys(totals)) totals[k] += bands[k];
      console.log(`${store.slug.padEnd(24)} >=500:${String(bands.b500).padStart(6)}  300-500:${String(bands.b300).padStart(6)}  200-300:${String(bands.b200).padStart(6)}  100-200:${String(bands.b100).padStart(6)}  <100:${String(bands.b0).padStart(7)}`);
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

#!/usr/bin/env node
/**
 * READ-ONLY. Quantifies the account_key over-merge blast radius from the owner
 * staging tables. A physical account has exactly one PRIMARY OWNER (code 'P'),
 * so any account_key (branch|type|last4) carrying >1 distinct primary-owner CIF
 * is proof that ≥2 distinct physical accounts were merged into one HubSpot
 * record (cross-associating unrelated owners).
 *
 *   railway run node scripts/quantify-collisions.js
 *
 * Reads DATABASE_URL from the environment (injected by `railway run`). Makes no
 * writes and touches no HubSpot data.
 */
const { Pool } = require('pg');

const TABLES = {
  dda:   'stg_deposit_owners',
  loans: 'stg_loan_owners',
  cd:    'stg_time_deposit_owners',
};

async function quantify(pool, tbl) {
  const summary = await pool.query(`
    WITH prim AS (
      SELECT account_key, cif_number
      FROM ${tbl}
      WHERE account_key IS NOT NULL
        AND UPPER(TRIM(relationship)) = 'P'
        AND cif_number IS NOT NULL AND cif_number <> ''
    ),
    collided AS (
      SELECT account_key, COUNT(DISTINCT cif_number) AS prim_owners
      FROM prim
      GROUP BY account_key
      HAVING COUNT(DISTINCT cif_number) > 1
    )
    SELECT
      (SELECT COUNT(*) FROM ${tbl})                                            AS owner_rows,
      (SELECT COUNT(DISTINCT account_key) FROM ${tbl} WHERE account_key IS NOT NULL) AS distinct_account_keys,
      (SELECT COUNT(*) FROM collided)                                          AS collision_keys,
      (SELECT COALESCE(SUM(prim_owners - 1), 0) FROM collided)                 AS extra_accounts_merged_away,
      (SELECT COALESCE(SUM(prim_owners), 0) FROM collided)                     AS customers_cross_linked
  `);

  const samples = await pool.query(`
    SELECT account_key,
           COUNT(DISTINCT cif_number) AS primary_owners,
           STRING_AGG(DISTINCT cif_number, ', ' ORDER BY cif_number) AS cifs
    FROM ${tbl}
    WHERE account_key IS NOT NULL
      AND UPPER(TRIM(relationship)) = 'P'
      AND cif_number IS NOT NULL AND cif_number <> ''
    GROUP BY account_key
    HAVING COUNT(DISTINCT cif_number) > 1
    ORDER BY COUNT(DISTINCT cif_number) DESC, account_key
    LIMIT 10
  `);

  return { summary: summary.rows[0], samples: samples.rows };
}

(async () => {
  const cs = process.env.DATABASE_URL;
  if (!cs) { console.error('DATABASE_URL not set (run via `railway run`).'); process.exit(2); }
  const pool = new Pool({ connectionString: cs, ssl: cs.includes('localhost') ? false : { rejectUnauthorized: false } });

  let grandKeys = 0, grandExtra = 0;
  for (const [src, tbl] of Object.entries(TABLES)) {
    let r;
    try { r = await quantify(pool, tbl); }
    catch (e) { console.log(`\n## ${src} (${tbl}) — query failed: ${e.message}`); continue; }
    const s = r.summary;
    grandKeys += Number(s.collision_keys);
    grandExtra += Number(s.extra_accounts_merged_away);
    console.log(`\n## ${src}  (${tbl})`);
    console.log(`  owner rows in staging        : ${s.owner_rows}`);
    console.log(`  distinct account_keys        : ${s.distinct_account_keys}`);
    console.log(`  COLLIDED keys (>1 primary)   : ${s.collision_keys}`);
    console.log(`  extra accounts merged away   : ${s.extra_accounts_merged_away}`);
    console.log(`  customers cross-linked       : ${s.customers_cross_linked}`);
    if (r.samples.length) {
      console.log(`  sample collided keys:`);
      for (const x of r.samples) console.log(`    ${x.account_key}  (${x.primary_owners} primaries: ${x.cifs})`);
    }
  }
  console.log(`\n=== TOTAL across DDA/Loans/CDs: ${grandKeys} collided account_keys, ~${grandExtra} distinct accounts merged into another. ===`);
  await pool.end();
  process.exit(0);
})().catch(e => { console.error(e.message); process.exit(1); });

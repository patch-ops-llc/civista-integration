#!/usr/bin/env node
/**
 * Lightweight post-backfill diagnostic. Queries the small bookkeeping tables
 * directly via the in-container pg pool (run with `railway ssh node
 * scripts/diag.js`) so it never hits the heavy HTTP aggregation endpoints that
 * time out on the full-size staging tables.
 *
 * Prints:
 *   - latest sync_log row per table_name (ship/skip/fail + association stats)
 *   - sync_errors grouped by error_type and severity
 *   - shipped_records ledger counts per source_table
 */
const { pool } = require('../db/init');

async function latestSyncLog() {
  // DISTINCT ON gives the most recent row per table_name.
  const { rows } = await pool.query(`
    SELECT DISTINCT ON (table_name)
      table_name, records_attempted, records_created, records_failed,
      records_skipped, completed_at, error_details
    FROM sync_log
    ORDER BY table_name, started_at DESC
  `);
  return rows;
}

async function errorBreakdown() {
  const byType = await pool.query(
    `SELECT error_type, COUNT(*)::int AS n FROM sync_errors GROUP BY error_type ORDER BY n DESC`
  );
  let bySev = { rows: [] };
  try {
    bySev = await pool.query(
      `SELECT severity, COUNT(*)::int AS n FROM sync_errors GROUP BY severity ORDER BY n DESC`
    );
  } catch { /* severity column may not exist in older schemas */ }
  return { byType: byType.rows, bySev: bySev.rows };
}

async function shippedCounts() {
  const { rows } = await pool.query(
    `SELECT source_table, COUNT(*)::int AS n FROM shipped_records GROUP BY source_table ORDER BY source_table`
  );
  return rows;
}

(async () => {
  console.log('=== latest sync_log per table ===');
  for (const r of await latestSyncLog()) {
    const det = r.error_details ? ` details=${JSON.stringify(r.error_details)}` : '';
    console.log(
      `${String(r.table_name).padEnd(22)} attempted=${r.records_attempted ?? 0} created=${r.records_created ?? 0} ` +
      `failed=${r.records_failed ?? 0} skipped=${r.records_skipped ?? 0} done=${r.completed_at ? new Date(r.completed_at).toISOString() : '-'}${det}`
    );
  }

  console.log('\n=== shipped_records ledger (per source_table) ===');
  for (const r of await shippedCounts()) {
    console.log(`${String(r.source_table).padEnd(22)} ${r.n}`);
  }

  console.log('\n=== sync_errors ===');
  const { byType, bySev } = await errorBreakdown();
  console.log('by_type:', JSON.stringify(byType));
  console.log('by_severity:', JSON.stringify(bySev));

  console.log('\n=== hard-failure samples (error severity, non-benign types) ===');
  const samples = await pool.query(`
    SELECT source_table, source_key, error_type, LEFT(error_message, 180) AS msg
    FROM sync_errors
    WHERE error_type IN ('hubspot_record','hubspot_batch_failed','classification','date_unparseable','validation')
    ORDER BY created_at DESC
    LIMIT 40
  `);
  for (const r of samples.rows) {
    console.log(`[${r.error_type}] ${r.source_table || ''} key=${r.source_key || '-'} :: ${r.msg}`);
  }

  process.exit(0);
})().catch(e => { console.error(e.message); process.exit(1); });

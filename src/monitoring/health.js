const { pool } = require('../../db/init');

/**
 * Health status. Surfaces:
 *   - database connectivity
 *   - last sync per table with shipped/error/skipped counts
 *   - per-run error counts (from sync_errors)
 *   - recent unresolved errors (last 20)
 *   - overall reconciled/unhealthy flag
 */
async function getHealthStatus() {
  const client = await pool.connect();
  try {
    // Only consider the 6 real staging-table sync runs for the overall
    // healthy/unhealthy verdict. Meta sync_log rows like 'cif:unclassified'
    // (created for CIF classification quarantines) carry records_failed > 0
    // by design — they record EXPECTED operator-visible quarantine samples
    // per UAT scenario 10. Letting those flip /health to "unhealthy" would
    // contradict scenario 3, which expects {"status":"healthy",...} as long
    // as the actual pipeline tables are processing cleanly.
    const lastSyncs = await client.query(`
      SELECT DISTINCT ON (table_name)
        id, table_name, started_at, completed_at,
        records_attempted, records_created, records_updated,
        records_failed, records_skipped, error_details, row_count
      FROM sync_log
      WHERE table_name IN (
        'stg_contacts', 'stg_companies', 'stg_deposits',
        'stg_loans', 'stg_time_deposits', 'stg_debit_cards'
      )
      ORDER BY table_name, started_at DESC
    `);

    const tables = {};
    let hasErrors = false;

    for (const row of lastSyncs.rows) {
      const errs = await client.query(
        `SELECT error_type, COUNT(*) as c FROM sync_errors WHERE run_id = $1 GROUP BY error_type`,
        [row.id]
      );
      const errCounts = {};
      let errTotal = 0;
      for (const e of errs.rows) {
        errCounts[e.error_type] = parseInt(e.c);
        errTotal += parseInt(e.c);
      }
      if (errTotal > 0 || row.records_failed > 0) hasErrors = true;

      tables[row.table_name] = {
        last_run_id: row.id,
        started_at: row.started_at,
        completed_at: row.completed_at,
        source_row_count: row.row_count,
        records_attempted: row.records_attempted,
        records_shipped: row.records_created,
        records_failed: row.records_failed,
        records_skipped: row.records_skipped,
        errors: { total: errTotal, by_type: errCounts },
        error_details: row.error_details,
      };
    }

    const recentErrors = await client.query(`
      SELECT id, run_id, source_table, source_key, error_type, error_message, created_at
      FROM sync_errors
      ORDER BY created_at DESC
      LIMIT 20
    `);

    const quarantineTotal = await client.query(
      `SELECT COUNT(*) as c FROM sync_errors WHERE error_type IN ('classification','validation','hubspot_record')`
    );

    return {
      status: hasErrors ? 'unhealthy' : 'healthy',
      database: 'connected',
      tables,
      quarantine_total: parseInt(quarantineTotal.rows[0].c),
      recent_errors: recentErrors.rows,
    };
  } catch (err) {
    return {
      status: 'unhealthy',
      database: 'disconnected',
      error: err.message,
    };
  } finally {
    client.release();
  }
}

module.exports = { getHealthStatus };

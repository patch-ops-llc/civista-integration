const { pool } = require('../../db/init');

// Heavy columns the diff result never needs: they are stripped before sending
// to HubSpot (see INTERNAL_COLUMNS in src/sync/hubspot.js) and are large JSONB
// blobs. Excluding them from the SELECT keeps the full-table pull (232k+ rows
// at production scale) from loading hundreds of MB of verbatim CSV/audit data
// into the Node heap. row_hash is intentionally kept — recordShipped needs it.
const DIFF_EXCLUDED_COLUMNS = new Set(['raw_csv', 'coercions', 'hubspot_verify_diff']);

// How many changed rows to pull from Postgres per page. The previous design
// SELECTed every changed row in one shot — at first-run production scale that
// is the whole table (252k+ deposits), and materializing all of them plus the
// per-row HubSpot input objects and the byKey Map was the heap blow-up that
// killed the big-table loads (Loans / Deposits / Contacts) while the small
// tables finished. Keyset pagination by `id` bounds memory to one page and is
// safe even as recordShipped writes the ledger mid-run: we only ever advance
// past ids we've already processed, so a freshly-shipped row never reappears
// on a later page. Override with DIFF_BATCH_SIZE for tuning on Railway.
const DIFF_BATCH_SIZE = parseInt(process.env.DIFF_BATCH_SIZE, 10) || 2000;

async function selectableColumns(stagingTable) {
  const r = await pool.query(
    `SELECT column_name FROM information_schema.columns
     WHERE table_name = $1 ORDER BY ordinal_position`,
    [stagingTable]
  );
  return r.rows
    .map(row => row.column_name)
    .filter(c => !DIFF_EXCLUDED_COLUMNS.has(c));
}

/**
 * Lightweight diff summary — counts only, no row materialization. Returns the
 * total staging row count and the rows whose key column is NULL/empty (which
 * can't be tracked and must be quarantined). The count of rows that actually
 * need syncing (`toSync`) is derived by the caller as it streams pages, so we
 * never run a second full anti-join COUNT or hold the changed set in memory.
 */
async function getDiffSummary(stagingTable, keyColumn) {
  const nullKey = await pool.query(
    `SELECT * FROM ${stagingTable} WHERE ${keyColumn} IS NULL OR ${keyColumn} = ''`
  );
  const total = await pool.query(`SELECT COUNT(*)::int AS c FROM ${stagingTable}`);
  return {
    total: total.rows[0].c,
    nullKeyRows: nullKey.rows,
  };
}

/**
 * Diff engine backed by the shipped_records ledger, streamed in keyset pages.
 *
 * For each row in staging, if (source_table, source_key, row_hash) already
 * exists in shipped_records, it has been sent and is unchanged — skip.
 * Otherwise it's new or changed — yield it for syncing.
 *
 * Pages are ordered by `id` and walked with `id > lastId`, so the generator is
 * resilient to recordShipped writes that happen between pages (the just-shipped
 * rows have ids <= lastId and are never revisited). Each yielded value is an
 * array of staging rows (at most batchSize).
 */
async function* iterateChangedRows(stagingTable, keyColumn, batchSize = DIFF_BATCH_SIZE) {
  const cols = await selectableColumns(stagingTable);
  if (!cols.includes('id')) {
    // Every staging table has a SERIAL id; if it's somehow absent we cannot
    // keyset-paginate safely. Fail loud rather than silently OFFSET-scan.
    throw new Error(`iterateChangedRows: ${stagingTable} has no 'id' column for keyset pagination`);
  }
  const colList = cols.map(c => `s."${c}"`).join(', ');
  let lastId = 0;
  for (;;) {
    const sql = `
      SELECT ${colList}
      FROM ${stagingTable} s
      WHERE s.id > $2
        AND s.${keyColumn} IS NOT NULL
        AND s.${keyColumn} <> ''
        AND NOT EXISTS (
          SELECT 1
          FROM shipped_records sr
          WHERE sr.source_table = $1
            AND sr.source_key = s.${keyColumn}
            AND sr.row_hash = s.row_hash
        )
      ORDER BY s.id
      LIMIT $3
    `;
    const page = await pool.query(sql, [stagingTable, lastId, batchSize]);
    if (page.rows.length === 0) break;
    lastId = page.rows[page.rows.length - 1].id;
    yield page.rows;
    if (page.rows.length < batchSize) break;
  }
}

/**
 * Convenience wrapper preserving the old single-shot contract for callers that
 * still want the full changed set in memory (tests, ad-hoc scripts). The
 * orchestrator no longer uses this — it streams via iterateChangedRows — so at
 * production scale the heavy path stays bounded.
 */
async function getChangedRows(stagingTable, keyColumn) {
  const { total, nullKeyRows } = await getDiffSummary(stagingTable, keyColumn);
  const toSync = [];
  for await (const page of iterateChangedRows(stagingTable, keyColumn)) {
    for (const r of page) toSync.push(r);
  }
  const skipped = total - toSync.length - nullKeyRows.length;
  return { toSync, skipped, nullKeyRows, total };
}

/**
 * Record that a batch of rows was successfully shipped to HubSpot.
 * Takes succeeded entries from batchUpsert: [{ sourceKey, hubspotId, wasNew }]
 * and the matching source row data (for the row_hash).
 */
async function recordShipped(sourceTable, successes, sourceRowsByKey) {
  if (!successes || successes.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const { sourceKey, hubspotId } of successes) {
      const row = sourceRowsByKey.get(sourceKey);
      if (!row) continue;
      await client.query(
        `INSERT INTO shipped_records (source_table, source_key, row_hash, hubspot_id, shipped_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (source_table, source_key)
         DO UPDATE SET row_hash = EXCLUDED.row_hash,
                       hubspot_id = EXCLUDED.hubspot_id,
                       shipped_at = NOW()`,
        [sourceTable, sourceKey, row.row_hash, hubspotId]
      );
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  getChangedRows,
  getDiffSummary,
  iterateChangedRows,
  recordShipped,
  DIFF_BATCH_SIZE,
};

const fs = require('fs');
const path = require('path');
const { pool } = require('../../db/init');
const { parseAndStage, verifyDbPersistence } = require('../ingestion/csv-parser');
const { checkCircuitBreaker } = require('../ingestion/circuit-breaker');
const { getChangedRows, recordShipped } = require('../ingestion/diff-engine');
const { recordErrorBatch, ERROR_TYPES } = require('../monitoring/errors');
const loud = require('../monitoring/loud');
const { TABLES } = require('../transform/hubspot-mapping');
const {
  syncContacts,
  syncCompanies,
  syncDeposits,
  syncLoans,
  syncTimeDeposits,
  syncDebitCards,
} = require('./hubspot');

// CSV filename → logical source key used by parseAndStage / TABLES.
const FILE_SOURCE_MAP = {
  'HubSpot_CIF.csv': 'cif',
  'HubSpot_DDA.csv': 'dda',
  'HubSpot_Loan.csv': 'loans',
  'HubSpot_CD.csv': 'cd',
  'HubSpot_Debit_Card.csv': 'debit_cards',
};

// Accept any filename that begins with the canonical stem and ends with .csv.
// This intentionally covers operator/brief variants like:
//   - canonical:               "HubSpot_CIF.csv"
//   - macOS Finder dedup:      "HubSpot_CIF 1.csv"
//   - Windows Explorer dedup:  "HubSpot_CIF (1).csv"
//   - date-stamped delivery:   "HubSpot_CIF_20260508.csv" / "HubSpot_CIF-20260508.csv"
//   - UAT brief naming:        "HubSpot_CIFTRUNC.csv"
//   - operator backups/variants: "HubSpot_CIF_backup.csv", "HubSpot_CIF_v2.csv"
// We rely on the CSV header validator (validateHeaders in csv-parser.js) to
// reject files whose content doesn't match the expected schema for that
// source — filename strictness was a flimsy guard and caused real UAT
// failures when operators dropped files with reasonable variant names.
//
// Ordering note: canonical stems do not overlap as prefixes of each other
// (CIF / DDA / Loan / CD / Debit_Card), so prefix-based matching is
// unambiguous between sources. If a future source added one that did overlap
// (e.g. a "HubSpot_CDx" sibling alongside "HubSpot_CD"), this regex would
// need a terminator like (?![A-Za-z]) — not needed today.
const ACCEPTED_SUFFIX_DESCRIPTION =
  'any HubSpot_<source>*.csv (canonical, dedup suffix, date stamp, TRUNC, _v2, _backup, ...)';

function buildAcceptPattern(canonical) {
  const stem = canonical.replace(/\.csv$/, '');
  const escaped = stem.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`^${escaped}.*\\.csv$`, 'i');
}

// Resolve, for each canonical source, the single file in incomingDir that
// should be processed this run. When multiple candidates match (e.g. operator
// dropped both "HubSpot_CIF.csv" and "HubSpot_CIF 1.csv"), we pick the most
// recent by mtime and return the rest as duplicates for the caller to
// quarantine — silently dropping a stale copy is a data-integrity hazard.
function resolveSourceFiles(incomingDir, files, syncOrder) {
  const resolved = {};
  const duplicates = [];
  for (const canonical of syncOrder) {
    const pattern = buildAcceptPattern(canonical);
    const matches = files
      .filter(f => pattern.test(f))
      .map(name => {
        const fullPath = path.join(incomingDir, name);
        let mtime = 0;
        try { mtime = fs.statSync(fullPath).mtimeMs; } catch { /* deleted between readdir and stat */ }
        return { name, fullPath, mtime };
      })
      .sort((a, b) => b.mtime - a.mtime);
    if (matches.length === 0) continue;
    resolved[canonical] = matches[0];
    for (let i = 1; i < matches.length; i++) {
      duplicates.push({ canonical, ...matches[i] });
    }
  }
  return { resolved, duplicates };
}

// For each staging table, which HubSpot sync function, which column is the
// unique id, and which source CSV the rows came from (for loud-event context).
const STAGING_SYNC = {
  stg_contacts:      { syncFn: syncContacts,     keyColumn: 'cif_number',    objectLabel: 'contacts',      sourceCsv: 'HubSpot_CIF.csv' },
  stg_companies:     { syncFn: syncCompanies,    keyColumn: 'cif_number',    objectLabel: 'companies',     sourceCsv: 'HubSpot_CIF.csv' },
  stg_deposits:      { syncFn: syncDeposits,     keyColumn: 'primary_key',   objectLabel: 'deposits',      sourceCsv: 'HubSpot_DDA.csv' },
  stg_loans:         { syncFn: syncLoans,        keyColumn: 'primary_key',   objectLabel: 'loans',         sourceCsv: 'HubSpot_Loan.csv' },
  stg_time_deposits: { syncFn: syncTimeDeposits, keyColumn: 'primary_key',   objectLabel: 'time_deposits', sourceCsv: 'HubSpot_CD.csv' },
  stg_debit_cards:   { syncFn: syncDebitCards,   keyColumn: 'composite_key', objectLabel: 'debit_cards',   sourceCsv: 'HubSpot_Debit_Card.csv' },
};

async function createSyncLog(tableName, rowCount, fileHash) {
  const result = await pool.query(
    `INSERT INTO sync_log (table_name, started_at, row_count, file_hash)
     VALUES ($1, NOW(), $2, $3) RETURNING id`,
    [tableName, rowCount, fileHash]
  );
  return result.rows[0].id;
}

async function updateSyncLog(logId, updates) {
  const sets = [];
  const values = [logId];
  let paramIdx = 2;
  for (const [key, val] of Object.entries(updates)) {
    sets.push(`${key} = $${paramIdx}`);
    values.push(key === 'error_details' ? JSON.stringify(val) : val);
    paramIdx++;
  }
  sets.push(`completed_at = NOW()`);
  await pool.query(`UPDATE sync_log SET ${sets.join(', ')} WHERE id = $1`, values);
}

/**
 * Sync one staging table end-to-end (diff → HubSpot → ledger).
 * Assumes staging is already populated by parseAndStage.
 */
async function syncStagingTable(stagingTable, runId) {
  const { syncFn, keyColumn, objectLabel, sourceCsv } = STAGING_SYNC[stagingTable];

  const { toSync, skipped, nullKeyRows, total } = await getChangedRows(stagingTable, keyColumn);
  console.log(`${stagingTable}: total=${total}, to_sync=${toSync.length}, unchanged=${skipped}, null_key=${nullKeyRows.length}`);

  // Null-key rows can't be upserted to HubSpot — quarantine to sync_errors.
  if (nullKeyRows.length > 0) {
    await recordErrorBatch(nullKeyRows.map(r => ({
      runId,
      sourceTable: stagingTable,
      errorType: ERROR_TYPES.VALIDATION,
      errorMessage: `Missing key column (${keyColumn}) — cannot upsert to HubSpot`,
      recordSnapshot: r,
    })));
  }

  let totalShipped = 0, totalFailed = 0, totalInvalid = 0;

  if (toSync.length > 0) {
    // Index rows by key so we can write the row_hash into shipped_records after a successful send.
    const byKey = new Map();
    for (const r of toSync) byKey.set(r[keyColumn], r);

    const { succeeded, failed, invalidInputs } = await syncFn(toSync, {
      sourceTable: stagingTable,
      sourceCsv,
      runId,
    });

    await recordShipped(stagingTable, succeeded, byKey);
    totalShipped = succeeded.length;
    totalFailed = failed.length;
    totalInvalid = invalidInputs.length;

    if (invalidInputs.length > 0) {
      await recordErrorBatch(invalidInputs.map(i => ({
        runId,
        sourceTable: stagingTable,
        errorType: ERROR_TYPES.VALIDATION,
        errorMessage: `[${objectLabel}] ${i.reason}`,
        recordSnapshot: i.input,
      })));
    }
    if (failed.length > 0) {
      await recordErrorBatch(failed.map(f => ({
        runId,
        sourceTable: stagingTable,
        sourceKey: f.sourceKey,
        errorType: ERROR_TYPES.HUBSPOT_RECORD,
        errorMessage: `[${objectLabel}] ${f.reason}`,
      })));
    }
  }

  const quarantineCount = nullKeyRows.length + totalInvalid + totalFailed;
  const reconciled = (totalShipped + skipped + quarantineCount) === total;

  return { stagingTable, total, totalShipped, skipped, totalFailed, totalInvalid, nullKeyCount: nullKeyRows.length, quarantineCount, reconciled };
}

/**
 * Sync one CSV file end-to-end. For CIF this produces contacts + companies
 * and returns an array of sub-reports; for other sources, a single-element array.
 */
async function syncFile(source, filePath) {
  const sourceLabel = source === 'cif' ? 'CIF→(contacts+companies)' : TABLES[source].staging;
  console.log(`\n========== Processing ${path.basename(filePath)} (${sourceLabel}) ==========`);

  // Circuit breaker runs against the source-level row count (the CSV).
  const { rowCount, fileHash, byTable, unclassified, sinceTs } = await parseAndStage(filePath, source);

  // HASH B verification: scope to rows inserted in THIS parse (loaded_at
  // >= sinceTs) so overlapping /sync invocations don't race on each
  // other's pending rows.
  for (const stagingTable of Object.keys(byTable)) {
    const v = await verifyDbPersistence(stagingTable, sinceTs);
    console.log(`HASH B verify ${stagingTable}: ${v.ok}/${v.total} ok, ${v.mismatch} mismatch, ${v.legacy} legacy`);
  }

  const cbResult = await checkCircuitBreaker(source, rowCount);
  const results = [];

  if (!cbResult.safe) {
    // Record one run for the source-as-whole, then loud-alarm.
    const runId = await createSyncLog(source, rowCount, fileHash);
    await loud.alarm({
      event: 'circuit_breaker',
      message: `${source}: ${cbResult.reason}. Sync halted; file will be quarantined.`,
      runId,
      sourceTable: source,
      context: { previousCount: cbResult.previousCount, currentCount: cbResult.currentCount },
    });
    await updateSyncLog(runId, {
      records_attempted: 0, records_skipped: rowCount,
      error_details: { circuit_breaker: cbResult.reason },
    });
    results.push({
      source, runId, sourceRowCount: rowCount, shippedCount: 0, errorCount: 1,
      skippedUnchanged: 0, quarantineCount: rowCount, reconciled: false,
      skipped: true, reason: cbResult.reason,
    });
    return results;
  }

  // For CIF only: record classification misses now that staging is loaded.
  if (source === 'cif' && unclassified.length > 0) {
    // Create an ambient run_id for classification errors (one per source run).
    const runId = await createSyncLog(`${source}:unclassified`, unclassified.length, fileHash);
    await loud.warn({
      event: 'unclassified_cif',
      message: `${unclassified.length} CIF rows could not be classified as contact or company (likely NULL TaxIdType or partial name data)`,
      runId,
      sourceTable: 'stg_cif',
      context: { sample: unclassified.slice(0, 3).map(r => ({ CIFNum: r.CIFNum, TaxIdType: r.TaxIdType, FirstName: r.FirstName, LastName: r.LastName })) },
    });
    await recordErrorBatch(unclassified.map(r => ({
      runId,
      sourceTable: 'stg_cif',
      sourceKey: r.CIFNum || null,
      errorType: ERROR_TYPES.CLASSIFICATION,
      errorMessage: 'CIF row could not be classified as contact or company',
      recordSnapshot: r,
    })));
    await updateSyncLog(runId, {
      records_attempted: unclassified.length,
      records_failed: unclassified.length,
      error_details: { unclassified_count: unclassified.length },
    });
  }

  // For each staging table touched by the parse, diff + sync.
  for (const stagingTable of Object.keys(byTable)) {
    const runId = await createSyncLog(stagingTable, byTable[stagingTable], fileHash);
    try {
      const r = await syncStagingTable(stagingTable, runId);
      await updateSyncLog(runId, {
        records_attempted: r.totalShipped + r.totalFailed + r.totalInvalid,
        records_created: r.totalShipped,
        records_failed: r.totalFailed + r.totalInvalid,
        records_skipped: r.skipped,
        error_details: r.reconciled ? null : { reconciliation_mismatch: true, ...r },
      });
      console.log(`${stagingTable}: shipped=${r.totalShipped} skipped=${r.skipped} quarantined=${r.quarantineCount} reconciled=${r.reconciled}`);
      results.push({
        source, stagingTable, runId,
        sourceRowCount: byTable[stagingTable],
        shippedCount: r.totalShipped,
        errorCount: r.quarantineCount,
        skippedUnchanged: r.skipped,
        quarantineCount: r.quarantineCount,
        reconciled: r.reconciled,
      });
    } catch (err) {
      console.error(`Error syncing ${stagingTable}: ${err.message}`);
      await recordErrorBatch([{
        runId, sourceTable: stagingTable, errorType: ERROR_TYPES.INFRA,
        errorMessage: err.message, recordSnapshot: { stack: err.stack },
      }]);
      await updateSyncLog(runId, {
        records_attempted: 0,
        records_failed: byTable[stagingTable],
        error_details: { error: err.message },
      });
      results.push({
        source, stagingTable, runId,
        sourceRowCount: byTable[stagingTable],
        shippedCount: 0, errorCount: 1,
        skippedUnchanged: 0, quarantineCount: 0,
        reconciled: false, error: err.message,
      });
    }
  }

  return results;
}

async function archiveFile(filePath, archiveDir) {
  const date = new Date().toISOString().split('T')[0];
  const dest = path.join(archiveDir, date);
  fs.mkdirSync(dest, { recursive: true });
  const filename = path.basename(filePath);
  fs.renameSync(filePath, path.join(dest, filename));
  console.log(`Archived ${filename} → ${dest}/`);
}

async function quarantineFile(filePath, quarantineDir, result) {
  const date = new Date().toISOString().split('T')[0];
  const dest = path.join(quarantineDir, date);
  fs.mkdirSync(dest, { recursive: true });
  const filename = path.basename(filePath);
  const destPath = path.join(dest, filename);
  fs.renameSync(filePath, destPath);
  fs.writeFileSync(`${destPath}.error.json`, JSON.stringify({ quarantinedAt: new Date().toISOString(), result }, null, 2));
  console.warn(`QUARANTINED ${filename} → ${dest}/ (see .error.json)`);
}

async function runFullSync(incomingDir, archiveDir, quarantineDir) {
  const header = `Starting full sync at ${new Date().toISOString()}`;
  const bar = '='.repeat(header.length);
  console.log(`\n${bar}`);
  console.log(header);
  console.log(bar);

  if (!quarantineDir) quarantineDir = path.join(path.dirname(archiveDir), 'quarantine');

  const results = [];

  if (!fs.existsSync(incomingDir)) {
    console.log(`No incoming directory found at ${incomingDir}`);
    return { runs: results, reconciled: true };
  }

  const files = fs.readdirSync(incomingDir).filter(f => f.endsWith('.csv'));
  if (files.length === 0) {
    console.log('No CSV files found in incoming directory');
    return { runs: results, reconciled: true };
  }

  const syncOrder = [
    'HubSpot_CIF.csv', 'HubSpot_DDA.csv', 'HubSpot_Loan.csv',
    'HubSpot_CD.csv', 'HubSpot_Debit_Card.csv',
  ];

  const { resolved, duplicates } = resolveSourceFiles(incomingDir, files, syncOrder);

  // Quarantine duplicate matches before processing so a stale copy can't be
  // re-picked next run. The chosen-most-recent file proceeds normally.
  if (duplicates.length > 0) {
    fs.mkdirSync(quarantineDir, { recursive: true });
    for (const dup of duplicates) {
      await loud.warn({
        event: 'duplicate_source_csv_resolved',
        message: `Multiple files matched ${dup.canonical}. Picked most recent (${resolved[dup.canonical].name}); quarantining ${dup.name}.`,
        context: { canonical: dup.canonical, picked: resolved[dup.canonical].name, quarantined: dup.name, mtime: new Date(dup.mtime).toISOString() },
      });
      try {
        fs.renameSync(dup.fullPath, path.join(quarantineDir, dup.name));
      } catch (e) {
        console.warn(`Failed to quarantine duplicate ${dup.name}: ${e.message}`);
      }
    }
  }

  for (const filename of syncOrder) {
    const match = resolved[filename];
    if (!match) {
      await loud.warn({
        event: 'missing_csv',
        message: `Expected nightly file missing: ${filename}. Skipping this source for this run.`,
        context: { incomingDir, expected: filename, accepted: ACCEPTED_SUFFIX_DESCRIPTION, found: files },
      });
      continue;
    }

    const source = FILE_SOURCE_MAP[filename];
    if (!source) continue;

    const filePath = match.fullPath;
    const fileResults = await syncFile(source, filePath);
    results.push(...fileResults);

    const anyBad = fileResults.some(r => r.error || r.skipped || !r.reconciled);
    if (anyBad) {
      await quarantineFile(filePath, quarantineDir, fileResults);
    } else {
      await archiveFile(filePath, archiveDir);
    }
  }

  const allReconciled = results.every(r => r.reconciled);
  console.log(`\nSync complete. ${results.length} staging-table syncs processed. All reconciled: ${allReconciled}`);
  return { runs: results, reconciled: allReconciled };
}

module.exports = { runFullSync, syncFile, FILE_SOURCE_MAP };

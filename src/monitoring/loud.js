/**
 * Loud-warnings module — single funnel for every noteworthy event in the
 * pipeline. The contract: if it doesn't go through here, it doesn't get
 * persisted, and it won't show in the UI.
 *
 * For each call, three things happen:
 *   1. console banner (warn or error) — the SSE hook in event-stream.js
 *      forwards stdout to the UI log panel, so the operator sees it live.
 *   2. INSERT into sync_errors with severity + error_type so it survives
 *      across sessions and is queryable from /api/issues.
 *   3. (For mapping issues only) UPSERT into mapping_issues so the UI's
 *      Data Issues panel can keep a persistent counter.
 *
 * Per memory financial_data_rules.md: "ALL loud failures must go to the UI;
 * ALL failures must be very loud." Use this module for every such event.
 */

const { recordError } = require('./errors');
const { drawBox } = require('./box');

function banner(severity, event, message, context) {
  const log = severity === 'error' ? console.error : console.warn;
  const tag = severity === 'error' ? '✘ ALARM' : '⚠ WARN';
  const lines = [`${tag}  ${event}`, message];
  if (context && Object.keys(context).length > 0) {
    lines.push(`context: ${JSON.stringify(context)}`);
  }
  for (const out of drawBox(lines)) log(out);
}

async function persist({ severity, event, message, runId, sourceTable, sourceKey, context }) {
  try {
    // Single INSERT with the right severity. The previous two-statement
    // INSERT-then-UPDATE-by-message approach was racy under concurrent
    // identical events (e.g. parallel emit from multiple staging tables).
    await recordError({
      runId: runId || null,
      sourceTable: sourceTable || null,
      sourceKey: sourceKey || null,
      errorType: event,
      errorMessage: message,
      recordSnapshot: context || null,
      severity,
    });
  } catch (e) {
    // We don't want loud.* itself to throw — that would mask the original
    // event. But we shouldn't be silent either, so log to stderr directly.
    console.error(`loud.${severity}() failed to persist: ${e.message || e.code || e}`);
  }
}

/**
 * Emit a warning. Use for recoverable issues, held mappings, missing optional
 * inputs, anything the operator should see but that doesn't block the pipeline.
 */
async function warn({ event, message, runId, sourceTable, sourceKey, context }) {
  banner('warning', event, message, context);
  await persist({ severity: 'warning', event, message, runId, sourceTable, sourceKey, context });
}

/**
 * Emit an error. Use for blocking issues, hash mismatches, HubSpot rejections,
 * anything that means data didn't make it through correctly.
 */
async function alarm({ event, message, runId, sourceTable, sourceKey, context }) {
  banner('error', event, message, context);
  await persist({ severity: 'error', event, message, runId, sourceTable, sourceKey, context });
}

/**
 * Record (or increment count of) a persistent mapping issue. Use when a
 * CSV->HubSpot mapping is held (send:false) or when we discover a column
 * whose source data doesn't conform to its HubSpot property type.
 */
async function mappingIssue({ sourceCsv, sourceColumn, hsObject, hsProperty, hsType, problem, sampleValue, rowsAffected }) {
  const { pool } = require('../../db/init');
  // Only update last_seen if this call actually contributes new data
  // (rowsAffected > 0 OR a new sample). Boot-time reseeds with rowsAffected=0
  // shouldn't reorder the UI's "newest issues" list on every redeploy.
  const inc = rowsAffected || 0;
  const hasNewSample = !!sampleValue;
  const shouldBumpLastSeen = inc > 0 || hasNewSample;
  await pool.query(
    `INSERT INTO mapping_issues
       (source_csv, source_column, hs_object, hs_property, hs_type, problem, sample_value, rows_affected, first_seen, last_seen)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW())
     ON CONFLICT (source_csv, source_column, hs_property)
     DO UPDATE SET
       problem       = EXCLUDED.problem,
       sample_value  = COALESCE(EXCLUDED.sample_value, mapping_issues.sample_value),
       rows_affected = mapping_issues.rows_affected + EXCLUDED.rows_affected,
       hs_type       = EXCLUDED.hs_type,
       last_seen     = CASE WHEN $9 THEN NOW() ELSE mapping_issues.last_seen END`,
    [sourceCsv, sourceColumn, hsObject, hsProperty, hsType, problem, sampleValue || null, inc, shouldBumpLastSeen]
  );
  // Banner only when there's new info — don't spam the live log on each
  // boot.
  if (shouldBumpLastSeen) {
    banner('warning', 'mapping_held',
      `${sourceCsv} · ${sourceColumn} → ${hsProperty} (${hsType}) · ${problem}`,
      { sample: sampleValue, rows: inc });
  }
}

module.exports = { warn, alarm, mappingIssue };

const crypto = require('crypto');
const {
  TABLES,
  CIF_CONTACT_FIELDS,
  CIF_COMPANY_FIELDS,
} = require('../transform/hubspot-mapping');
const {
  normalizeBoolean,
  normalizeDeceased,
  normalizeNumber,
  normalizeDate,
  normalizeEmail,
  coerceDateForHubSpot,
  coerceEmailForHubSpot,
  coerceZip,
} = require('../transform/normalize');
const loud = require('../monitoring/loud');
const { emitHubspot } = require('../monitoring/event-stream');
const { pool } = require('../../db/init');

const HUBSPOT_API_BASE = 'https://api.hubapi.com';
const BATCH_SIZE = 100;
const MAX_RETRIES = 10;
const INITIAL_DELAY = 100;

// Internal columns that must be stripped before sending to HubSpot.
const INTERNAL_COLUMNS = new Set([
  'id', 'row_hash', 'loaded_at', 'synced_at',
  'raw_csv', 'db_persist_hash', 'db_verified_at',
  'hubspot_persist_hash', 'hubspot_verified_at',
  'hubspot_verify_diff', 'needs_review',
]);

// Custom-object types that have a required `name` property.
// Per HubSpot schema: requiredProperties=['name'], primaryDisplayProperty='name'.
const CUSTOM_OBJECTS_REQUIRING_NAME = new Set([
  '2-60442978', // deposits
  '2-60442977', // loans
  '2-60442980', // time_deposits
  '2-60442979', // debit_cards
]);

async function hubspotFetch(path, options = {}) {
  const url = `${HUBSPOT_API_BASE}${path}`;
  const method = (options.method || 'GET').toUpperCase();
  const headers = {
    'Authorization': `Bearer ${process.env.HUBSPOT_API_KEY}`,
    'Content-Type': 'application/json',
    ...options.headers,
  };

  // Inspect the body shape so the wire-log can surface useful detail
  // (batch size, idProperty) without leaking PII into the UI feed.
  let bodyMeta = null;
  if (options.body) {
    try {
      const b = JSON.parse(options.body);
      bodyMeta = {
        inputs: Array.isArray(b.inputs) ? b.inputs.length : undefined,
        idProperty: b.idProperty,
        properties: Array.isArray(b.properties) ? b.properties.length : undefined,
        limit: b.limit,
      };
    } catch { /* non-JSON body */ }
  }

  let delay = INITIAL_DELAY;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const startedAt = Date.now();
    const res = await fetch(url, { ...options, headers });

    if (res.status === 429) {
      if (attempt === MAX_RETRIES) {
        const retryBody = await res.text().catch(() => '');
        emitHubspot({ method, path, status: 429, durationMs: Date.now() - startedAt, attempt, body: bodyMeta, note: 'rate limited — gave up' });
        throw new Error(`Rate limited after ${MAX_RETRIES} retries on ${path}: ${retryBody}`);
      }
      emitHubspot({ method, path, status: 429, durationMs: Date.now() - startedAt, attempt, body: bodyMeta, note: `backoff ${delay}ms` });
      console.log(`Rate limited on ${path}, retrying in ${delay}ms (attempt ${attempt + 1})`);
      await new Promise(r => setTimeout(r, delay));
      delay *= 2;
      continue;
    }

    const text = await res.text();
    let parsed;
    try { parsed = JSON.parse(text); } catch { parsed = { raw: text }; }

    // Wire-log: every completed call surfaces here with timing + sizes so
    // the operator can demonstrate to the client exactly what happened.
    const responseMeta = {
      results: Array.isArray(parsed?.results) ? parsed.results.length : undefined,
      total: parsed?.total,
      message: parsed?.message,
    };
    emitHubspot({
      method, path, status: res.status, durationMs: Date.now() - startedAt,
      attempt, body: bodyMeta, response: responseMeta,
    });

    return { status: res.status, ok: res.ok, body: parsed };
  }
}

/**
 * Special-case the Deceased flag — source uses a literal space character
 * for "alive," which normalizeBoolean would treat as false (correct) but
 * we keep the dedicated function to preserve the original CLAUDE.md rule.
 */
function coerceDeceased(value) {
  return normalizeDeceased(String(value ?? ''));
}

/**
 * Build a HubSpot-ready payload from a staging row using the mapping.
 *
 * Per memory financial_data_rules.md (Rule 3): coercion is opt-in per
 * property via the `coerce` field. Default = no coercion = trim and send
 * the raw value. If the value doesn't match HubSpot's expected type,
 * HubSpot will reject the record and we surface the rejection loudly.
 *
 * Returns { props, coercions, suspectKeys } where:
 *   props        — what gets sent to HubSpot
 *   coercions    — audit array: [{ prop, csv, from, to, coerce }]
 *                  written to staging row's `coercions` JSONB column
 *   suspectKeys  — set of prop names where coerce_email flagged a bad value;
 *                  caller uses this to isolate the row into a 1-row batch
 *   problems     — non-coerce-related per-row problems (date_only could not
 *                  parse, etc.) for loud surfacing
 */
function buildPayload(row, fields) {
  const props = {};
  const coercions = [];
  const suspectKeys = new Set();
  const problems = [];

  for (const f of fields) {
    const { csv, prop, type } = f;
    const raw = row[prop];

    // Held mapping (`send: false`). Field is preserved in raw_csv per
    // rule 3 but not transmitted. Surfaced once in mapping_issues at boot.
    if (f.send === false) continue;

    // Empty / null → omit from payload entirely (HubSpot treats absent props as no-change).
    if (raw === null || raw === undefined || raw === '') continue;

    // Special non-opt-in coercions retained from the source-data CLAUDE.md edge cases.
    if (prop === 'deceased_flag_yn') {
      const v = coerceDeceased(raw);
      props[prop] = v;
      // Always audit because we transformed the value.
      coercions.push({ prop, csv, from: raw, to: v, coerce: 'deceased_flag_special' });
      continue;
    }

    // Opt-in coercions per the mapping's `coerce` flag.
    if (f.coerce === 'yn_to_bool') {
      const v = normalizeBoolean(String(raw));
      props[prop] = v;
      coercions.push({ prop, csv, from: raw, to: v, coerce: 'yn_to_bool' });
      continue;
    }
    if (f.coerce === 'date_only') {
      const r = coerceDateForHubSpot(raw);
      if (r.problem) {
        problems.push({ csv, prop, raw, problem: r.problem });
        continue; // unparseable → omit; raw still in raw_csv
      }
      if (r.value !== null && r.value !== undefined && r.value !== raw) {
        coercions.push({ prop, csv, from: raw, to: r.value, coerce: 'date_only' });
      }
      props[prop] = r.value;
      continue;
    }
    if (f.coerce === 'zip_format') {
      const r = coerceZip(raw);
      if (r.value === null) continue; // empty after trim → omit
      if (r.value !== raw) {
        coercions.push({ prop, csv, from: raw, to: r.value, coerce: 'zip_format' });
      }
      props[prop] = r.value;
      continue;
    }
    if (f.coerce === 'email_strict') {
      const r = coerceEmailForHubSpot(raw);
      if (r.problem) {
        // Unparseable email → OMIT the property (raw still preserved in raw_csv).
        // HubSpot batch upsert is all-or-nothing: sending a known-invalid value
        // (e.g. "No Email", "n/a", a bare domain) gets the whole 100-row batch
        // rejected with INVALID_EMAIL, destroying 99 valid records with it.
        // Mirrors the date_only handling above; the suspect_email warning still
        // fires for data-quality visibility.
        problems.push({ csv, prop, raw, problem: r.problem });
        continue; // omit; raw still in raw_csv
      }
      if (r.value !== raw) {
        coercions.push({ prop, csv, from: raw, to: r.value, coerce: 'email_strict' });
      }
      props[prop] = r.value;
      continue;
    }

    // No coerce opt-in → trim string, leave numbers/bools/etc as their string form.
    // HubSpot will validate and reject if needed; rejections surface loudly.
    const trimmed = typeof raw === 'string' ? raw.trim() : String(raw);
    if (trimmed === '') continue;
    if (trimmed !== raw) {
      coercions.push({ prop, csv, from: raw, to: trimmed, coerce: 'trim' });
    }
    props[prop] = trimmed;
  }

  return { props, coercions, suspectKeys, problems };
}

/**
 * Validate that every input has its unique idProperty value. Anything
 * missing is caught BEFORE we call HubSpot and returned as invalidInputs
 * so the caller can quarantine it.
 */
function validateBatchInputs(inputs, idProperty) {
  const valid = [];
  const invalid = [];
  for (const input of inputs) {
    const val = input?.properties?.[idProperty];
    if (val === undefined || val === null || val === '') {
      invalid.push({ reason: `Missing idProperty value (${idProperty})`, input });
    } else {
      valid.push(input);
    }
  }
  return { valid, invalid };
}

/**
 * Determine which propertie(s) HubSpot rejected on a failed single-record
 * upsert, so the caller can strip just those and retry instead of dropping the
 * whole record. Returns a Set of property names.
 *
 *  - 409 (CONFLICT): contact email-uniqueness collision — the offending
 *    property is always `email`.
 *  - 400 (VALIDATION_ERROR): HubSpot embeds the offending property name(s) in
 *    the message string as `"name":"<prop>"` (e.g. INVALID_EMAIL on `email`,
 *    INVALID_OPTION on `tax_id_type`). Some responses also expose a structured
 *    `errors` array. Parse both.
 */
function collectOffendingProps(status, body, only) {
  const props = new Set();

  if (status === 409 && only?.properties?.email !== undefined) {
    props.add('email');
  }

  const msg = body?.message || body?.raw || '';
  if (typeof msg === 'string') {
    const re = /"name"\s*:\s*"([^"]+)"/g;
    let m;
    while ((m = re.exec(msg)) !== null) props.add(m[1]);
  }
  if (Array.isArray(body?.errors)) {
    for (const e of body.errors) if (e && e.name) props.add(e.name);
  }

  return props;
}

/**
 * Batch upsert. Returns per-input outcome:
 *   succeeded: [{ sourceKey, hubspotId, wasNew }]
 *   failed:    [{ sourceKey, reason }]
 * Any input whose idProperty value isn't echoed back in the response is
 * treated as failed — we never silently assume success.
 */
async function batchUpsert(objectType, idProperty, inputs, opts = {}) {
  const { runId = null, sourceTable = null, batchSize = BATCH_SIZE } = opts;
  const succeeded = [];
  const failed = [];

  for (let i = 0; i < inputs.length; i += batchSize) {
    const batch = inputs.slice(i, i + batchSize);
    const sentKeys = new Set(batch.map(b => b.properties?.[idProperty]).filter(Boolean));

    let response;
    try {
      response = await hubspotFetch(`/crm/v3/objects/${objectType}/batch/upsert`, {
        method: 'POST',
        body: JSON.stringify({ inputs: batch, idProperty }),
      });
    } catch (err) {
      await loud.alarm({
        event: 'hubspot_batch_failed',
        message: `Batch ${objectType} [${i}-${i+batch.length}] request failed: ${err.message}`,
        runId, sourceTable,
        context: { batchSize: batch.length, sentKeys: [...sentKeys].slice(0, 5) },
      });
      for (const k of sentKeys) failed.push({ sourceKey: k, reason: `Batch request failed: ${err.message}` });
      continue;
    }

    if (response.ok) {
      const results = Array.isArray(response.body?.results) ? response.body.results : [];
      const returnedKeys = new Set();
      for (const r of results) {
        const k = r?.properties?.[idProperty];
        if (!k) continue;
        returnedKeys.add(k);
        succeeded.push({ sourceKey: k, hubspotId: r.id, wasNew: !!r.new });
      }
      for (const k of sentKeys) {
        if (!returnedKeys.has(k)) {
          failed.push({ sourceKey: k, reason: `HubSpot did not echo back idProperty — record not confirmed` });
          await loud.alarm({
            event: 'hubspot_record_rejected',
            message: `${sourceTable || objectType}: HubSpot did not echo back idProperty for ${idProperty}=${k}`,
            runId, sourceTable, sourceKey: k,
          });
        }
      }
    } else if (batch.length > 1) {
      // Batch rejected as a unit (HubSpot batch upsert is all-or-nothing).
      // Retry one record at a time so a single bad row (e.g. a shared-email
      // 409) doesn't sink the other 99 valid records.
      const sub = await batchUpsert(objectType, idProperty, batch, { ...opts, batchSize: 1 });
      for (const s of sub.succeeded) succeeded.push(s);
      for (const f of sub.failed) failed.push(f);
    } else {
      // Single record still failing. Rather than dropping the whole record
      // (silent data loss), figure out which propertie(s) HubSpot is rejecting,
      // strip ONLY those, and retry once so the record still syncs by its
      // idProperty. The stripped raw values stay in raw_csv. Covers:
      //   409 email-uniqueness collision — spouses/family/joint accounts share
      //       one email; HubSpot enforces unique contact email.
      //   400 INVALID_EMAIL — malformed addresses our pre-validator can't catch
      //       (trailing comma, bad TLD like ".comq"/".met").
      //   400 INVALID_OPTION — a source enum value not in HubSpot's picklist
      //       (e.g. tax_id_type="E", which isn't in [I,T,M,A,C,N,O,R]).
      const only = batch[0];
      const status = response.status;
      const errMsg = response.body?.message || response.body?.raw || `HTTP ${status}`;
      const sk = only?.properties?.[idProperty];

      const offending = collectOffendingProps(status, response.body, only);
      offending.delete(idProperty); // never strip the record's identity
      if (CUSTOM_OBJECTS_REQUIRING_NAME.has(objectType)) offending.delete('name'); // required
      const strippable = [...offending].filter(p => only?.properties?.[p] !== undefined);

      if (strippable.length > 0) {
        const retryProps = { ...only.properties };
        for (const p of strippable) delete retryProps[p];
        let retry;
        try {
          retry = await hubspotFetch(`/crm/v3/objects/${objectType}/batch/upsert`, {
            method: 'POST',
            body: JSON.stringify({ inputs: [{ ...only, properties: retryProps }], idProperty }),
          });
        } catch (e) { retry = null; }
        const r = retry && retry.ok && Array.isArray(retry.body?.results) ? retry.body.results[0] : null;
        if (r && r.properties?.[idProperty]) {
          succeeded.push({ sourceKey: r.properties[idProperty], hubspotId: r.id, wasNew: !!r.new });
          await loud.warn({
            event: 'record_synced_without_props',
            message: `${sourceTable || objectType} ${idProperty}=${sk}: HubSpot rejected [${strippable.join(', ')}] — synced without them (raw preserved in raw_csv)`,
            runId, sourceTable, sourceKey: sk,
            context: { strippedProps: strippable, status },
          });
          continue;
        }
      }

      await loud.alarm({
        event: 'hubspot_batch_failed',
        message: `Record ${objectType} ${idProperty}=${sk} rejected ${status}: ${errMsg}`,
        runId, sourceTable,
        context: { status, sentKeys: [...sentKeys].slice(0, 5) },
      });
      for (const k of sentKeys) failed.push({ sourceKey: k, reason: `HubSpot ${status}: ${errMsg}` });
    }

    // Only log running totals for real (multi-record) batches — the per-record
    // fallback would otherwise flood the log with one line per record.
    if (batch.length > 1) {
      console.log(`Batch upsert ${objectType} [${i}-${i + batch.length}]: running totals ok=${succeeded.length} failed=${failed.length}`);
    }
  }

  return { succeeded, failed };
}

/**
 * Generic sync: takes staging rows, a fields list, HubSpot object ID, an
 * idProperty, and the source staging table (for HASH C verification updates).
 * Returns { succeeded, failed, invalidInputs }.
 *
 * Per memory financial_data_rules.md, this also:
 *  - injects properties[idProperty] (fixes the composite_key bug)
 *  - injects properties.name = row[idProperty] for custom objects (HubSpot
 *    requires it on every deposits/loans/time_deposits/debit_cards record)
 *  - surfaces date-coercion problems via loud.warn
 *  - reads back successful records and verifies HASH C
 */
async function syncRows(rows, fields, objectType, idProperty, opts = {}) {
  const { sourceTable = null, sourceCsv = null, runId = null, emailDedup = null } = opts;

  const dbPayloadByKey = new Map();
  const inputs = [];
  const suspectInputKeys = new Set();

  for (const row of rows) {
    const { props, coercions, suspectKeys, problems } = buildPayload(row, fields);

    // Persist the coercion audit onto the staging row's `coercions` JSONB
    // column. Required by memory rule 3: every transformation must be
    // explained on the row, not just inferred from logs.
    if (sourceTable && coercions.length > 0) {
      try {
        await pool.query(
          `UPDATE ${sourceTable} SET coercions = $1 WHERE ${idProperty} = $2`,
          [JSON.stringify(coercions), row[idProperty]]
        );
      } catch (e) {
        await loud.alarm({
          event: 'coercion_audit_write_failed',
          message: `Could not persist coercion audit on ${sourceTable} ${idProperty}=${row[idProperty]}: ${e.message}`,
          runId, sourceTable, sourceKey: row[idProperty] || null,
        });
      }
    }

    // Loud-surface coercion problems (couldn't parse a date that was opt-in,
    // suspect email, etc). The property is omitted (or sent raw); raw value
    // stays in raw_csv. The operator sees this before HubSpot rejects.
    for (const p of problems) {
      const event =
        p.prop === 'email' ? 'email_suspect' :
        p.problem.startsWith('unparseable date') ? 'date_unparseable' :
        'coercion_problem';
      await loud.warn({
        event,
        message: `${sourceCsv || sourceTable}.${p.csv} → ${p.prop}: ${p.problem}`,
        runId, sourceTable, sourceKey: row[idProperty] || null,
        context: { rawValue: p.raw, prop: p.prop, csv: p.csv },
      });
    }

    // Proactive email de-dup (contacts only). HubSpot enforces unique contact
    // email, but Civista data routinely shares one email across CIFs
    // (spouses/family/joint accounts). On a clean load this would otherwise
    // 409 and force the slow per-record fallback for every poisoned batch.
    // Keep the email on the first CIF that claims it this run and omit it on
    // the rest (raw value stays in raw_csv). The per-record 409 fallback below
    // still covers collisions with contacts that pre-date this run.
    if (emailDedup && objectType === 'contacts' && typeof props.email === 'string' && props.email) {
      if (emailDedup.seen.has(props.email)) {
        delete props.email;
        emailDedup.dropped++;
      } else {
        emailDedup.seen.add(props.email);
      }
    }

    // Always include the idProperty value in properties (fixes the
    // composite_key bug for debit cards; harmless for the rest).
    if (row[idProperty] !== undefined && row[idProperty] !== null && props[idProperty] === undefined) {
      props[idProperty] = String(row[idProperty]);
    }

    // Custom objects require `name` (verified via HubSpot schema API:
    // requiredProperties=['name'], primaryDisplayProperty='name'). Use the
    // unique key as the display label.
    if (CUSTOM_OBJECTS_REQUIRING_NAME.has(objectType) && !props.name && row[idProperty]) {
      props.name = String(row[idProperty]);
    }

    const sk = props[idProperty];
    if (sk) {
      dbPayloadByKey.set(sk, props);
      // Mark this row as suspect IF buildPayload flagged any suspect
      // properties (currently only email_strict). Suspect rows get isolated
      // into 1-row batches downstream so a HubSpot rejection doesn't kill
      // the other 99 records in the batch.
      if (suspectKeys.size > 0) suspectInputKeys.add(sk);
    }

    inputs.push({ idProperty, id: row[idProperty], properties: props });
  }

  const { valid, invalid } = validateBatchInputs(inputs, idProperty);

  // Loud surface for invalid (no idProperty) inputs.
  for (const i of invalid) {
    await loud.alarm({
      event: 'idproperty_missing',
      message: `${sourceTable || objectType}: ${i.reason}`,
      runId, sourceTable,
      context: { input: i.input },
    });
  }

  // Split into clean (default 100/batch) and suspect (forced 1/batch).
  // Suspect rows are isolated so a per-record rejection (e.g., HubSpot
  // refusing an invalid email) only fails THAT row, never poisoning the
  // 99 other valid records that share its batch. Per Q7 / memory rule 5.
  const cleanInputs = valid.filter(v => !suspectInputKeys.has(v.properties?.[idProperty]));
  const suspectInputs = valid.filter(v => suspectInputKeys.has(v.properties?.[idProperty]));

  let succeeded = [];
  let failed = [];
  if (cleanInputs.length > 0) {
    const r = await batchUpsert(objectType, idProperty, cleanInputs, { runId, sourceTable, batchSize: BATCH_SIZE });
    succeeded = succeeded.concat(r.succeeded);
    failed = failed.concat(r.failed);
  }
  if (suspectInputs.length > 0) {
    console.log(`Isolating ${suspectInputs.length} suspect row(s) into 1-record batches for ${objectType}`);
    const r = await batchUpsert(objectType, idProperty, suspectInputs, { runId, sourceTable, batchSize: 1 });
    succeeded = succeeded.concat(r.succeeded);
    failed = failed.concat(r.failed);
  }

  // HASH C — read back from HubSpot and verify the persisted properties
  // match the DB-side payload. Gated by VERIFY_HUBSPOT_READBACK env var.
  // Records that fail HASH C must NOT enter shipped_records — otherwise the
  // diff engine would skip them on future runs and they'd never be retried.
  // We move them from succeeded → failed here.
  let finalSucceeded = succeeded;
  let finalFailed = failed;
  if (succeeded.length > 0 && sourceTable && process.env.VERIFY_HUBSPOT_READBACK !== '0') {
    const { mismatchedKeys } = await verifyHubspotPersistence({
      objectType, idProperty, sourceTable, succeeded, dbPayloadByKey, runId,
    });
    if (mismatchedKeys.size > 0) {
      finalSucceeded = succeeded.filter(s => !mismatchedKeys.has(s.sourceKey));
      const moved = succeeded
        .filter(s => mismatchedKeys.has(s.sourceKey))
        .map(s => ({ sourceKey: s.sourceKey, reason: 'HASH C mismatch — HubSpot value did not match DB payload (record present in HubSpot but flagged needs_review)' }));
      finalFailed = failed.concat(moved);
    }
  }

  return { succeeded: finalSucceeded, failed: finalFailed, invalidInputs: invalid };
}

/**
 * Read each successfully-shipped record back from HubSpot and compare the
 * returned properties to the DB-side payload we sent. Writes
 * hubspot_persist_hash + hubspot_verified_at on match; on mismatch writes
 * hubspot_verify_diff (per-property {db, hubspot}) and sets needs_review.
 *
 * HubSpot responses serialize all property values as strings, so we compare
 * String(value) on both sides.
 */
async function verifyHubspotPersistence({ objectType, idProperty, sourceTable, succeeded, dbPayloadByKey, runId }) {
  // Union of property names we sent across the batch.
  const propNames = new Set();
  for (const p of dbPayloadByKey.values()) for (const k of Object.keys(p)) propNames.add(k);
  const properties = Array.from(propNames);

  // Track which sourceKeys failed verification — caller filters them out
  // of `succeeded` so they don't enter shipped_records and get skipped on
  // future runs (a known-corrupt row in HubSpot must be retried).
  const mismatchedKeys = new Set();

  for (let i = 0; i < succeeded.length; i += BATCH_SIZE) {
    const chunk = succeeded.slice(i, i + BATCH_SIZE);
    let response;
    try {
      response = await hubspotFetch(`/crm/v3/objects/${objectType}/batch/read`, {
        method: 'POST',
        body: JSON.stringify({
          inputs: chunk.map(s => ({ id: s.hubspotId })),
          properties,
        }),
      });
    } catch (err) {
      await loud.alarm({
        event: 'hubspot_readback_failed',
        message: `Read-back request failed for ${objectType}: ${err.message}`,
        runId, sourceTable,
        context: { batchSize: chunk.length },
      });
      // Treat the whole chunk as unverifiable → mismatched (don't enter ledger).
      for (const s of chunk) mismatchedKeys.add(s.sourceKey);
      continue;
    }
    if (!response.ok) {
      await loud.alarm({
        event: 'hubspot_readback_failed',
        message: `Read-back HTTP ${response.status} for ${objectType}: ${response.body?.message || response.body?.raw || ''}`,
        runId, sourceTable,
        context: { batchSize: chunk.length, status: response.status },
      });
      for (const s of chunk) mismatchedKeys.add(s.sourceKey);
      continue;
    }
    const byId = new Map((response.body?.results || []).map(r => [r.id, r.properties || {}]));

    for (const s of chunk) {
      // canonicalize lookup key — both sides should be String, but be defensive
      const lookupKey = s.sourceKey == null ? null : String(s.sourceKey);
      const dbPayload = lookupKey == null ? null : dbPayloadByKey.get(lookupKey);
      if (!dbPayload) {
        // Should never happen — succeeded entry came from the batch we built.
        // Loud-alarm so it can't be silent.
        await loud.alarm({
          event: 'hubspot_readback_no_payload',
          message: `Cannot HASH C verify ${sourceTable} hubspot_id=${s.hubspotId}: no DB payload for sourceKey=${s.sourceKey}`,
          runId, sourceTable, sourceKey: s.sourceKey,
        });
        mismatchedKeys.add(s.sourceKey);
        continue;
      }
      const hsProps = byId.get(s.hubspotId);
      if (!hsProps) {
        await pool.query(
          `UPDATE ${sourceTable} SET needs_review = true WHERE ${idProperty} = $1`,
          [s.sourceKey]
        );
        await loud.alarm({
          event: 'hubspot_persist_mismatch',
          message: `Read-back returned no record for hubspot_id=${s.hubspotId} sourceKey=${s.sourceKey} on ${objectType}`,
          runId, sourceTable, sourceKey: s.sourceKey,
        });
        mismatchedKeys.add(s.sourceKey);
        continue;
      }

      // Compare DB-side payload to HubSpot-returned values for the same props.
      const diff = {};
      for (const k of Object.keys(dbPayload)) {
        const a = canonicalScalar(dbPayload[k]);
        const b = canonicalScalar(hsProps[k]);
        if (a !== b) diff[k] = { db: dbPayload[k], hubspot: hsProps[k] ?? null };
      }

      const dbHash = hashCanonical(dbPayload, Object.keys(dbPayload));
      const hsHash = hashCanonical(hsProps, Object.keys(dbPayload));

      if (Object.keys(diff).length === 0) {
        // HASH C matched. Write the hash + clear the prior diff. DO NOT
        // touch needs_review — if HASH B set it (CSV→DB lost something),
        // we must preserve that signal. Per memory financial_data_rules.md:
        // a successful HASH C does not absolve a HASH B failure.
        await pool.query(
          `UPDATE ${sourceTable}
             SET hubspot_persist_hash = $1, hubspot_verified_at = NOW(),
                 hubspot_verify_diff = NULL
           WHERE ${idProperty} = $2`,
          [hsHash, s.sourceKey]
        );
      } else {
        await pool.query(
          `UPDATE ${sourceTable}
             SET hubspot_persist_hash = $1, hubspot_verified_at = NOW(),
                 hubspot_verify_diff = $2, needs_review = true
           WHERE ${idProperty} = $3`,
          [hsHash, JSON.stringify(diff), s.sourceKey]
        );
        await loud.alarm({
          event: 'hubspot_persist_mismatch',
          message: `HASH C mismatch on ${sourceTable} ${idProperty}=${s.sourceKey}: properties differ → ${Object.keys(diff).join(', ')}`,
          runId, sourceTable, sourceKey: s.sourceKey,
          context: { diff, hubspotId: s.hubspotId, dbHash, hsHash },
        });
        mismatchedKeys.add(s.sourceKey);
      }
    }
  }

  return { mismatchedKeys };
}

function canonicalScalar(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return String(v);
}

function hashCanonical(obj, keys) {
  const sorted = {};
  for (const k of [...keys].sort()) sorted[k] = canonicalScalar(obj[k]);
  return crypto.createHash('sha256').update(JSON.stringify(sorted)).digest('hex');
}

// Exported per-table sync functions — each one is a thin wrapper around syncRows.
// All take optional opts: { sourceTable, sourceCsv, runId } so the row-level
// machinery can do HASH C verification and emit loud events with full context.
async function syncContacts(rows, opts)     { return syncRows(rows, CIF_CONTACT_FIELDS, 'contacts', 'cif_number', opts); }
async function syncCompanies(rows, opts)    { return syncRows(rows, CIF_COMPANY_FIELDS, 'companies', 'cif_number', opts); }
async function syncDeposits(rows, opts)     { return syncRows(rows, TABLES.dda.fields, TABLES.dda.object, TABLES.dda.idProperty, opts); }
async function syncLoans(rows, opts)        { return syncRows(rows, TABLES.loans.fields, TABLES.loans.object, TABLES.loans.idProperty, opts); }
async function syncTimeDeposits(rows, opts) { return syncRows(rows, TABLES.cd.fields, TABLES.cd.object, TABLES.cd.idProperty, opts); }
async function syncDebitCards(rows, opts)   { return syncRows(rows, TABLES.debit_cards.fields, TABLES.debit_cards.object, TABLES.debit_cards.idProperty, opts); }

module.exports = {
  hubspotFetch,
  batchUpsert,
  validateBatchInputs,
  buildPayload,
  verifyHubspotPersistence,
  syncContacts,
  syncCompanies,
  syncDeposits,
  syncLoans,
  syncTimeDeposits,
  syncDebitCards,
  BATCH_SIZE,
  INTERNAL_COLUMNS,
};

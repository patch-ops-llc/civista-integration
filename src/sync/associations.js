/**
 * Association engine — links account records back to their owner Contacts /
 * Companies after each account table syncs (the client's #1 gap).
 *
 * Model (Option B): each physical account is one record (keyed by account_key)
 * with multiple labeled owner associations. The owner list lives in the
 * stg_*_owners tables produced by the parser; account + owner HubSpot IDs are
 * resolved from the shipped_records ledger. Relationship codes map to HubSpot
 * association labels via relationship-map.js, and labels resolve to portal
 * type IDs via association-labels.js.
 *
 *   - Deposits / Loans / Time Deposits → labeled associations (PRIMARY OWNER,
 *     CO-OWNER, BENEFICIARY, TRUSTEE, ...) to Contacts and Companies.
 *   - Debit Cards → unlabeled (default) association to the single owner, per
 *     the production config.
 *
 * Idempotency: every created edge is recorded in shipped_associations, and
 * edges already present are filtered out before sending — so steady-state
 * nightly runs create ~0 associations.
 */

const { pool } = require('../../db/init');
const { ACCOUNT_SOURCES } = require('../transform/hubspot-mapping');
const { labelFor } = require('../transform/relationship-map');
const { getLabelIndex } = require('./association-labels');
const { hubspotFetch } = require('./hubspot');
const loud = require('../monitoring/loud');

const OWNER_LEDGER = { contact: 'stg_contacts', company: 'stg_companies' };
const OWNER_OBJECT = { contact: 'contacts', company: 'companies' };

// How many owner rows to resolve per Postgres page, and how many edges per
// HubSpot v4 batch call (HubSpot caps batch association create at 100).
const ASSOC_PAGE_SIZE = parseInt(process.env.ASSOC_PAGE_SIZE, 10) || 1000;
const HS_ASSOC_CHUNK = 100;
// Cap loud-warn volume so a systemic miss (e.g. contacts not loaded yet)
// surfaces a handful of samples instead of 250k identical banners.
const MAX_WARN_SAMPLES = 10;

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

/** Resolve source_key -> hubspot_id from the ledger for a set of keys. */
async function resolveLedger(sourceTable, keys) {
  const map = new Map();
  const distinct = uniq(keys.filter(k => k !== null && k !== undefined && k !== ''));
  if (distinct.length === 0) return map;
  for (const part of chunk(distinct, 1000)) {
    const r = await pool.query(
      `SELECT source_key, hubspot_id FROM shipped_records
       WHERE source_table = $1 AND source_key = ANY($2)`,
      [sourceTable, part]
    );
    for (const row of r.rows) map.set(row.source_key, row.hubspot_id);
  }
  return map;
}

/** Which already-exist edges are in the ledger, for a page of candidate edges. */
async function existingEdges(fromObject, toObject, fromIds) {
  const set = new Set();
  const distinct = uniq(fromIds);
  if (distinct.length === 0) return set;
  for (const part of chunk(distinct, 1000)) {
    const r = await pool.query(
      `SELECT from_id, to_id, type_id FROM shipped_associations
       WHERE from_object = $1 AND to_object = $2 AND from_id = ANY($3)`,
      [fromObject, toObject, part]
    );
    for (const row of r.rows) set.add(`${row.from_id}|${row.to_id}|${row.type_id}`);
  }
  return set;
}

async function ledgerInsert(edges) {
  if (edges.length === 0) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const e of edges) {
      await client.query(
        `INSERT INTO shipped_associations (from_object, from_id, to_object, to_id, type_id)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT DO NOTHING`,
        [e.fromObject, e.fromId, e.toObject, e.toId, e.typeId]
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

/**
 * Send one batch (<=100) of edges for a directed object pair via the v4 API.
 * HubSpot association create is itself idempotent, so a re-send is a no-op;
 * we still pre-filter via the ledger to keep call volume near zero on deltas.
 * Returns { created, failed }.
 */
async function createEdgeBatch(fromObject, toObject, edges, { runId, sourceTable }) {
  const inputs = edges.map(e => ({
    from: { id: e.fromId },
    to: { id: e.toId },
    types: [{ associationCategory: e.category, associationTypeId: e.typeId }],
  }));
  let res;
  try {
    res = await hubspotFetch(`/crm/v4/associations/${fromObject}/${toObject}/batch/create`, {
      method: 'POST',
      body: JSON.stringify({ inputs }),
    });
  } catch (err) {
    await loud.alarm({
      event: 'association_batch_failed',
      message: `Association batch ${fromObject}->${toObject} request failed: ${err.message}`,
      runId, sourceTable,
      context: { batchSize: edges.length },
    });
    return { created: 0, failed: edges.length };
  }
  if (!res.ok) {
    const msg = res.body?.message || res.body?.raw || `HTTP ${res.status}`;
    await loud.alarm({
      event: 'association_batch_rejected',
      message: `Association batch ${fromObject}->${toObject} rejected ${res.status}: ${msg}`,
      runId, sourceTable,
      context: { batchSize: edges.length, status: res.status },
    });
    return { created: 0, failed: edges.length };
  }
  await ledgerInsert(edges);
  return { created: edges.length, failed: 0 };
}

/**
 * Build, filter, and send candidate edges for one page. Mutates the running
 * stats object. `buildEdge(row)` returns an edge or a reason string.
 */
async function flushPage(rows, buildEdge, stats, ctx) {
  // Bucket valid edges by directed pair so each pair calls its own endpoint.
  const byPair = new Map(); // `${from}->${to}` -> { fromObject, toObject, edges: [] }
  for (const row of rows) {
    const result = buildEdge(row);
    if (typeof result === 'string') {
      stats.unresolved++;
      if (stats.samples.length < MAX_WARN_SAMPLES) stats.samples.push({ reason: result, row: sampleOf(row) });
      continue;
    }
    if (!result) { stats.unresolved++; continue; }
    const key = `${result.fromObject}->${result.toObject}`;
    let b = byPair.get(key);
    if (!b) { b = { fromObject: result.fromObject, toObject: result.toObject, edges: [] }; byPair.set(key, b); }
    b.edges.push(result);
  }

  for (const { fromObject, toObject, edges } of byPair.values()) {
    // De-dupe within the page and drop edges already in the ledger.
    const seen = new Set();
    const deduped = [];
    for (const e of edges) {
      const sig = `${e.fromId}|${e.toId}|${e.typeId}`;
      if (seen.has(sig)) continue;
      seen.add(sig);
      deduped.push(e);
    }
    const already = await existingEdges(fromObject, toObject, deduped.map(e => e.fromId));
    const toSend = deduped.filter(e => !already.has(`${e.fromId}|${e.toId}|${e.typeId}`));
    stats.skippedExisting += deduped.length - toSend.length;

    for (const part of chunk(toSend, HS_ASSOC_CHUNK)) {
      const { created, failed } = await createEdgeBatch(fromObject, toObject, part, ctx);
      stats.created += created;
      stats.failed += failed;
    }
  }
}

function sampleOf(row) {
  return {
    account_key: row.account_key,
    cif_number: row.cif_number,
    relationship: row.relationship,
    composite_key: row.composite_key,
  };
}

/**
 * Labeled associations for an account source (dda / loans / cd). Streams the
 * owner table in keyset pages so memory stays bounded at prod volume.
 */
async function syncAccountAssociations(source, { runId } = {}) {
  const cfg = ACCOUNT_SOURCES[source];
  const accountObject = cfg.object;
  const stats = { source, created: 0, skippedExisting: 0, unresolved: 0, failed: 0, samples: [] };

  // Resolve label type-IDs for both owner pairs up front (cached for the run).
  let idx;
  try {
    idx = {
      contact: await getLabelIndex(accountObject, OWNER_OBJECT.contact),
      company: await getLabelIndex(accountObject, OWNER_OBJECT.company),
    };
  } catch (err) {
    await loud.alarm({
      event: 'association_labels_unavailable',
      message: `Could not load association labels for ${source} (${accountObject}): ${err.message}`,
      runId, sourceTable: cfg.staging,
    });
    return { ...stats, failed: -1, error: err.message };
  }

  let lastId = 0;
  for (;;) {
    const page = await pool.query(
      `SELECT id, account_key, cif_number, relationship
       FROM ${cfg.ownerStaging}
       WHERE id > $1 AND account_key IS NOT NULL AND cif_number IS NOT NULL AND cif_number <> ''
       ORDER BY id LIMIT $2`,
      [lastId, ASSOC_PAGE_SIZE]
    );
    if (page.rows.length === 0) break;
    lastId = page.rows[page.rows.length - 1].id;

    // Resolve account + owner ids for this page in bulk.
    const accountMap = await resolveLedger(cfg.staging, page.rows.map(r => r.account_key));
    const contactMap = await resolveLedger(OWNER_LEDGER.contact, page.rows.map(r => r.cif_number));
    const companyMap = await resolveLedger(OWNER_LEDGER.company, page.rows.map(r => r.cif_number));

    const buildEdge = (row) => {
      const accountId = accountMap.get(row.account_key);
      if (!accountId) return `account not shipped (account_key=${row.account_key})`;

      let kind, ownerId;
      if (contactMap.has(row.cif_number)) { kind = 'contact'; ownerId = contactMap.get(row.cif_number); }
      else if (companyMap.has(row.cif_number)) { kind = 'company'; ownerId = companyMap.get(row.cif_number); }
      else return `owner CIF not shipped (cif=${row.cif_number})`;

      const label = labelFor(source, kind, row.relationship);
      if (!label) return `no label for relationship code "${row.relationship}"`;
      const typeId = idx[kind].byLabel.get(label);
      if (!typeId) return `label "${label}" has no association type id in this portal`;

      return {
        fromObject: accountObject,
        toObject: OWNER_OBJECT[kind],
        fromId: accountId,
        toId: ownerId,
        typeId,
        category: 'USER_DEFINED',
      };
    };

    await flushPage(page.rows, buildEdge, stats, { runId, sourceTable: cfg.staging });
  }

  return stats;
}

/**
 * Unlabeled (default) associations for Debit Cards → owner. One row per card,
 * so we read stg_debit_cards directly rather than an owner table.
 */
async function syncDebitCardAssociations({ runId } = {}) {
  const DEBIT_OBJECT = '2-60442979';
  const stats = { source: 'debit_cards', created: 0, skippedExisting: 0, unresolved: 0, failed: 0, samples: [] };

  let idx;
  try {
    idx = {
      contact: await getLabelIndex(DEBIT_OBJECT, OWNER_OBJECT.contact),
      company: await getLabelIndex(DEBIT_OBJECT, OWNER_OBJECT.company),
    };
  } catch (err) {
    await loud.alarm({
      event: 'association_labels_unavailable',
      message: `Could not load default association type for debit cards: ${err.message}`,
      runId, sourceTable: 'stg_debit_cards',
    });
    return { ...stats, failed: -1, error: err.message };
  }

  let lastId = 0;
  for (;;) {
    const page = await pool.query(
      `SELECT id, composite_key, cif_number
       FROM stg_debit_cards
       WHERE id > $1 AND composite_key IS NOT NULL AND cif_number IS NOT NULL AND cif_number <> ''
       ORDER BY id LIMIT $2`,
      [lastId, ASSOC_PAGE_SIZE]
    );
    if (page.rows.length === 0) break;
    lastId = page.rows[page.rows.length - 1].id;

    const cardMap = await resolveLedger('stg_debit_cards', page.rows.map(r => r.composite_key));
    const contactMap = await resolveLedger(OWNER_LEDGER.contact, page.rows.map(r => r.cif_number));
    const companyMap = await resolveLedger(OWNER_LEDGER.company, page.rows.map(r => r.cif_number));

    const buildEdge = (row) => {
      const cardId = cardMap.get(row.composite_key);
      if (!cardId) return `debit card not shipped (composite_key=${row.composite_key})`;

      let kind, ownerId;
      if (contactMap.has(row.cif_number)) { kind = 'contact'; ownerId = contactMap.get(row.cif_number); }
      else if (companyMap.has(row.cif_number)) { kind = 'company'; ownerId = companyMap.get(row.cif_number); }
      else return `owner CIF not shipped (cif=${row.cif_number})`;

      const typeId = idx[kind].defaultTypeId;
      if (!typeId) return `no default association type id for debit-card -> ${kind} in this portal`;

      return {
        fromObject: DEBIT_OBJECT,
        toObject: OWNER_OBJECT[kind],
        fromId: cardId,
        toId: ownerId,
        typeId,
        // Use the category HubSpot actually reports for the unlabeled type
        // (USER_DEFINED for these custom-object pairs), not a hardcoded guess.
        category: idx[kind].defaultCategory || 'HUBSPOT_DEFINED',
      };
    };

    await flushPage(page.rows, buildEdge, stats, { runId, sourceTable: 'stg_debit_cards' });
  }

  return stats;
}

/**
 * Entry point — build associations for one logical source after its account
 * table has synced. Surfaces a warn with sampled unresolved reasons (capped)
 * and returns per-source counts for the orchestrator's report.
 */
async function syncAssociations(source, opts = {}) {
  const stats = source === 'debit_cards'
    ? await syncDebitCardAssociations(opts)
    : await syncAccountAssociations(source, opts);

  console.log(`associations[${source}]: created=${stats.created} skipped_existing=${stats.skippedExisting} unresolved=${stats.unresolved} failed=${stats.failed}`);
  if (stats.unresolved > 0) {
    await loud.warn({
      event: 'associations_unresolved',
      message: `${source}: ${stats.unresolved} owner link(s) could not be created (account/owner not yet shipped, or unmapped label). First ${stats.samples.length} samples attached.`,
      runId: opts.runId || null,
      sourceTable: ACCOUNT_SOURCES[source]?.staging || 'stg_debit_cards',
      context: { samples: stats.samples },
    });
  }
  return stats;
}

module.exports = {
  syncAssociations,
  syncAccountAssociations,
  syncDebitCardAssociations,
};

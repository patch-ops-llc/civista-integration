#!/usr/bin/env node
/**
 * Demo-grade HubSpot sandbox schema repair.
 *
 * Idempotent. Runs against whichever portal HUBSPOT_API_KEY resolves to.
 * Refuses to run if `meta.last_portal_id` is set and disagrees with the
 * portal the key targets — same guard as scripts/cutover-portal.js.
 *
 * Repairs (all align HubSpot schema to CSV semantics — per "CSV is the boss"):
 *
 *   A) estatement_disclosure_acceptance_date on Contacts AND Companies:
 *      currently typed `date`, source DiscAcpt is Y/N.
 *      → archive + recreate as bool/booleancheckbox, same name.
 *
 *   B) date_of_birth on Contacts:
 *      currently typed `string`, source Birthday is a date.
 *      → archive + recreate as date type, same name.
 *
 *   C) relationship on Deposits, Loans, Time Deposits (3 objects):
 *      property does not exist, source CSVs carry P/C/M/B/X.
 *      → CREATE as enumeration/select with descriptive labels.
 *
 * Closes UAT failures: "eStatement Disclosure Date missing" and
 * "Verification of Segments / filter by Primary vs Co-Owner".
 *
 * Run:
 *   railway run --service=civista-integration node scripts/repair_demo_schema.js
 * Or locally with HUBSPOT_API_KEY and DATABASE_URL set in env.
 */
const { pool } = require('../db/init');

// ---- repair targets ----

const DISCLOSURE_NAME = 'estatement_disclosure_acceptance_date';
const DISCLOSURE_LABEL = 'eStatement Disclosure Acceptance';

const DOB_NAME = 'date_of_birth';
const DOB_LABEL = 'Date of Birth';

const RELATIONSHIP_NAME = 'relationship';
const RELATIONSHIP_LABEL = 'Relationship';
const RELATIONSHIP_OPTIONS = [
  { label: 'Primary Account Holder', value: 'P', displayOrder: 0, hidden: false },
  { label: 'Co-Account Holder',      value: 'C', displayOrder: 1, hidden: false },
  { label: 'Multi-Party',            value: 'M', displayOrder: 2, hidden: false },
  { label: 'Beneficiary',            value: 'B', displayOrder: 3, hidden: false },
  { label: 'Other / Unknown',        value: 'X', displayOrder: 4, hidden: false },
];

const ACCOUNT_OBJECTS = [
  { object: '2-60442978', label: 'Deposits',      groupName: 'deposits_information' },
  { object: '2-60442977', label: 'Loans',         groupName: 'loans_information' },
  { object: '2-60442980', label: 'Time Deposits', groupName: 'time_deposits_information' },
];

// ---- HubSpot helpers ----

async function getCurrentPortalId() {
  const apiKey = process.env.HUBSPOT_API_KEY;
  if (!apiKey) throw new Error('HUBSPOT_API_KEY is not set');
  const r = await fetch('https://api.hubapi.com/account-info/v3/details', {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`HubSpot account-info HTTP ${r.status}: ${body.message || ''}`);
  const portalId = String(body.portalId || body.hubId || '');
  if (!portalId) throw new Error('HubSpot returned no portal id');
  return portalId;
}

async function getStoredPortalId() {
  const r = await pool.query(`SELECT value FROM meta WHERE key = 'last_portal_id'`);
  return r.rows[0]?.value || null;
}

async function hsRequest(method, path, body) {
  const apiKey = process.env.HUBSPOT_API_KEY;
  const init = {
    method,
    headers: { Authorization: `Bearer ${apiKey}` },
  };
  if (body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const r = await fetch(`https://api.hubapi.com${path}`, init);
  let parsed = {};
  try { parsed = await r.json(); } catch { /* DELETE has no body */ }
  return { ok: r.ok, status: r.status, body: parsed };
}

async function getProperty(object, name) {
  const r = await hsRequest('GET', `/crm/v3/properties/${object}/${name}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`GET ${object}/${name} HTTP ${r.status}: ${r.body.message || ''}`);
  return r.body;
}

async function archiveProperty(object, name) {
  const r = await hsRequest('DELETE', `/crm/v3/properties/${object}/${name}`);
  if (!r.ok && r.status !== 404) {
    throw new Error(`DELETE ${object}/${name} HTTP ${r.status}: ${r.body.message || ''}`);
  }
}

async function createProperty(object, body) {
  const r = await hsRequest('POST', `/crm/v3/properties/${object}`, body);
  if (!r.ok) throw new Error(`POST property ${object} HTTP ${r.status}: ${r.body.message || ''}`);
}

// ---- repair operations ----

async function repairDisclosure(object, groupName) {
  const existing = await getProperty(object, DISCLOSURE_NAME);
  if (existing && existing.type === 'bool' && existing.fieldType === 'booleancheckbox') {
    console.log(`  ${object}.${DISCLOSURE_NAME}: already bool/booleancheckbox → no change`);
    return;
  }
  if (existing) {
    console.log(`  ${object}.${DISCLOSURE_NAME}: archiving existing ${existing.type}/${existing.fieldType}...`);
    await archiveProperty(object, DISCLOSURE_NAME);
  }
  await createProperty(object, {
    name: DISCLOSURE_NAME,
    label: DISCLOSURE_LABEL,
    groupName,
    type: 'bool',
    fieldType: 'booleancheckbox',
    options: [
      { label: 'Yes', value: 'true',  displayOrder: 0, hidden: false },
      { label: 'No',  value: 'false', displayOrder: 1, hidden: false },
    ],
  });
  console.log(`  ${object}.${DISCLOSURE_NAME}: recreated as bool/booleancheckbox`);
}

async function repairDateOfBirth() {
  const object = 'contacts';
  const existing = await getProperty(object, DOB_NAME);
  if (existing && existing.type === 'date' && existing.fieldType === 'date') {
    console.log(`  ${object}.${DOB_NAME}: already date/date → no change`);
    return;
  }
  if (existing) {
    console.log(`  ${object}.${DOB_NAME}: archiving existing ${existing.type}/${existing.fieldType}...`);
    await archiveProperty(object, DOB_NAME);
  }
  await createProperty(object, {
    name: DOB_NAME,
    label: DOB_LABEL,
    groupName: 'contactinformation',
    type: 'date',
    fieldType: 'date',
  });
  console.log(`  ${object}.${DOB_NAME}: recreated as date/date`);
}

async function ensureRelationship({ object, label, groupName }) {
  const existing = await getProperty(object, RELATIONSHIP_NAME);
  if (existing) {
    const existingValues = new Set((existing.options || []).map(o => o.value));
    const wantedValues = new Set(RELATIONSHIP_OPTIONS.map(o => o.value));
    const sameType = existing.type === 'enumeration' && existing.fieldType === 'select';
    const sameOptionSet = existingValues.size === wantedValues.size
      && [...wantedValues].every(v => existingValues.has(v));
    if (sameType && sameOptionSet) {
      console.log(`  ${object} (${label}).${RELATIONSHIP_NAME}: already enumeration with P/C/M/B/X → no change`);
      return;
    }
    console.log(`  ${object} (${label}).${RELATIONSHIP_NAME}: archiving stale property (${existing.type}/${existing.fieldType})...`);
    await archiveProperty(object, RELATIONSHIP_NAME);
  }
  await createProperty(object, {
    name: RELATIONSHIP_NAME,
    label: RELATIONSHIP_LABEL,
    groupName,
    type: 'enumeration',
    fieldType: 'select',
    options: RELATIONSHIP_OPTIONS,
  });
  console.log(`  ${object} (${label}).${RELATIONSHIP_NAME}: created enumeration with ${RELATIONSHIP_OPTIONS.length} options`);
}

// ---- main ----

async function main() {
  const current = await getCurrentPortalId();
  const stored = await getStoredPortalId();
  if (stored && stored !== current) {
    throw new Error(
      `Portal mismatch: current=${current}, stored=${stored}. ` +
      `Refusing to run. Verify HUBSPOT_API_KEY targets the intended portal; ` +
      `if you really meant to switch, run scripts/cutover-portal.js first.`
    );
  }
  console.log(`=== repair_demo_schema (portal ${current}) ===`);

  console.log('Repair A: estatement_disclosure_acceptance_date → bool');
  await repairDisclosure('contacts', 'contactinformation');
  await repairDisclosure('companies', 'companyinformation');

  console.log('Repair B: date_of_birth → date');
  await repairDateOfBirth();

  console.log('Repair C: relationship enum on account objects');
  for (const target of ACCOUNT_OBJECTS) {
    await ensureRelationship(target);
  }

  console.log('=== done ===');
  process.exit(0);
}

main().catch((e) => {
  console.error('repair_demo_schema failed:', e.message || e);
  process.exit(1);
});

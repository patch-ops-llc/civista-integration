#!/usr/bin/env node
/**
 * Portal cutover safeguard. Run this once when HUBSPOT_API_KEY is changed
 * to point at a different HubSpot portal (sandbox→prod or prod→sandbox).
 *
 * Why: the staging tables and `shipped_records`/`hubspot_id_map` hold
 * `hubspot_id` values that are valid only inside the portal they came
 * from. If the API key flips to a different portal those IDs no longer
 * exist; subsequent syncs would either skip rows that need to ship (diff
 * engine sees a matching row_hash and treats them as already shipped) or
 * write to ID slots that don't exist. Either way: silent corruption.
 *
 * What this does:
 *   1. Confirms the current HUBSPOT_API_KEY's portal id by hitting
 *      /account-info/v3/details.
 *   2. TRUNCATEs every staging table + shipped_records + hubspot_id_map
 *      + sync_log + sync_errors so the next sync starts fresh against
 *      the new portal.
 *   3. Updates `meta.last_portal_id` to the new value so the boot guard
 *      in index.js stops blocking.
 *
 * Run via Railway shell so the key never leaves Railway env vars:
 *   railway run --service=civista-integration node scripts/cutover-portal.js
 */
const { pool } = require('../db/init');

const TABLES_TO_TRUNCATE = [
  'sync_log',
  'sync_errors',
  'mapping_issues',
  'hubspot_id_map',
  'shipped_records',
  'shipped_associations',
  'stg_contacts',
  'stg_companies',
  'stg_deposits',
  'stg_loans',
  'stg_time_deposits',
  'stg_debit_cards',
  'stg_deposit_owners',
  'stg_loan_owners',
  'stg_time_deposit_owners',
];

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

async function main() {
  const current = await getCurrentPortalId();
  const stored = await getStoredPortalId();
  console.log('=== portal cutover ===');
  console.log(`  current portal id (from API key): ${current}`);
  console.log(`  stored portal id  (from meta):    ${stored ?? '(none)'}`);

  if (stored === current) {
    console.log('  → portal ids match. No cutover needed.');
    process.exit(0);
  }

  console.log('  → cutover required. TRUNCATEing staging + ledger tables...');
  for (const t of TABLES_TO_TRUNCATE) {
    try {
      await pool.query(`TRUNCATE ${t} RESTART IDENTITY`);
      console.log(`    ✓ ${t}`);
    } catch (e) {
      // Table may not exist yet on a brand-new portal; ALTER TABLEs in
      // db/init.js create what's missing. Skip cleanly.
      console.log(`    (skipped ${t}: ${e.code || e.message})`);
    }
  }

  await pool.query(
    `INSERT INTO meta (key, value, updated_at) VALUES ('last_portal_id', $1, NOW())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
    [current]
  );
  console.log(`  ✓ meta.last_portal_id updated to ${current}`);
  console.log('=== cutover complete ===');
  process.exit(0);
}

main().catch((e) => {
  console.error('cutover-portal.js failed:', e.message || e);
  process.exit(1);
});

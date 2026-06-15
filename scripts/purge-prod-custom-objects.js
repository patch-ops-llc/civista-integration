/**
 * Purge ALL records in the FOUR Civista custom objects (Deposits, Loans,
 * Time Deposits, Debit Cards) on the PRODUCTION portal.
 *
 * Why this exists: prod was originally imported under Ivan's per-owner
 * `primary_key` scheme, so the custom objects hold ~380k duplicate per-owner
 * records keyed differently than this pipeline (which upserts by `account_key`
 * / `composite_key`). Those stale records can't be matched/updated by the sync,
 * so they must be archived before the deduplicated backfill.
 *
 * SAFETY:
 *   - Touches ONLY the 4 custom objects. Contacts and Companies are never
 *     included (they upsert in place by cif_number and must be preserved).
 *   - Refuses to run unless ACK_PORTAL_ID is set AND matches the live portal
 *     the key points at (prevents a stray sandbox/other-portal wipe).
 *   - Refuses to archive unless CONFIRM === 'PURGE-PROD-CUSTOM-OBJECTS'.
 *     Without CONFIRM it runs a read-only DRY RUN (prints counts, archives
 *     nothing).
 *   - Uses HubSpot batch/archive (soft delete, ~30-day recovery window).
 *
 * Run via Railway shell so the key never leaves Railway env vars:
 *
 *   # dry run (counts only):
 *   railway run --service=civista-integration \
 *     ACK_PORTAL_ID=50181316 node scripts/purge-prod-custom-objects.js
 *
 *   # real purge:
 *   railway run --service=civista-integration \
 *     ACK_PORTAL_ID=50181316 CONFIRM=PURGE-PROD-CUSTOM-OBJECTS \
 *     node scripts/purge-prod-custom-objects.js
 */

const API_BASE = 'https://api.hubapi.com';
const API_KEY = process.env.HUBSPOT_API_KEY;
const CONFIRM_PHRASE = 'PURGE-PROD-CUSTOM-OBJECTS';

// The four custom objects on PROD. Contacts/Companies are intentionally absent.
const OBJECTS = [
  ['2-60107989', 'Deposits'],
  ['2-60108336', 'Loans'],
  ['2-60108759', 'Time Deposits'],
  ['2-60107457', 'Debit Cards'],
];

// Hard guard: never allow standard people/company objects through this script.
const FORBIDDEN = new Set(['contacts', 'companies', '0-1', '0-2']);

if (!API_KEY) {
  console.error('HUBSPOT_API_KEY env var is required');
  process.exit(1);
}

const ackPortal = process.env.ACK_PORTAL_ID;
if (!ackPortal) {
  console.error(
    'ACK_PORTAL_ID is required. Set it to the portal id you intend to purge\n' +
    '(production is 50181316) so this can confirm the key points there.'
  );
  process.exit(2);
}

const armed = process.env.CONFIRM === CONFIRM_PHRASE;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function hs(method, path, body) {
  for (let attempt = 0; attempt < 8; attempt++) {
    let res;
    try {
      res = await fetch(`${API_BASE}${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: body ? JSON.stringify(body) : undefined,
      });
    } catch (e) {
      const wait = 500 * 2 ** attempt;
      console.log(`  network err ${e.message}, retry ${wait}ms`);
      await sleep(wait);
      continue;
    }
    if (res.status === 429) {
      const wait = 500 * 2 ** attempt;
      console.log(`  429, backoff ${wait}ms`);
      await sleep(wait);
      continue;
    }
    const text = await res.text();
    let json;
    try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
    return { status: res.status, body: json };
  }
  return { status: 0, body: { error: 'gave up after retries' } };
}

async function total(obj) {
  const { body } = await hs('POST', `/crm/v3/objects/${obj}/search`, { limit: 1 });
  return body && typeof body.total === 'number' ? body.total : '?';
}

async function* listIds(obj, limit = 100) {
  let after = null;
  while (true) {
    let path = `/crm/v3/objects/${obj}?limit=${limit}`;
    if (after) path += `&after=${after}`;
    const { status, body } = await hs('GET', path);
    if (status >= 400) {
      console.log(`  list error ${status}: ${body.message || JSON.stringify(body)}`);
      return;
    }
    for (const r of body.results || []) yield r.id;
    after = body.paging && body.paging.next ? body.paging.next.after : null;
    if (!after) return;
  }
}

async function batchArchive(obj, ids) {
  return hs('POST', `/crm/v3/objects/${obj}/batch/archive`, {
    inputs: ids.map((id) => ({ id })),
  });
}

async function safetyCheck() {
  const { status, body } = await hs('GET', '/account-info/v3/details');
  if (status >= 400) {
    console.error(`SAFETY CHECK FAILED: cannot fetch account-info (${status}). Refusing.`);
    process.exit(2);
  }
  const portalId = body.portalId;
  const acctType = body.accountType;
  console.log(`Portal check: portalId=${portalId} accountType=${acctType} (ACK_PORTAL_ID=${ackPortal})`);
  if (String(portalId) !== String(ackPortal)) {
    console.error(
      `SAFETY CHECK FAILED: key targets portal ${portalId}, not ACK_PORTAL_ID=${ackPortal}. Refusing.`
    );
    process.exit(2);
  }
  for (const [obj] of OBJECTS) {
    if (FORBIDDEN.has(obj)) {
      console.error(`SAFETY CHECK FAILED: forbidden object ${obj} in purge list. Refusing.`);
      process.exit(2);
    }
  }
}

async function purge(obj, label) {
  console.log(`\n=== ${label} (${obj}) ===`);
  const pre = await total(obj);
  console.log(`  before: ${pre}`);
  if (pre === 0) return;
  if (!armed) {
    console.log('  DRY RUN — set CONFIRM=PURGE-PROD-CUSTOM-OBJECTS to archive. Skipping.');
    return;
  }
  let archived = 0;
  while (true) {
    const ids = [];
    for await (const id of listIds(obj, 100)) {
      ids.push(id);
      if (ids.length >= 100) break;
    }
    if (ids.length === 0) break;
    const { status, body } = await batchArchive(obj, ids);
    if (status >= 400) {
      console.log(`  archive err ${status}: ${(body.message || JSON.stringify(body)).slice(0, 200)}`);
      await sleep(2000);
      continue;
    }
    archived += ids.length;
    if (archived % 1000 === 0) console.log(`  archived ${archived}...`);
    await sleep(100); // gentle on the rate limiter
  }
  const post = await total(obj);
  console.log(`  archived ${archived}; after: ${post}`);
}

(async () => {
  await safetyCheck();
  if (!armed) {
    console.log('\n*** DRY RUN *** (no CONFIRM) — reporting counts only, archiving nothing.\n');
  } else {
    console.log('\n*** ARMED *** — archiving all records in the 4 custom objects.\n');
  }
  for (const [obj, label] of OBJECTS) await purge(obj, label);
  console.log('\n=== final tally ===');
  for (const [obj, label] of OBJECTS) {
    console.log(`  ${label.padEnd(15)}: ${await total(obj)}`);
  }
})();

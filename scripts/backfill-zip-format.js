#!/usr/bin/env node
/**
 * One-off backfill: reformat existing HubSpot contact + company ZIP codes from
 * raw 9-digit ("448705016") to ZIP+4 ("44870-5016").
 *
 * Why this is needed: the nightly pipeline reformats the zip at SEND time, but
 * the diff engine only re-ships a record when its raw CSV row hash changes — so
 * records loaded before the fix keep their 9-digit zip until something else
 * about them changes. This script remediates the already-loaded records
 * directly, independent of the CSV pipeline.
 *
 * Only clean 9-digit values are touched; 5-digit, already-hyphenated, and any
 * other shapes are left exactly as-is. Idempotent — re-running finds nothing to
 * do once everything is ZIP+4.
 *
 * SAFETY: dry-run by default. It only writes when APPLY=1 is set.
 *   Preview:  railway run --service=civista-integration node scripts/backfill-zip-format.js
 *   Apply:    railway run --service=civista-integration APPLY=1 node scripts/backfill-zip-format.js
 */
const { coerceZip } = require('../src/transform/normalize');

const API_BASE = 'https://api.hubapi.com';
const API_KEY = process.env.HUBSPOT_API_KEY;
const APPLY = process.env.APPLY === '1' || process.env.APPLY === 'true';
if (!API_KEY) { console.error('HUBSPOT_API_KEY not set'); process.exit(1); }

const H = { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' };

async function hs(method, path, body) {
  for (let attempt = 0, delay = 200; ; attempt++) {
    const res = await fetch(`${API_BASE}${path}`, { method, headers: H, body: body ? JSON.stringify(body) : undefined });
    if (res.status === 429 && attempt < 8) { await new Promise(r => setTimeout(r, delay)); delay *= 2; continue; }
    const text = await res.text();
    let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
    return { status: res.status, ok: res.ok, body: json };
  }
}

async function backfillObject(objectType, label) {
  console.log(`\n── ${label} ──`);
  let after = null, scanned = 0, toFix = 0, updated = 0, failed = 0;
  const samples = [];

  for (;;) {
    const qs = new URLSearchParams({ limit: '100', properties: 'zip' });
    if (after) qs.set('after', after);
    const page = await hs('GET', `/crm/v3/objects/${objectType}?${qs.toString()}`);
    if (!page.ok) { console.error(`  ✗ list failed [${page.status}]: ${page.body?.message || ''}`); break; }

    const updates = [];
    for (const o of page.body?.results || []) {
      scanned++;
      const zip = o.properties?.zip;
      if (zip == null || zip === '') continue;
      const next = coerceZip(zip).value;
      if (next && next !== zip) {
        toFix++;
        if (samples.length < 8) samples.push(`${zip} → ${next}`);
        updates.push({ id: o.id, properties: { zip: next } });
      }
    }

    if (updates.length > 0 && APPLY) {
      for (let i = 0; i < updates.length; i += 100) {
        const batch = updates.slice(i, i + 100);
        const r = await hs('POST', `/crm/v3/objects/${objectType}/batch/update`, { inputs: batch });
        if (r.ok) updated += batch.length;
        else { failed += batch.length; console.error(`  ✗ batch update failed [${r.status}]: ${r.body?.message || ''}`); }
      }
    }

    after = page.body?.paging?.next?.after;
    if (!after) break;
  }

  console.log(`  scanned=${scanned}  need_fix=${toFix}  ${APPLY ? `updated=${updated}  failed=${failed}` : '(dry-run, no writes)'}`);
  if (samples.length) console.log(`  examples: ${samples.join(', ')}`);
  return { scanned, toFix, updated, failed };
}

(async () => {
  console.log(APPLY ? '── APPLYING zip backfill ──' : '── DRY RUN (set APPLY=1 to write) ──');
  const c = await backfillObject('contacts', 'Contacts');
  const co = await backfillObject('companies', 'Companies');
  console.log(`\n── Summary ──`);
  console.log(`contacts:  scanned=${c.scanned} need_fix=${c.toFix} updated=${c.updated} failed=${c.failed}`);
  console.log(`companies: scanned=${co.scanned} need_fix=${co.toFix} updated=${co.updated} failed=${co.failed}`);
  if (!APPLY) console.log(`\nNothing was written. Re-run with APPLY=1 to apply.`);
  process.exit((c.failed + co.failed) > 0 ? 1 : 0);
})().catch(e => { console.error('backfill-zip-format.js failed:', e.message || e); process.exit(1); });

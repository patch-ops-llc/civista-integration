#!/usr/bin/env node
/**
 * READ-ONLY validation of the Jun 2026 data-review formatting changes.
 *
 *   1) Confirms the number-property display hints are live in HubSpot
 *      (interest_rate = percentage; balances = currency + symbol).
 *   2) Confirms a sample of association labels are now suffix-free.
 *   3) Pulls a live sample of records and shows the actual stored values
 *      (zip, interest_rate, current_balance) so we can see real data.
 *
 * No writes. Run via Railway shell so the key never leaves the env:
 *   railway run --service=civista-integration node scripts/validate-formatting.js
 */
const API_BASE = 'https://api.hubapi.com';
const API_KEY = process.env.HUBSPOT_API_KEY;
if (!API_KEY) { console.error('HUBSPOT_API_KEY not set'); process.exit(1); }

const H = { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' };

const DEPOSITS = '2-60442978', LOANS = '2-60442977', TIME_DEPOSITS = '2-60442980';

const NUMBER_PROPS = [
  { obj: DEPOSITS,      name: 'interest_rate',      want: 'percentage' },
  { obj: LOANS,         name: 'interest_rate',      want: 'percentage' },
  { obj: TIME_DEPOSITS, name: 'interest_rate',      want: 'percentage' },
  { obj: DEPOSITS,      name: 'current_balance',    want: 'currency' },
  { obj: DEPOSITS,      name: 'yesterdays_balance', want: 'currency' },
  { obj: LOANS,         name: 'current_balance',    want: 'currency' },
  { obj: LOANS,         name: 'original_balance',   want: 'currency' },
  { obj: TIME_DEPOSITS, name: 'current_balance',    want: 'currency' },
];

async function getJson(url, opts = {}) {
  const res = await fetch(url, { headers: H, ...opts });
  const text = await res.text();
  let body; try { body = JSON.parse(text); } catch { body = { raw: text }; }
  return { status: res.status, ok: res.ok, body };
}

async function checkProps() {
  console.log('── 1) Number property display hints ──');
  let ok = 0, bad = 0;
  for (const p of NUMBER_PROPS) {
    const r = await getJson(`${API_BASE}/crm/v3/properties/${p.obj}/${p.name}`);
    const hint = r.body?.numberDisplayHint;
    const sym = r.body?.showCurrencySymbol;
    const pass = hint === p.want && (p.want !== 'currency' || sym === true);
    console.log(`  ${pass ? '✓' : '✗'} ${p.obj} :: ${p.name} → ${hint}${p.want === 'currency' ? ` (showCurrencySymbol=${sym})` : ''}`);
    pass ? ok++ : bad++;
  }
  console.log(`  → ${ok}/${NUMBER_PROPS.length} correct\n`);
  return bad === 0;
}

async function checkLabels() {
  console.log('── 2) Association labels (sample: deposits → contacts) ──');
  const r = await getJson(`${API_BASE}/crm/v4/associations/${DEPOSITS}/contacts/labels`);
  const labels = (r.body?.results || []).filter(x => x.label).map(x => x.label).sort();
  const withSuffix = labels.filter(l => / (DDA|Loan|CD)(\b| Company)/.test(l));
  console.log(`  ${labels.length} labeled types; ${withSuffix.length} still carry a product suffix`);
  console.log(`  sample: ${labels.slice(0, 6).join(' | ')}`);
  if (withSuffix.length) console.log(`  ✗ still suffixed: ${withSuffix.slice(0, 5).join(' | ')}`);
  else console.log('  ✓ all sampled labels are suffix-free');
  console.log('');
  return withSuffix.length === 0;
}

async function sampleContacts() {
  console.log('── 3a) Live contact ZIP values (current state) ──');
  const r = await getJson(`${API_BASE}/crm/v3/objects/contacts?limit=15&properties=zip,cif_number`);
  const rows = (r.body?.results || []).map(o => o.properties?.zip).filter(z => z != null && z !== '');
  let nineDigit = 0, plusFour = 0, other = 0;
  for (const z of rows) {
    if (/^\d{9}$/.test(z)) nineDigit++;
    else if (/^\d{5}-\d{4}$/.test(z)) plusFour++;
    else other++;
  }
  console.log(`  sampled ${rows.length} non-empty zips: ${nineDigit} raw 9-digit, ${plusFour} ZIP+4 (formatted), ${other} other`);
  console.log(`  examples: ${rows.slice(0, 8).join(', ')}`);
  console.log('');
}

async function sampleDeposits() {
  console.log('── 3b) Live deposit values (interest_rate / current_balance) ──');
  const r = await getJson(`${API_BASE}/crm/v3/objects/${DEPOSITS}?limit=8&properties=interest_rate,current_balance,account_key`);
  for (const o of (r.body?.results || [])) {
    const p = o.properties || {};
    console.log(`  ${(p.account_key || o.id).padEnd(22)} interest_rate=${p.interest_rate ?? '-'}  current_balance=${p.current_balance ?? '-'}`);
  }
  console.log('  (API returns raw stored values; the %/$ formatting is applied by the HubSpot UI via the display hints above)\n');
}

(async () => {
  const a = await checkProps();
  const b = await checkLabels();
  await sampleContacts();
  await sampleDeposits();
  console.log('── Summary ──');
  console.log(`number formats: ${a ? 'OK' : 'NEEDS ATTENTION'}`);
  console.log(`labels suffix-free (sample): ${b ? 'OK' : 'NEEDS ATTENTION'}`);
  process.exit(0);
})().catch(e => { console.error('validate-formatting.js failed:', e.message || e); process.exit(1); });

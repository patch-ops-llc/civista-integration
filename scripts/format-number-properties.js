#!/usr/bin/env node
/**
 * Set the display format on the account custom-object number properties so the
 * UI renders them the way Civista expects (data review, Jun 2026):
 *
 *   - interest_rate  → Percentage. HubSpot stores percentage values as the
 *     canonical decimal and renders them x100 with a "%", so the stored
 *     ".0003" / ".0368" shows as "0.03%" / "3.68%". The underlying value is
 *     unchanged — this is purely a display setting, so reporting/calculations
 *     keep the precise decimal.
 *   - balance fields → Currency. The stored number "360" then renders as
 *     "$360.00" (account default currency, two decimals) instead of "360".
 *
 * Only the display hint is changed; values are not touched. Idempotent — safe
 * to re-run (it just re-asserts the same hint).
 *
 * Run via Railway shell so the key never leaves the env:
 *   railway run --service=civista-integration node scripts/format-number-properties.js
 */
const API_BASE = 'https://api.hubapi.com';
const API_KEY = process.env.HUBSPOT_API_KEY;

if (!API_KEY) {
  console.error('HUBSPOT_API_KEY not set');
  process.exit(1);
}

const DEPOSITS = '2-60442978';
const LOANS = '2-60442977';
const TIME_DEPOSITS = '2-60442980';

// { objectType, name, numberDisplayHint }
const UPDATES = [
  // Interest rates → percentage
  { objectType: DEPOSITS,      name: 'interest_rate',      numberDisplayHint: 'percentage' },
  { objectType: LOANS,         name: 'interest_rate',      numberDisplayHint: 'percentage' },
  { objectType: TIME_DEPOSITS, name: 'interest_rate',      numberDisplayHint: 'percentage' },
  // Balances → currency
  { objectType: DEPOSITS,      name: 'current_balance',    numberDisplayHint: 'currency' },
  { objectType: DEPOSITS,      name: 'yesterdays_balance', numberDisplayHint: 'currency' },
  { objectType: LOANS,         name: 'current_balance',    numberDisplayHint: 'currency' },
  { objectType: LOANS,         name: 'original_balance',   numberDisplayHint: 'currency' },
  { objectType: TIME_DEPOSITS, name: 'current_balance',    numberDisplayHint: 'currency' },
];

async function patchProperty({ objectType, name, numberDisplayHint }) {
  const body = { numberDisplayHint };
  // showCurrencySymbol must be on for the currency symbol to render.
  if (numberDisplayHint === 'currency') body.showCurrencySymbol = true;
  const res = await fetch(`${API_BASE}/crm/v3/properties/${objectType}/${name}`, {
    method: 'PATCH',
    headers: { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch { json = { raw: text }; }
  return { status: res.status, ok: res.ok, body: json };
}

(async () => {
  let ok = 0, failed = 0;
  for (const u of UPDATES) {
    const r = await patchProperty(u);
    if (r.ok) {
      console.log(`✓ ${u.objectType} :: ${u.name} → ${u.numberDisplayHint} (display=${r.body?.numberDisplayHint})`);
      ok++;
    } else {
      console.log(`✗ ${u.objectType} :: ${u.name} → ${u.numberDisplayHint} [${r.status}]: ${r.body?.message || r.body?.raw || ''}`);
      failed++;
    }
  }
  console.log(`\nDone. ${ok} updated, ${failed} failed.`);
  process.exit(failed > 0 ? 1 : 0);
})().catch((e) => {
  console.error('format-number-properties.js failed:', e.message || e);
  process.exit(1);
});

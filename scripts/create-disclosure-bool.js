/**
 * Create the boolean `estatement_disclosure_accepted` property on Contacts and
 * Companies.
 *
 * Why a new property: the source column DiscAcpt is Y/N (boolean), but the prod
 * portal's pre-existing `estatement_disclosure_acceptance_date` is typed `date`
 * and is referenced by client-built lists/workflows, so HubSpot refuses to
 * delete+recreate it as a checkbox. Rather than touch the client's automation,
 * the pipeline writes the boolean to this dedicated property.
 *
 * Idempotent: a 409 (already exists) is treated as success and the existing
 * state is printed.
 *
 * Run via Railway shell so the key never leaves the env:
 *   railway ssh node scripts/create-disclosure-bool.js
 */

const API_BASE = 'https://api.hubapi.com';
const API_KEY = process.env.HUBSPOT_API_KEY;

if (!API_KEY) {
  console.error('HUBSPOT_API_KEY not set');
  process.exit(1);
}

const NAME = 'estatement_disclosure_accepted';
const LABEL = 'eStatement Disclosure Accepted';

const TARGETS = [
  { objectType: 'contacts', groupName: 'contactinformation' },
  { objectType: 'companies', groupName: 'companyinformation' },
];

async function hs(method, path, body) {
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json;
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  return { status: res.status, body: json };
}

(async () => {
  for (const { objectType, groupName } of TARGETS) {
    console.log(`\n=== ${objectType} :: ${NAME} ===`);
    const create = await hs('POST', `/crm/v3/properties/${objectType}`, {
      name: NAME,
      label: LABEL,
      groupName,
      type: 'bool',
      fieldType: 'booleancheckbox',
      options: [
        { label: 'Yes', value: 'true', displayOrder: 0, hidden: false },
        { label: 'No', value: 'false', displayOrder: 1, hidden: false },
      ],
    });
    if (create.status === 201) {
      console.log('✓ Created bool/booleancheckbox');
    } else if (create.status === 409) {
      console.log('→ Already exists (409) — verifying');
    } else {
      console.log(`✗ Create failed (${create.status}): ${JSON.stringify(create.body)}`);
    }
    const verify = await hs('GET', `/crm/v3/properties/${objectType}/${NAME}`);
    if (verify.status === 200) {
      const { name, label, type, fieldType, groupName: g } = verify.body;
      console.log(JSON.stringify({ name, label, type, fieldType, groupName: g }));
      if (type !== 'bool') console.log('⚠ type is not bool — check this property');
    } else {
      console.log(`✗ Verify failed (${verify.status})`);
    }
  }
  process.exit(0);
})();

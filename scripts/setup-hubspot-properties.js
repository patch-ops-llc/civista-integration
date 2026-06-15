/**
 * Creates unique identifier properties on HubSpot objects.
 * Must run once before the first sync — enables batch upsert to match
 * records by these keys instead of creating duplicates.
 *
 * Idempotent: if a property already exists, prints its current state.
 */

const API_BASE = 'https://api.hubapi.com';
const API_KEY = process.env.HUBSPOT_API_KEY;

if (!API_KEY) {
  console.error('HUBSPOT_API_KEY not set');
  process.exit(1);
}

const PROPERTIES = [
  { objectType: 'contacts',    name: 'cif_number',    groupName: 'contactinformation', label: 'CIF Number' },
  { objectType: 'companies',   name: 'cif_number',    groupName: 'companyinformation', label: 'CIF Number' },
  // account_key is the account-level upsert identity for the three account
  // objects (one HubSpot record per physical account; multiple owners are
  // attached as labeled associations). primary_key stays as a (now non-keying)
  // attribute carried from the canonical/primary-owner row.
  { objectType: '2-60107989',  name: 'account_key',   groupName: 'deposits_information', label: 'Account Key' },
  { objectType: '2-60108336',  name: 'account_key',   groupName: 'loans_information',    label: 'Account Key' },
  { objectType: '2-60108759',  name: 'account_key',   groupName: 'time_deposits_information', label: 'Account Key' },
  { objectType: '2-60107989',  name: 'primary_key',   groupName: 'deposits_information', label: 'Primary Key' },
  { objectType: '2-60108336',  name: 'primary_key',   groupName: 'loans_information',    label: 'Primary Key' },
  { objectType: '2-60108759',  name: 'primary_key',   groupName: 'time_deposits_information', label: 'Primary Key' },
  { objectType: '2-60107457',  name: 'composite_key', groupName: 'debit_cards_information',   label: 'Composite Key' },
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
  try { json = JSON.parse(text); } catch { json = { raw: text }; }
  return { status: res.status, body: json };
}

async function createProperty({ objectType, name, groupName, label }) {
  const payload = {
    name,
    label,
    groupName,
    type: 'string',
    fieldType: 'text',
    hasUniqueValue: true,
  };
  const { status, body } = await hs('POST', `/crm/v3/properties/${objectType}`, payload);
  return { status, body };
}

async function getProperty(objectType, name) {
  const { status, body } = await hs('GET', `/crm/v3/properties/${objectType}/${name}`);
  return { status, body };
}

(async () => {
  for (const prop of PROPERTIES) {
    console.log(`\n═══ ${prop.objectType} :: ${prop.name} ═══`);

    const create = await createProperty(prop);
    if (create.status === 201) {
      console.log(`✓ Created`);
    } else if (create.status === 409) {
      console.log(`→ Already exists (409) — will verify existing state`);
    } else {
      console.log(`✗ Create failed (${create.status}):`);
      console.log(JSON.stringify(create.body, null, 2));
      // Continue anyway — maybe the group name is wrong but prop exists
    }

    const verify = await getProperty(prop.objectType, prop.name);
    if (verify.status === 200) {
      const { name, label, type, fieldType, hasUniqueValue, groupName, createdAt } = verify.body;
      console.log(JSON.stringify({ name, label, type, fieldType, hasUniqueValue, groupName, createdAt }, null, 2));
      if (!hasUniqueValue) {
        console.log(`⚠  hasUniqueValue is FALSE — this property will NOT work for upsert matching`);
      }
    } else {
      console.log(`✗ Verify failed (${verify.status}):`);
      console.log(JSON.stringify(verify.body, null, 2));
    }
  }
})();

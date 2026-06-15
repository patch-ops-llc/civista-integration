#!/usr/bin/env node
/**
 * READ-ONLY. Pulls the live HubSpot v4 association labels + type IDs for every
 * account<->owner object pair and writes a spec snapshot to
 * docs/association-spec.<portalId>.json. Use it to capture the source-of-truth
 * config for a portal and to verify, via the legend in relationship-map.js,
 * that every expected label resolves to a type ID before running a sync.
 *
 * It makes no writes to HubSpot or the database.
 *
 * Run via Railway shell so the key never leaves the env:
 *   railway run --service=civista-integration node scripts/pull-association-labels.js
 */
const fs = require('fs');
const path = require('path');
const { fetchAssociationLabels, buildLabelIndex } = require('../src/sync/association-labels');
const { OBJECT_SUFFIX, RELATIONSHIP_NAMES, labelFor } = require('../src/transform/relationship-map');

const API_KEY = process.env.HUBSPOT_API_KEY;
if (!API_KEY) {
  console.error('HUBSPOT_API_KEY not set');
  process.exit(1);
}

// Account object id per logical source + the two owner objects.
const ACCOUNT_OBJECTS = {
  dda:   '2-60107989',
  loans: '2-60108336',
  cd:    '2-60108759',
};
const DEBIT_CARDS = '2-60107457';
const OWNER_OBJECTS = [['contact', 'contacts'], ['company', 'companies']];

async function portalId() {
  const r = await fetch('https://api.hubapi.com/account-info/v3/details', {
    headers: { Authorization: `Bearer ${API_KEY}` },
  });
  const body = await r.json().catch(() => ({}));
  return String(body.portalId || body.hubId || 'unknown');
}

(async () => {
  const pid = await portalId();
  console.log(`Pulling association labels for portal ${pid}\n`);

  const spec = { portalId: pid, capturedAt: new Date().toISOString(), pairs: {} };
  let missing = 0;

  for (const [source, accountObj] of Object.entries(ACCOUNT_OBJECTS)) {
    for (const [kind, ownerObj] of OWNER_OBJECTS) {
      const results = await fetchAssociationLabels(accountObj, ownerObj);
      const { byLabel, defaultTypeId } = buildLabelIndex(results);
      spec.pairs[`${accountObj}->${ownerObj}`] = {
        source, ownerKind: kind, defaultTypeId,
        labels: Object.fromEntries(byLabel),
      };

      // Verify every legend code resolves to a configured label.
      const unresolved = [];
      for (const code of Object.keys(RELATIONSHIP_NAMES)) {
        const want = labelFor(source, kind, code);
        if (want && !byLabel.has(want)) unresolved.push(`${code} -> "${want}"`);
      }
      missing += unresolved.length;
      console.log(`${accountObj} -> ${ownerObj} (${source}/${kind}): ${byLabel.size} labels, default=${defaultTypeId ?? 'none'}`);
      if (unresolved.length) console.log(`  ⚠ unresolved: ${unresolved.join(', ')}`);
    }
  }

  // Debit cards: unlabeled (default) link only.
  for (const [kind, ownerObj] of OWNER_OBJECTS) {
    const results = await fetchAssociationLabels(DEBIT_CARDS, ownerObj);
    const { byLabel, defaultTypeId } = buildLabelIndex(results);
    spec.pairs[`${DEBIT_CARDS}->${ownerObj}`] = {
      source: 'debit_cards', ownerKind: kind, defaultTypeId,
      labels: Object.fromEntries(byLabel),
    };
    console.log(`${DEBIT_CARDS} -> ${ownerObj} (debit_cards/${kind}): default=${defaultTypeId ?? 'none'}`);
  }

  const outDir = path.join(__dirname, '..', 'docs');
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `association-spec.${pid}.json`);
  fs.writeFileSync(outPath, JSON.stringify(spec, null, 2));
  console.log(`\nSpec written to ${outPath}`);
  console.log(missing === 0
    ? '✓ All legend codes resolve to configured labels.'
    : `⚠ ${missing} legend code(s) have no matching label in this portal — configure them before syncing associations.`);
  process.exit(0);
})().catch((e) => {
  console.error('pull-association-labels.js failed:', e.message || e);
  process.exit(1);
});

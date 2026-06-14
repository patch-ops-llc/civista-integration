#!/usr/bin/env node
/**
 * One-time migration: strip the product / owner-kind suffix from the existing
 * HubSpot association labels so each relationship reads as just its name
 * ("PRIMARY OWNER", "CO-OWNER") instead of "PRIMARY OWNER DDA" /
 * "PRIMARY OWNER DDA Company".
 *
 * Per the Civista data review (Jun 2026): the client wants the owner
 * relationship to be consistent regardless of product (DDA / Loan / CD) or
 * owner kind (contact / company). This renames the labels IN PLACE via the v4
 * update endpoint, which preserves each label's associationTypeId and every
 * association already created with it — nothing has to be re-linked.
 *
 * Safe to re-run:
 *   - A label already at its bare name is skipped (idempotent).
 *   - If two source labels in the same pair would collapse to the same name
 *     (e.g. the portal's duplicate "CROSS-REF CD Compan" typo next to
 *     "CROSS-REF CD Company"), only the first is renamed; the rest are left
 *     alone and reported so a human can clean up the stray label.
 *   - Any live label that doesn't match a known "<NAME> <SUFFIX>[ Company]"
 *     pattern is left untouched and reported.
 *
 * Preview without writing:  DRY_RUN=1 node scripts/rename-association-labels.js
 * Apply:                    node scripts/rename-association-labels.js
 *
 * Run via Railway shell so the key never leaves the env:
 *   railway run --service=civista-integration node scripts/rename-association-labels.js
 *
 * After applying, re-run scripts/pull-association-labels.js to confirm every
 * legend code resolves to a configured (now suffix-free) label.
 */
const { fetchAssociationLabels } = require('../src/sync/association-labels');
const { RELATIONSHIP_NAMES, OBJECT_SUFFIX } = require('../src/transform/relationship-map');

const API_BASE = 'https://api.hubapi.com';
const API_KEY = process.env.HUBSPOT_API_KEY;
const DRY_RUN = process.env.DRY_RUN === '1' || process.env.DRY_RUN === 'true';

if (!API_KEY) {
  console.error('HUBSPOT_API_KEY not set');
  process.exit(1);
}

// Account object id per logical source + the two owner objects.
const ACCOUNT_OBJECTS = { dda: '2-60442978', loans: '2-60442977', cd: '2-60442980' };
const OWNER_OBJECTS = [['contact', 'contacts'], ['company', 'companies']];

/**
 * Build a lookup of the OLD label string -> bare relationship name for a given
 * (source, ownerKind), e.g. "PRIMARY OWNER DDA Company" -> "PRIMARY OWNER".
 * Reconstructing the old strings (rather than blindly trimming) means a typo
 * label that doesn't match the old scheme is left untouched, not mangled.
 */
function oldLabelToBareName(source, ownerKind) {
  const suffix = OBJECT_SUFFIX[source];
  const map = new Map();
  for (const name of Object.values(RELATIONSHIP_NAMES)) {
    const base = `${name} ${suffix}`;
    const oldLabel = ownerKind === 'company' ? `${base} Company` : base;
    map.set(oldLabel, name);
  }
  return map;
}

async function updateLabel(fromObj, toObj, associationTypeId, label) {
  const res = await fetch(`${API_BASE}/crm/v4/associations/${fromObj}/${toObj}/labels`, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ associationTypeId, label }),
  });
  const text = await res.text();
  let body; try { body = JSON.parse(text); } catch { body = { raw: text }; }
  return { status: res.status, ok: res.ok, body };
}

(async () => {
  console.log(DRY_RUN
    ? '── DRY RUN — no labels will be modified ──\n'
    : '── Applying label renames ──\n');

  let renamed = 0, skippedAlready = 0, skippedCollision = 0, unmatched = 0, failed = 0;

  for (const [source, accountObj] of Object.entries(ACCOUNT_OBJECTS)) {
    for (const [kind, ownerObj] of OWNER_OBJECTS) {
      const bareNames = new Set(Object.values(RELATIONSHIP_NAMES));
      const oldMap = oldLabelToBareName(source, kind);
      const results = await fetchAssociationLabels(accountObj, ownerObj);

      // Track which bare names already exist / get claimed in this pair so we
      // never create a duplicate label within the same association definition.
      const claimed = new Set(
        results.filter(r => r.label && bareNames.has(r.label)).map(r => r.label)
      );

      console.log(`\n${accountObj} -> ${ownerObj} (${source}/${kind})`);
      for (const r of results) {
        if (!r.label) continue; // the unlabeled/default type — leave it
        if (bareNames.has(r.label)) { skippedAlready++; continue; } // already bare

        const target = oldMap.get(r.label);
        if (!target) {
          console.log(`  ?  unmatched label left as-is: "${r.label}" (typeId ${r.typeId})`);
          unmatched++;
          continue;
        }
        if (claimed.has(target)) {
          console.log(`  ⚠  collision: "${r.label}" -> "${target}" already exists in this pair; leaving "${r.label}" (typeId ${r.typeId}) for manual cleanup`);
          skippedCollision++;
          continue;
        }

        if (DRY_RUN) {
          console.log(`  →  would rename "${r.label}" -> "${target}" (typeId ${r.typeId})`);
          claimed.add(target);
          renamed++;
          continue;
        }

        const upd = await updateLabel(accountObj, ownerObj, r.typeId, target);
        if (upd.ok) {
          console.log(`  ✓  "${r.label}" -> "${target}" (typeId ${r.typeId})`);
          claimed.add(target);
          renamed++;
        } else {
          console.log(`  ✗  failed "${r.label}" -> "${target}" (typeId ${r.typeId}) [${upd.status}]: ${upd.body?.message || upd.body?.raw || ''}`);
          failed++;
        }
      }
    }
  }

  console.log(`\n── Summary ──`);
  console.log(`${DRY_RUN ? 'would rename' : 'renamed'}: ${renamed}`);
  console.log(`already bare (skipped): ${skippedAlready}`);
  console.log(`collisions (skipped, manual cleanup): ${skippedCollision}`);
  console.log(`unmatched (left as-is): ${unmatched}`);
  console.log(`failed: ${failed}`);
  if (!DRY_RUN) console.log(`\nNext: re-run scripts/pull-association-labels.js to verify every legend code resolves.`);
  process.exit(failed > 0 ? 1 : 0);
})().catch((e) => {
  console.error('rename-association-labels.js failed:', e.message || e);
  process.exit(1);
});

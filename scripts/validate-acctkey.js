#!/usr/bin/env node
/**
 * READ-ONLY validation for Keith's new account-level key column.
 *
 * Operates purely on a CSV file — no database, no HubSpot. It confirms the new
 * key does what we need before we adopt it: every owner row of one physical
 * account shares the same key, and grouping on it removes the last-4 collisions
 * that the provisional branch|type|last4 key produced.
 *
 *   node scripts/validate-acctkey.js <csvPath> <dda|loans|cd>
 *
 * Auto-detects the new key column (a header like "AcctKey" / "AccountKey" that
 * isn't the existing PrimaryKey). Reports, for the NEW key vs the OLD key:
 *   - collisions  = keys covering >1 distinct PRIMARY-owner CIF  (NEW must be 0)
 *   - grouping    = distinct keys, multi-owner groups (proves it collapses owners)
 *   - integrity   = blank keys, and key groups whose branch/type/last4 disagree
 *
 * Exit code 0 = PASS (new key safe to adopt), 1 = FAIL, 2 = usage/IO error.
 */
const fs = require('fs');
const { parse } = require('csv-parse/sync');

// Logical column -> candidate CSV headers per source (resolved case-insensitively).
const SRC = {
  dda:   { cif: 'CIF#',   rel: 'relationship', branch: 'branchnum', type: 'accttype', last4: 'Acctlast4' },
  loans: { cif: 'CIFNum', rel: 'relationship', branch: 'branchnum', type: 'accttype', last4: 'acctlast4' },
  cd:    { cif: 'CIFNum', rel: 'relationship', branch: 'branchnum', type: 'accttype', last4: 'AcctLast4' },
};

const norm = (v) => (v === null || v === undefined ? '' : String(v).trim());
const normCode = (v) => norm(v).toUpperCase();

function buildResolver(headers) {
  const idx = new Map();
  for (const h of headers) idx.set(h.trim().toLowerCase(), h);
  return (name) => idx.get(String(name).trim().toLowerCase());
}

/** Find the new account-key column: prefer a strict acct/account-key name, never PrimaryKey. */
function detectKeyColumn(headers) {
  const notPrimary = (h) => !/primary/i.test(h);
  const strict = headers.filter(h => /\b(acct|account)[ _]?key\b/i.test(h) && notPrimary(h));
  if (strict.length === 1) return { col: strict[0], how: 'strict acct/account-key match' };
  const broad = headers.filter(h => /(key|hash)/i.test(h) && notPrimary(h));
  if (broad.length === 1) return { col: broad[0], how: 'sole key/hash column (non-PrimaryKey)' };
  return { col: null, candidates: broad.length ? broad : headers };
}

function collisions(rows, keyOf, cifOf, relOf) {
  // key -> Set of distinct primary-owner CIFs
  const primByKey = new Map();
  for (const r of rows) {
    if (normCode(relOf(r)) !== 'P') continue;
    const k = keyOf(r);
    const cif = norm(cifOf(r));
    if (k === '' || cif === '') continue;
    if (!primByKey.has(k)) primByKey.set(k, new Set());
    primByKey.get(k).add(cif);
  }
  const collided = [];
  for (const [k, set] of primByKey) if (set.size > 1) collided.push({ key: k, primaries: Array.from(set) });
  return collided;
}

function validateFile(csvPath, source) {
  const cfg = SRC[source];
  if (!cfg) throw new Error(`unknown source "${source}" (expected dda|loans|cd)`);
  const text = fs.readFileSync(csvPath, 'utf8');
  const rows = parse(text, { columns: true, trim: true, skip_empty_lines: true, relax_column_count: true, relax_quotes: true, bom: true });
  if (rows.length === 0) throw new Error('file has no data rows');

  const headers = Object.keys(rows[0]);
  const resolve = buildResolver(headers);
  const det = detectKeyColumn(headers);

  const cifH = resolve(cfg.cif), relH = resolve(cfg.rel);
  const brH = resolve(cfg.branch), tyH = resolve(cfg.type), l4H = resolve(cfg.last4);

  const cifOf = (r) => r[cifH];
  const relOf = (r) => r[relH];
  const oldKeyOf = (r) => {
    const p = [norm(r[brH]), norm(r[tyH]), norm(r[l4H])];
    return p.some(x => x === '') ? '' : p.join('|');
  };

  const result = { source, csvPath, rows: rows.length, headers, detected: det, pass: false, problems: [] };
  if (!cifH) result.problems.push(`could not find CIF column (expected ~"${cfg.cif}")`);
  if (!relH) result.problems.push(`could not find relationship column (expected ~"${cfg.rel}")`);
  if (!det.col) {
    result.problems.push(`could not unambiguously detect the new account-key column. Candidates: ${(det.candidates || []).join(', ')}`);
    return result; // can't validate without the key
  }
  const newKeyH = det.col;
  const newKeyOf = (r) => norm(r[newKeyH]);

  // Integrity: blanks on the new key.
  const blanks = rows.filter(r => newKeyOf(r) === '').length;

  // Grouping stats on the new key.
  const byNew = new Map();
  for (const r of rows) { const k = newKeyOf(r); if (k === '') continue; (byNew.get(k) || byNew.set(k, []).get(k)).push(r); }
  let multiOwner = 0;
  let inconsistent = 0;
  const inconsistentSamples = [];
  for (const [k, grp] of byNew) {
    if (grp.length > 1) multiOwner++;
    // Within one account key, branch/type/last4 should be constant.
    const sig = new Set(grp.map(r => `${norm(r[brH])}|${norm(r[tyH])}|${norm(r[l4H])}`));
    if (sig.size > 1) { inconsistent++; if (inconsistentSamples.length < 5) inconsistentSamples.push({ key: k, variants: Array.from(sig) }); }
  }

  const newCollisions = collisions(rows, newKeyOf, cifOf, relOf);
  const oldCollisions = collisions(rows, oldKeyOf, cifOf, relOf);

  result.newKeyColumn = newKeyH;
  result.distinctNewKeys = byNew.size;
  result.multiOwnerAccounts = multiOwner;
  result.blankNewKeys = blanks;
  result.inconsistentGroups = inconsistent;
  result.inconsistentSamples = inconsistentSamples;
  result.newKeyCollisions = newCollisions.length;
  result.oldKeyCollisions = oldCollisions.length;
  result.newCollisionSamples = newCollisions.slice(0, 10);

  if (blanks > 0) result.problems.push(`${blanks} row(s) have a blank ${newKeyH}`);
  if (newCollisions.length > 0) result.problems.push(`${newCollisions.length} new-key group(s) still cover >1 primary owner (key is not account-unique)`);
  if (inconsistent > 0) result.problems.push(`${inconsistent} new-key group(s) span >1 branch|type|last4 (key may merge unrelated accounts)`);

  result.pass = result.problems.length === 0 && multiOwner > 0;
  if (multiOwner === 0) result.problems.push('no key grouped >1 owner row — key may be per-row, not per-account (would reintroduce duplicates)');
  return result;
}

function printResult(r) {
  console.log(`\n===== ${r.source.toUpperCase()}  (${r.csvPath}) =====`);
  console.log(`rows: ${r.rows}`);
  if (r.detected && r.detected.col) console.log(`detected new key column: "${r.detected.col}"  (${r.detected.how})`);
  if (r.newKeyColumn) {
    console.log(`distinct accounts (new key) : ${r.distinctNewKeys}`);
    console.log(`multi-owner accounts        : ${r.multiOwnerAccounts}`);
    console.log(`blank new keys              : ${r.blankNewKeys}`);
    console.log(`NEW key collisions (>1 prim): ${r.newKeyCollisions}   <-- must be 0`);
    console.log(`OLD key collisions (b|t|l4) : ${r.oldKeyCollisions}   (for comparison)`);
    console.log(`groups spanning >1 b|t|l4   : ${r.inconsistentGroups}`);
    if (r.inconsistentSamples && r.inconsistentSamples.length) {
      for (const s of r.inconsistentSamples) console.log(`    ${s.key} -> ${s.variants.join(' , ')}`);
    }
    if (r.newCollisionSamples && r.newCollisionSamples.length) {
      console.log('  sample NEW-key collisions:');
      for (const s of r.newCollisionSamples) console.log(`    ${s.key}  (${s.primaries.length} primaries: ${s.primaries.join(', ')})`);
    }
  }
  if (r.problems.length) { console.log('PROBLEMS:'); for (const p of r.problems) console.log(`  - ${p}`); }
  console.log(r.pass ? '\n✓ PASS — new key is account-consistent and collision-free.' : '\n✘ FAIL — see problems above.');
}

module.exports = { validateFile, printResult, SRC };

if (require.main === module) {
  const [csvPath, source] = process.argv.slice(2);
  if (!csvPath || !source) { console.error('usage: node scripts/validate-acctkey.js <csvPath> <dda|loans|cd>'); process.exit(2); }
  try {
    const r = validateFile(csvPath, source);
    printResult(r);
    process.exit(r.pass ? 0 : 1);
  } catch (e) { console.error(`error: ${e.message}`); process.exit(2); }
}

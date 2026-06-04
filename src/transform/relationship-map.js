/**
 * Relationship-code legend (source: Ivan / Civista — Silver Lake export).
 *
 * The account files (DDA / Loans / CDs) tag each owner with a single-letter
 * `relationship` code. The codes are NON-INTUITIVE (B = CUSTODIAN, not
 * Beneficiary; H = BENEFICIARY; M = TRUSTEE) — guessing would mislabel
 * relationships on a financial system, so this map is authoritative.
 *
 * HubSpot association labels were configured per object x owner-kind with these
 * exact strings:
 *   - Contacts:  "<NAME> <SUFFIX>"            e.g. "PRIMARY OWNER DDA"
 *   - Companies: "<NAME> <SUFFIX> Company"    e.g. "PRIMARY OWNER DDA Company"
 * where SUFFIX is DDA / Loan / CD. `labelFor()` reproduces those strings so the
 * association engine can match them to the live association-type IDs pulled
 * from HubSpot (see src/sync/associations.js + scripts/pull-association-labels.js).
 */

// code -> base relationship name (identical across all three account objects).
const RELATIONSHIP_NAMES = {
  A: 'POA',
  B: 'CUSTODIAN',
  C: 'CO-OWNER',
  D: 'ADMINISTRATOR',
  E: 'EXECUTOR',
  F: 'CO-SIGNER',
  G: 'GUARANTOR',
  H: 'BENEFICIARY',
  I: 'INFLUENCE',
  J: 'SECRETARY',
  K: 'DEPUTY',
  L: 'LTD ENDORS',
  M: 'TRUSTEE',
  N: 'GUARDIAN',
  O: 'BENEFICIAL OWNER',
  P: 'PRIMARY OWNER',
  Q: 'INQRY ONLY',
  R: 'REP PAYEE',
  S: 'SIGNER',
  T: 'ALT. SS#',
  U: 'PRESIDENT',
  W: 'AGENT',
  X: 'CROSS-REF',
};

// Logical source key (matches TABLES / ACCOUNT_SOURCES) -> label suffix.
const OBJECT_SUFFIX = {
  dda: 'DDA',
  loans: 'Loan',
  cd: 'CD',
};

// The relationship code that marks the primary owner — used to pick the
// canonical row when collapsing multiple owner rows into one account record.
const PRIMARY_OWNER_CODE = 'P';

/**
 * Normalize a raw `relationship` value to a single uppercase code letter.
 * Source values arrive trimmed (csv-parser), but Ivan's legend keys carried
 * trailing whitespace, so we defend against both. Returns null for blanks.
 */
function normalizeCode(raw) {
  if (raw === null || raw === undefined) return null;
  const c = String(raw).trim().toUpperCase();
  return c === '' ? null : c;
}

/**
 * Exact HubSpot association label for (source, ownerKind, code), or null if the
 * code/source is unknown. ownerKind is 'contact' or 'company'.
 */
function labelFor(source, ownerKind, code) {
  const suffix = OBJECT_SUFFIX[source];
  if (!suffix) return null;
  const norm = normalizeCode(code);
  const name = norm ? RELATIONSHIP_NAMES[norm] : null;
  if (!name) return null;
  const base = `${name} ${suffix}`;
  return ownerKind === 'company' ? `${base} Company` : base;
}

module.exports = {
  RELATIONSHIP_NAMES,
  OBJECT_SUFFIX,
  PRIMARY_OWNER_CODE,
  normalizeCode,
  labelFor,
};

/**
 * Relationship-code legend (source: Ivan / Civista — Silver Lake export).
 *
 * The account files (DDA / Loans / CDs) tag each owner with a single-letter
 * `relationship` code. The codes are NON-INTUITIVE (B = CUSTODIAN, not
 * Beneficiary; H = BENEFICIARY; M = TRUSTEE) — guessing would mislabel
 * relationships on a financial system, so this map is authoritative.
 *
 * Association label scheme (updated per Civista data review, Jun 2026):
 * labels are now just the bare relationship name — e.g. "PRIMARY OWNER",
 * "CO-OWNER" — with NO product suffix and NO " Company" suffix. The old scheme
 * ("PRIMARY OWNER DDA", "PRIMARY OWNER DDA Company") made the same role read
 * differently per account product and per owner kind, which the client flagged
 * as inconsistent. The relationship is the same regardless of which product or
 * owner type it sits on, so one clean label is used everywhere.
 *
 * `labelFor()` returns that bare name so the association engine can match it to
 * the live association-type IDs pulled from HubSpot (see src/sync/associations.js
 * + scripts/pull-association-labels.js). The portal labels must be renamed to
 * match — run scripts/rename-association-labels.js once to strip the suffixes
 * from the existing labels (preserves type IDs and existing associations).
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
 * code/source is unknown.
 *
 * The label is the bare relationship name (e.g. "PRIMARY OWNER") with no
 * product or owner-kind suffix — the same role reads identically across DDA /
 * Loan / CD and across contact / company owners. `source` is still validated
 * against the known account sources so debit-card / unmapped sources don't
 * accidentally get a labeled association; `ownerKind` is retained for signature
 * stability but no longer changes the label.
 */
function labelFor(source, ownerKind, code) {
  if (!OBJECT_SUFFIX[source]) return null;
  const norm = normalizeCode(code);
  const name = norm ? RELATIONSHIP_NAMES[norm] : null;
  if (!name) return null;
  return name;
}

module.exports = {
  RELATIONSHIP_NAMES,
  OBJECT_SUFFIX,
  PRIMARY_OWNER_CODE,
  normalizeCode,
  labelFor,
};

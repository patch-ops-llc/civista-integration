/**
 * Pure value coercion helpers. The old object-building functions
 * (normalizeCifContact, normalizeDda, etc.) are gone — their job is
 * now done by buildPayload() in src/sync/hubspot.js which uses the
 * canonical mapping in src/transform/hubspot-mapping.js.
 *
 * These helpers are called from hubspot.js during type coercion.
 */

function normalizeEmail(val) {
  if (!val || val.toLowerCase() === 'none' || val.trim() === '') return null;
  return val.trim();
}

/**
 * Validate an email for HubSpot's email property type. HubSpot rejects values
 * like "collect" or " " with INVALID_EMAIL — and because batch upsert is
 * all-or-nothing, ONE invalid email kills 99 valid records in the batch.
 *
 * Per memory financial_data_rules.md: the raw value is preserved verbatim in
 * raw_csv on the staging row. This helper only decides whether the value is
 * fit to transmit.
 *
 * Returns { value, problem? }:
 *   value:    trimmed email if it parses as user@host.tld, else null
 *   problem:  human-readable reason when the value was present but invalid
 */
// Source CRM uses sentinel strings (alongside blanks) to indicate "no email
// on file / do not contact via email" for a customer. These are explicit
// opt-out signals from Civista's data, not malformed addresses, so they
// resolve to null without firing the email_suspect loud.warn that genuine
// malformed inputs (like "abc123" or "foo@") still trigger.
const NO_EMAIL_INDICATORS = new Set(['', 'none', 'collect', 'decline', 'declined']);

function coerceEmailForHubSpot(val) {
  if (val === null || val === undefined) return { value: null };
  const raw = String(val);
  const trimmed = raw.trim();
  if (NO_EMAIL_INDICATORS.has(trimmed.toLowerCase())) return { value: null };
  // Loose RFC-ish check: local@domain.tld with no whitespace or angle brackets.
  // Aligns with HubSpot's built-in INVALID_EMAIL validator without going full RFC 5322.
  if (/^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/.test(trimmed)) return { value: trimmed.toLowerCase() };
  const preview = trimmed.length > 40 ? trimmed.slice(0, 40) + '…' : trimmed;
  return { value: null, problem: `unparseable email: "${preview}"` };
}

function normalizeBoolean(val) {
  if (!val) return false;
  return val.trim().toUpperCase() === 'Y';
}

/**
 * Deceased flag — source convention: single space or 'N' means alive (false),
 * 'Y' means deceased (true). Required by CLAUDE.md edge case #5.
 */
function normalizeDeceased(val) {
  if (!val || val.trim() === '' || val === ' ' || val.trim().toUpperCase() === 'N') return false;
  return val.trim().toUpperCase() === 'Y';
}

function normalizeNumber(val) {
  if (!val || val.trim() === '') return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : num;
}

function normalizeDate(val) {
  if (!val || val.trim() === '') return null;
  const trimmed = val.trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
    if (trimmed.includes(' ')) {
      return new Date(trimmed.replace(' ', 'T')).toISOString();
    }
    return trimmed;
  }
  return trimmed;
}

/**
 * Coerce a raw CSV value into something HubSpot's `date` (midnight-UTC) type
 * will accept, OR signal that the value cannot be coerced.
 *
 * HubSpot's `date` type requires either a `YYYY-MM-DD` string or a unix-millis
 * value at exactly midnight UTC. Many Civista columns mapped to date ship
 * full timestamps (`2026-03-18 19:24:34.013000000`) which HubSpot rejects.
 *
 * Per memory financial_data_rules.md: we never destroy source data — the raw
 * value is preserved verbatim in raw_csv JSONB on the staging row. This helper
 * only produces the value sent to HubSpot.
 *
 * Returns:
 *   { value: 'YYYY-MM-DD' }            on success
 *   { value: null }                    when input is empty/null (no problem to surface)
 *   { value: null, problem: '<reason>' } when input is present but unparseable
 */
function coerceDateForHubSpot(val) {
  if (val === null || val === undefined) return { value: null };
  const raw = String(val);
  if (raw.trim() === '') return { value: null };
  const trimmed = raw.trim();

  // Fast path: already YYYY-MM-DD with no time → pass through.
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return { value: trimmed };

  // YYYY-MM-DD HH:MM:SS[.fraction] → strip the time, keep the date portion.
  const m = trimmed.match(/^(\d{4}-\d{2}-\d{2})[T\s]\d{2}:\d{2}/);
  if (m) return { value: m[1] };

  // No `new Date()` last-resort. JS Date is permissive and accepts ambiguous
  // inputs like "1" → 2001-01-01 or "03/18/26" → varies by locale. For
  // financial data we refuse to materialize a date out of garbage; surface
  // the problem and let the operator decide. Raw value is still preserved
  // verbatim in raw_csv on the staging row.
  const preview = trimmed.length > 40 ? trimmed.slice(0, 40) + '…' : trimmed;
  return { value: null, problem: `unparseable date (refusing JS Date fallback): "${preview}"` };
}

module.exports = {
  normalizeEmail,
  normalizeBoolean,
  normalizeDeceased,
  normalizeNumber,
  normalizeDate,
  coerceDateForHubSpot,
  coerceEmailForHubSpot,
};

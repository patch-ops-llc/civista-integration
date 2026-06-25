const fs = require('fs');
const crypto = require('crypto');
const { Transform } = require('stream');
const { parse } = require('csv-parse');
const { pool } = require('../../db/init');
const {
  TABLES,
  CIF_CONTACT_FIELDS,
  CIF_COMPANY_FIELDS,
  buildAccountKey,
  classifyCifRow,
} = require('../transform/hubspot-mapping');
const { normalizeCode, PRIMARY_OWNER_CODE } = require('../transform/relationship-map');
const loud = require('../monitoring/loud');

/**
 * Canonical-JSON hash of a row object. HASH A in the three-stage design.
 * Sort keys for stability so identical rows hash identically regardless of
 * key insertion order. Uses the verbatim values — no trim, no normalize.
 *
 * Two normalization rules MATTER for HASH A == HASH B to round-trip
 * cleanly through Postgres JSONB:
 *   - undefined / null / '' all collapse to '' before hashing. csv-parse
 *     emits '' for empty cells; JSON.stringify drops `undefined` keys
 *     entirely; JSONB returns the stored representation. Without this,
 *     "missing column" rows hash differently before vs. after INSERT.
 *   - Strip the UTF-8 BOM from the FIRST key only. Silver Lake / Jack
 *     Henry exports occasionally start the file with `﻿`, which
 *     csv-parse pulls into the first header. JSONB stores the BOM
 *     verbatim, so it round-trips, but downstream column lookups would
 *     mismatch. Strip once at the top.
 */
function canonicalRowHash(row) {
  const sorted = {};
  for (const k of Object.keys(row).sort()) {
    const v = row[k];
    sorted[k] = (v === null || v === undefined) ? '' : v;
  }
  return crypto.createHash('sha256').update(JSON.stringify(sorted)).digest('hex');
}

/**
 * Strip a leading UTF-8 BOM from any keys that have one. csv-parse pulls
 * the BOM into the first header on Windows-saved files. We do this on a
 * COPY so the verbatim row passed to canonicalRowHash above is preserved
 * (the JSONB raw_csv keeps the BOM if it was there) — only the dictionary
 * we use for column lookup is normalized.
 */
function stripBomKeys(row) {
  const out = {};
  for (const k of Object.keys(row)) {
    out[k.replace(/^﻿/, '')] = row[k];
  }
  return out;
}

/**
 * QuoteFixer — stream transform that rewrites RFC-non-compliant CSV
 * from Silver Lake (unescaped double-quotes inside quoted fields, e.g.
 * `"405 "I" Street"`) into RFC-4180-compliant CSV by doubling any quote
 * that appears mid-field.
 *
 * State machine:
 *   inQuote = false → copy bytes through; flip to true on every `"`
 *   inQuote = true  → `"` is ambiguous:
 *     - followed by delimiter/newline/EOF ⇒ true close-quote (flip to false)
 *     - followed by another `"`          ⇒ already-escaped (pass both through)
 *     - otherwise                         ⇒ embedded unescaped quote — double it
 *
 * Without this, a single bad address halts the entire CIF parse.
 */
class QuoteFixer extends Transform {
  constructor(opts) {
    super(opts);
    this.inQuote = false;
    this.buf = '';
    this.fixCount = 0;
  }
  _transform(chunk, enc, cb) {
    const s = this.buf + chunk.toString('utf8');
    let out = '';
    let i = 0;
    // Process all but the last char so we always have a lookahead.
    while (i < s.length - 1) {
      const c = s[i], n = s[i + 1];
      if (!this.inQuote) {
        out += c;
        if (c === '"') this.inQuote = true;
      } else {
        if (c === '"') {
          if (n === ',' || n === '\n' || n === '\r') {
            out += c;
            this.inQuote = false;
          } else if (n === '"') {
            out += c + n;
            i++;
          } else {
            out += c + c;
            this.fixCount++;
          }
        } else {
          out += c;
        }
      }
      i++;
    }
    this.buf = s.substring(i);
    cb(null, out);
  }
  _flush(cb) {
    cb(null, this.buf);
  }
}

/**
 * Derive expected headers for a source from the mapping module.
 * For CIF, use the union of contact + company CSV columns since the
 * raw CSV doesn't know which row is which yet.
 */
function expectedHeadersFor(source) {
  if (source === 'cif') {
    const set = new Set();
    for (const f of CIF_CONTACT_FIELDS) set.add(f.csv);
    for (const f of CIF_COMPANY_FIELDS) set.add(f.csv);
    return Array.from(set);
  }
  return TABLES[source].fields.map(f => f.csv);
}

function generateRowHash(values) {
  return crypto.createHash('sha256').update(values.join('|')).digest('hex');
}

function generateCompositeKeyFromRaw(row) {
  const cif  = row['CIF#']           || '';
  const acct = row['Acctlast4']      || '';
  const type = row['AcctType']       || '';
  const card = row['Last4DebitCard'] || '';
  const exp  = row['Expiredate']     || '';
  return `${cif}${acct}${type}${card}${exp}`;
}

function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', (c) => hash.update(c));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

function validateHeaders(actual, expected) {
  const trimmedActual = actual.map(h => h.trim());
  const missing = expected.filter(h => !trimmedActual.includes(h));
  if (missing.length > 0) {
    throw new Error(`Missing expected headers: ${missing.join(', ')}`);
  }
  return trimmedActual;
}

/**
 * Map a raw CSV row into an object keyed by HubSpot property names,
 * using the supplied field list. Any field in the list whose CSV column
 * isn't in the row gets null. Fields in the row that aren't in the list
 * are dropped (but the source row is preserved verbatim in raw_csv JSONB
 * on the staging row, per memory financial_data_rules.md).
 *
 * Trim happens on the COPY produced here, never on the source row passed in.
 */
function mapRowToHubspotProps(row, fieldList) {
  const out = {};
  for (const { csv, prop } of fieldList) {
    const v = row[csv];
    if (v === undefined || v === null) {
      out[prop] = null;
    } else if (typeof v === 'string') {
      const trimmed = v.trim();
      out[prop] = trimmed === '' ? null : trimmed;
    } else {
      out[prop] = v === '' ? null : v;
    }
  }
  return out;
}

/**
 * Parse a CSV and stage it into the appropriate Postgres staging table(s).
 *
 *   source === 'cif'          → routes to stg_contacts or stg_companies per row
 *   source === 'dda'          → stg_deposits
 *   source === 'loans'        → stg_loans
 *   source === 'cd'           → stg_time_deposits
 *   source === 'debit_cards'  → stg_debit_cards
 *
 * Returns a summary { rowCount, fileHash, byTable: { stg_contacts: N, ... }, unclassified: [...] }.
 */
async function parseAndStage(filePath, source) {
  const expected = expectedHeadersFor(source);
  const fileHash = await hashFile(filePath);

  // Capture the start timestamp BEFORE any INSERTs so verifyDbPersistence
  // can scope its SELECT to "rows from this parse" and not race with
  // overlapping /sync invocations.
  const sinceTs = new Date().toISOString();

  // Buckets to collect rows per target staging table.
  const buckets = {};
  // For account sources (DDA / Loans / CDs) the same physical account appears
  // as one CSV row per owner. We collect every owner row here and, at end,
  // collapse them onto one account record per account_key while keeping every
  // owner row for the association engine (Option B — one record per physical
  // account with multiple labeled owner associations).
  const accountEntries = [];
  const isAccountSource = !!(TABLES[source] && TABLES[source].ownerStaging);
  const unclassified = [];  // raw rows that classifyCifRow returns 'unclassified' for
  let rowCount = 0;

  // Track rows we had to skip at the parser level (malformed CSV — NOT silent).
  const parseSkipped = [];
  const quoteFixer = new QuoteFixer();

  const parser = fs.createReadStream(filePath)
    .pipe(quoteFixer)
    .pipe(parse({
      columns: true,
      trim: true,
      skip_empty_lines: true,
      relax_column_count: true,
      skip_records_with_error: true,
    }));

  parser.on('skip', (err) => {
    const msg = err && err.message ? err.message : 'unknown parse error';
    parseSkipped.push({ message: msg, at: err?.lines });
    console.warn(`⚠  CSV row skipped (line ${err?.lines ?? '?'}): ${msg}`);
  });

  return new Promise((resolve, reject) => {
    parser.on('headers', (headers) => {
      try { validateHeaders(headers, expected); }
      catch (e) { parser.destroy(e); }
    });

    parser.on('data', (rawRow) => {
      rowCount++;

      // HASH A: hash the verbatim raw CSV row before any modification.
      // This is the immutable source-of-truth hash; HASH B re-computes the
      // same digest from raw_csv after INSERT to prove DB persistence is
      // lossless. Per memory financial_data_rules.md.
      const rowHash = canonicalRowHash(rawRow);

      // BOM-stripped view used ONLY for column lookup. The verbatim rawRow
      // (BOM included if present) goes into raw_csv unchanged.
      const lookupRow = stripBomKeys(rawRow);

      if (source === 'cif') {
        const cls = classifyCifRow(lookupRow);
        if (cls === 'unclassified') {
          unclassified.push(rawRow);
          return;
        }
        const targetTable = cls === 'contact' ? 'stg_contacts' : 'stg_companies';
        const fields = cls === 'contact' ? CIF_CONTACT_FIELDS : CIF_COMPANY_FIELDS;
        const mapped = mapRowToHubspotProps(lookupRow, fields);
        mapped.row_hash = rowHash;
        mapped.raw_csv = rawRow; // preserved verbatim, JSONB on the staging row
        (buckets[targetTable] ||= []).push(mapped);
        return;
      }

      const config = TABLES[source];
      const mapped = mapRowToHubspotProps(lookupRow, config.fields);
      if (source === 'debit_cards') {
        mapped.composite_key = generateCompositeKeyFromRaw(lookupRow);
      }
      mapped.row_hash = rowHash;
      mapped.raw_csv = rawRow;

      if (isAccountSource) {
        // Derive the account-level key and collect for end-of-parse collapse.
        // Pass lookupRow so buildAccountKey can read the client-provided account
        // key column (ACCOUNT_KEY_SOURCE_COLUMN) when present.
        mapped.account_key = buildAccountKey(mapped, lookupRow);
        accountEntries.push({
          mapped,
          accountKey: mapped.account_key,
          code: normalizeCode(mapped.relationship),
        });
        return;
      }

      (buckets[config.staging] ||= []).push(mapped);
    });

    parser.on('end', async () => {
      try {
        // Collapse account owner-rows into one record per physical account, and
        // stage the full owner list separately for the association engine.
        if (isAccountSource && accountEntries.length > 0) {
          const config = TABLES[source];
          const groups = new Map();
          let nullKeyGroupSeq = 0;
          for (const e of accountEntries) {
            // Rows with no derivable account key become their own singleton
            // group so we never merge under-identified accounts. The record
            // keeps account_key = null and is quarantined downstream (loud) by
            // the existing null-key path.
            const gkey = e.accountKey != null ? e.accountKey : `__nullkey_${nullKeyGroupSeq++}`;
            let g = groups.get(gkey);
            if (!g) { g = []; groups.set(gkey, g); }
            g.push(e);
          }

          // COLLISION GUARD (interim, pending a true unique account number from
          // Civista's mapping doc). The provisional account_key
          // (branch|type|last4) is NOT guaranteed unique: distinct physical
          // accounts that share branch+type+last4 collide into one group and
          // would be merged into a single HubSpot record, cross-associating
          // unrelated owners. This is a confirmed production privacy issue
          // (prod key 101|D|1000 merged an individual's and a business's
          // accounts). A physical account has exactly ONE primary owner (code
          // 'P'), so a group carrying >1 distinct primary-owner CIF is proof of
          // a collision. We refuse to merge such groups — every row in them is
          // re-keyed to its own per-row primary_key so unrelated owners can
          // never share a record. Legit single-account groups (including real
          // multi-owner joint accounts) still collapse normally. This does NOT
          // by itself clean records already merged in HubSpot — that requires a
          // re-sync once a real account-level identifier is in place.
          const finalGroups = new Map();
          let collisionKeys = 0;
          let collisionRows = 0;
          const collisionSamples = [];
          for (const [gkey, entries] of groups) {
            const distinctPrimaries = new Set(
              entries
                .filter(e => e.code === PRIMARY_OWNER_CODE)
                .map(e => e.mapped.cif_number)
                .filter(c => c !== null && c !== undefined && c !== '')
            );
            if (distinctPrimaries.size > 1) {
              collisionKeys++;
              collisionRows += entries.length;
              if (collisionSamples.length < 10) {
                collisionSamples.push({ account_key: gkey, primary_cifs: Array.from(distinctPrimaries) });
              }
              let splitSeq = 0;
              for (const e of entries) {
                const pk = e.mapped.primary_key;
                const safeKey = pk ? `PK:${String(pk).trim()}` : `${gkey}__split_${splitSeq++}`;
                e.mapped.account_key = safeKey;
                e.accountKey = safeKey;
                finalGroups.set(safeKey, [e]);
              }
            } else {
              finalGroups.set(gkey, entries);
            }
          }

          const accountRows = [];
          const ownerRows = [];
          for (const entries of finalGroups.values()) {
            // Canonical row = primary owner (code 'P') if present, else first.
            const canonical = entries.find(e => e.code === PRIMARY_OWNER_CODE) || entries[0];
            accountRows.push(canonical.mapped);
            for (const e of entries) {
              ownerRows.push({
                account_key: e.accountKey,
                cif_number: e.mapped.cif_number,
                relationship: e.mapped.relationship,
                primary_key: e.mapped.primary_key,
                row_hash: e.mapped.row_hash,
                raw_csv: e.mapped.raw_csv,
              });
            }
          }
          buckets[config.staging] = accountRows;
          await insertRows(config.ownerStaging, ownerRows);
          if (collisionKeys > 0) {
            await loud.alarm({
              event: 'account_key_collision',
              message: `${source}: ${collisionKeys} account_key(s) covered >1 distinct primary owner — the provisional branch|type|last4 key is not unique. ${collisionRows} owner rows were split to per-row keys to prevent cross-customer merges. A true unique account number (from Civista's mapping doc) is required for correct deduplication.`,
              sourceTable: config.staging,
              context: { samples: collisionSamples },
            });
          }
          console.log(`Collapsed ${accountEntries.length} ${source} owner rows into ${accountRows.length} accounts (${collisionKeys} collision key(s) split to per-row keys); staged ${ownerRows.length} owner links into ${config.ownerStaging}`);
        }

        const byTable = {};
        for (const [table, rows] of Object.entries(buckets)) {
          await insertRows(table, rows);
          byTable[table] = rows.length;
        }
        if (quoteFixer.fixCount > 0) {
          console.warn(`⚠  QuoteFixer repaired ${quoteFixer.fixCount} embedded quotes in ${filePath}`);
        }
        resolve({ rowCount, fileHash, byTable, unclassified, parseSkipped, quotesFixed: quoteFixer.fixCount, sinceTs });
      } catch (err) { reject(err); }
    });

    parser.on('error', reject);
  });
}

async function insertRows(stagingTable, rows) {
  if (rows.length === 0) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // Staging is ephemeral — rebuilt every run. Diff engine uses shipped_records as the
    // audit ledger, so this DELETE does not cause the "everything looks new" bug from before.
    await client.query(`DELETE FROM ${stagingTable}`);

    // Determine the column set from the first row — we built these uniformly in the caller.
    const columns = Object.keys(rows[0]);
    const chunkSize = 500;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const placeholders = chunk.map((_, ri) => {
        const off = ri * columns.length;
        return `(${columns.map((_, ci) => `$${off + ci + 1}`).join(', ')})`;
      }).join(', ');
      // raw_csv is JSONB — pg accepts a JS object only if we stringify it.
      const values = chunk.flatMap(r => columns.map(c => {
        const v = r[c];
        if (c === 'raw_csv') return v == null ? null : JSON.stringify(v);
        return v == null ? null : v;
      }));
      await client.query(
        `INSERT INTO ${stagingTable} (${columns.join(', ')}) VALUES ${placeholders}`,
        values
      );
    }

    await client.query('COMMIT');
    console.log(`Staged ${rows.length} rows into ${stagingTable}`);
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * HASH B verification: re-read raw_csv from the staging table for rows
 * inserted in this parse run, recompute the canonical row hash, and
 * compare to the stored row_hash (HASH A from parse time). Equal →
 * write db_persist_hash; unequal → flag needs_review + loud alarm.
 *
 * Scoped by `loaded_at >= sinceTs` (the timestamp at the start of this
 * parse) so concurrent /sync invocations don't double-verify each
 * other's pending rows. Without this, two overlapping syncs would race
 * on UPDATE and double-count.
 *
 * Returns { total, ok, mismatch, legacy } counts for the run.
 */
async function verifyDbPersistence(stagingTable, sinceTs, runId = null) {
  const params = [sinceTs];
  const result = await pool.query(
    `SELECT id, row_hash, raw_csv FROM ${stagingTable}
     WHERE db_persist_hash IS NULL AND loaded_at >= $1`,
    params
  );
  let ok = 0, mismatch = 0, legacy = 0;
  for (const r of result.rows) {
    if (!r.raw_csv) {
      // Pre-existing row without raw_csv (legacy data from a prior version
      // OR a parse that pre-dates the lossless-preservation refactor). We
      // CANNOT verify A==B for these — surface that loudly so the operator
      // knows the success counter doesn't apply.
      await pool.query(
        `UPDATE ${stagingTable}
           SET db_persist_hash = $1, db_verified_at = NOW(), needs_review = true
         WHERE id = $2`,
        [r.row_hash, r.id]
      );
      await loud.warn({
        event: 'db_persist_unverifiable',
        message: `${stagingTable} id=${r.id} has no raw_csv — HASH B cannot be verified; row flagged needs_review`,
        runId, sourceTable: stagingTable,
      });
      legacy++;
      continue;
    }
    const recomputed = canonicalRowHash(r.raw_csv);
    if (recomputed === r.row_hash) {
      await pool.query(
        `UPDATE ${stagingTable} SET db_persist_hash = $1, db_verified_at = NOW() WHERE id = $2`,
        [recomputed, r.id]
      );
      ok++;
    } else {
      await pool.query(
        `UPDATE ${stagingTable} SET db_persist_hash = $1, db_verified_at = NOW(), needs_review = true WHERE id = $2`,
        [recomputed, r.id]
      );
      await loud.alarm({
        event: 'db_persist_mismatch',
        message: `HASH A != HASH B on ${stagingTable} id=${r.id}: csv_hash=${r.row_hash} db_hash=${recomputed}`,
        runId,
        sourceTable: stagingTable,
        context: { csv_hash: r.row_hash, db_hash: recomputed },
      });
      mismatch++;
    }
  }
  return { total: result.rows.length, ok, mismatch, legacy };
}

module.exports = {
  parseAndStage,
  hashFile,
  generateRowHash,
  validateHeaders,
  expectedHeadersFor,
  verifyDbPersistence,
  canonicalRowHash,
};

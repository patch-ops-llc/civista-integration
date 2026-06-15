#!/usr/bin/env node
/**
 * SCHEMA CHECK — fully transparent, no black boxes.
 *
 * For every HubSpot object we sync to, this script:
 *   1. Prints the exact SQL that will be run against Postgres, then runs it
 *   2. Prints the exact curl that will be run against HubSpot, then runs it
 *   3. Diffs the two schemas field by field
 *   4. Shows row counts RIGHT NOW from Postgres, HubSpot, and the source CSV
 *
 * Every SQL query and HTTP call is printed in copy-pasteable form so you
 * can re-run anything by hand. Nothing is hidden.
 *
 * Usage:
 *   DATABASE_URL=postgresql://... HUBSPOT_API_KEY=pat-... \
 *     INCOMING_DIR=/path/to/csvs node scripts/schema-check.js
 *
 *   DATABASE_URL and HUBSPOT_API_KEY are required.
 *   INCOMING_DIR is optional — skips CSV row counts if unset.
 */

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const GRAY = '\x1b[90m';
const YELLOW = '\x1b[33m';
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const CYAN = '\x1b[36m';
const BOLD = '\x1b[1m';
const RESET = '\x1b[0m';

const DB_URL = process.env.DATABASE_URL;
const HS_KEY = process.env.HUBSPOT_API_KEY;
const INCOMING = process.env.INCOMING_DIR;
const FIELD_DELAY_MS = parseInt(process.env.FIELD_DELAY_MS || '1000', 10);
const INTERACTIVE = process.env.INTERACTIVE !== '0';

if (!DB_URL) { console.error('DATABASE_URL not set'); process.exit(1); }
if (!HS_KEY) { console.error('HUBSPOT_API_KEY not set'); process.exit(1); }

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function waitForKeypress(promptText) {
  return new Promise((resolve) => {
    if (!INTERACTIVE || !process.stdin.isTTY) {
      console.log(`${GRAY}${promptText} (non-TTY mode — continuing)${RESET}`);
      return resolve();
    }
    process.stdout.write(`\n${BOLD}${YELLOW}${promptText}${RESET} `);
    process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.once('data', () => {
      process.stdin.setRawMode(false);
      process.stdin.pause();
      process.stdout.write('\n');
      resolve();
    });
  });
}

const pool = new Pool({ connectionString: DB_URL });

// Staging table ↔ HubSpot object ↔ CSV file
const OBJECTS = [
  { stagingTable: 'stg_contacts',      hsObject: 'contacts',   csvFile: 'HubSpot_CIF.csv',        label: 'Contacts' },
  { stagingTable: 'stg_companies',     hsObject: 'companies',  csvFile: 'HubSpot_CIF.csv',        label: 'Companies' },
  { stagingTable: 'stg_deposits',      hsObject: '2-60107989', csvFile: 'HubSpot_DDA.csv',        label: 'Deposits' },
  { stagingTable: 'stg_loans',         hsObject: '2-60108336', csvFile: 'HubSpot_Loan.csv',       label: 'Loans' },
  { stagingTable: 'stg_time_deposits', hsObject: '2-60108759', csvFile: 'HubSpot_CD.csv',         label: 'Time Deposits' },
  { stagingTable: 'stg_debit_cards',   hsObject: '2-60107457', csvFile: 'HubSpot_Debit_Card.csv', label: 'Debit Cards' },
];

function showCmd(cmd) {
  console.log(`${GRAY}  $ ${cmd}${RESET}`);
}
function showResult(text) {
  console.log(text.split('\n').map(l => `    ${l}`).join('\n'));
}

// Redact the bearer in logged curl output so nothing leaks if output is pasted.
function redactedCurl(cmd) {
  return cmd.replace(HS_KEY, '$HUBSPOT_API_KEY');
}

async function pgQuery(sql, params = []) {
  // Show the SQL in a copy-pasteable form (with $1 placeholders substituted for readability).
  let display = sql;
  params.forEach((p, i) => {
    display = display.replace(`$${i + 1}`, `'${p}'`);
  });
  showCmd(`psql $DATABASE_URL -c "${display.replace(/\s+/g, ' ').trim()}"`);
  const result = await pool.query(sql, params);
  return result;
}

async function hsGet(url) {
  const curlCmd = `curl -sH "Authorization: Bearer $HUBSPOT_API_KEY" "${url}"`;
  showCmd(curlCmd);
  const res = await fetch(url, { headers: { Authorization: `Bearer ${HS_KEY}` } });
  const body = await res.json();
  return { status: res.status, body };
}

async function hsPost(url, payload) {
  const jsonPayload = JSON.stringify(payload);
  const curlCmd = `curl -sX POST -H "Authorization: Bearer $HUBSPOT_API_KEY" -H "Content-Type: application/json" -d '${jsonPayload}' "${url}"`;
  showCmd(curlCmd);
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${HS_KEY}`,
      'Content-Type': 'application/json',
    },
    body: jsonPayload,
  });
  const body = await res.json();
  return { status: res.status, body };
}

function countCsvRows(filePath) {
  if (!fs.existsSync(filePath)) return null;
  const content = fs.readFileSync(filePath, 'utf8');
  // Count newline-terminated lines minus header. Ignore trailing blank line.
  const lines = content.split('\n').filter(l => l.trim() !== '');
  return Math.max(0, lines.length - 1);
}

async function checkObject({ stagingTable, hsObject, csvFile, label }) {
  console.log('');
  console.log(`${BOLD}${CYAN}════════════════════════════════════════════════════════════════${RESET}`);
  console.log(`${BOLD}${CYAN}  ${label}  (${stagingTable} ↔ HubSpot ${hsObject})${RESET}`);
  console.log(`${BOLD}${CYAN}════════════════════════════════════════════════════════════════${RESET}`);

  // ───────────────────────── POSTGRES SCHEMA ─────────────────────────
  console.log('');
  console.log(`${BOLD}[1/4] POSTGRES SCHEMA — columns and types${RESET}`);
  const pgCols = await pgQuery(
    `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
    [stagingTable]
  );
  if (pgCols.rows.length === 0) {
    showResult(`${YELLOW}⚠ table ${stagingTable} does not exist${RESET}`);
  } else {
    for (const c of pgCols.rows) {
      showResult(`${c.column_name.padEnd(40)} ${c.data_type}`);
      await sleep(FIELD_DELAY_MS);
    }
  }

  // ───────────────────────── HUBSPOT SCHEMA ─────────────────────────
  console.log('');
  console.log(`${BOLD}[2/4] HUBSPOT SCHEMA — properties and types${RESET}`);
  const hsRes = await hsGet(`https://api.hubapi.com/crm/v3/properties/${hsObject}`);
  if (hsRes.status !== 200) {
    showResult(`${RED}HubSpot returned ${hsRes.status}: ${JSON.stringify(hsRes.body)}${RESET}`);
    return;
  }
  const hsProps = hsRes.body.results || [];
  const hsPropMap = new Map(hsProps.map(p => [p.name, p]));
  showResult(`(showing only non-HubSpot-defined properties — total ${hsProps.length} including standard)`);
  const custom = hsProps.filter(p => !p.hubspotDefined);
  for (const p of custom) {
    const flags = p.hasUniqueValue ? ' [UNIQUE]' : '';
    showResult(`${p.name.padEnd(45)} ${(p.type + '/' + p.fieldType).padEnd(25)}${flags}`);
    await sleep(FIELD_DELAY_MS);
  }

  // ───────────────────────── SCHEMA MATCH DIFF ─────────────────────────
  console.log('');
  console.log(`${BOLD}[3/4] SCHEMA MATCH — Postgres column vs HubSpot property${RESET}`);
  const pgColNames = pgCols.rows.map(r => r.column_name);
  const internalCols = new Set(['id', 'row_hash', 'loaded_at', 'synced_at', 'record_type']);
  let matchCount = 0, missingInHs = 0, missingInPg = 0;

  // PG columns that should exist in HubSpot
  for (const col of pgColNames) {
    if (internalCols.has(col)) continue;
    const hsMatch = hsPropMap.get(col);
    if (hsMatch) {
      showResult(`${GREEN}✓${RESET} ${col.padEnd(40)} pg:text → hs:${hsMatch.type}/${hsMatch.fieldType}`);
      matchCount++;
    } else {
      showResult(`${RED}✗ pg-only${RESET} ${col.padEnd(40)} (no HubSpot property with this name)`);
      missingInHs++;
    }
    await sleep(FIELD_DELAY_MS);
  }

  // HubSpot custom props that aren't in PG (informational — some HS props don't correspond to source data)
  const pgColSet = new Set(pgColNames);
  const hsOnly = custom.filter(p => !pgColSet.has(p.name));
  if (hsOnly.length > 0) {
    showResult('');
    showResult(`${YELLOW}HubSpot-only custom properties (not populated by this sync):${RESET}`);
    for (const p of hsOnly) {
      showResult(`  ${p.name.padEnd(45)} ${p.type}/${p.fieldType}`);
      missingInPg++;
    }
  }

  showResult('');
  showResult(`SUMMARY: ${matchCount} matched, ${missingInHs} in Postgres only, ${missingInPg} in HubSpot only`);

  // ───────────────────────── ROW COUNTS — RIGHT NOW ─────────────────────────
  console.log('');
  console.log(`${BOLD}[4/4] ROW COUNTS — RIGHT NOW${RESET}`);

  // Postgres
  const pgCount = await pgQuery(`SELECT COUNT(*)::int AS c FROM ${stagingTable}`);
  showResult(`Postgres: ${pgCount.rows[0].c} rows in ${stagingTable}`);

  // HubSpot — use search API with empty filter to get total
  console.log('');
  const hsCount = await hsPost(
    `https://api.hubapi.com/crm/v3/objects/${hsObject}/search`,
    { limit: 1 }
  );
  if (hsCount.status === 200) {
    showResult(`HubSpot: ${hsCount.body.total} total records in ${hsObject}`);
  } else {
    showResult(`${RED}HubSpot count error (${hsCount.status}): ${JSON.stringify(hsCount.body)}${RESET}`);
  }

  // CSV (if INCOMING_DIR set)
  if (INCOMING) {
    console.log('');
    const csvPath = path.join(INCOMING, csvFile);
    showCmd(`wc -l "${csvPath}"`);
    const csvRows = countCsvRows(csvPath);
    if (csvRows === null) {
      showResult(`${YELLOW}(CSV not found at ${csvPath})${RESET}`);
    } else {
      showResult(`CSV: ${csvRows} data rows in ${csvFile} (header excluded)`);
    }
  }
}

(async () => {
  console.log('');
  console.log(`${BOLD}${CYAN}CIVISTA SYNC — Schema & Row Count Check${RESET}`);
  console.log(`${GRAY}Run: ${new Date().toISOString()}${RESET}`);
  console.log(`${GRAY}DB:  ${DB_URL.replace(/:[^:@]+@/, ':***@')}${RESET}`);
  console.log(`${GRAY}HS:  bearer token ending …${HS_KEY.slice(-4)}${RESET}`);
  console.log(`${GRAY}CSV: ${INCOMING || '(not provided — CSV counts skipped)'}${RESET}`);

  try {
    for (let i = 0; i < OBJECTS.length; i++) {
      await checkObject(OBJECTS[i]);
      if (i < OBJECTS.length - 1) {
        await waitForKeypress(`Press any key for next object (${OBJECTS[i + 1].label})...`);
      }
    }
  } catch (err) {
    console.error(`${RED}ERROR: ${err.message}${RESET}`);
    console.error(err.stack);
    process.exit(1);
  } finally {
    await pool.end();
  }

  console.log('');
  console.log(`${BOLD}${GREEN}Done.${RESET}`);
})();

/**
 * Build a small, COHERENT canary slice of the 5 Silver Lake CSVs for a prod
 * smoke test before the full backfill.
 *
 * Coherence strategy: pick the first N rows from each account file
 * (DDA / Loan / CD) and the first M debit-card rows, collect every owner CIF
 * referenced by those rows, then slice the CIF file down to exactly those CIFs.
 * That guarantees every account/card in the canary has its owner Contact/Company
 * present, so owner associations resolve with zero "unresolved".
 *
 * Reads from an archived full set and writes the 5 sliced CSVs to an output dir.
 * No HubSpot or DB access — pure file transform. CSVs stay on the Railway volume.
 *
 *   railway ssh sh -c 'SRC=/app/data/archive/2026-05-30 OUT=/app/data/incoming \
 *     N_ACCOUNTS=120 N_DEBIT=80 node scripts/make-canary-slice.js'
 */
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');

const SRC = process.env.SRC || '/app/data/archive/2026-05-30';
const OUT = process.env.OUT || '/app/data/incoming';
const N_ACCOUNTS = parseInt(process.env.N_ACCOUNTS || '120', 10);
const N_DEBIT = parseInt(process.env.N_DEBIT || '80', 10);

const FILES = {
  cif: 'HubSpot_CIF.csv',
  dda: 'HubSpot_DDA.csv',
  loan: 'HubSpot_Loan.csv',
  cd: 'HubSpot_CD.csv',
  debit: 'HubSpot_Debit_Card.csv',
};

function readRows(file) {
  const text = fs.readFileSync(path.join(SRC, file), 'utf8');
  // relax_quotes mirrors the ingestion parser's tolerance for the bank's
  // occasionally-malformed quoting; bom strips a leading BOM if present.
  const rows = parse(text, { relax_quotes: true, skip_empty_lines: true, bom: true });
  return { header: rows[0], data: rows.slice(1) };
}

function colIndex(header, candidates) {
  for (const c of candidates) {
    const i = header.indexOf(c);
    if (i !== -1) return i;
  }
  throw new Error(`none of [${candidates.join(', ')}] found in header: ${header.join(',')}`);
}

function escapeField(v) {
  const s = v == null ? '' : String(v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function writeCsv(file, header, rows) {
  const lines = [header, ...rows].map((r) => r.map(escapeField).join(','));
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, file), lines.join('\n') + '\n');
  return rows.length;
}

(function main() {
  const ownerCifs = new Set();
  const sliced = {};

  // Account files: take first N rows, collect owner CIFs.
  for (const key of ['dda', 'loan', 'cd']) {
    const { header, data } = readRows(FILES[key]);
    const cifIdx = colIndex(header, ['CIF#', 'CIFNum']);
    const take = data.slice(0, N_ACCOUNTS);
    take.forEach((r) => ownerCifs.add(String(r[cifIdx]).trim()));
    sliced[key] = { header, rows: take };
  }

  // Debit cards: take first M rows, collect owner CIFs.
  {
    const { header, data } = readRows(FILES.debit);
    const cifIdx = colIndex(header, ['CIF#', 'CIFNum']);
    const take = data.slice(0, N_DEBIT);
    take.forEach((r) => ownerCifs.add(String(r[cifIdx]).trim()));
    sliced.debit = { header, rows: take };
  }

  // CIF file: keep only rows whose CIFNum is an owner referenced above.
  {
    const { header, data } = readRows(FILES.cif);
    const cifIdx = colIndex(header, ['CIFNum', 'CIF#']);
    const rows = data.filter((r) => ownerCifs.has(String(r[cifIdx]).trim()));
    sliced.cif = { header, rows };
  }

  console.log(`Canary slice from ${SRC} -> ${OUT}`);
  console.log(`  owner CIFs referenced: ${ownerCifs.size}`);
  const order = ['cif', 'dda', 'loan', 'cd', 'debit'];
  for (const key of order) {
    const n = writeCsv(FILES[key], sliced[key].header, sliced[key].rows);
    console.log(`  ${FILES[key]}: ${n} rows`);
  }
  const matchedCifs = sliced.cif.rows.length;
  if (matchedCifs < ownerCifs.size) {
    console.log(`  NOTE: ${ownerCifs.size - matchedCifs} owner CIF(s) not found in CIF file ` +
      `(their associations will be unresolved). Usually 0 for a clean set.`);
  }
  process.exit(0);
})();

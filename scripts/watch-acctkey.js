#!/usr/bin/env node
/**
 * Watches for Keith's three `*_AcctKey_Added` test files and auto-runs the
 * read-only validator (scripts/validate-acctkey.js) the moment all three have
 * landed and finished copying.
 *
 *   node scripts/watch-acctkey.js [extraDir ...]
 *
 * Polls a set of likely drop locations (Downloads, Desktop, Documents, OneDrive
 * variants, the project root, ./incoming) plus any dirs passed as args. A file
 * is considered "ready" only once its size is unchanged across two polls, so we
 * never validate a half-copied file.
 *
 * Env:
 *   WATCH_TIMEOUT_MS  give-up timeout (default 16h)
 *   WATCH_INTERVAL_MS poll interval   (default 20s)
 *
 * READ-ONLY: only reads the CSVs and prints results. Touches no DB/HubSpot.
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const { validateFile, printResult } = require('./validate-acctkey');

const FILES = {
  dda:   'HubSpot_DDA_AcctKey_Added.csv',
  loans: 'HubSpot_Loan_AcctKey_Added.csv',
  cd:    'HubSpot_CD_AcctKey_Added.csv',
};

const home = os.homedir();
const DIRS = [
  ...process.argv.slice(2),
  process.cwd(),
  path.join(process.cwd(), 'incoming'),
  path.join(home, 'Downloads'),
  path.join(home, 'Desktop'),
  path.join(home, 'Documents'),
  path.join(home, 'OneDrive', 'Downloads'),
  path.join(home, 'OneDrive', 'Desktop'),
  path.join(home, 'OneDrive', 'Documents'),
  path.join(home, 'Civista Integration'),
].filter((d, i, a) => d && a.indexOf(d) === i);

const TIMEOUT = parseInt(process.env.WATCH_TIMEOUT_MS, 10) || 16 * 60 * 60 * 1000;
const INTERVAL = parseInt(process.env.WATCH_INTERVAL_MS, 10) || 20 * 1000;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function findFile(name) {
  for (const d of DIRS) {
    const p = path.join(d, name);
    try { const st = fs.statSync(p); if (st.isFile()) return { path: p, size: st.size }; } catch { /* not here */ }
  }
  return null;
}

(async () => {
  console.log(`[watch-acctkey] watching for ${Object.values(FILES).join(', ')}`);
  console.log(`[watch-acctkey] dirs:\n  ${DIRS.join('\n  ')}`);
  const started = Date.now();
  const lastSize = {};
  let announced = {};

  while (Date.now() - started < TIMEOUT) {
    const found = {};
    let allStable = true;
    for (const [src, name] of Object.entries(FILES)) {
      const hit = findFile(name);
      if (!hit) { allStable = false; lastSize[src] = undefined; continue; }
      if (!announced[src]) { console.log(`[watch-acctkey] saw ${name} at ${hit.path} (${hit.size} bytes)`); announced[src] = true; }
      // stable only if size matches previous poll and is non-zero
      if (lastSize[src] === hit.size && hit.size > 0) found[src] = hit.path;
      else allStable = false;
      lastSize[src] = hit.size;
    }

    if (allStable && Object.keys(found).length === Object.keys(FILES).length) {
      console.log(`\n[watch-acctkey] all three files present and stable — validating...`);
      let allPass = true;
      for (const [src, p] of Object.entries(found)) {
        try { const r = validateFile(p, src); printResult(r); if (!r.pass) allPass = false; }
        catch (e) { console.error(`[watch-acctkey] ${src} validation error: ${e.message}`); allPass = false; }
      }
      console.log(`\n[watch-acctkey] RESULT: ${allPass ? 'ALL PASS ✓ — new key is safe to adopt.' : 'FAILURES ✘ — do not adopt yet; see above.'}`);
      process.exit(allPass ? 0 : 1);
    }
    await sleep(INTERVAL);
  }
  console.error(`[watch-acctkey] timed out after ${Math.round((Date.now() - started) / 60000)} min without all files appearing.`);
  process.exit(2);
})();

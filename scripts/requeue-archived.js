#!/usr/bin/env node
/**
 * Move a previously-archived batch of source CSVs back into the incoming
 * directory so runFullSync re-processes them. The diff engine only re-ships
 * rows whose row_hash isn't already in shipped_records, so re-queueing is a
 * safe, idempotent way to retry rows that failed on a prior run (e.g. after a
 * code fix) without re-shipping everything.
 *
 * Usage:
 *   node scripts/requeue-archived.js            # most recent archive date
 *   node scripts/requeue-archived.js 2026-06-16 # a specific archive date
 *
 * Honors INCOMING_DIR / ARCHIVE_DIR (defaults match index.js).
 */
const fs = require('fs');
const path = require('path');

const INCOMING_DIR = process.env.INCOMING_DIR || path.join(__dirname, '..', 'incoming');
const ARCHIVE_DIR = process.env.ARCHIVE_DIR || path.join(__dirname, '..', 'archive');

function pickDate(arg) {
  if (arg) return arg;
  const dirs = fs.readdirSync(ARCHIVE_DIR, { withFileTypes: true })
    .filter(d => d.isDirectory() && /^\d{4}-\d{2}-\d{2}$/.test(d.name))
    .map(d => d.name)
    .sort();
  return dirs[dirs.length - 1] || null;
}

(function main() {
  const date = pickDate(process.argv[2]);
  if (!date) {
    console.error('No dated archive folders found under', ARCHIVE_DIR);
    process.exit(1);
  }
  const srcDir = path.join(ARCHIVE_DIR, date);
  if (!fs.existsSync(srcDir)) {
    console.error('Archive folder does not exist:', srcDir);
    process.exit(1);
  }

  fs.mkdirSync(INCOMING_DIR, { recursive: true });
  const files = fs.readdirSync(srcDir).filter(f => f.toLowerCase().endsWith('.csv'));
  if (files.length === 0) {
    console.error('No CSV files in', srcDir);
    process.exit(1);
  }

  let moved = 0;
  for (const f of files) {
    const from = path.join(srcDir, f);
    const to = path.join(INCOMING_DIR, f);
    fs.renameSync(from, to);
    console.log(`requeued ${f}  ->  ${INCOMING_DIR}`);
    moved++;
  }
  console.log(`\nDone. ${moved} file(s) moved from archive/${date} back to incoming.`);
})();

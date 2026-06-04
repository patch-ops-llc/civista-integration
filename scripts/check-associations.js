// READ-ONLY QA helper. Reads /api/issues and prints the association engine's
// per-source results (created / skipped / failed / unresolved) next to the
// account-record sync counts. Use after a full re-sync to confirm accounts are
// deduped and owner links were created.
//
//   railway run --service=civista-integration node scripts/check-associations.js
const URL = process.env.PUBLIC_URL || 'https://civista-integration-production.up.railway.app';
const user = process.env.SFTP_USER || 'civista';
const pass = process.env.SFTP_PASS;
if (!pass) { console.error('SFTP_PASS not present in environment.'); process.exit(2); }
const auth = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');

(async () => {
  const res = await fetch(`${URL}/api/issues`, { headers: { Authorization: auth } });
  if (!res.ok) { console.error(`HTTP ${res.status}`); process.exit(1); }
  const j = await res.json();

  const per = j.recent_run?.per_source_table || {};
  console.log('=== Account records synced (deduped, one per physical account) ===');
  for (const t of ['stg_deposits', 'stg_loans', 'stg_time_deposits', 'stg_debit_cards']) {
    const v = per[t] || {};
    console.log(`${t.padEnd(18)} created=${v.records_created ?? 0}  skipped=${v.records_skipped ?? 0}  failed=${v.records_failed ?? 0}`);
  }

  const assoc = j.recent_run?.associations || {};
  console.log('\n=== Owner associations ===');
  let totalCreated = 0, totalFailed = 0, totalUnresolved = 0;
  for (const src of ['dda', 'loans', 'cd', 'debit_cards']) {
    const a = assoc[src] || {};
    totalCreated += a.created || 0; totalFailed += a.failed || 0; totalUnresolved += a.unresolved || 0;
    console.log(`${src.padEnd(12)} created=${a.created ?? 0}  skipped_existing=${a.skipped_existing ?? 0}  failed=${a.failed ?? 0}  unresolved=${a.unresolved ?? 0}  completed_at=${a.completed_at || '-'}`);
  }
  console.log(`TOTAL        created=${totalCreated}  failed=${totalFailed}  unresolved=${totalUnresolved}`);

  if (totalFailed > 0) console.log('\n⚠ Some associations failed (HubSpot rejected) — inspect sync_errors / logs.');
  if (totalUnresolved > 0) console.log('⚠ Some owner links unresolved — owner CIF or account not yet shipped (often the partial-load issue). Re-run after a full load.');
  if (totalFailed === 0 && totalUnresolved === 0 && totalCreated > 0) console.log('\n✓ Associations created cleanly with no failures or unresolved links.');
})().catch(e => { console.error(e.message); process.exit(1); });

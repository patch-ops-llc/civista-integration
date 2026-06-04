// Reads /api/issues and prints a compact per-source sync summary.
const URL = process.env.PUBLIC_URL || 'https://civista-integration-production.up.railway.app';
const user = process.env.SFTP_USER || 'civista';
const pass = process.env.SFTP_PASS;
const auth = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');

(async () => {
  const res = await fetch(`${URL}/api/issues`, { headers: { Authorization: auth } });
  if (!res.ok) { console.error(`HTTP ${res.status}`); process.exit(1); }
  const j = await res.json();
  const per = j.recent_run?.per_source_table || {};
  let a = 0, c = 0, s = 0, f = 0;
  for (const [t, v] of Object.entries(per)) {
    a += v.records_attempted || 0; c += v.records_created || 0;
    s += v.records_skipped || 0; f += v.records_failed || 0;
    console.log(`${t.padEnd(18)} attempted=${v.records_attempted ?? 0}  created=${v.records_created ?? 0}  skipped=${v.records_skipped ?? 0}  failed=${v.records_failed ?? 0}  completed_at=${v.completed_at || '-'}`);
  }
  console.log(`TOTAL              attempted=${a}  created=${c}  skipped=${s}  failed=${f}`);
  const sev = j.recent_run?.by_severity || {};
  if (Object.keys(sev).length) console.log('severity(24h):', JSON.stringify(sev));
})().catch(e => { console.error(e.message); process.exit(1); });

// Dumps recent sync error aggregates + sample messages from /api/issues
// so we can see WHY records are being rejected by HubSpot.
const URL = process.env.PUBLIC_URL || 'https://civista-integration-production.up.railway.app';
const user = process.env.SFTP_USER || 'civista';
const pass = process.env.SFTP_PASS;
const auth = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');

(async () => {
  const res = await fetch(`${URL}/api/issues`, { headers: { Authorization: auth } });
  if (!res.ok) { console.error(`HTTP ${res.status}`); process.exit(1); }
  const j = await res.json();
  console.log('by_type:', JSON.stringify(j.recent_run?.by_type || {}));
  console.log('by_severity:', JSON.stringify(j.recent_run?.by_severity || {}));
  const samples = (j.recent_run?.samples || []).filter(s => s.source_table === 'stg_contacts');
  console.log(`\n--- contacts error samples (${samples.length}) ---`);
  const seen = new Map();
  for (const s of samples) {
    const key = (s.error_message || '').slice(0, 200);
    seen.set(key, (seen.get(key) || 0) + 1);
  }
  for (const [msg, n] of [...seen.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12)) {
    console.log(`[x${n}] ${msg}`);
  }
})().catch(e => { console.error(e.message); process.exit(1); });

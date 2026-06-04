// Connects to /api/logs (SSE), prints the ring-buffer backlog + a few seconds
// of live events, then exits. Used to inspect recent runtime activity.
const URL = process.env.PUBLIC_URL || 'https://civista-integration-production.up.railway.app';
const user = process.env.SFTP_USER || 'civista';
const pass = process.env.SFTP_PASS;
const auth = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');
const MS = Number(process.env.TAIL_MS || 5000);

(async () => {
  const ctrl = new AbortController();
  const res = await fetch(`${URL}/api/logs`, { headers: { Authorization: auth }, signal: ctrl.signal });
  if (!res.ok) { console.error(`HTTP ${res.status}`); process.exit(1); }
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  const timer = setTimeout(() => ctrl.abort(), MS);
  let buf = '';
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        const chunk = buf.slice(0, idx); buf = buf.slice(idx + 2);
        for (const line of chunk.split('\n')) {
          if (line.startsWith('data:')) {
            try {
              const e = JSON.parse(line.slice(5).trim());
              console.log(`[${e.level || 'info'}] ${e.at || ''} ${e.message || ''}`);
            } catch {}
          }
        }
      }
    }
  } catch (e) { /* aborted */ }
  clearTimeout(timer);
  process.exit(0);
})().catch(e => { console.error(e.message); process.exit(1); });

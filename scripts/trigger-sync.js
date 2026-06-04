// One-off manual sync trigger. Reads SFTP_USER/SFTP_PASS from the env
// (injected by `railway run`) so no secret is ever printed. POSTs /sync
// against the public Railway host, then prints the JSON ack.
const URL = process.env.PUBLIC_URL || 'https://civista-integration-production.up.railway.app';
const user = process.env.SFTP_USER || 'civista';
const pass = process.env.SFTP_PASS;

if (!pass) {
  console.error('SFTP_PASS not present in environment — cannot authenticate.');
  process.exit(2);
}

const auth = 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64');

(async () => {
  try {
    const res = await fetch(`${URL}/sync`, { method: 'POST', headers: { Authorization: auth } });
    const text = await res.text();
    console.log(`POST /sync -> HTTP ${res.status}`);
    console.log(text);
    process.exit(res.ok ? 0 : 1);
  } catch (err) {
    console.error('Request failed:', err.message);
    process.exit(1);
  }
})();

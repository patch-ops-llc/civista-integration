const express = require('express');
const path = require('path');
const fs = require('fs');
const os = require('os');
const cron = require('node-cron');
const multer = require('multer');
const { pool, initDb } = require('./db/init');
const { runFullSync } = require('./src/sync/orchestrator');
const { getHealthStatus } = require('./src/monitoring/health');
const { startSftpServer } = require('./src/ingestion/sftp-server');
const { describeError } = require('./src/monitoring/errors');
const loud = require('./src/monitoring/loud');

// Surface async crashes that would otherwise be invisible.
//
// On uncaughtException we MUST terminate — Node's default behavior is to
// exit, and an event handler keeps the process alive in a corrupted state
// (open DB transactions, leaked clients, half-applied changes). For a
// financial pipeline that's the worst of both worlds. Log loud, then exit.
process.on('unhandledRejection', (reason) => {
  loud.alarm({
    event: 'unhandled_rejection',
    message: describeError(reason),
    context: { stack: reason && reason.stack ? String(reason.stack).split('\n').slice(0, 5).join('\n') : null },
  }).catch(() => {});
});
process.on('uncaughtException', (err) => {
  loud.alarm({
    event: 'uncaught_exception',
    message: describeError(err),
    context: { stack: err && err.stack ? String(err.stack).split('\n').slice(0, 5).join('\n') : null },
  })
    .catch(() => {})
    .finally(() => {
      // Give the loud.alarm INSERT 500ms to flush, then bail. Do NOT keep
      // running on a corrupted heap — the supervisor (Railway) will restart.
      setTimeout(() => process.exit(1), 500);
    });
});

const app = express();
const port = process.env.PORT || 3000;

const INCOMING_DIR = process.env.INCOMING_DIR || path.join(__dirname, 'incoming');
const ARCHIVE_DIR = process.env.ARCHIVE_DIR || path.join(__dirname, 'archive');
const QUARANTINE_DIR = process.env.QUARANTINE_DIR || path.join(__dirname, 'quarantine');

// MANUAL_SYNC_TOKEN gates the two operator-callable mutating routes
// (POST /sync and POST /upload). Required — if unset, those routes
// fail closed with 503. The token must be sent as X-Sync-Token on every
// request. /health remains unauthenticated for Railway's healthcheck.
const MANUAL_SYNC_TOKEN = process.env.MANUAL_SYNC_TOKEN || null;

fs.mkdirSync(INCOMING_DIR, { recursive: true });
fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
fs.mkdirSync(QUARANTINE_DIR, { recursive: true });

const TMP_UPLOAD_DIR = path.join(os.tmpdir(), 'civista_uploads');
fs.mkdirSync(TMP_UPLOAD_DIR, { recursive: true });
const upload = multer({ dest: TMP_UPLOAD_DIR });

app.use(express.json());

function requireSyncToken(req, res) {
  if (!MANUAL_SYNC_TOKEN) {
    res.status(503).json({ error: 'MANUAL_SYNC_TOKEN not configured' });
    return false;
  }
  const provided = req.get('X-Sync-Token');
  if (provided !== MANUAL_SYNC_TOKEN) {
    loud.warn({
      event: 'sync_token_rejected',
      message: `Auth rejected on ${req.method} ${req.path}`,
      context: { ip: req.ip || req.socket.remoteAddress, hasHeader: !!provided },
    }).catch(() => {});
    res.status(401).json({ error: 'Invalid or missing X-Sync-Token' });
    return false;
  }
  return true;
}

// Track boot time so /health can offer a startup grace period. During the
// first STARTUP_GRACE_MS after process start, /health returns 200 even if
// the DB is briefly unreachable (which happens during Railway redeploys
// when Postgres is reconnecting). After the grace window, /health flips
// to fail-fast 503 so Railway recycles the service if the DB really is
// dead. Without this, the rolling-redeploy window of DB churn was killing
// every new deploy.
const PROCESS_BOOT_AT = Date.now();
const STARTUP_GRACE_MS = 60_000;

app.get('/health', async (req, res) => {
  const inGrace = (Date.now() - PROCESS_BOOT_AT) < STARTUP_GRACE_MS;
  const timeoutMs = 2000;
  const timer = new Promise((resolve) => setTimeout(() => resolve({ status: 'unhealthy', database: 'unreachable', error: `health check timed out after ${timeoutMs}ms` }), timeoutMs));
  let status;
  try {
    status = await Promise.race([getHealthStatus(), timer]);
  } catch (err) {
    status = { status: 'unhealthy', database: 'unreachable', error: describeError(err) };
  }
  if (status && status.database === 'connected') return res.status(200).json(status);
  if (inGrace) {
    return res.status(200).json({ ...status, status: 'starting', database: 'starting', grace_remaining_ms: STARTUP_GRACE_MS - (Date.now() - PROCESS_BOOT_AT) });
  }
  return res.status(503).json(status || { status: 'unhealthy', database: 'unknown' });
});

// -------------------- Sync --------------------
let syncRunning = false;
let portalGuardOk = false; // set true by checkPortalCutover() at boot

app.post('/sync', async (req, res) => {
  if (!requireSyncToken(req, res)) return;
  if (!portalGuardOk) {
    return res.status(503).json({ error: 'Portal cutover guard failed; sync disabled. See logs / run scripts/cutover-portal.js.' });
  }
  if (syncRunning) return res.status(409).json({ error: 'Sync already in progress' });
  syncRunning = true;
  res.json({ message: 'Sync started', startedAt: new Date().toISOString() });
  try {
    await runFullSync(INCOMING_DIR, ARCHIVE_DIR, QUARANTINE_DIR);
  } catch (err) {
    await loud.alarm({
      event: 'manual_sync_failed',
      message: `Manual sync threw: ${describeError(err)}`,
      context: { stack: err && err.stack ? String(err.stack).split('\n').slice(0, 5).join('\n') : null },
    });
  } finally {
    syncRunning = false;
  }
});

// REST upload fallback per original SOW WS-001 TASK-002. Token-authenticated
// multipart upload. Files land in /incoming/ and the next /sync (manual or
// nightly cron) picks them up. Civista's MOVEit can use this if SFTP is
// unavailable.
app.post('/upload', upload.array('files'), (req, res) => {
  if (!requireSyncToken(req, res)) return;
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: 'No files uploaded' });
  }
  const received = [];
  for (const file of req.files) {
    const dest = path.join(INCOMING_DIR, file.originalname);
    fs.renameSync(file.path, dest);
    received.push(file.originalname);
  }
  res.json({ message: `Received ${received.length} files`, files: received });
});

// -------------------- Nightly cron --------------------
cron.schedule('0 2 * * *', async () => {
  if (!portalGuardOk) {
    await loud.alarm({
      event: 'cron_skip_portal_guard',
      message: 'Nightly cron skipped: portal cutover guard has not passed since boot. Run scripts/cutover-portal.js or restart the service.',
    });
    return;
  }
  if (syncRunning) {
    await loud.warn({ event: 'cron_skip', message: 'Nightly cron found a sync already in progress; skipped this tick' });
    return;
  }
  console.log('Cron: starting nightly sync');
  syncRunning = true;
  try {
    await runFullSync(INCOMING_DIR, ARCHIVE_DIR, QUARANTINE_DIR);
  } catch (err) {
    await loud.alarm({
      event: 'cron_failed',
      message: `Nightly cron sync threw: ${describeError(err)}`,
      context: { stack: err && err.stack ? String(err.stack).split('\n').slice(0, 5).join('\n') : null },
    });
  } finally {
    syncRunning = false;
  }
}, { timezone: 'America/New_York' });

// Start SFTP server if configured
startSftpServer({
  incomingDir: INCOMING_DIR,
  onFileReceived: (filePath) => {
    console.log(`SFTP file received: ${path.basename(filePath)}`);
  },
});

// Sandbox→prod portal cutover guard. On boot, fetch the current HubSpot
// portal id and compare to what's stored in `meta`. If different (or
// first ever), refuse to proceed until the operator runs
// `scripts/cutover-portal.js` to TRUNCATE staging+ledger and update meta.
async function checkPortalCutover() {
  const apiKey = process.env.HUBSPOT_API_KEY;
  if (!apiKey) {
    await loud.alarm({
      event: 'hubspot_key_missing',
      message: 'HUBSPOT_API_KEY env var is unset; refusing to proceed.',
    });
    return false;
  }
  let currentPortalId = null;
  try {
    const r = await fetch('https://api.hubapi.com/account-info/v3/details', {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) {
      await loud.alarm({
        event: 'hubspot_account_info_failed',
        message: `HubSpot account-info HTTP ${r.status}: ${body.message || 'no body'}`,
      });
      return false;
    }
    currentPortalId = String(body.portalId || body.hubId || '');
  } catch (e) {
    await loud.alarm({
      event: 'hubspot_account_info_failed',
      message: `HubSpot account-info fetch failed: ${describeError(e)}`,
    });
    return false;
  }
  if (!currentPortalId) {
    await loud.alarm({
      event: 'hubspot_account_info_empty',
      message: 'HubSpot returned no portal id; cannot enforce cutover guard.',
    });
    return false;
  }
  const stored = await pool.query(`SELECT value FROM meta WHERE key = 'last_portal_id'`);
  const storedPortalId = stored.rows[0]?.value || null;
  if (storedPortalId === null) {
    await pool.query(
      `INSERT INTO meta (key, value, updated_at) VALUES ('last_portal_id', $1, NOW())
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
      [currentPortalId]
    );
    console.log(`Portal cutover guard: registered first-ever portal id = ${currentPortalId}`);
    return true;
  }
  if (storedPortalId !== currentPortalId) {
    await loud.alarm({
      event: 'portal_cutover_required',
      message: `HUBSPOT_API_KEY portal (${currentPortalId}) differs from last-known portal (${storedPortalId}). REFUSING to start. Run scripts/cutover-portal.js to TRUNCATE staging+ledger and acknowledge the cutover.`,
      context: { stored: storedPortalId, current: currentPortalId },
    });
    return false;
  }
  console.log(`Portal cutover guard: portal ${currentPortalId} unchanged ✓`);
  return true;
}

// Start Express first, then init DB and run the portal cutover guard.
app.listen(port, () => {
  console.log(`civista-integration listening on port ${port}`);
  initDb()
    .then(() => {
      console.log('Database initialized');
      return checkPortalCutover();
    })
    .then((ok) => {
      portalGuardOk = ok;
      if (!ok) {
        console.error('Portal cutover guard FAILED — service will refuse /sync until resolved.');
      }
    })
    .catch((err) => console.error('Boot tasks failed (will retry on next request):', describeError(err)));
});

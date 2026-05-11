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
const { testAuth, uploadFile } = require('./src/sync/sftp-client');
const { hubspotFetch } = require('./src/sync/hubspot');
const { eventStream, installConsoleHook, emitHubspot } = require('./src/monitoring/event-stream');
const { TABLES, CIF_CONTACT_FIELDS, CIF_COMPANY_FIELDS } = require('./src/transform/hubspot-mapping');
const { describeError } = require('./src/monitoring/errors');
const loud = require('./src/monitoring/loud');

// Forward all console output to SSE subscribers so the UI log panel sees it.
installConsoleHook();

// Surface async crashes that would otherwise be invisible to the UI log
// stream. Routes through loud.alarm so they're persisted and shown in
// /api/issues (per memory financial_data_rules.md — every failure loud).
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

// Feature flags (Railway env vars). Defaults are conservative for prod:
// the debug UI, wire log, and schema-check route are OFF unless explicitly
// enabled. Manual /sync requires a token; without one set, /sync returns 403
// and only the cron can trigger a sync.
const ENABLE_DEBUG_UI    = process.env.ENABLE_DEBUG_UI === '1';
const ENABLE_WIRE_LOG    = process.env.ENABLE_WIRE_LOG === '1';
const ENABLE_SCHEMA_CHECK = process.env.ENABLE_SCHEMA_CHECK === '1';
const MANUAL_SYNC_TOKEN  = process.env.MANUAL_SYNC_TOKEN || null;

fs.mkdirSync(INCOMING_DIR, { recursive: true });
fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
fs.mkdirSync(QUARANTINE_DIR, { recursive: true });

// Files uploaded by the browser land in the OS tmp dir, then we SFTP them
// into our own SFTP endpoint. We never write them directly to /incoming/ —
// that would bypass the SFTP path we need to prove works.
const TMP_UPLOAD_DIR = path.join(os.tmpdir(), 'civista_browser_uploads');
fs.mkdirSync(TMP_UPLOAD_DIR, { recursive: true });
const upload = multer({ dest: TMP_UPLOAD_DIR });

app.use(express.json());

// Bank-grade HTTP Basic auth on every route except /health.
//
// Credentials match SFTP_USER + SFTP_PASS (per user direction: same creds
// for UI and SFTP to keep operator memory simple). The password never
// appears in the front-end code; the browser handles Basic auth natively
// via the WWW-Authenticate prompt.
//
// Comparison uses crypto.timingSafeEqual on SHA-256 digests of both the
// expected and given values. SHA-256 outputs are always 32 bytes, so the
// equal-length precondition holds regardless of input length and we don't
// leak password length via timing.
//
// Failed attempts are loud-surfaced with the offending IP so brute-force
// attempts show up in /api/issues.
const crypto = require('crypto');
function digestForCompare(s) {
  return crypto.createHash('sha256').update(String(s || ''), 'utf8').digest();
}
const EXPECTED_USER_DIGEST = digestForCompare(process.env.SFTP_USER || '');
const EXPECTED_PASS_DIGEST = digestForCompare(process.env.SFTP_PASS || '');
const HAS_AUTH_CREDS = !!(process.env.SFTP_USER && process.env.SFTP_PASS);

app.use((req, res, next) => {
  // /health must remain unauthenticated for Railway healthcheck.
  if (req.path === '/health') return next();
  // If creds aren't configured we fail closed: 503 with a loud alarm.
  if (!HAS_AUTH_CREDS) {
    loud.alarm({
      event: 'ui_auth_misconfigured',
      message: 'SFTP_USER or SFTP_PASS not set; refusing all UI/API requests',
      context: { path: req.path },
    }).catch(() => {});
    return res.status(503).send('Service auth misconfigured');
  }
  const header = req.get('Authorization') || '';
  if (!header.startsWith('Basic ')) {
    res.set('WWW-Authenticate', 'Basic realm="civista-integration", charset="UTF-8"');
    return res.status(401).send('Authentication required');
  }
  let user = '', pass = '';
  try {
    const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
    const idx = decoded.indexOf(':');
    if (idx >= 0) {
      user = decoded.slice(0, idx);
      pass = decoded.slice(idx + 1);
    }
  } catch (_) { /* malformed header */ }

  const userOk = crypto.timingSafeEqual(digestForCompare(user), EXPECTED_USER_DIGEST);
  const passOk = crypto.timingSafeEqual(digestForCompare(pass), EXPECTED_PASS_DIGEST);
  if (!userOk || !passOk) {
    loud.warn({
      event: 'ui_basic_auth_rejected',
      message: `UI basic auth rejected for user="${user.slice(0, 24)}"`,
      context: { ip: req.ip || req.socket.remoteAddress, path: req.path },
    }).catch(() => {});
    res.set('WWW-Authenticate', 'Basic realm="civista-integration", charset="UTF-8"');
    return res.status(401).send('Invalid credentials');
  }
  next();
});

// Static assets for the debug UI — only when ENABLE_DEBUG_UI=1.
// When the flag is off, GET / and any /index.html etc. return 404 so the
// service is API-only in prod by default.
if (ENABLE_DEBUG_UI) {
  app.use(express.static(path.join(__dirname, 'public')));
} else {
  app.get('/', (req, res) => res.status(404).send('Debug UI disabled (set ENABLE_DEBUG_UI=1)'));
}

// Service info — public, no auth, no flag.
app.get('/api/info', (req, res) => {
  res.json({
    service: 'civista-integration',
    version: '1.0.0',
    description: 'Civista Bank HubSpot nightly data sync pipeline',
    features: {
      debug_ui: ENABLE_DEBUG_UI,
      wire_log: ENABLE_WIRE_LOG,
      schema_check: ENABLE_SCHEMA_CHECK,
      manual_sync: !!MANUAL_SYNC_TOKEN,
    },
  });
});

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
  // Race the DB-backed health query against a 2s timeout.
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
    // Don't fail healthcheck during boot. Tell Railway we're starting.
    return res.status(200).json({ ...status, status: 'starting', database: 'starting', grace_remaining_ms: STARTUP_GRACE_MS - (Date.now() - PROCESS_BOOT_AT) });
  }
  return res.status(503).json(status || { status: 'unhealthy', database: 'unknown' });
});

// -------------------- Sync --------------------
let syncRunning = false;
let portalGuardOk = false; // set true by checkPortalCutover() at boot

// Manual sync route. Auth is enforced by the Basic Auth middleware
// at the top of the request chain; if you got here, you're authorized.
// (X-Sync-Token is honored as an OPTIONAL secondary auth path for
// non-browser clients but no longer required.)
app.post('/sync', async (req, res) => {
  if (MANUAL_SYNC_TOKEN) {
    const provided = req.get('X-Sync-Token');
    // If a token was provided, it must match. Missing is fine (Basic
    // Auth already gated). Wrong token is suspicious — log it loud.
    if (provided && provided !== MANUAL_SYNC_TOKEN) {
      await loud.warn({
        event: 'manual_sync_auth_rejected',
        message: 'POST /sync attempted with a wrong X-Sync-Token (Basic auth had passed)',
        context: { hasHeader: !!provided },
      });
      return res.status(403).json({ error: 'X-Sync-Token mismatch' });
    }
  }
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

// -------------------- SFTP auth (test only) --------------------
app.post('/api/sftp-auth', async (req, res) => {
  const { host, port, username, password } = req.body || {};
  if (!host || !port || !username || !password) {
    return res.status(400).json({ error: 'host, port, username, password are required' });
  }
  try {
    await testAuth({ host, port, username, password });
    res.json({ ok: true });
  } catch (e) {
    // Diagnostic on failure: log enough to identify autofill / whitespace
    // issues without leaking the password. Compare received vs expected
    // length and first/last char codes.
    const expected = process.env.SFTP_PASS || '';
    const recv = String(password);
    const summary = {
      length_recv: recv.length,
      length_expected: expected.length,
      first_char_code: recv.charCodeAt(0) || null,
      last_char_code: recv.charCodeAt(recv.length - 1) || null,
      has_leading_ws: /^\s/.test(recv),
      has_trailing_ws: /\s$/.test(recv),
      username_recv: username,
    };
    console.warn(`SFTP auth failed from UI: ${e.message} · diagnostic=${JSON.stringify(summary)}`);
    res.status(401).json({ error: e.message });
  }
});

// -------------------- SFTP upload from browser --------------------
// Browser sends a single file + creds. Server uses creds to SFTP-put the
// file into its own SFTP endpoint (same host). If SFTP is down or creds
// are bad, the upload fails — which is exactly the guarantee we want.
app.post('/api/sftp-upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'no file' });
  let creds;
  try { creds = JSON.parse(req.body.creds || '{}'); }
  catch { return res.status(400).json({ error: 'invalid creds json' }); }
  if (!creds.host || !creds.port || !creds.username || !creds.password) {
    return res.status(400).json({ error: 'creds must include host, port, username, password' });
  }

  const remoteName = path.basename(req.file.originalname);
  try {
    await uploadFile(creds, req.file.path, remoteName);
    console.log(`Browser→SFTP: pushed ${remoteName} to ${creds.host}:${creds.port}`);
    res.json({ ok: true, remoteName });
  } catch (e) {
    console.warn(`SFTP upload failed for ${remoteName}: ${e.message}`);
    res.status(502).json({ error: e.message });
  } finally {
    // Clean up the temp upload file. Log if it fails so we don't quietly leak tmpfs.
    fs.unlink(req.file.path, (err) => {
      if (err && err.code !== 'ENOENT') {
        console.warn(`Failed to clean tmp upload ${req.file.path}: ${err.code || err.message}`);
      }
    });
  }
});

// -------------------- Legacy direct upload (kept for CLI use) --------------------
app.post('/upload', upload.array('files'), (req, res) => {
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

// -------------------- SSE log stream --------------------
app.get('/api/stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders();
  res.write('retry: 3000\n\n');
  let detached = false;
  const detach = () => {
    if (detached) return;
    detached = true;
    eventStream.off('log', send);
  };
  const send = (payload) => {
    if (detached) return;
    try {
      res.write(`event: log\ndata: ${JSON.stringify(payload)}\n\n`);
    } catch (e) {
      // Likely client disconnect mid-write — detach so we don't leak listeners
      // and keep emitting into a dead socket on every future log event.
      detach();
    }
  };
  send({ level: 'info', message: 'SSE log stream connected', at: new Date().toISOString() });
  // HubSpot wire-log feed — separate SSE event name. Only attached when
  // ENABLE_WIRE_LOG=1 because emitHubspot also no-ops when disabled.
  const sendHubspot = (record) => {
    if (detached) return;
    try {
      res.write(`event: hubspot\ndata: ${JSON.stringify(record)}\n\n`);
    } catch (e) {
      detach();
    }
  };
  eventStream.on('log', send);
  if (ENABLE_WIRE_LOG) eventStream.on('hubspot', sendHubspot);
  const fullDetach = () => {
    detach();
    if (ENABLE_WIRE_LOG) eventStream.off('hubspot', sendHubspot);
  };
  req.on('close', fullDetach);
  res.on('error', fullDetach);
});

// -------------------- Schema check (live HubSpot + Postgres) --------------------
const SCHEMA_OBJECTS = [
  { stagingTable: 'stg_contacts',      hsObject: 'contacts',   csvFile: 'HubSpot_CIF.csv',        label: 'Contacts',       fields: CIF_CONTACT_FIELDS },
  { stagingTable: 'stg_companies',     hsObject: 'companies',  csvFile: 'HubSpot_CIF.csv',        label: 'Companies',      fields: CIF_COMPANY_FIELDS },
  { stagingTable: 'stg_deposits',      hsObject: '2-60442978', csvFile: 'HubSpot_DDA.csv',        label: 'Deposits',       fields: TABLES.dda.fields },
  { stagingTable: 'stg_loans',         hsObject: '2-60442977', csvFile: 'HubSpot_Loan.csv',       label: 'Loans',          fields: TABLES.loans.fields },
  { stagingTable: 'stg_time_deposits', hsObject: '2-60442980', csvFile: 'HubSpot_CD.csv',         label: 'Time Deposits',  fields: TABLES.cd.fields },
  { stagingTable: 'stg_debit_cards',   hsObject: '2-60442979', csvFile: 'HubSpot_Debit_Card.csv', label: 'Debit Cards',    fields: TABLES.debit_cards.fields },
];

async function countCsvRows(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return new Promise((resolve, reject) => {
    let count = 0;
    let remainder = '';
    const s = fs.createReadStream(filePath, { encoding: 'utf8' });
    s.on('data', (chunk) => {
      const lines = (remainder + chunk).split('\n');
      remainder = lines.pop();
      count += lines.length;
    });
    s.on('end', () => { if (remainder.trim()) count++; resolve(Math.max(0, count - 1)); });
    s.on('error', reject);
  });
}

app.get('/api/schema-check', async (req, res) => {
  // Gated by ENABLE_SCHEMA_CHECK so prod doesn't accidentally hammer
  // HubSpot 12 calls per /api/schema-check load. Also stays on-click only
  // in the UI per Q12.
  if (!ENABLE_SCHEMA_CHECK) {
    return res.status(404).json({ error: 'Schema check disabled (set ENABLE_SCHEMA_CHECK=1)' });
  }
  try {
    const apiKey = process.env.HUBSPOT_API_KEY;
    if (!apiKey) return res.status(500).json({ error: 'HUBSPOT_API_KEY not set' });

    const objects = [];
    for (const o of SCHEMA_OBJECTS) {
      const objectErrors = [];

      // Postgres columns
      let pgCols;
      try {
        pgCols = await pool.query(
          `SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
          [o.stagingTable]
        );
      } catch (e) {
        const msg = `Postgres column lookup failed: ${describeError(e)}`;
        console.error(`schema-check[${o.label}] ${msg}`);
        objects.push({ label: o.label, stagingTable: o.stagingTable, hsObject: o.hsObject,
          matched: 0, missingInHs: 0, missingInPg: 0,
          pgRowCount: null, hsRowCount: null, csvRowCount: null,
          fields: [], errors: [msg] });
        continue;
      }
      const pgColSet = new Set(pgCols.rows.map(r => r.column_name));
      // Internal columns we never expect to find on the HubSpot side.
      // Includes the audit columns added in the railway-only refactor:
      // raw_csv (verbatim source), db_persist_hash + db_verified_at
      // (HASH B), hubspot_persist_hash + hubspot_verified_at +
      // hubspot_verify_diff (HASH C), needs_review (mismatch flag),
      // coercions (transformation audit). All staging-only.
      const internal = new Set([
        'id', 'row_hash', 'loaded_at', 'synced_at',
        'raw_csv',
        'db_persist_hash', 'db_verified_at',
        'hubspot_persist_hash', 'hubspot_verified_at', 'hubspot_verify_diff',
        'needs_review',
        'coercions',
      ]);

      // HubSpot properties — must surface failures, not silently treat 401 as "0 properties".
      let hsProps = [];
      const propPath = `/crm/v3/properties/${o.hsObject}`;
      try {
        const startedAt = Date.now();
        const hsRes = await fetch(`https://api.hubapi.com${propPath}`, {
          headers: { Authorization: `Bearer ${apiKey}` },
        });
        const hsText = await hsRes.text();
        let hsBody = {};
        try { hsBody = JSON.parse(hsText); } catch { hsBody = { raw: hsText }; }
        emitHubspot({
          method: 'GET', path: propPath, status: hsRes.status,
          durationMs: Date.now() - startedAt,
          response: { results: Array.isArray(hsBody.results) ? hsBody.results.length : undefined, message: hsBody.message },
          source: 'schema-check',
        });
        if (!hsRes.ok) {
          const msg = `HubSpot ${hsRes.status} on /properties/${o.hsObject}: ${hsBody.message || hsBody.raw || 'no body'}`;
          console.error(`schema-check[${o.label}] ${msg}`);
          objectErrors.push(msg);
        } else {
          hsProps = Array.isArray(hsBody.results) ? hsBody.results : [];
        }
      } catch (e) {
        const msg = `HubSpot fetch failed for ${o.hsObject}: ${describeError(e)}`;
        console.error(`schema-check[${o.label}] ${msg}`);
        objectErrors.push(msg);
      }
      const hsPropMap = new Map(hsProps.map(p => [p.name, p]));

      // Diff: for each non-internal PG column, find matching HS property.
      const rows = [];
      let matched = 0, missingInHs = 0, missingInPg = 0;
      for (const c of pgCols.rows) {
        if (internal.has(c.column_name)) continue;
        const hs = hsPropMap.get(c.column_name);
        if (hs) {
          rows.push({ pgColumn: c.column_name, hsProp: hs.name, hsType: hs.type + '/' + hs.fieldType, status: 'match' });
          matched++;
        } else {
          rows.push({ pgColumn: c.column_name, hsProp: '', hsType: '', status: 'pg-only' });
          missingInHs++;
        }
      }
      // HS-only custom properties that we don't populate.
      const customHs = hsProps.filter(p => !p.hubspotDefined);
      for (const p of customHs) {
        if (pgColSet.has(p.name)) continue;
        rows.push({ pgColumn: '', hsProp: p.name, hsType: p.type + '/' + p.fieldType, status: 'hs-only' });
        missingInPg++;
      }

      // Postgres row count — distinguish "missing table" from "query failed".
      let pgCount = null;
      try {
        const c = await pool.query(`SELECT COUNT(*)::int AS c FROM ${o.stagingTable}`);
        pgCount = c.rows[0].c;
      } catch (e) {
        const msg = `Postgres row count failed on ${o.stagingTable}: ${describeError(e)}`;
        console.error(`schema-check[${o.label}] ${msg}`);
        objectErrors.push(msg);
      }

      // HubSpot total (search API returns total) — must surface 4xx/5xx, not pretend "—".
      let hsCount = null;
      const searchPath = `/crm/v3/objects/${o.hsObject}/search`;
      try {
        const startedAt = Date.now();
        const sr = await fetch(`https://api.hubapi.com${searchPath}`, {
          method: 'POST',
          headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ limit: 1 }),
        });
        const srText = await sr.text();
        let sb = {};
        try { sb = JSON.parse(srText); } catch { sb = { raw: srText }; }
        emitHubspot({
          method: 'POST', path: searchPath, status: sr.status,
          durationMs: Date.now() - startedAt,
          body: { limit: 1 },
          response: { total: sb.total, message: sb.message },
          source: 'schema-check',
        });
        if (!sr.ok) {
          const msg = `HubSpot ${sr.status} on search ${o.hsObject}: ${sb.message || sb.raw || 'no body'}`;
          console.error(`schema-check[${o.label}] ${msg}`);
          objectErrors.push(msg);
        } else {
          hsCount = sb.total ?? null;
        }
      } catch (e) {
        const msg = `HubSpot search failed for ${o.hsObject}: ${describeError(e)}`;
        console.error(`schema-check[${o.label}] ${msg}`);
        objectErrors.push(msg);
      }

      // CSV row count if file is in /incoming/
      const csvPath = path.join(INCOMING_DIR, o.csvFile);
      let csvCount = null;
      try {
        csvCount = await countCsvRows(csvPath);
      } catch (e) {
        const msg = `CSV row count failed for ${o.csvFile}: ${describeError(e)}`;
        console.warn(`schema-check[${o.label}] ${msg}`);
        objectErrors.push(msg);
      }

      objects.push({
        label: o.label,
        stagingTable: o.stagingTable,
        hsObject: o.hsObject,
        matched, missingInHs, missingInPg,
        pgRowCount: pgCount,
        hsRowCount: hsCount,
        csvRowCount: csvCount,
        fields: rows,
        errors: objectErrors,
      });
    }
    res.json({ objects });
  } catch (e) {
    const msg = describeError(e);
    console.error('schema-check failed:', msg);
    res.status(500).json({ error: msg });
  }
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

// -------------------- Issues feed (UI Data Issues panel) --------------------
app.get('/api/issues', async (req, res) => {
  try {
    const mappingIssues = await pool.query(
      `SELECT id, source_csv, source_column, hs_object, hs_property, hs_type, problem,
              sample_value, rows_affected, first_seen, last_seen
       FROM mapping_issues ORDER BY last_seen DESC`
    );

    // Aggregate sync_errors across the most-recent 24h (a single nightly run
    // may produce multiple run_ids — one per staging table — so MAX(run_id)
    // alone misses most of them). 24h is wide enough to catch yesterday's
    // run if you check in the morning.
    const lastRun = await pool.query(`SELECT MAX(id) AS id FROM sync_log`);
    const runId = lastRun.rows[0]?.id || null;

    const bySeverity = await pool.query(
      `SELECT severity, COUNT(*)::int AS c FROM sync_errors
       WHERE created_at > NOW() - INTERVAL '24 hours'
       GROUP BY severity`
    );
    const byType = await pool.query(
      `SELECT error_type, COUNT(*)::int AS c FROM sync_errors
       WHERE created_at > NOW() - INTERVAL '24 hours'
       GROUP BY error_type ORDER BY c DESC`
    );
    const samples = await pool.query(
      `SELECT id, run_id, source_table, source_key, error_type, severity, error_message, created_at
       FROM sync_errors ORDER BY created_at DESC LIMIT 50`
    );

    // Hash health across all staging tables (cumulative across runs).
    const stagingTables = ['stg_contacts','stg_companies','stg_deposits','stg_loans','stg_time_deposits','stg_debit_cards'];
    const hashHealth = { csv_to_db: { ok: 0, mismatch: 0, pending: 0 }, db_to_hubspot: { ok: 0, mismatch: 0, pending: 0 } };
    for (const t of stagingTables) {
      try {
        const r = await pool.query(`
          SELECT
            COUNT(*) FILTER (WHERE db_persist_hash IS NOT NULL AND needs_review IS NOT TRUE)::int AS ab_ok,
            COUNT(*) FILTER (WHERE db_persist_hash IS NOT NULL AND needs_review IS TRUE)::int AS ab_mismatch,
            COUNT(*) FILTER (WHERE db_persist_hash IS NULL)::int AS ab_pending,
            COUNT(*) FILTER (WHERE hubspot_persist_hash IS NOT NULL AND hubspot_verify_diff IS NULL)::int AS bc_ok,
            COUNT(*) FILTER (WHERE hubspot_persist_hash IS NOT NULL AND hubspot_verify_diff IS NOT NULL)::int AS bc_mismatch,
            COUNT(*) FILTER (WHERE hubspot_persist_hash IS NULL)::int AS bc_pending
          FROM ${t}
        `);
        const row = r.rows[0];
        hashHealth.csv_to_db.ok += row.ab_ok;
        hashHealth.csv_to_db.mismatch += row.ab_mismatch;
        hashHealth.csv_to_db.pending += row.ab_pending;
        hashHealth.db_to_hubspot.ok += row.bc_ok;
        hashHealth.db_to_hubspot.mismatch += row.bc_mismatch;
        hashHealth.db_to_hubspot.pending += row.bc_pending;
      } catch { /* table may not exist yet */ }
    }

    res.json({
      mapping_issues: mappingIssues.rows,
      recent_run: {
        run_id: runId,
        by_severity: Object.fromEntries(bySeverity.rows.map(r => [r.severity, r.c])),
        by_type:     Object.fromEntries(byType.rows.map(r => [r.error_type, r.c])),
        samples:     samples.rows,
      },
      hash_health: hashHealth,
    });
  } catch (e) {
    const msg = describeError(e);
    console.error('issues query failed:', msg);
    res.status(500).json({ error: msg });
  }
});

// Coercion audit — every transformation recorded on the staging row.
// Per Q5 / memory rule 3: every coercion is auditable. This endpoint
// aggregates the `coercions` JSONB across all 6 staging tables so the UI
// can show "what was transformed and why" before the operator approves
// the data going to HubSpot.
app.get('/api/coercions', async (req, res) => {
  const STAGING = ['stg_contacts','stg_companies','stg_deposits','stg_loans','stg_time_deposits','stg_debit_cards'];
  try {
    const byCoerce = {};
    const samples = []; // up to 25 sample rows showing actual from/to pairs
    for (const t of STAGING) {
      let r;
      try {
        r = await pool.query(`
          SELECT id, ${t === 'stg_debit_cards' ? 'composite_key AS source_key' : (t === 'stg_contacts' || t === 'stg_companies' ? 'cif_number AS source_key' : 'primary_key AS source_key')}, coercions
          FROM ${t}
          WHERE coercions IS NOT NULL AND jsonb_array_length(coercions) > 0
          ORDER BY id DESC
          LIMIT 1000
        `);
      } catch (e) { continue; }
      for (const row of r.rows) {
        const arr = row.coercions || [];
        for (const c of arr) {
          const k = c.coerce || 'unknown';
          if (!byCoerce[k]) byCoerce[k] = { count: 0, props: {} };
          byCoerce[k].count++;
          byCoerce[k].props[c.prop] = (byCoerce[k].props[c.prop] || 0) + 1;
          if (samples.length < 25) {
            samples.push({ table: t, source_key: row.source_key, ...c });
          }
        }
      }
    }
    res.json({ by_coerce: byCoerce, samples });
  } catch (e) {
    res.status(500).json({ error: describeError(e) });
  }
});

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
//
// Per memory rule 6: sandbox is Civista's child portal linked to the
// live account; switching keys without clearing the sandbox-portal
// hubspot_ids in shipped_records would silently corrupt prod sync.
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
    // First ever boot against a portal — record it.
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

// Seed mapping_issues from any field flagged `send: false` in the mapping.
// One row per held mapping so the UI's Data Issues panel always reflects
// current state, even before any /sync runs. Each held mapping replaces
// what would otherwise be ~80 per-record HubSpot rejections per sync.
async function seedMappingIssues() {
  const allFields = [
    { csv: 'HubSpot_CIF.csv', hsObject: 'contacts',    fields: CIF_CONTACT_FIELDS },
    { csv: 'HubSpot_CIF.csv', hsObject: 'companies',   fields: CIF_COMPANY_FIELDS },
    { csv: 'HubSpot_DDA.csv', hsObject: '2-60442978',  fields: TABLES.dda.fields },
    { csv: 'HubSpot_Loan.csv', hsObject: '2-60442977', fields: TABLES.loans.fields },
    { csv: 'HubSpot_CD.csv', hsObject: '2-60442980',   fields: TABLES.cd.fields },
    { csv: 'HubSpot_Debit_Card.csv', hsObject: '2-60442979', fields: TABLES.debit_cards.fields },
  ];
  for (const grp of allFields) {
    for (const f of grp.fields) {
      if (f.send !== false) continue;
      await loud.mappingIssue({
        sourceCsv: grp.csv, sourceColumn: f.csv,
        hsObject: grp.hsObject, hsProperty: f.prop, hsType: f.type,
        problem: f.holdReason || 'Mapping held — middleware will not transmit',
      });
    }
  }
}

// Demo reset endpoint. TRUNCATEs Postgres state, archives every record
// in the 6 HubSpot objects, clears the on-disk file dirs. Restores the
// service to a pristine pre-sync state for clean demos. The demo flag
// gates this since it's destructive and prod must never expose it.
const ENABLE_DEMO_RESET = process.env.ENABLE_DEMO_RESET === '1';

async function wipeHubspotObject(objectType) {
  let archived = 0;
  // Page through, accumulating ids 100 at a time, then batch-archive.
  let after = null;
  while (true) {
    const qs = after ? `?limit=100&after=${encodeURIComponent(after)}` : '?limit=100';
    const list = await hubspotFetch(`/crm/v3/objects/${objectType}${qs}`, { method: 'GET' });
    if (!list.ok) break;
    const ids = (list.body?.results || []).map(r => r.id);
    if (ids.length === 0) break;
    const archive = await hubspotFetch(`/crm/v3/objects/${objectType}/batch/archive`, {
      method: 'POST',
      body: JSON.stringify({ inputs: ids.map(id => ({ id })) }),
    });
    if (!archive.ok) break;
    archived += ids.length;
    after = list.body?.paging?.next?.after || null;
    if (!after) break;
  }
  return archived;
}

app.post('/api/demo-reset', async (req, res) => {
  if (!ENABLE_DEMO_RESET) {
    return res.status(404).json({ error: 'Demo reset disabled (set ENABLE_DEMO_RESET=1)' });
  }
  if (syncRunning) {
    return res.status(409).json({ error: 'Cannot reset while sync is running' });
  }
  await loud.warn({
    event: 'demo_reset_triggered',
    message: 'Demo reset starting: TRUNCATE Postgres state + archive HubSpot records + clear on-disk dirs',
  });
  const result = { postgres: {}, hubspot: {}, files: {} };
  try {
    // 1. TRUNCATE Postgres (preserves meta so portal guard stays valid).
    const tables = [
      'sync_errors','sync_log','mapping_issues','shipped_records','hubspot_id_map',
      'stg_contacts','stg_companies','stg_deposits','stg_loans','stg_time_deposits','stg_debit_cards',
    ];
    for (const t of tables) {
      try { await pool.query(`TRUNCATE ${t} RESTART IDENTITY`); result.postgres[t] = 'truncated'; }
      catch (e) { result.postgres[t] = 'skip:' + (e.code || e.message); }
    }
    // 2. Archive every record in the 6 HubSpot objects.
    const objects = ['contacts','companies','2-60442978','2-60442977','2-60442980','2-60442979'];
    for (const obj of objects) {
      result.hubspot[obj] = await wipeHubspotObject(obj);
    }
    // 3. Clear on-disk file dirs.
    for (const dir of [INCOMING_DIR, ARCHIVE_DIR, QUARANTINE_DIR]) {
      try {
        for (const entry of fs.readdirSync(dir)) {
          const full = path.join(dir, entry);
          const stat = fs.lstatSync(full);
          if (stat.isDirectory()) fs.rmSync(full, { recursive: true, force: true });
          else fs.unlinkSync(full);
        }
        result.files[dir] = 'cleared';
      } catch (e) { result.files[dir] = 'skip:' + (e.code || e.message); }
    }
    // 4. Re-seed mapping_issues so held mappings reappear immediately.
    await seedMappingIssues();
    res.json({ ok: true, result });
  } catch (e) {
    res.status(500).json({ ok: false, error: describeError(e), result });
  }
});

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
      return seedMappingIssues();
    })
    .then(() => console.log('Mapping issues seeded'))
    .catch((err) => console.error('Boot tasks failed (will retry on next request):', describeError(err)));
});

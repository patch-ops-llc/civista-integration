const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('ssh2');
const loud = require('../monitoring/loud');
const { drawBox } = require('../monitoring/box');

/**
 * Resolve the SSH host key. Two options:
 *   1. SFTP_HOST_KEY_PEM  — full PEM contents as env var (preferred on Railway,
 *                           containers have no persistent disk for a key file)
 *   2. SFTP_HOST_KEY      — filesystem path to a PEM file (local dev)
 *
 * Returns a Buffer of the key, or null if neither is available.
 */
function resolveHostKey() {
  if (process.env.SFTP_HOST_KEY_PEM && process.env.SFTP_HOST_KEY_PEM.trim() !== '') {
    const pem = process.env.SFTP_HOST_KEY_PEM;
    const tmp = path.join(os.tmpdir(), 'civista_sftp_hostkey');
    fs.writeFileSync(tmp, pem, { mode: 0o600 });
    return { source: 'env SFTP_HOST_KEY_PEM', buffer: Buffer.from(pem) };
  }
  const p = process.env.SFTP_HOST_KEY;
  if (p && fs.existsSync(p)) {
    return { source: `file ${p}`, buffer: fs.readFileSync(p) };
  }
  return null;
}

function startSftpServer(options = {}) {
  const {
    port = parseInt(process.env.SFTP_PORT || '2222', 10),
    incomingDir = path.join(__dirname, '../../incoming'),
    onFileReceived,
  } = options;

  const key = resolveHostKey();
  if (!key) {
    for (const out of drawBox([
      'SFTP server NOT started: no host key configured',
      'Set SFTP_HOST_KEY_PEM (env var) or SFTP_HOST_KEY (file path)',
    ])) console.log(out);
    return null;
  }

  // Refuse to start with an empty password — an empty SFTP_PASS would accept
  // any connection from the configured username.
  if (!process.env.SFTP_PASS || process.env.SFTP_PASS.trim() === '') {
    for (const out of drawBox([
      'SFTP server NOT started: SFTP_PASS is empty or unset',
      'Refusing to run with unauthenticated access',
    ])) console.error(out);
    return null;
  }

  // Refuse to start if SFTP_PORT collides with the HTTP port. Otherwise we
  // crash later with EADDRINUSE and Railway kills the deploy with no
  // informative message. Surface the conflict at the right place.
  const httpPort = parseInt(process.env.PORT || '3000', 10);
  if (port === httpPort) {
    for (const out of drawBox([
      'SFTP server NOT started: SFTP_PORT === PORT',
      `Both want :${port}. Set PORT to a different value (Railway default is 8080) or set SFTP_PORT to something other than ${port}.`,
      'Common fix: remove PORT env var so Railway auto-sets it.',
    ])) console.error(out);
    return null;
  }

  fs.mkdirSync(incomingDir, { recursive: true });

  const hostKey = key.buffer;
  const allowedUser = process.env.SFTP_USER || 'civista';
  const allowedPass = process.env.SFTP_PASS;

  const server = new Server({ hostKeys: [hostKey] }, (client) => {
    console.log('SFTP client connected');

    // Auth: accept both 'password' (programmatic clients like ssh2-sftp-client
    // and OpenSSH) and 'keyboard-interactive' (Cyberduck, FileZilla, and most
    // modern GUI SFTP clients default to this; they often refuse to fall back
    // to plain password if it's not advertised). Both methods are functionally
    // password auth — keyboard-interactive just wraps it in a prompt round-trip.
    // Public key auth is intentionally not offered.
    const ADVERTISED = ['password', 'keyboard-interactive'];
    const rejectWithLog = (ctx) => {
      ctx.reject(ADVERTISED);
      loud.warn({
        event: 'sftp_auth_rejected',
        message: `SFTP auth rejected for user=${ctx.username}`,
        context: { username: ctx.username, method: ctx.method },
      }).catch(() => {});
    };

    client.on('authentication', (ctx) => {
      if (ctx.method === 'password') {
        if (ctx.username === allowedUser && ctx.password === allowedPass) return ctx.accept();
        return rejectWithLog(ctx);
      }
      if (ctx.method === 'keyboard-interactive') {
        // Send a single password prompt. The client renders it and sends the
        // typed response back as responses[0]. Validate identically to the
        // password method.
        return ctx.prompt([{ prompt: 'Password: ', echo: false }], (responses) => {
          if (ctx.username === allowedUser && responses && responses[0] === allowedPass) {
            return ctx.accept();
          }
          rejectWithLog(ctx);
        });
      }
      // 'none' probe or unsupported method (publickey, hostbased): tell the
      // client what we support so it can retry with the right method.
      ctx.reject(ADVERTISED);
    });

    client.on('ready', () => {
      console.log('SFTP client authenticated');

      client.on('session', (accept) => {
        const session = accept();

        session.on('sftp', (accept) => {
          const sftp = accept();
          const openFiles = new Map();
          let handleCount = 0;

          sftp.on('OPEN', (reqid, filename, flags) => {
            const handle = Buffer.alloc(4);
            const filePath = path.join(incomingDir, path.basename(filename));
            handle.writeUInt32BE(handleCount++);
            const stream = fs.createWriteStream(filePath);
            // pendingCallbacks holds write callbacks awaiting the stream's
            // 'drain' event when backpressure was triggered, so a stream
            // error can fail every still-pending WRITE rather than letting
            // some get OK'd while others are stuck.
            const entry = { path: filePath, stream, writeError: null, pending: [] };
            stream.on('error', (err) => {
              entry.writeError = err;
              // Fail every callback that hasn't been resolved yet.
              const pend = entry.pending.splice(0);
              for (const cb of pend) {
                try { cb(err); } catch {}
              }
              loud.alarm({
                event: 'sftp_write_error',
                message: `SFTP write stream error on ${path.basename(filePath)}: ${err.code || err.message}`,
                context: { path: filePath, code: err.code },
              }).catch(() => {});
            });
            openFiles.set(handle.toString('hex'), entry);
            sftp.handle(reqid, handle);
          });

          sftp.on('WRITE', (reqid, handle, offset, data) => {
            const file = openFiles.get(handle.toString('hex'));
            if (!file) {
              sftp.status(reqid, 4); // FAILURE
              return;
            }
            if (file.writeError) {
              sftp.status(reqid, 4, `disk write failed: ${file.writeError.code || file.writeError.message}`);
              return;
            }
            // Track this write so a later stream-level error can fail it.
            const cb = (err) => {
              const ix = file.pending.indexOf(cb);
              if (ix >= 0) file.pending.splice(ix, 1);
              if (err) {
                file.writeError = err;
                console.error(`✘ SFTP WRITE failed on ${path.basename(file.path)}: ${err.code || err.message}`);
                sftp.status(reqid, 4, `write failed: ${err.code || err.message}`);
              } else {
                sftp.status(reqid, 0); // OK
              }
            };
            file.pending.push(cb);
            // write() returns false when internal buffer is full → backpressure.
            // ssh2 doesn't pipeline aggressively but we still respect it: when
            // backpressure hits, callbacks accumulate until 'drain'.
            const ok = file.stream.write(data, cb);
            if (!ok) {
              file.stream.once('drain', () => { /* callbacks fire as writes flush */ });
            }
          });

          sftp.on('CLOSE', (reqid, handle) => {
            const key = handle.toString('hex');
            const file = openFiles.get(key);
            if (!file) {
              sftp.status(reqid, 0);
              return;
            }
            // If we already saw a write error, report failure — and DO NOT fire
            // onFileReceived (the file is corrupt/incomplete).
            if (file.writeError) {
              file.stream.destroy();
              openFiles.delete(key);
              loud.alarm({
                event: 'sftp_close_aborted',
                message: `SFTP delivery aborted on ${path.basename(file.path)} due to prior write error`,
                context: { path: file.path, code: file.writeError.code, reason: file.writeError.message },
              }).catch(() => {});
              sftp.status(reqid, 4, `upload aborted: ${file.writeError.code || file.writeError.message}`);
              return;
            }
            // Wait for the stream to fully flush. If 'finish' fires we're OK;
            // if 'error' fires, the write didn't actually land — surface it.
            file.stream.end((err) => {
              openFiles.delete(key);
              if (err || file.writeError) {
                const e = err || file.writeError;
                console.error(`✘ SFTP CLOSE flush failed on ${path.basename(file.path)}: ${e.code || e.message}`);
                sftp.status(reqid, 4, `flush failed: ${e.code || e.message}`);
                return;
              }
              console.log(`SFTP: received ${path.basename(file.path)}`);
              if (onFileReceived) {
                try {
                  onFileReceived(file.path);
                } catch (cbErr) {
                  console.error(`✘ onFileReceived callback threw for ${path.basename(file.path)}: ${cbErr.message}`);
                }
              }
              sftp.status(reqid, 0);
            });
          });
        });
      });
    });

    client.on('end', () => {
      console.log('SFTP client disconnected');
    });
  });

  server.listen(port, '0.0.0.0', () => {
    for (const out of drawBox([
      `SFTP server listening on port ${port}`,
      `Host key source: ${key.source}`,
      `Auth: password, user: ${allowedUser}`,
      `Incoming dir: ${incomingDir}`,
    ])) console.log(out);
  });

  return server;
}

module.exports = { startSftpServer };

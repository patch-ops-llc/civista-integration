const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { Server, utils: { sftp: { STATUS_CODE, OPEN_MODE } } } = require('ssh2');
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
    quarantineDir = process.env.QUARANTINE_DIR || path.join(__dirname, '../../quarantine'),
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
  fs.mkdirSync(quarantineDir, { recursive: true });

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
            const filePath = path.join(incomingDir, path.basename(filename));
            const handle = Buffer.alloc(4);
            handle.writeUInt32BE(handleCount++);
            const key = handle.toString('hex');

            // Honor the open mode. Cyberduck verifies uploads by re-opening
            // the file in READ mode after CLOSE; if we always created a write
            // stream we'd truncate the just-uploaded file and then have no
            // READ handler to serve the verification, which Cyberduck reports
            // as "Broken transport; encountered EOF."
            const isWrite = !!(flags & (OPEN_MODE.WRITE | OPEN_MODE.APPEND | OPEN_MODE.CREAT | OPEN_MODE.TRUNC));
            const isRead = !!(flags & OPEN_MODE.READ);

            if (isRead && !isWrite) {
              // Read-only open: serve bytes from the file. We don't actually
              // open a fs stream here — READ requests give us offset+length
              // so we use a per-handle file descriptor.
              try {
                const fd = fs.openSync(filePath, 'r');
                const stat = fs.fstatSync(fd);
                openFiles.set(key, { kind: 'read', path: filePath, fd, size: stat.size });
                return sftp.handle(reqid, handle);
              } catch (e) {
                return sftp.status(reqid, e.code === 'ENOENT' ? STATUS_CODE.NO_SUCH_FILE : STATUS_CODE.FAILURE);
              }
            }

            // Write path (existing behavior).
            const stream = fs.createWriteStream(filePath);
            // pendingCallbacks holds write callbacks awaiting the stream's
            // 'drain' event when backpressure was triggered, so a stream
            // error can fail every still-pending WRITE rather than letting
            // some get OK'd while others are stuck.
            const entry = { kind: 'write', path: filePath, stream, writeError: null, pending: [] };
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
            openFiles.set(key, entry);
            sftp.handle(reqid, handle);
          });

          // READ: serve bytes from a read-mode handle. ssh2 calls this with
          // (reqid, handle, offset, length). We respond with sftp.data() on
          // success or sftp.status() with EOF when offset >= size.
          sftp.on('READ', (reqid, handle, offset, length) => {
            const entry = openFiles.get(handle.toString('hex'));
            if (!entry || entry.kind !== 'read') {
              return sftp.status(reqid, STATUS_CODE.FAILURE);
            }
            if (offset >= entry.size) {
              return sftp.status(reqid, STATUS_CODE.EOF);
            }
            const toRead = Math.min(length, entry.size - offset);
            const buf = Buffer.alloc(toRead);
            try {
              fs.readSync(entry.fd, buf, 0, toRead, offset);
              sftp.data(reqid, buf);
            } catch (e) {
              sftp.status(reqid, STATUS_CODE.FAILURE);
            }
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
            // Read-mode handle: just close the fd. No flush required.
            if (file.kind === 'read') {
              try { fs.closeSync(file.fd); } catch { /* already closed */ }
              openFiles.delete(key);
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

          // The minimal upload-only SFTP server above (OPEN/WRITE/CLOSE) is
          // sufficient for programmatic clients like ssh2-sftp-client. But
          // interactive clients (Cyberduck, FileZilla, OpenSSH `sftp` CLI,
          // MOVEit Automation) probe the working directory with REALPATH /
          // OPENDIR / READDIR / STAT on connect and refuse to proceed if
          // those return "Operation unsupported."
          //
          // The handlers below implement those operations while keeping the
          // server scoped to incomingDir — every path is normalized to its
          // basename within incomingDir (path traversal is rejected) so a
          // client can't escape to /etc/passwd or similar.

          const DIR_HANDLE = 'dir';  // sentinel for OPENDIR/READDIR
          const dirCursors = new Map();  // handle.toString('hex') → dir abs path

          // Map a client-requested path to a real filesystem location.
          // Two virtual roots are exposed:
          //   /incoming   → incomingDir   (default; what clients see at "/")
          //   /quarantine → quarantineDir (UAT scenario 10: operators must be
          //                                able to inspect quarantined files)
          // Everything else collapses to incomingDir for backward compat with
          // existing programmatic clients that just PUT to "/".
          function resolveDir(input) {
            const raw = String(input || '').trim();
            const norm = raw.replace(/\\/g, '/').replace(/\/+$/, '');
            // Recognize both "/quarantine" and "quarantine" (after cd quarantine).
            if (norm === '/quarantine' || norm === 'quarantine' || norm === quarantineDir) {
              return quarantineDir;
            }
            if (norm === '/incoming' || norm === 'incoming' || norm === incomingDir) {
              return incomingDir;
            }
            return incomingDir; // default root
          }
          function isDirRequest(input) {
            const raw = String(input || '').trim();
            if (raw === '' || raw === '.' || raw === '/') return true;
            const norm = raw.replace(/\\/g, '/').replace(/\/+$/, '');
            if (norm === '/quarantine' || norm === 'quarantine') return true;
            if (norm === '/incoming' || norm === 'incoming') return true;
            if (norm === incomingDir || norm === quarantineDir) return true;
            return false;
          }
          // Resolve a path that may be a file inside one of the two roots.
          function safePath(input) {
            const raw = String(input || '');
            const norm = raw.replace(/\\/g, '/');
            // /quarantine/<file>
            if (norm.startsWith('/quarantine/') || norm.startsWith('quarantine/')) {
              const base = path.basename(norm);
              return path.join(quarantineDir, base);
            }
            // /incoming/<file> (or any bare basename — default root)
            const base = path.basename(norm);
            return path.join(incomingDir, base);
          }

          // REALPATH: clients ask for canonical absolute path of "." or other
          // relative refs. We resolve "." / "/" → /incoming (default working
          // dir), and "/quarantine" → the quarantine root.
          sftp.on('REALPATH', (reqid, p) => {
            const requested = String(p || '.');
            let resolved;
            if (requested === '.' || requested === '/' || requested === '') {
              resolved = '/incoming';
            } else if (isDirRequest(requested)) {
              const dir = resolveDir(requested);
              resolved = (dir === quarantineDir) ? '/quarantine' : '/incoming';
            } else {
              // File path — preserve the virtual prefix the client used.
              const norm = requested.replace(/\\/g, '/');
              const base = path.basename(norm);
              if (norm.startsWith('/quarantine/') || norm.startsWith('quarantine/')) {
                resolved = '/quarantine/' + base;
              } else if (norm.startsWith('/incoming/') || norm.startsWith('incoming/')) {
                resolved = '/incoming/' + base;
              } else {
                resolved = '/incoming/' + base;
              }
            }
            sftp.name(reqid, [{ filename: resolved, longname: resolved, attrs: {} }]);
          });

          // STAT / LSTAT: file metadata. Required for `ls -l` and for clients
          // that check whether a file already exists before overwriting.
          const statHandler = (reqid, p) => {
            try {
              const target = isDirRequest(p) ? resolveDir(p) : safePath(p);
              const st = fs.statSync(target);
              sftp.attrs(reqid, {
                mode: st.mode,
                uid: st.uid,
                gid: st.gid,
                size: st.size,
                atime: Math.floor(st.atimeMs / 1000),
                mtime: Math.floor(st.mtimeMs / 1000),
              });
            } catch (e) {
              sftp.status(
                reqid,
                e.code === 'ENOENT' ? STATUS_CODE.NO_SUCH_FILE : STATUS_CODE.FAILURE,
              );
            }
          };
          sftp.on('STAT', statHandler);
          sftp.on('LSTAT', statHandler);

          // FSTAT: stat by open file handle. Cyberduck uses this after OPEN-
          // for-read to learn the size before issuing READ requests.
          sftp.on('FSTAT', (reqid, handle) => {
            const file = openFiles.get(handle.toString('hex'));
            if (!file) return sftp.status(reqid, STATUS_CODE.FAILURE);
            try {
              const st = file.kind === 'read'
                ? fs.fstatSync(file.fd)
                : fs.statSync(file.path);
              sftp.attrs(reqid, {
                mode: st.mode, uid: st.uid, gid: st.gid, size: st.size,
                atime: Math.floor(st.atimeMs / 1000),
                mtime: Math.floor(st.mtimeMs / 1000),
              });
            } catch (e) {
              sftp.status(reqid, STATUS_CODE.FAILURE);
            }
          });

          // SETSTAT / FSETSTAT: clients send chmod/chown/utime after upload.
          // We don't honor permission changes (the file is owned by the
          // container's user) but acknowledging keeps clients happy.
          sftp.on('SETSTAT', (reqid) => sftp.status(reqid, STATUS_CODE.OK));
          sftp.on('FSETSTAT', (reqid) => sftp.status(reqid, STATUS_CODE.OK));

          // OPENDIR: client wants to list a directory. Record which root they
          // asked for so READDIR can list the right files.
          let dirHandleCounter = 0xD17EC70F;
          sftp.on('OPENDIR', (reqid, p) => {
            const targetDir = resolveDir(p);
            const handle = Buffer.alloc(4);
            handle.writeUInt32BE((dirHandleCounter++) >>> 0);
            const key = handle.toString('hex');
            dirCursors.set(key, { cursor: 0, dir: targetDir });
            sftp.handle(reqid, handle);
          });

          // READDIR: paginated dir listing. Return all entries on the first
          // call, then EOF on the second to terminate the client's loop.
          sftp.on('READDIR', (reqid, handle) => {
            const key = handle.toString('hex');
            const state = dirCursors.get(key);
            if (state === undefined) return sftp.status(reqid, STATUS_CODE.FAILURE);
            if (state.cursor > 0) return sftp.status(reqid, STATUS_CODE.EOF);
            const listDir = state.dir || incomingDir;
            let entries;
            try {
              entries = fs.readdirSync(listDir).map((name) => {
                const full = path.join(listDir, name);
                let st;
                try { st = fs.statSync(full); } catch { st = { mode: 0, size: 0, atimeMs: 0, mtimeMs: 0 }; }
                const isDir = !!(st.isDirectory && st.isDirectory());
                const longname = `${isDir ? 'd' : '-'}rw-r--r-- 1 owner owner ${String(st.size).padStart(10)} ${new Date(st.mtimeMs).toISOString().slice(0, 16).replace('T', ' ')} ${name}`;
                return {
                  filename: name,
                  longname,
                  attrs: {
                    mode: st.mode || (isDir ? 0o040755 : 0o100644),
                    uid: 0, gid: 0,
                    size: st.size || 0,
                    atime: Math.floor((st.atimeMs || 0) / 1000),
                    mtime: Math.floor((st.mtimeMs || 0) / 1000),
                  },
                };
              });
            } catch (e) {
              return sftp.status(reqid, STATUS_CODE.FAILURE);
            }
            state.cursor = 1;
            sftp.name(reqid, entries);
          });

          // CLOSE on a dir handle — we share the CLOSE handler above by
          // first checking if it's a dir cursor.
          const origCloseHandler = sftp.listeners('CLOSE')[0];
          sftp.removeAllListeners('CLOSE');
          sftp.on('CLOSE', (reqid, handle) => {
            const key = handle.toString('hex');
            if (dirCursors.has(key)) {
              dirCursors.delete(key);
              return sftp.status(reqid, STATUS_CODE.OK);
            }
            return origCloseHandler(reqid, handle);
          });

          // MKDIR / RMDIR: no-op success — we have one fixed dir, clients
          // sometimes try to create or remove paths during sync.
          sftp.on('MKDIR', (reqid) => sftp.status(reqid, STATUS_CODE.OK));
          sftp.on('RMDIR', (reqid) => sftp.status(reqid, STATUS_CODE.OK));

          // REMOVE: allow deleting a file in incomingDir (operator may
          // want to clear a stale drop before re-uploading).
          sftp.on('REMOVE', (reqid, p) => {
            try {
              fs.unlinkSync(safePath(p));
              sftp.status(reqid, STATUS_CODE.OK);
            } catch (e) {
              sftp.status(reqid, e.code === 'ENOENT' ? STATUS_CODE.NO_SUCH_FILE : STATUS_CODE.FAILURE);
            }
          });

          // RENAME: useful for clients that upload to a .tmp name then rename
          // atomically to the final name once the bytes are flushed.
          sftp.on('RENAME', (reqid, oldPath, newPath) => {
            try {
              fs.renameSync(safePath(oldPath), safePath(newPath));
              sftp.status(reqid, STATUS_CODE.OK);
            } catch (e) {
              sftp.status(reqid, e.code === 'ENOENT' ? STATUS_CODE.NO_SUCH_FILE : STATUS_CODE.FAILURE);
            }
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

/**
 * Mirrors stdout/stderr writes to a rotating log file so the operator can
 * tail the live application log from a Railway shell session.
 *
 * Usage (called once, very early in boot):
 *   require('./src/monitoring/log-file').installFileLogger();
 *
 * Tail from a Railway shell:
 *   railway ssh -s civista-integration
 *   tail -f /tmp/civista.log
 *
 * Why monkey-patch process.stdout/stderr instead of console.*?
 *   Some libraries (ssh2, multer, pg) write directly to process.stdout
 *   without going through console.log. Patching at the stdout level
 *   captures everything — including stack traces from uncaught exceptions
 *   and SSH library debug output.
 *
 * Rotation: simple size-based. When the file exceeds LOG_FILE_MAX_BYTES,
 * it gets renamed to <path>.1 and a fresh file is opened. One rotation
 * kept so disk usage is bounded at ~2x the cap.
 */
const fs = require('fs');
const path = require('path');

const DEFAULT_PATH = '/tmp/civista.log';
const DEFAULT_MAX_BYTES = 10 * 1024 * 1024; // 10 MB

let installed = false;

function installFileLogger(options = {}) {
  if (installed) return; // idempotent
  installed = true;

  const logPath = options.path || process.env.LOG_FILE || DEFAULT_PATH;
  const maxBytes = options.maxBytes || DEFAULT_MAX_BYTES;

  try {
    fs.mkdirSync(path.dirname(logPath), { recursive: true });
  } catch { /* may already exist */ }

  let stream = fs.createWriteStream(logPath, { flags: 'a' });
  let bytesWritten = 0;
  try { bytesWritten = fs.statSync(logPath).size; } catch { /* new file */ }

  function rotateIfNeeded() {
    if (bytesWritten < maxBytes) return;
    try {
      stream.end();
      const rotated = `${logPath}.1`;
      try { fs.unlinkSync(rotated); } catch { /* not present */ }
      fs.renameSync(logPath, rotated);
      stream = fs.createWriteStream(logPath, { flags: 'a' });
      bytesWritten = 0;
    } catch (e) {
      // If rotation fails, keep writing to the existing stream. Disk pressure
      // surfaces elsewhere (loud.alarm for write errors).
    }
  }

  function writeToFile(chunk) {
    try {
      const buf = typeof chunk === 'string' ? Buffer.from(chunk) : chunk;
      stream.write(buf);
      bytesWritten += buf.length;
      rotateIfNeeded();
    } catch { /* don't let log mirror break the app */ }
  }

  // Patch stdout and stderr to mirror to file. Preserve original signatures
  // so all overloads (chunk; chunk+encoding; chunk+callback; chunk+encoding+callback)
  // continue to work.
  const origStdoutWrite = process.stdout.write.bind(process.stdout);
  process.stdout.write = function (chunk, encoding, callback) {
    writeToFile(chunk);
    return origStdoutWrite(chunk, encoding, callback);
  };
  const origStderrWrite = process.stderr.write.bind(process.stderr);
  process.stderr.write = function (chunk, encoding, callback) {
    writeToFile(chunk);
    return origStderrWrite(chunk, encoding, callback);
  };

  // Write a header so a fresh tail session sees the boot context.
  writeToFile(`\n=== civista-integration log start ${new Date().toISOString()} pid=${process.pid} ===\n`);

  return { path: logPath, maxBytes };
}

module.exports = { installFileLogger };

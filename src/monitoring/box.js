/**
 * Draws a Unicode box around lines of text, sized to fit the content.
 *
 * Inputs:
 *   lines      array of content strings (one logical line each)
 *   maxWidth   wrap a line at word boundaries if it exceeds this many cols
 *              (default 100; pass Infinity to never wrap)
 *
 * Output: array of strings ready to be logged one-per-line. The box width
 * is derived from the widest wrapped line — no guessing, no fixed bar.
 *
 * Notes:
 *   - For terminal width (column count) we use String.length. JavaScript
 *     reports code units; the warning symbols we use (⚠ ✘) are single BMP
 *     code points so .length === 1 for them. East-Asian Width emoji would
 *     be off-by-one but we don't emit any in this codebase.
 *   - Wrap is at word boundaries. A single word longer than maxWidth is
 *     emitted on its own line (accept the overflow rather than break mid-word).
 *   - Empty input returns an empty array; caller should not log anything.
 */
function drawBox(lines, { maxWidth = 100 } = {}) {
  if (!Array.isArray(lines) || lines.length === 0) return [];

  const wrapped = [];
  for (const line of lines) {
    const text = String(line ?? '');
    if (text.length <= maxWidth) {
      wrapped.push(text);
      continue;
    }
    const words = text.split(' ');
    let current = '';
    for (const word of words) {
      if (!current) {
        current = word;
      } else if ((current + ' ' + word).length <= maxWidth) {
        current += ' ' + word;
      } else {
        wrapped.push(current);
        current = word;
      }
    }
    if (current) wrapped.push(current);
  }

  const innerWidth = Math.max(...wrapped.map(l => l.length));
  const bar = '═'.repeat(innerWidth + 2);
  const out = [`╔${bar}╗`];
  for (const line of wrapped) {
    out.push(`║ ${line}${' '.repeat(innerWidth - line.length)} ║`);
  }
  out.push(`╚${bar}╝`);
  return out;
}

module.exports = { drawBox };

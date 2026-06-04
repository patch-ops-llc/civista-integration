/**
 * Association label / type-ID resolver.
 *
 * HubSpot's v4 association API addresses each labeled relationship by a numeric
 * `associationTypeId` that is portal-specific. The handoff's captured spec
 * files are not in this repo, so rather than depend on a committed snapshot we
 * pull the live labels per object pair at run time and build:
 *   - byLabel:        Map(exact label text -> associationTypeId)  [USER_DEFINED]
 *   - defaultTypeId:  the unlabeled HUBSPOT_DEFINED type id (label === null)
 *
 * The association engine matches the legend strings from relationship-map.js
 * (e.g. "PRIMARY OWNER DDA") against byLabel; debit cards use defaultTypeId
 * (unlabeled link to the owner), matching the production config.
 *
 * Results are cached per (from,to) pair for the life of the process so a
 * nightly run pulls each pair's labels at most once.
 */

const { hubspotFetch } = require('./hubspot');

const _cache = new Map(); // `${from}:${to}` -> { byLabel, defaultTypeId, results }

/**
 * Raw fetch of the label list for a directed object pair. Returns the array of
 * { category, typeId, label } entries. Throws loudly on a non-OK response so a
 * missing/forbidden pair never silently yields "no labels".
 */
async function fetchAssociationLabels(fromObjectType, toObjectType) {
  const res = await hubspotFetch(`/crm/v4/associations/${fromObjectType}/${toObjectType}/labels`);
  if (!res.ok) {
    const msg = res.body?.message || res.body?.raw || `HTTP ${res.status}`;
    throw new Error(`v4 labels ${fromObjectType}->${toObjectType}: ${msg}`);
  }
  return Array.isArray(res.body?.results) ? res.body.results : [];
}

function buildLabelIndex(results) {
  const byLabel = new Map();
  let defaultTypeId = null;
  for (const r of results) {
    if (r.label === null || r.label === undefined) {
      // The unlabeled association for this pair. Prefer a HUBSPOT_DEFINED
      // entry but accept whatever the API returns as the null-label type.
      if (defaultTypeId === null || r.category === 'HUBSPOT_DEFINED') {
        defaultTypeId = r.typeId;
      }
    } else {
      byLabel.set(r.label, r.typeId);
    }
  }
  return { byLabel, defaultTypeId };
}

/**
 * Cached label index for a directed pair. The category to send alongside a
 * labeled type is always USER_DEFINED; for the default it's HUBSPOT_DEFINED.
 */
async function getLabelIndex(fromObjectType, toObjectType) {
  const key = `${fromObjectType}:${toObjectType}`;
  if (_cache.has(key)) return _cache.get(key);
  const results = await fetchAssociationLabels(fromObjectType, toObjectType);
  const idx = { ...buildLabelIndex(results), results };
  _cache.set(key, idx);
  return idx;
}

function clearCache() {
  _cache.clear();
}

module.exports = {
  fetchAssociationLabels,
  buildLabelIndex,
  getLabelIndex,
  clearCache,
};

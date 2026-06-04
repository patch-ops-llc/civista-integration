# Operations Runbook

Day-to-day operations guide for PatchOps team members managing the Civista sync.

---

## Checking System Health

**Endpoint:** `GET /health`

Returns JSON showing:
- Database connection status
- Last sync time per table
- Record counts (attempted, created, updated, failed, skipped)
- Any errors from the most recent run

```
healthy   = all tables synced without errors
unhealthy = at least one table had failures or errors
```

---

## Triggering a Manual Sync

**Endpoint:** `POST /sync`

Use when:
- Civista re-exports files after a failed nightly run
- You need to sync during the day for a specific reason
- Testing after a fix

**Steps:**
1. Place CSV files in the `/incoming/` directory (via SFTP or the upload endpoint)
2. Hit `POST /sync`
3. Response confirms sync started; check `/health` for results

**Important:** Only one sync runs at a time. If the nightly cron is already running, the manual trigger returns HTTP 409.

---

## Uploading Files Manually

**Endpoint:** `POST /upload` (multipart form)

Use this if SFTP isn't available. Accepts multiple CSV files in a single request.

```bash
curl -X POST https://{host}/upload \
  -F "files=@HubSpot_CIF.csv" \
  -F "files=@HubSpot_DDA.csv"
```

---

## Common Scenarios

### Scenario: Circuit Breaker Tripped

**Symptom:** Sync log shows "CIRCUIT BREAKER: Skipping {table}" with a percentage drop.

**What happened:** Tonight's file has 30%+ fewer records than last time.

**Action:**
1. Contact Civista IT — ask if their export job completed successfully
2. If they confirm it's fine and the lower count is expected (e.g., data cleanup): you'll need to run the sync with the understanding that this is intentional
3. If it was a partial export: ask them to re-run and drop the new file

### Scenario: HubSpot Rate Limited

**Symptom:** Logs show "Rate limited" messages with retry attempts.

**What happened:** HubSpot told us to slow down. The system retries automatically (up to 10 times with exponential backoff).

**Action:** Usually resolves itself. If you see "Rate limited after 10 retries" — HubSpot may be having issues or we're hitting a plan limit. Check HubSpot status page or API usage dashboard.

### Scenario: Batch Failed

**Symptom:** `records_failed > 0` in health endpoint, error_details has specifics.

**What happened:** A batch of 100 records couldn't be sent to HubSpot. Other batches likely succeeded.

**Action:**
1. Check error_details — usually a field validation error or missing required property
2. The failed records will be retried on the next sync (they remain "unsynced" in the diff engine)
3. If it's a schema mismatch, check that HubSpot property definitions haven't changed

### Scenario: No Files Found

**Symptom:** Sync log shows "No CSV files found in incoming directory."

**What happened:** The nightly SFTP delivery didn't arrive.

**Action:** Check with Civista IT about their export schedule. Verify SFTP connectivity.

### Scenario: Duplicate Records in HubSpot

**Symptom:** Multiple HubSpot records with the same CIF number.

**What happened:** The `cif_number` property wasn't set as a unique identifier on the HubSpot object.

**Action:** This must be configured in HubSpot via their schema API BEFORE the first sync. See [Deployment](./deployment.md) for first-run setup steps.

---

## Schedule

| Event | Time | Timezone |
|-------|------|----------|
| Nightly sync | 2:00 AM | Eastern (America/New_York) |

---

## Key Logs to Watch

All logging goes to stdout (Railway captures this automatically).

| Log Pattern | Meaning |
|-------------|---------|
| `Starting full sync` | Sync kicked off |
| `=== Syncing {table} ({n} rows) ===` | Processing a specific file |
| `CIRCUIT BREAKER: Skipping` | File failed safety check |
| `{table}: {n} to sync, {n} unchanged` | Diff engine results |
| `Batch upsert {type}: {n}/{total}` | Progress through batches |
| `Rate limited...retrying` | HubSpot told us to slow down |
| `Archived {file}` | File moved to archive |
| `Sync complete. {n} tables processed.` | Done |

---

## Archived Files

Processed CSVs are moved to `/archive/{YYYY-MM-DD}/`. These serve as a record of exactly what was processed on each date. They're useful for:
- Debugging "what did the data look like on date X?"
- Re-processing if needed (copy back to `/incoming/` and trigger sync)

---

## Account model & associations

Deposits, Loans, and Time Deposits use the **deduplicated account model**: a single
physical account that appears in the source as multiple owner rows (one per owner,
each with its own `PrimaryKey` and a single-letter `relationship` code) is collapsed
into **one** HubSpot record keyed on `account_key` (currently `branch_number | account_type |
last_4_account_digits` — see `buildAccountKey` in `src/transform/hubspot-mapping.js`).
Every owner row is retained in the `stg_*_owners` tables and turned into a labeled
association after the account record syncs.

- Relationship codes → HubSpot labels: `src/transform/relationship-map.js` (Ivan's legend;
  e.g. `B = CUSTODIAN`, `H = BENEFICIARY`, `M = TRUSTEE`).
- Labels → portal association-type IDs are pulled live (`src/sync/association-labels.js`).
- The association engine is `src/sync/associations.js`, run by the orchestrator after each
  account/debit-card table syncs. Debit Cards link to their single owner **unlabeled**.
- Idempotency: created edges are recorded in `shipped_associations`; existing edges are
  filtered before sending, so steady-state nightly runs create ~0 associations.

**Before the first sync against a portal**, ensure `account_key` is a unique property on the
three account objects: `node scripts/setup-hubspot-properties.js`. Then verify every legend
code resolves to a configured label: `node scripts/pull-association-labels.js` (read-only;
writes a spec snapshot to `docs/association-spec.<portalId>.json`).

Disable association building entirely with `ENABLE_ASSOCIATIONS=0`.

### Relevant env vars
| Var | Purpose |
|-----|---------|
| `VERIFY_HUBSPOT_READBACK` | HASH C read-back. Set `0` for the initial full backfill (halves API volume / avoids big-table stalls), then re-enable (`1` or unset) for nightly deltas. |
| `DIFF_BATCH_SIZE` | Changed-row page size for the keyset-streamed diff (default 2000). Lower if memory-pressured at prod scale. |
| `ASSOC_PAGE_SIZE` | Owner rows resolved per page in the association engine (default 1000). |
| `ENABLE_ASSOCIATIONS` | `0` disables association building. |

---

## Full re-sync + QA (sandbox)

After the dedup/association build, the sandbox holds the *old* per-owner-row duplicate
records, so reload it cleanly rather than merging in place (client confirmed records are
unused):

1. Confirm the key targets sandbox portal `51313397`, then `POST /admin/reset-sandbox`
   (wipes the 6 HubSpot objects + ledger + owner tables + `shipped_associations`).
2. Ensure `account_key` is unique in HubSpot: `node scripts/setup-hubspot-properties.js`.
3. (Backfill) set `VERIFY_HUBSPOT_READBACK=0`, drop all 5 CSVs in `/incoming/`, `POST /sync`.
4. QA:
   - `node scripts/check-status.js` — account counts now reflect physical accounts (no
     per-owner duplicates).
   - `node scripts/check-associations.js` — `created > 0`, `failed = 0`, `unresolved = 0`.
   - Spot-check a multi-owner account (e.g. DDA last4 `3509`): one Deposit record with one
     PRIMARY OWNER + the co-owners/signers attached.
   - `/api/issues` `hash_health` shows no mismatches.
5. Re-enable `VERIFY_HUBSPOT_READBACK` for nightly deltas.

---

## Production cutover

Prod currently holds the same per-owner duplicates created by the original `PrimaryKey`
import, and the client confirmed records are unused. Cut over by reloading under the new
model:

1. Point `HUBSPOT_API_KEY` at prod (portal `50181316`).
2. `railway run --service=civista-integration node scripts/cutover-portal.js` — clears the
   portal-specific ledger/staging/`shipped_associations` and acknowledges the cutover so the
   boot guard stops blocking `/sync`.
3. `node scripts/setup-hubspot-properties.js` and `node scripts/pull-association-labels.js`
   against prod to confirm `account_key` uniqueness and label resolution.
4. Backfill with `VERIFY_HUBSPOT_READBACK=0`, then QA as above, then re-enable read-back.

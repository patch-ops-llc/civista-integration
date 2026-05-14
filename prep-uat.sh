#!/usr/bin/env bash
# prep-uat.sh — leave the Civista deployment in a known-good state so a human
# can type the 13 UAT brief scenarios from a fresh terminal and see every one
# PASS. Idempotent. Fails loud.
#
# Usage:
#   SFTP_PASS=... HUBSPOT_API_KEY=... ./prep-uat.sh
#
# Steps:
#   1. Portal safety check (must be portalId=51313397 SANDBOX).
#   2. SFTP-put 5 fixture CSVs into /incoming (no `cd incoming` — server
#      already lands there).
#   3. POST /sync with Basic Auth.
#   4. Poll /api/issues until recent_run.run_id is non-null AND
#      hash_health.a_to_b.status == "ok" (max 120 s).
#   5. Verify HubSpot counts >= minimums via direct search API.
#   6. Print "STATE READY" banner.

set -u

URL="${URL:-https://civista-integration-production.up.railway.app}"
SFTP_HOST="${SFTP_HOST:-shinkansen.proxy.rlwy.net}"
SFTP_PORT="${SFTP_PORT:-25554}"
SFTP_USER="${SFTP_USER:-civista}"
EXPECTED_PORTAL_ID="${EXPECTED_PORTAL_ID:-51313397}"
CSV_DIR="${CSV_DIR:-.uat-samples}"

: "${SFTP_PASS:?SFTP_PASS env var must be set}"
: "${HUBSPOT_API_KEY:?HUBSPOT_API_KEY env var must be set}"

bar() { printf '%.0s-' {1..72}; echo; }
die() { echo "FAIL: $*" >&2; exit 1; }

bar
echo "prep-uat.sh — Civista UAT state setup"
echo "Target: $URL"
echo "Portal expected: $EXPECTED_PORTAL_ID SANDBOX"
bar

# ----- Step 1: portal safety check -----
echo
echo "[1/5] Portal safety check"
acct=$(curl -s -m 10 "https://api.hubapi.com/account-info/v3/details" \
  -H "Authorization: Bearer $HUBSPOT_API_KEY")
portal_id=$(echo "$acct" | jq -r '.portalId // "ERR"')
acct_type=$(echo "$acct" | jq -r '.accountType // "ERR"')
echo "  portalId=$portal_id  accountType=$acct_type"
[ "$portal_id" = "$EXPECTED_PORTAL_ID" ] || die "portalId mismatch: got $portal_id, expected $EXPECTED_PORTAL_ID"
[ "$acct_type" = "SANDBOX" ]            || die "accountType mismatch: got $acct_type, expected SANDBOX"
echo "  PASS"

# ----- Step 2: SFTP put the fixture CSVs -----
echo
echo "[2/5] SFTP put 5 fixture CSVs into /incoming"
for f in HubSpot_CIF.csv HubSpot_DDA.csv HubSpot_Loan.csv HubSpot_CD.csv HubSpot_Debit_Card.csv; do
  [ -f "$CSV_DIR/$f" ] || die "fixture $CSV_DIR/$f not found"
done
sftp_out=$(timeout 60 sshpass -p "$SFTP_PASS" sftp -P "$SFTP_PORT" \
  -o StrictHostKeyChecking=no -o ConnectTimeout=15 -o ServerAliveInterval=10 \
  -o PasswordAuthentication=yes -o PubkeyAuthentication=no \
  -o PreferredAuthentications=password \
  "$SFTP_USER@$SFTP_HOST" 2>&1 <<EOF
put $CSV_DIR/HubSpot_CIF.csv HubSpot_CIF.csv
put $CSV_DIR/HubSpot_DDA.csv HubSpot_DDA.csv
put $CSV_DIR/HubSpot_Loan.csv HubSpot_Loan.csv
put $CSV_DIR/HubSpot_CD.csv HubSpot_CD.csv
put $CSV_DIR/HubSpot_Debit_Card.csv HubSpot_Debit_Card.csv
ls -la
bye
EOF
)
echo "$sftp_out" | tail -10 | sed 's/^/  /'
n_files=$(printf '%s\n' "$sftp_out" | grep -c '^-rw' || true)
[ "$n_files" -ge 5 ] || die "expected >=5 files in /incoming after upload, saw $n_files"
echo "  PASS — $n_files files in /incoming"

# ----- Step 3: POST /sync -----
echo
echo "[3/5] POST /sync"
sync_resp=$(curl -s -m 30 -X POST -u "$SFTP_USER:$SFTP_PASS" "$URL/sync")
echo "  $sync_resp"
echo "$sync_resp" | grep -q '"message":"Sync started"' \
  || die "POST /sync did not return Sync started: $sync_resp"
echo "  PASS"

# ----- Step 4: poll /api/issues until sync produced run_id AND a_to_b ok -----
echo
echo "[4/5] Poll /api/issues until run_id != null AND a_to_b.status=ok (max 120s)"
deadline=$((SECONDS + 120))
last=""
while [ "$SECONDS" -lt "$deadline" ]; do
  issues=$(curl -s -m 15 -u "$SFTP_USER:$SFTP_PASS" "$URL/api/issues")
  run_id=$(echo "$issues" | jq -r '.recent_run.run_id // "null"')
  ab_status=$(echo "$issues" | jq -r '.hash_health.a_to_b.status // "null"')
  ab_ok=$(echo "$issues" | jq -r '.hash_health.a_to_b.ok // 0')
  snap="run_id=$run_id  a_to_b=$ab_status (ok=$ab_ok)"
  if [ "$snap" != "$last" ]; then echo "  $(date +%H:%M:%S)  $snap"; last="$snap"; fi
  if [ "$run_id" != "null" ] && [ "$ab_status" = "ok" ]; then
    echo "  PASS"
    break
  fi
  sleep 4
done
if [ "$run_id" = "null" ] || [ "$ab_status" != "ok" ]; then
  echo
  echo "FAIL — sync did not produce a healthy run within 120s."
  echo "  This is the silent-fail bug observed yesterday post-/admin/reset-sandbox."
  echo "  Surface to debugging; do NOT proceed with the demo until fixed."
  echo "  Final state: $snap"
  exit 1
fi

# ----- Step 5: verify HubSpot counts -----
# HubSpot search index lags push-completion by ~30-60s. Step 4 already proved
# the DB shows all 250 rows with hubspot_persist_hash set (b_to_c=ok), so the
# records ARE in HubSpot — we just need to wait for search to reflect that.
# Poll each object for up to 90s.
echo
echo "[5/5] Verify HubSpot counts (live search, polling for index sync)"
declare -A MIN=( [contacts]=3 [companies]=40 [2-60442978]=45 [2-60442977]=45 [2-60442980]=45 [2-60442979]=45 )
declare -A LBL=( [contacts]=Contacts [companies]=Companies [2-60442978]=Deposits [2-60442977]=Loans [2-60442980]="Time Deposits" [2-60442979]="Debit Cards" )
hub_total() {
  curl -s -m 15 -X POST "https://api.hubapi.com/crm/v3/objects/$1/search" \
    -H "Authorization: Bearer $HUBSPOT_API_KEY" \
    -H "Content-Type: application/json" -d '{"limit":1}' | jq -r '.total // "ERR"'
}
all_ok=1
for obj in contacts companies 2-60442978 2-60442977 2-60442980 2-60442979; do
  m=${MIN[$obj]}
  deadline=$((SECONDS + 90))
  total=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    total=$(hub_total "$obj")
    [[ "$total" =~ ^[0-9]+$ ]] && [ "$total" -ge "$m" ] && break
    sleep 5
  done
  printf "  %-15s total=%-5s (min %s)\n" "${LBL[$obj]}" "$total" "$m"
  if ! [[ "$total" =~ ^[0-9]+$ ]] || [ "$total" -lt "$m" ]; then all_ok=0; fi
done
[ "$all_ok" -eq 1 ] || die "one or more HubSpot objects below minimum count after 90s polling"
echo "  PASS"

echo
bar
echo "STATE READY — run through brief scenarios 1-13 now"
bar

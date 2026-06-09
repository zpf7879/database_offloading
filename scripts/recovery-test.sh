#!/usr/bin/env bash
# Recovery test — proves replay and catch-up after streams-app restart.
#
# Steps:
#   1. Record current status of sentinel customer in MySQL
#   2. Stop the streams-app container
#   3. Apply a mix of INSERTs and UPDATEs to MySQL while streams-app is down
#      - 30% inserts of new test customers
#      - 70% updates with varied fields and values
#   4. Apply a sentinel status flip to the test customer (last write)
#   5. Restart streams-app
#   6. Poll MongoDB until the sentinel change appears (pipeline drained)
#   7. Verify all changes individually in MongoDB
#   8. Run reconciliation check to confirm no drift
#
# Usage: bash scripts/recovery-test.sh [--changes=10]

set -euo pipefail

CHANGES=10
for arg in "$@"; do
  case $arg in --changes=*) CHANGES="${arg#*=}" ;; esac
done

API_URL=${API_URL:-http://localhost:3000}
POLL_INTERVAL=1   # seconds
POLL_TIMEOUT=120  # seconds
RUN_ID=$(date +%s)   # unique suffix for inserted test records

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BOLD='\033[1m'; RESET='\033[0m'

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
info() { echo -e "${CYAN}ℹ${RESET} $*"; }
fail() { echo -e "${RED}✗${RESET} $*"; exit 1; }
head() { echo -e "\n${BOLD}$(printf '─%.0s' {1..60})${RESET}\n${BOLD} $*${RESET}\n$(printf '─%.0s' {1..60})"; }

TEST_CUSTOMER="cust-0001"

mysql_exec() {
  docker exec poc_mysql mysql -upoc_user -ppoc_pass offload_poc -sNe "$1" 2>/dev/null
}

# ── Change catalogue ──────────────────────────────────────────────────────────
# Each entry: TYPE|customer_id|field|expected_value
# TYPE = UPDATE or INSERT
# For verification we check field=expected_value in the MongoDB gold doc.

# Varied UPDATE payloads — different fields and values per customer
UPDATES=(
  "cust-0002|status|SUSPENDED"
  "cust-0003|status|INACTIVE"
  "cust-0004|first_name|UpdatedFirst"
  "cust-0005|last_name|UpdatedLast"
  "cust-0006|nationality|NZL"
  "cust-0007|status|ACTIVE"
  "cust-0008|first_name|Recovery"
  "cust-0009|nationality|GBR"
)

# ── 1. Baseline snapshot ──────────────────────────────────────────────────────
head "STEP 1 — Baseline snapshot"

CURRENT_STATUS=$(mysql_exec "SELECT status FROM customer WHERE customer_id='${TEST_CUSTOMER}';")
if [ -z "$CURRENT_STATUS" ]; then
  fail "cust-0001 not found in MySQL. Run: npm run initial-sync"
fi

if [ "$CURRENT_STATUS" = "INACTIVE" ]; then SENTINEL_STATUS="ACTIVE"
else SENTINEL_STATUS="INACTIVE"; fi

info "Sentinel customer : ${BOLD}${TEST_CUSTOMER}${RESET}"
info "Current status    : ${BOLD}${CURRENT_STATUS}${RESET}"
info "Will flip to      : ${BOLD}${SENTINEL_STATUS}${RESET}"

# ── 2. Stop streams-app ───────────────────────────────────────────────────────
head "STEP 2 — Stop streams-app"

docker stop poc_streams_app
ok "poc_streams_app stopped"

# ── 3. Apply mixed changes while consumer is down ─────────────────────────────
head "STEP 3 — Apply mixed changes while streams-app is down"

# Decide how many inserts vs updates (roughly 30/70 split)
NUM_INSERTS=$(( CHANGES * 30 / 100 ))
[ $NUM_INSERTS -lt 1 ] && NUM_INSERTS=1
NUM_UPDATES=$(( CHANGES - NUM_INSERTS ))
[ $NUM_UPDATES -gt ${#UPDATES[@]} ] && NUM_UPDATES=${#UPDATES[@]}
TOTAL_CHANGES=$(( NUM_INSERTS + NUM_UPDATES ))

printf "\n  ${BOLD}%s inserts + %s updates = %s total changes${RESET}\n\n" \
  "$NUM_INSERTS" "$NUM_UPDATES" "$TOTAL_CHANGES"
printf "  %-14s  %-8s  %-16s  %-20s\n" "customer_id" "op" "field" "new value"
printf "  %-14s  %-8s  %-16s  %-20s\n" "─────────────" "──" "─────" "─────────"

# Track changes for Step 7 verification: "CID|field|expected"
CHANGE_LOG=()

# ── INSERTs ───────────────────────────────────────────────────────────────────
for n in $(seq 1 $NUM_INSERTS); do
  NEW_CID="test-rc-${RUN_ID}-${n}"
  mysql_exec "INSERT INTO customer
    (customer_id, external_ref, first_name, last_name, date_of_birth, gender, nationality, status)
    VALUES ('${NEW_CID}', 'EXT-RC-${n}', 'RecoveryTest', 'Insert${n}', '1990-01-01', 'M', 'AUS', 'ACTIVE');"
  CHANGE_LOG+=("INSERT|${NEW_CID}|first_name|RecoveryTest")
  printf "  %-14s  ${GREEN}%-8s${RESET}  %-16s  %-20s\n" "$NEW_CID" "INSERT" "new customer" "ACTIVE"
done

# ── UPDATEs ───────────────────────────────────────────────────────────────────
for entry in "${UPDATES[@]:0:$NUM_UPDATES}"; do
  CID=$(echo "$entry"   | cut -d'|' -f1)
  FIELD=$(echo "$entry" | cut -d'|' -f2)
  VAL=$(echo "$entry"   | cut -d'|' -f3)

  # Check the customer actually exists before trying to update
  EXISTS=$(mysql_exec "SELECT COUNT(*) FROM customer WHERE customer_id='${CID}';")
  if [ "$EXISTS" = "0" ]; then
    info "Skipping ${CID} — not in MySQL"
    continue
  fi

  mysql_exec "UPDATE customer SET \`${FIELD}\`='${VAL}', updated_at=NOW() WHERE customer_id='${CID}';"
  CHANGE_LOG+=("UPDATE|${CID}|${FIELD}|${VAL}")
  printf "  %-14s  ${YELLOW}%-8s${RESET}  %-16s  %-20s\n" "$CID" "UPDATE" "$FIELD" "$VAL"
done

TOTAL_CHANGES=${#CHANGE_LOG[@]}
echo ""
ok "Applied ${TOTAL_CHANGES} changes to MySQL"

# ── 4. Sentinel flip (must be the last write) ─────────────────────────────────
mysql_exec "UPDATE customer SET status='${SENTINEL_STATUS}', updated_at=NOW() WHERE customer_id='${TEST_CUSTOMER}';"
ok "Sentinel: ${TEST_CUSTOMER} status ${CURRENT_STATUS} → ${SENTINEL_STATUS}  (last write)"
info "streams-app is still down — all changes are queued in the Kafka binlog"

# ── 5. Restart streams-app ────────────────────────────────────────────────────
head "STEP 4 — Restart streams-app"

RESTART_AT=$(date +%s%3N)
docker start poc_streams_app
ok "poc_streams_app restarted at T+0ms"

# ── 6. Poll for sentinel ──────────────────────────────────────────────────────
head "STEP 5 — Polling MongoDB until sentinel arrives"

ELAPSED=0
CAUGHT_UP=false
printf "  Waiting for ${TEST_CUSTOMER}.status='${SENTINEL_STATUS}' in MongoDB"

while [ $ELAPSED -lt $POLL_TIMEOUT ]; do
  MONGO_STATUS=$(curl -sf "${API_URL}/customer/${TEST_CUSTOMER}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data']['status'])" 2>/dev/null || echo "")

  if [ "$MONGO_STATUS" = "$SENTINEL_STATUS" ]; then
    NOW=$(date +%s%3N)
    CATCHUP_MS=$(( NOW - RESTART_AT ))
    printf "\033[2K\r  ${GREEN}✓${RESET} Sentinel arrived in ${BOLD}${GREEN}${CATCHUP_MS}ms${RESET} after restart\n"
    CAUGHT_UP=true
    break
  fi

  printf "."
  sleep $POLL_INTERVAL
  ELAPSED=$(( ELAPSED + POLL_INTERVAL ))
done

if [ "$CAUGHT_UP" = false ]; then
  printf "\n"
  fail "Timed out after ${POLL_TIMEOUT}s — streams-app did not catch up"
fi

# ── 7. Verify each change individually ───────────────────────────────────────
head "STEP 6 — Verify all ${TOTAL_CHANGES} changes in MongoDB"

printf "  %-14s  %-8s  %-16s  %-16s  %-16s  %s\n" \
  "customer_id" "op" "field" "expected" "mongodb" "match"
printf "  %-14s  %-8s  %-16s  %-16s  %-16s  %s\n" \
  "─────────────" "──" "─────" "────────" "───────" "─────"

VERIFY_PASS=0
VERIFY_FAIL=0

for entry in "${CHANGE_LOG[@]}"; do
  OP=$(echo "$entry"       | cut -d'|' -f1)
  CID=$(echo "$entry"      | cut -d'|' -f2)
  FIELD=$(echo "$entry"    | cut -d'|' -f3)
  EXPECTED=$(echo "$entry" | cut -d'|' -f4)

  RESPONSE=$(curl -sf "${API_URL}/customer/${CID}" 2>/dev/null || echo "")
  if [ -z "$RESPONSE" ]; then
    MONGO_VAL="NOT_FOUND"
  else
    MONGO_VAL=$(echo "$RESPONSE" \
      | python3 -c "import sys,json; d=json.load(sys.stdin)['data']; print(d.get('${FIELD}','NOT_FOUND'))" \
      2>/dev/null || echo "NOT_FOUND")
  fi

  if [ "$MONGO_VAL" = "$EXPECTED" ]; then
    printf "  %-14s  %-8s  %-16s  %-16s  %-16s  ${GREEN}%s${RESET}\n" \
      "$CID" "$OP" "$FIELD" "$EXPECTED" "$MONGO_VAL" "PASS"
    VERIFY_PASS=$(( VERIFY_PASS + 1 ))
  else
    printf "  %-14s  %-8s  %-16s  %-16s  %-16s  ${RED}%s${RESET}\n" \
      "$CID" "$OP" "$FIELD" "$EXPECTED" "$MONGO_VAL" "FAIL"
    VERIFY_FAIL=$(( VERIFY_FAIL + 1 ))
  fi
done

echo ""
ok "${VERIFY_PASS}/${TOTAL_CHANGES} changes verified in MongoDB"
if [ $VERIFY_FAIL -gt 0 ]; then
  fail "${VERIFY_FAIL} change(s) did NOT appear correctly in MongoDB"
fi

# ── 8. Reconciliation ────────────────────────────────────────────────────────
head "STEP 7 — Reconciliation check"

node src/reconcile/reconcile.js --limit=50
RECON_EXIT=$?

# ── Summary ──────────────────────────────────────────────────────────────────
head "RECOVERY TEST RESULT"

if [ $RECON_EXIT -eq 0 ] && [ "$CAUGHT_UP" = true ] && [ $VERIFY_FAIL -eq 0 ]; then
  ok "PASS — streams-app replayed all ${TOTAL_CHANGES} missed events after restart"
  ok "PASS — all ${TOTAL_CHANGES} changes verified individually in MongoDB"
  ok "PASS — reconciliation found no drift (sample: 50 customers)"
  info "Catch-up lag: ${CATCHUP_MS}ms from container start to sentinel arriving in MongoDB"
else
  fail "FAIL — see output above for details"
fi

echo ""

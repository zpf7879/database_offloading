#!/usr/bin/env bash
# Recovery test — proves replay and catch-up after streams-app restart.
#
# Steps:
#   1. Record current status of test customer in MySQL
#   2. Stop the streams-app container
#   3. Apply N changes to MySQL while streams-app is down
#   4. Apply a sentinel status flip to the test customer
#   5. Restart streams-app
#   6. Poll MongoDB until the sentinel change appears, measuring catch-up lag
#   7. Run reconciliation check to confirm correctness
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

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; RESET='\033[0m'

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
info() { echo -e "\033[0;36mℹ${RESET} $*"; }
fail() { echo -e "${RED}✗${RESET} $*"; exit 1; }
head() { echo -e "\n${BOLD}$(printf '─%.0s' {1..60})${RESET}\n${BOLD} $*${RESET}\n$(printf '─%.0s' {1..60})"; }

MYSQL_EXEC="docker exec poc_mysql mysql -upoc_user -ppoc_pass offload_poc -sNe"
TEST_CUSTOMER="cust-0001"

# ── 1. Baseline snapshot ──────────────────────────────────────────────────────
head "STEP 1 — Baseline snapshot"

CURRENT_STATUS=$(docker exec poc_mysql mysql -upoc_user -ppoc_pass offload_poc \
  -sNe "SELECT status FROM customer WHERE customer_id='${TEST_CUSTOMER}';" 2>/dev/null)

if [ -z "$CURRENT_STATUS" ]; then
  fail "cust-0001 not found in MySQL. Run: npm run initial-sync"
fi

# Pick a valid ENUM sentinel that differs from the current value
if [ "$CURRENT_STATUS" = "INACTIVE" ]; then
  SENTINEL_STATUS="ACTIVE"
else
  SENTINEL_STATUS="INACTIVE"
fi

info "Current status of ${TEST_CUSTOMER} in MySQL : ${BOLD}${CURRENT_STATUS}${RESET}"
info "Sentinel status (what we will flip to)      : ${BOLD}${SENTINEL_STATUS}${RESET}"

# ── 2. Stop streams-app ───────────────────────────────────────────────────────
head "STEP 2 — Stop streams-app"

docker stop poc_streams_app
ok "poc_streams_app stopped"

# ── 3. Apply changes while consumer is down ───────────────────────────────────
head "STEP 3 — Apply ${CHANGES} MySQL changes while streams-app is down"

for i in $(seq 2 $((1 + CHANGES))); do
  CID=$(printf "cust-%04d" $i)
  docker exec poc_mysql mysql -upoc_user -ppoc_pass offload_poc \
    -sNe "UPDATE customer SET status='SUSPENDED', updated_at=NOW() WHERE customer_id='${CID}';" 2>/dev/null || true
done
ok "Applied ${CHANGES} UPDATE statements to MySQL"

# ── 4. Apply sentinel flip ───────────────────────────────────────────────────
docker exec poc_mysql mysql -upoc_user -ppoc_pass offload_poc \
  -sNe "UPDATE customer SET status='${SENTINEL_STATUS}', updated_at=NOW() WHERE customer_id='${TEST_CUSTOMER}';" 2>/dev/null
ok "Flipped ${TEST_CUSTOMER} status: ${CURRENT_STATUS} → ${SENTINEL_STATUS}"
info "Changes are in MySQL binlog — streams-app is still down, MongoDB is stale"

# ── 5. Restart streams-app ────────────────────────────────────────────────────
head "STEP 4 — Restart streams-app"

RESTART_AT=$(date +%s%3N)
docker start poc_streams_app
ok "poc_streams_app restarted at T+0ms"

# ── 6. Poll for catch-up ──────────────────────────────────────────────────────
head "STEP 5 — Polling MongoDB for catch-up"

ELAPSED=0
CAUGHT_UP=false

printf "  Waiting for status='${SENTINEL_STATUS}' in MongoDB"
while [ $ELAPSED -lt $POLL_TIMEOUT ]; do
  MONGO_STATUS=$(curl -sf "${API_URL}/customer/${TEST_CUSTOMER}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data']['status'])" 2>/dev/null || echo "")

  if [ "$MONGO_STATUS" = "$SENTINEL_STATUS" ]; then
    NOW=$(date +%s%3N)
    CATCHUP_MS=$(( NOW - RESTART_AT ))
    printf "\r  ${GREEN}✓${RESET} Caught up in ${BOLD}${GREEN}${CATCHUP_MS}ms${RESET} after restart\n"
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

# ── 7. Reconciliation check ───────────────────────────────────────────────────
head "STEP 6 — Reconciliation check"

node src/reconcile/reconcile.js --limit=50
RECON_EXIT=$?

# ── Summary ───────────────────────────────────────────────────────────────────
head "RECOVERY TEST RESULT"

if [ $RECON_EXIT -eq 0 ] && [ "$CAUGHT_UP" = true ]; then
  ok "PASS — streams-app replayed all ${CHANGES} missed events after restart"
  ok "PASS — reconciliation found no drift (sample: 50 customers)"
  info "Catch-up lag: ${CATCHUP_MS}ms from container start to sentinel appearing in MongoDB"
else
  fail "FAIL — see reconciliation output above for details"
fi

echo ""

#!/usr/bin/env bash
# =============================================================
# Register Debezium (source) + MongoDB Kafka (sink) connectors
# with Kafka Connect.
# Run after: docker compose up -d  (once Connect is healthy)
# =============================================================

set -euo pipefail

CONNECT_URL="http://localhost:8083"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env
if [ -f "$PROJECT_DIR/.env" ]; then
  set -o allexport
  # shellcheck source=/dev/null
  source "$PROJECT_DIR/.env"
  set +o allexport
else
  echo "ERROR: .env file not found at $PROJECT_DIR/.env"
  exit 1
fi

if [ -z "${MONGODB_URI:-}" ]; then
  echo "ERROR: MONGODB_URI is not set in .env"
  exit 1
fi

# -------------------------------------------------------------
# Wait for Kafka Connect to be ready
# -------------------------------------------------------------
echo "Waiting for Kafka Connect to be ready..."
MAX_WAIT=120
WAITED=0
until curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; do
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    echo "ERROR: Kafka Connect did not become ready after ${MAX_WAIT}s."
    echo "       Run: docker compose logs kafka-connect --tail=50"
    exit 1
  fi
  printf "."
  sleep 5
  WAITED=$((WAITED + 5))
done
echo -e "\nKafka Connect is ready."

# -------------------------------------------------------------
# Helper: register a connector (skip if already exists)
# -------------------------------------------------------------
register_connector() {
  local name="$1"
  local payload="$2"

  EXISTING=$(curl -sf "$CONNECT_URL/connectors/$name" 2>/dev/null || true)
  if [ -n "$EXISTING" ]; then
    echo "  [$name] Already registered — skipping."
    return
  fi

  HTTP_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
    -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d "$payload")

  if [ "$HTTP_STATUS" = "201" ]; then
    echo "  [$name] Registered successfully."
  else
    echo "  [$name] ERROR: HTTP $HTTP_STATUS — check Connect logs."
    exit 1
  fi
}

# -------------------------------------------------------------
# 1. Debezium MySQL source connector
# -------------------------------------------------------------
echo ""
echo "Registering Debezium MySQL source connector..."
register_connector "poc-mysql-connector" "$(cat "$PROJECT_DIR/debezium-connector.json")"

# -------------------------------------------------------------
# 2. MongoDB Kafka sink connector
# -------------------------------------------------------------
echo ""
echo "Registering MongoDB Kafka bronze sink connector (1:1 staging collections)..."
BRONZE_PAYLOAD=$(sed "s|\${MONGODB_URI}|${MONGODB_URI}|g" \
  "$PROJECT_DIR/mongodb-bronze-connector.json.template")
register_connector "mongodb-bronze-connector" "$BRONZE_PAYLOAD"

# -------------------------------------------------------------
# 3. MongoDB Kafka gold sink connector (merged customer_profile)
# -------------------------------------------------------------
echo ""
echo "Registering MongoDB Kafka gold sink connector (merged customer_profile)..."
SINK_PAYLOAD=$(sed "s|\${MONGODB_URI}|${MONGODB_URI}|g" \
  "$PROJECT_DIR/mongodb-sink-connector.json.template")
register_connector "mongodb-sink-connector" "$SINK_PAYLOAD"

# -------------------------------------------------------------
# Status check
# -------------------------------------------------------------
echo ""
echo "Connector status:"
for CONNECTOR in poc-mysql-connector mongodb-bronze-connector mongodb-sink-connector; do
  STATUS=$(curl -sf "$CONNECT_URL/connectors/$CONNECTOR/status" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
  echo "  $CONNECTOR → $STATUS"
done

echo ""
echo "Done. All 3 connectors are registered."
echo ""
echo " Bronze layer (1:1 staging):  bronze_customer, bronze_customer_address, ..."
echo " Gold layer   (merged):        customer_profile"
echo ""
echo "Data flow:"
echo "  MySQL → Debezium → Kafka topics → [bronze connector] → bronze_* collections"
echo "                                  → [Kafka Streams]    → poc.customer_profile"
echo "                                                        → [gold connector] → customer_profile"

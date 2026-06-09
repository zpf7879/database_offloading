# Database Offloading POC

A proof-of-concept for **mainframe read offload** using a real-time CDC pipeline.  
Changes in a MySQL source database stream into MongoDB Atlas in under a second, eliminating expensive relational joins from downstream consumers.

---

## Purpose

Banks and large enterprises run critical workloads on mainframes where every read is costly. This POC proves that:

- A **CDC pipeline** (Debezium → Kafka → Kafka Streams → MongoDB) can replicate source changes in near real-time
- A **denormalised document model** in MongoDB eliminates multi-table joins at read time
- **Multiple consumers** (EDC, Galaxy) can read from the offload layer independently without touching the source
- The pipeline is **resilient** — it recovers from consumer restarts with no data loss and handles duplicate events idempotently

The source system is simulated with MySQL. The same architecture applies when the source is an RDS instance or a mainframe via IBM Classic CDC.

---

## Architecture

```
MySQL (binlog)
    │
    ▼
Debezium MySQL Connector          CDC capture — tails binlog, emits row-level events
    │
    ▼
Apache Kafka                      Durable event log — one topic per source table
    │
    ▼
Kafka Streams (streams-app)       Stateful merge — re-keys child tables by customer_id,
    │                             aggregates into KTables, left-joins all 6 tables,
    │                             emits one merged profile per change
    ▼
MongoDB Kafka Sink Connector      ReplaceOne upsert keyed by customer_id
    │
    ▼
MongoDB Atlas                     Gold layer — one document per customer, all data embedded
    │
    ├── GET /customer/:id          EDC read path   — full profile
    └── GET /galaxy/customer/:id   Galaxy read path — slim summary
```

### Data layers

| Layer | Collections | Purpose |
|---|---|---|
| **Bronze** | `bronze_customer`, `bronze_customer_address`, … | 1:1 copy of source tables — raw CDC events for audit and replay |
| **Gold** | `customer_profile` | Merged document per customer — optimised for read offload |

### Gold document shape

```json
{
  "customer_id": "cust-0001",
  "first_name": "Alice",
  "last_name": "Nguyen",
  "status": "ACTIVE",
  "addresses":       [{ "address_type": "RESIDENTIAL", "city": "Sydney" }],
  "contacts":        [{ "contact_type": "EMAIL", "contact_value": "alice@example.com" }],
  "identifications": [{ "id_type": "PASSPORT", "id_number": "PA1234567" }],
  "tax_records":     [{ "tax_country": "AUS", "tax_id": "123456789" }],
  "relationships":   [{ "party_id_to": "cust-0002", "relationship_type": "JOINT_HOLDER" }]
}
```

---

## Setup

### Prerequisites

- Docker and Docker Compose
- Node.js 20+
- A MongoDB Atlas cluster (free tier is sufficient)

### 1. Clone and install

```bash
git clone https://github.com/zpf7879/database_offloading.git
cd database_offloading
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and set your MongoDB Atlas connection string:

```
MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?appName=database_offloading
MONGODB_DB=offload_poc
```

All other values can stay as-is for a local Docker setup.

### 3. Start the stack

```bash
docker compose up -d
```

This starts:

| Container | Purpose | Port |
|---|---|---|
| `poc_mysql` | MySQL source database | 3306 |
| `poc_zookeeper` | Zookeeper for Kafka | — |
| `poc_kafka` | Kafka broker | 9092 |
| `poc_kafka_connect` | Kafka Connect + Debezium + MongoDB connector | 8083 |
| `poc_streams_app` | Kafka Streams profile aggregator | — |
| `poc_api` | Read API (EDC + Galaxy endpoints) | 3000 |
| `poc_kafka_ui` | Kafka UI (topic browser) | 8080 |

### 4. Register CDC connectors

```bash
bash scripts/register-connectors.sh
```

This registers the Debezium MySQL source connector and the MongoDB sink connector with Kafka Connect.

### 5. Seed source data

```bash
npm run seed                # 1,000 customers (default)
npm run seed:large          # 5,000 customers
```

### 6. Sync to MongoDB

```bash
npm run initial-sync
```

Bulk-loads all MySQL customers into the MongoDB gold layer. Run once after seeding. CDC maintains the documents incrementally after this.

### 7. Verify

```bash
curl http://localhost:3000/health
# {"status":"ok"}

curl http://localhost:3000/customer/cust-0001
# full profile from MongoDB

npm run reconcile
# should show 100% OK
```

---

## Test Scenarios

### End-to-end demo

Inserts a new customer and updates an existing one in MySQL, then polls MongoDB until both changes appear in the bronze and gold layers. Prints end-to-end latency for each change.

```bash
npm run demo
```

**What it proves:** changes propagate from MySQL to MongoDB in under a second under normal load.

---

### Read path comparison

Two API endpoints return the same customer profile — one from MongoDB (offload path), one from MySQL (baseline join path). Both include latency in the response.

```bash
# Offload path — single MongoDB findOne
curl http://localhost:3000/customer/cust-0001

# Baseline path — 5-table relational join
curl http://localhost:3000/customer/cust-0001/baseline

# Galaxy path — slim summary for downstream consumer
curl http://localhost:3000/galaxy/customer/cust-0001
```

---

### Load test

Fires concurrent requests and reports p50 / p95 / p99 latency, throughput, and error rate.

```bash
npm run load:mongo       # MongoDB offload path — 50 rps for 30s
npm run load:baseline    # MySQL baseline path  — 50 rps for 30s
npm run load:galaxy      # Galaxy summary path  — 50 rps for 30s
npm run load:large       # MongoDB at scale     — 200 rps for 60s, 5,000 IDs
```

---

### Full benchmark

Seeds 5,000 customers, runs all three load tests, and writes results to `docs/benchmark_results.md`.

```bash
npm run benchmark
npm run benchmark -- --skip-seed    # skip seeding if already done
```

---

### Reconciliation

Samples up to N customers from MySQL and checks that each one has a matching, up-to-date document in the MongoDB gold layer. Reports `OK`, `STALE`, or `MISSING` per record with field-level diffs. Exits with code 1 if any issues are found.

```bash
npm run reconcile                                          # sample 200 customers
node src/reconcile/reconcile.js --limit=500 --verbose      # print every OK too
```

**What it proves:** source truth and offload layer are in sync.

---

### Recovery test

Proves the pipeline recovers from a consumer restart with no data loss:

1. Stops `poc_streams_app`
2. Applies a mix of INSERTs and UPDATEs to MySQL while it is down
3. Restarts the consumer
4. Polls MongoDB until a sentinel change appears and measures catch-up lag
5. Verifies every individual change by field and value
6. Runs reconciliation to confirm no drift

```bash
bash scripts/recovery-test.sh               # 10 changes (default)
bash scripts/recovery-test.sh --changes=25
```

**What it proves:** Kafka's durable offset log guarantees at-least-once delivery; the consumer replays all missed events on restart.

---

### Idempotency test

Three test cases that confirm duplicate events do not corrupt the gold layer:

| Test | Scenario | Expected result |
|---|---|---|
| Duplicate INSERT | Same customer PK inserted twice | Exactly 1 document in MongoDB |
| Duplicate UPDATE | Same field set to same value twice | Correct value, no phantom duplicates |
| Rapid succession | 5 updates to the same customer in quick succession | Last write wins, exactly 1 document |

```bash
npm run idempotency
```

**What it proves:** `ReplaceOne` upsert + Kafka Streams KTable semantics are idempotent — safe to reprocess any event.

---

## Repository structure

```
├── docker-compose.yml              Full stack definition
├── Dockerfile.connect              Custom Kafka Connect image with MongoDB connector
├── src/
│   ├── api/server.js               Express API — EDC and Galaxy read endpoints
│   ├── db/mongo.js                 MongoDB connection + index setup
│   ├── db/rds.js                   MySQL connection pool + baseline join query
│   ├── load/generator.js           Load generator (p50/p95/p99 reporting)
│   ├── reconcile/reconcile.js      Reconciliation check (MySQL vs MongoDB)
│   └── schema/
│       ├── init.sql                MySQL schema + base seed data
│       └── seeder.js               Bulk seeder (configurable count)
├── streams-app/                    Kafka Streams profile aggregator (Java)
├── scripts/
│   ├── demo.js                     End-to-end latency demo
│   ├── initial-sync.js             One-time bulk load MySQL → MongoDB
│   ├── recovery-test.sh            Consumer restart + catch-up test
│   ├── idempotency-test.js         Duplicate event handling test
│   ├── benchmark.sh                Full benchmark runner
│   └── register-connectors.sh     Registers Debezium + MongoDB connectors
└── docs/
    ├── pipeline.md                 Pipeline architecture detail
    ├── poc_checklist.md            Full POC scope and success criteria
    ├── week1_summary.md            Week 1 deliverables
    ├── week2_summary.md            Week 2 deliverables
    └── benchmark_results.md        Load test results (generated)
```

---

## What this POC proves

- The **offload pattern** works — MongoDB serves the same queries faster with less source system load
- The pipeline is **near real-time** — end-to-end lag is typically under 1 second
- The system is **resilient** — consumer restarts cause no data loss; catch-up is automatic
- The gold layer is **idempotent** — duplicate or replayed events produce the correct result
- **Multiple consumers** can read from the same offload layer with different response shapes

## What this POC does not prove

- z/OS log capture or VSAM semantics
- IBM Classic CDC operational behaviour
- CICS or logstream integration
- Production Atlas sizing or multi-region behaviour

These are covered in the next phase when a real mainframe source replaces the MySQL simulator.

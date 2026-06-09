# Week 2 Summary — Database Offloading POC

## Goal
Validate the reliability, correctness, and scalability of the offload pipeline built in Week 1. Prove that the system recovers from consumer failures without data loss, handles duplicate events idempotently, and performs under realistic load.

---

## What Was Built

### Reconciliation Script (`src/reconcile/reconcile.js`)
Compares MySQL source truth against the MongoDB gold layer for a configurable sample of customers. Reports each record as `OK`, `STALE` (field-level drift), or `MISSING` (document not yet in MongoDB). Exits with code 1 if any issues are found — safe to wire into CI or post-recovery checks.

```
npm run reconcile                         # sample 200 customers
node src/reconcile/reconcile.js --limit=500 --verbose
```

### Initial Sync Script (`scripts/initial-sync.js`)
One-time bulk load that reads all customers from MySQL via full join, assembles the merged `customer_profile` document shape, and bulk-upserts into MongoDB using `replaceOne`. Idempotent — safe to re-run. CDC continues incrementally after the sync completes.

```
npm run initial-sync
```

### Recovery Test (`scripts/recovery-test.sh`)
End-to-end proof of consumer restart and catch-up:
1. Stops `poc_streams_app`
2. Applies a realistic mix of INSERTs and UPDATEs to MySQL while the consumer is down
3. Restarts the consumer
4. Polls MongoDB until a sentinel change appears and measures catch-up lag
5. Verifies every individual change in MongoDB by field and value
6. Runs reconciliation to confirm no drift

```
bash scripts/recovery-test.sh             # default 10 changes
bash scripts/recovery-test.sh --changes=25
```

### Idempotency Test (`scripts/idempotency-test.js`)
Three test cases that confirm the pipeline handles duplicate and rapid-fire events correctly:

| Test | What it does | What it proves |
|---|---|---|
| Duplicate INSERT | Same PK inserted twice | Exactly 1 document in MongoDB |
| Duplicate UPDATE | Same field set to same value twice | Correct value, no phantom duplicates |
| Rapid succession | 5 updates in quick succession | Last write wins, exactly 1 document |

```
npm run idempotency
```

### Galaxy Consumer Endpoint (`GET /galaxy/customer/:id`)
A second read path on the API that returns the slim summary shape the Galaxy inquiry screen requires (display name, status, primary contact, primary address, relationship count). Reads from the same MongoDB gold layer with no source system calls — demonstrates how a second downstream consumer gets near-real-time data without touching MySQL.

### API Service in Docker (`docker-compose.yml`)
Added `poc_api` as a Docker Compose service so the API starts automatically with the rest of the stack. No separate terminal required.

```
docker compose up -d        # all services including poc_api on port 3000
```

### Large-Volume Load Test
New scripts and npm commands for scale testing:

| Command | What it runs |
|---|---|
| `npm run seed:large` | Seeds 5,000 customers into MySQL |
| `npm run load:large` | 200 rps for 60s across 5,000 customer IDs |
| `npm run load:galaxy` | 50 rps Galaxy endpoint load test |
| `npm run benchmark` | Full benchmark: seed → sync → all three load tests → results doc |

---

## Key Decisions Made

| Decision | Rationale |
|---|---|
| Sentinel-based recovery detection | Kafka topics are ordered per partition — if the last-written sentinel event arrives in MongoDB, all prior events must have arrived too |
| Valid ENUM value as sentinel | Freeform strings are rejected by MySQL's ENUM constraint; using `INACTIVE`/`ACTIVE` flip avoids silent UPDATE failures |
| Real IDs fetched from MySQL for recovery test | Generated IDs like `cust-0011` may not exist; querying MySQL ensures only real customers are used |
| `replaceOne` + KTable semantics for idempotency | Full document replacement keyed by `customer_id` means reprocessing any event produces the correct final state with no duplicates |
| `node:20-alpine` + volume mount for API container | Avoids a separate Dockerfile for the API; the image is small and the source is already on the host |

---

## Benchmark Results

See [`benchmark_results.md`](benchmark_results.md) for full output.

| Metric | MongoDB | MySQL baseline | Galaxy |
|---|---|---|---|
| p50 latency | _run `npm run benchmark`_ | | |
| p95 latency | | | |
| p99 latency | | | |
| Throughput | | | |

---

## Week 3 Preview (from checklist)
- Build read API benchmarks with detailed reporting
- Run baseline vs offload comparison and capture sync staleness at time of read
- Add operational dashboards (lag, throughput, error rate)
- Introduce more complex source entities
- Prepare demo flow and architecture report

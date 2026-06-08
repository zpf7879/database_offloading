# Week 1 Summary — Database Offloading POC

## Goal
Prove the offload architecture by streaming changes from a simulated MySQL source into MongoDB Atlas in near real-time using CDC.

---

## What Was Built

### Infrastructure (Docker)
- MySQL 8 — source database with binlog enabled for CDC
- Kafka + Zookeeper — event backbone
- Custom Kafka Connect image — built from `debezium/connect:2.6` with the MongoDB Kafka connector JAR added on top
- Kafka UI — visual monitoring at port 8080

### Data Pipeline
```
MySQL (binlog) → Debezium (source connector) → Kafka topics → MongoDB Kafka (sink connector) → MongoDB Atlas
```

### Source Schema (MySQL)
- 6 tables: `customer`, `customer_address`, `customer_contact`, `customer_identification`, `customer_tax`, `relationship`
- Seeded with realistic skewed data patterns (hot customers, multiple addresses, sparse attributes)
- Bulk seeder script available (`npm run seed -- --count=N`)

### Kafka Connectors
- `poc-mysql-connector` — Debezium MySQL source, captures all 6 tables via binlog, publishes to Kafka topics
- `mongodb-sink-connector` — MongoDB Kafka sink, upserts each topic into its own MongoDB collection with idempotent `ReplaceOneDefaultStrategy`

### Read API
- `GET /customer/:id` — reads from MongoDB (offload path)
- `GET /customer/:id/baseline` — reads from MySQL via multi-table join (baseline path)
- Both return latency in the response for direct comparison

### Load Generator
- `npm run load:mongo` and `npm run load:baseline`
- Reports p50 / p95 / p99 latency, throughput, and error rate

---

## Key Decisions Made

| Decision | Rationale |
|---|---|
| Debezium 2.6 base image | Debezium 2.x is not on Confluent Hub — base image ships MySQL connector built in |
| MongoDB Kafka sink connector | Replaced custom `consumer.js` — production-grade, no code to maintain |
| Maven Central JAR download | Most reliable distribution path for the MongoDB connector |
| Docker Buildx via GitHub releases | Amazon Linux 2023 ships an outdated buildx that can't build compose files |
| nvm for Node.js | Avoids dnf package conflicts with `nodejs-full-i18n` on Amazon Linux 2023 |

---

## Deliverables
- GitHub repo: `zpf7879/database_offloading`
- `scripts/week1_setup.sh` — fully automated 8-step setup for a fresh Amazon Linux 2023 EC2
- `scripts/register-connectors.sh` — registers both connectors with one command
- `docs/poc_checklist.md` — POC reference document
- `docs/TODO.md` — tracked and completed the migration to MongoDB Kafka connector

---

## Week 2 Preview (from checklist)
- Replay and recovery testing (stop/restart consumer, catch-up)
- Idempotency and reconciliation checks
- Second consumer scenario (Galaxy)
- Larger data volume testing

# RDS-Based Mainframe Offload POC Checklist

## TODO

- [ ] **Replace the custom `consumer.js` app with the [MongoDB Kafka Connector](https://www.mongodb.com/docs/kafka-connector/current/) (sink connector) to write CDC events from Kafka directly into MongoDB.** This removes the need to maintain a bespoke Node.js consumer and gives production-grade features out of the box: dead letter queues, configurable write strategies, idempotent upserts, and Atlas-native support.

This checklist is for a POC where **RDS simulates the source system** and **MongoDB acts as the operational read store / offload target**. It is designed to prove the **offload architecture, document model, sync pattern, and API read performance**, not z/OS-specific CDC behavior.

## 1. Scope and success criteria

### Confirm the goal
* Prove that the offload pattern works even if the source is simulated with RDS.
* Position the POC as a validation of **architecture and application behavior**, not a final validation of **z/OS, VSAM, or IBM CDC operational behavior**.
* Keep the objective tied to BPI's requirement to **match or exceed current performance**.

### Pick the first domain
* Start with **Customer Profile** and **Relationship** as the first functional scope.
* Keep phase 1 to one source domain and one or two consuming patterns only.

### Pick the first consumers
* Use **EDC** as the primary consumer path.
* Optionally include **Galaxy** as a second consumer scenario.
* If you include Galaxy, explicitly show how a currently batch-oriented flow would behave in a near-real-time model.

### Define measurable success metrics
* p50, p95, p99 read latency
* peak read throughput
* sync lag from source change to MongoDB availability
* recovery time after consumer restart
* correctness rate between source and target
* estimated reduction in source-system reads

## 2. POC architecture

### Use this logical flow
* RDS source tables
* CDC or change publisher
* message bus or event stream
* MongoDB operational collections
* API or query layer
* load generator and observability

### Keep the architecture aligned to the target direction
* The target design already assumes an **event-streaming architecture** with CDC-style capture, an access layer, and downstream consumers including MongoDB.
* Your POC should therefore prove the same flow shape, even though the source is RDS instead of mainframe.

## 3. Environment setup

### Source simulator
* Provision one **RDS MySQL** instance.
* Create a dedicated schema just for the POC.
* Avoid sharing this instance with production write traffic.

### Middleware
* Provision one eventing layer:
  * Kafka if you want the closest fit to the target architecture
  * a lightweight message broker only if speed matters more than realism
* Provision one CDC/publisher service:
  * binlog-based CDC if available
  * otherwise an application-level change publisher

### MongoDB
* Use a **3-node replica set** if possible.
* If you only need functional validation, a smaller environment is acceptable, but keep the data model and indexes production-shaped.
* Separate raw-ingest collections from app-facing collections.

### Client and tooling
* one API service for read testing
* one load generator
* one metrics stack for latency, lag, throughput, errors, and consumer backlog

## 4. Source data design in RDS

### Model the minimum source entities
Create relational tables that approximate the first BPI scope:

* customer
* customer_address
* customer_contact
* customer_tax
* customer_identification
* relationship
* arrangement or account link

### Seed realistic data volumes
* Create enough records to expose join pain, not just functional correctness.
* Use skewed data patterns:
  * hot customers
  * multiple addresses
  * multiple relationships
  * sparse optional attributes
  * update-heavy subsets

### Include change types
* inserts
* updates
* deletes
* late-arriving updates
* duplicate event simulation
* out-of-order event simulation

## 5. CDC or change publication

### Pick one mechanism
* Preferred: **binlog CDC**
* Acceptable fallback: application-triggered event publishing
* Last resort: polling on updated timestamps for a simple demo

### Recommended implementation for RDS to Kafka
For this POC, the simplest and most credible pattern is:

* **RDS MySQL** as the source
* **Debezium** as the CDC engine
* **Kafka Connect** as the runtime
* **Kafka topics** as the event backbone
* **MongoDB consumer** as the offload target

This follows the same CDC fan-out pattern already used in internal guidance: Debezium reads the source and writes to Kafka, then downstream consumers load MongoDB ODS and any other targets.

### RDS setup steps
* Create and attach an RDS parameter group suitable for CDC.
* Enable the MySQL settings needed for binlog-based CDC.
* Reboot the RDS instance after applying the parameter group changes.
* Confirm the RDS endpoint, port, and security group rules allow access from the CDC worker.

### Kafka setup steps
* Run a Kafka cluster and Kafka Connect.
* For a POC, a self-hosted Kafka cluster is sufficient and is already considered representative for CDC and Kafka testing in internal guidance.
* Deploy the Debezium MySQL connector into Kafka Connect.
* Publish CDC events into Kafka topics, typically one topic per table.

### Suggested starting scope
Start with only the core source tables needed for the first demo:

* customer
* relationship

Keep the initial scope small so you can prove correctness, sync lag, replay, and read-offload behavior before adding more entities.

### Suggested Debezium connector shape
A minimal connector should include:

* RDS hostname and port
* CDC user credentials
* a unique database server id
* topic prefix
* include list for the POC database
* include list for the first tables
* Kafka topic for schema history

### Standardize the event envelope
Each event should include:

* event_id
* source_table
* operation_type
* source_pk
* event_time
* sequence or version
* payload_before if needed
* payload_after
* correlation_id for debugging

### Test failure handling
* replay after consumer restart
* duplicate delivery
* idempotent upsert
* poison message routing
* lag monitoring

### What this proves vs. what it does not
This setup proves:

* source-to-Kafka CDC flow
* near-real-time event propagation
* fan-out to MongoDB and other consumers
* replay and recovery behavior

This setup does not prove:

* z/OS log capture specifics
* VSAM semantics
* IBM Classic CDC runtime behavior
* CICS or logstream operational details

## 6. MongoDB target design

### Do not mirror the relational schema 1:1
Build app-ready documents instead.

### Suggested document shape
Use one primary collection such as `customer_profile` with embedded subdocuments:

* customer core
* demographics
* contact methods
* addresses
* identification
* tax details
* relationships
* selected arrangements or summaries

### Separate technical and business collections
* raw events collection
* transformed operational collection
* optional audit or reconciliation collection

### Indexes
Start with:
* customer_id
* external_reference if any
* relationship.party_id
* updated_at
* selected lookup fields used by the API

### Data quality rules
* one canonical customer per key
* deterministic merge rules
* null-handling standards
* delete semantics clearly defined

## 7. Read API and access pattern

### Build one realistic read path
Implement at least one endpoint that simulates the offloaded read path, for example:

* get customer profile by customer id
* get profile plus relationships
* get profile summary for inquiry screen

### Compare two modes
* baseline mode: read from source-side relational join path
* offload mode: read from MongoDB document path

### Measure
* response time
* DB calls
* rows scanned
* CPU impact if available
* sync staleness at time of read

## 8. Test plan

### Functional tests
* insert new customer
* update address
* update contact details
* add relationship
* remove relationship
* delete or deactivate a record
* verify target correctness after each change

### Resilience tests
* stop consumer and restart
* message duplication
* partial publish failure
* backlog catch-up
* target node restart

### Performance tests
* steady-state read traffic
* burst traffic
* mixed read and write simulation
* lag under high update rate
* large-profile document retrieval

## 9. Benchmark methodology

### Baseline
* Record current relational read behavior from the simulated source path.
* Capture:
  * latency
  * throughput
  * query complexity
  * row scans
  * concurrent user behavior

### Target
* Run the same business queries via MongoDB.
* Compare:
  * median latency
  * tail latency
  * throughput headroom
  * operational simplicity

### What to report
* "same correctness"
* "faster read latency"
* "lower query complexity"
* "acceptable sync lag"
* "clear path to offload read traffic"

## 10. Guardrails and caveats

### Be explicit about what this POC proves
* It proves the **offload pattern**, **document design**, **near-real-time sync**, and **consumer read behavior**.
* It does **not** prove z/OS log capture, VSAM semantics, CICS integration, or IBM CDC operational setup.

### Keep the narrative aligned to BPI context
* BPI has discussed a **MariaDB-based POC** and a plan to offload about **5% of the user base**.
* The POC sample systems already discussed include **EDC, Galaxy, Speedy, and Fraud**.
* The larger direction is a **read-offload / ODS pattern** for mainframe modernization.

## 11. Recommended delivery plan

### Phase 1
* RDS source simulator
* one CDC/publisher path
* one MongoDB operational collection
* one API endpoint
* one benchmark report

### Phase 2
* add replay and recovery testing
* add second consumer scenario such as Galaxy
* harden idempotency and reconciliation
* test larger data volumes

### Phase 3
* introduce more complex source entities
* add operational dashboards
* prepare mapping to actual mainframe-side integration assumptions

## 12. Final deliverables

By the end of the POC, prepare:

* architecture diagram
* source-to-target field mapping
* event schema
* MongoDB document model
* index design
* benchmark summary
* limitations section
* next-step plan for real mainframe-source validation

## 13. Recommended first build order

### Week 1
* define entities
* create RDS schema
* seed sample data
* design target document model

### Week 2
* implement CDC or publisher
* wire message bus
* load MongoDB
* build reconciliation checks

### Week 3
* build read API
* run baseline and target benchmarks
* capture sync lag and failure scenarios

### Week 4
* refine indexes
* optimize document structure
* prepare demo flow and report

## 14. Simple decision summary

If your goal is:
* proving **mainframe-specific integration**, RDS is not enough
* proving **the offload architecture and business outcome**, RDS is a very practical and credible starting point

That is the right way to frame this POC internally and with the account team.

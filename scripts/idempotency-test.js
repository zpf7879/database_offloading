/**
 * Idempotency test — verifies that duplicate MySQL writes do not cause
 * duplicate or corrupted documents in the MongoDB gold layer.
 *
 * The MongoDB sink connector uses ReplaceOneDefaultStrategy, and the
 * Kafka Streams KTable aggregation is keyed by customer_id, so any
 * event processed more than once should produce exactly the same result.
 *
 * Test cases:
 *   1. DUPLICATE INSERT  — insert a customer twice (same PK); expect 1 doc in MongoDB
 *   2. DUPLICATE UPDATE  — update the same field twice with the same value; expect correct final value
 *   3. RAPID SUCCESSION  — fire 5 updates to the same customer in quick succession; expect last value wins
 *
 * Usage:
 *   node scripts/idempotency-test.js
 */
import "dotenv/config";
import mysql from "mysql2/promise";
import { MongoClient } from "mongodb";
import crypto from "crypto";

// ── ANSI helpers ──────────────────────────────────────────────────────────────
const G  = (s) => `\x1b[32m${s}\x1b[0m`;
const R  = (s) => `\x1b[31m${s}\x1b[0m`;
const Y  = (s) => `\x1b[33m${s}\x1b[0m`;
const C  = (s) => `\x1b[36m${s}\x1b[0m`;
const B  = (s) => `\x1b[1m${s}\x1b[0m`;
const ok   = (s) => console.log(`${G("✓")} ${s}`);
const fail = (s) => console.log(`${R("✗")} ${s}`);
const info = (s) => console.log(`${C("ℹ")} ${s}`);
const head = (s) => console.log(`\n${B("─".repeat(60))}\n${B(` ${s}`)}\n${"─".repeat(60)}`);

const POLL_INTERVAL_MS = 500;
const POLL_TIMEOUT_MS  = 30_000;

// ── Connections ───────────────────────────────────────────────────────────────
async function getMysql() {
  return mysql.createConnection({
    host:     process.env.MYSQL_HOST     || "localhost",
    port:     parseInt(process.env.MYSQL_PORT || "3306"),
    user:     process.env.MYSQL_USER     || "poc_user",
    password: process.env.MYSQL_PASSWORD || "poc_pass",
    database: process.env.MYSQL_DATABASE || "offload_poc",
  });
}

async function getMongo() {
  const client = new MongoClient(process.env.MONGODB_URI, {
    connectTimeoutMS: 10_000,
    serverSelectionTimeoutMS: 10_000,
  });
  await client.connect();
  return { client, db: client.db(process.env.MONGODB_DB || "offload_poc") };
}

// ── Poll helper ───────────────────────────────────────────────────────────────
async function pollUntil(fn, label) {
  const start = Date.now();
  process.stdout.write(`  ⏳ Waiting for ${label}`);
  while (Date.now() - start < POLL_TIMEOUT_MS) {
    const result = await fn();
    if (result !== null && result !== undefined && result !== false) {
      const ms = Date.now() - start;
      process.stdout.write(`\r  ${G("✓")} ${label} — ${G(`${ms}ms`)}\n`);
      return result;
    }
    process.stdout.write(".");
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
  process.stdout.write(`\r  ${R("✗")} ${label} — timed out\n`);
  return null;
}

// ── Assertions ────────────────────────────────────────────────────────────────
let passed = 0;
let failed = 0;

function assert(condition, message) {
  if (condition) { ok(message); passed++; }
  else           { fail(message); failed++; }
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`\n${B("═".repeat(60))}`);
  console.log(B("  Idempotency Test — CDC Pipeline"));
  console.log(`${B("═".repeat(60))}\n`);

  const mysqlConn = await getMysql();
  const { client: mongoClient, db } = await getMongo();
  const col = db.collection("customer_profile");

  ok("Connected to MySQL and MongoDB");

  // ── TEST 1: Duplicate INSERT ───────────────────────────────────────────────
  head("TEST 1 — Duplicate INSERT (same PK inserted twice)");

  const dupId = `idem-dup-${crypto.randomUUID().slice(0, 8)}`;
  info(`Inserting ${B(dupId)} twice into MySQL…`);

  await mysqlConn.execute(
    `INSERT IGNORE INTO customer
       (customer_id, external_ref, first_name, last_name, date_of_birth, gender, nationality, status)
     VALUES (?, ?, 'Idem', 'DupTest', '1990-01-01', 'M', 'AUS', 'ACTIVE')`,
    [dupId, `EXT-${dupId}`]
  );
  // Second insert — MySQL INSERT IGNORE silently skips duplicate PKs
  await mysqlConn.execute(
    `INSERT IGNORE INTO customer
       (customer_id, external_ref, first_name, last_name, date_of_birth, gender, nationality, status)
     VALUES (?, ?, 'Idem', 'DupTest', '1990-01-01', 'M', 'AUS', 'ACTIVE')`,
    [dupId, `EXT-${dupId}`]
  );
  info("Two INSERT IGNORE statements fired");

  // Wait for doc to appear
  await pollUntil(
    () => col.findOne({ customer_id: dupId }),
    `customer_profile: ${dupId}`
  );

  // Count docs with this ID — must be exactly 1
  const dupCount = await col.countDocuments({ customer_id: dupId });
  assert(dupCount === 1,
    `Exactly 1 document in MongoDB for ${dupId} (got ${dupCount})`);

  // ── TEST 2: Duplicate UPDATE ───────────────────────────────────────────────
  head("TEST 2 — Duplicate UPDATE (same field set to same value twice)");

  const updateId = "cust-0002";
  const targetStatus = "SUSPENDED";
  info(`Sending status='${targetStatus}' to ${B(updateId)} twice…`);

  await mysqlConn.execute(
    "UPDATE customer SET status=?, updated_at=NOW() WHERE customer_id=?",
    [targetStatus, updateId]
  );
  await new Promise((r) => setTimeout(r, 200)); // slight gap so binlog has 2 distinct events
  await mysqlConn.execute(
    "UPDATE customer SET status=?, updated_at=NOW() WHERE customer_id=?",
    [targetStatus, updateId]
  );

  // Wait for MongoDB to reflect the value
  const afterDup = await pollUntil(
    () => col.findOne({ customer_id: updateId, status: targetStatus }),
    `customer_profile ${updateId} status=${targetStatus}`
  );

  assert(afterDup !== null,
    `MongoDB reflects status='${targetStatus}' for ${updateId}`);

  const dupUpdateCount = await col.countDocuments({ customer_id: updateId });
  assert(dupUpdateCount === 1,
    `Still exactly 1 document for ${updateId} after duplicate update (got ${dupUpdateCount})`);

  // ── TEST 3: Rapid succession updates (last write wins) ────────────────────
  head("TEST 3 — Rapid succession (5 updates, last value must win)");

  const rapidId = "cust-0003";
  const values  = ["ACTIVE", "INACTIVE", "SUSPENDED", "ACTIVE", "INACTIVE"];
  const lastVal = values[values.length - 1];

  info(`Firing ${values.length} rapid status updates to ${B(rapidId)}: ${values.join(" → ")}`);

  for (const val of values) {
    await mysqlConn.execute(
      "UPDATE customer SET status=?, updated_at=NOW() WHERE customer_id=?",
      [val, rapidId]
    );
  }

  // Poll until the last value appears
  const afterRapid = await pollUntil(
    () => col.findOne({ customer_id: rapidId, status: lastVal }),
    `customer_profile ${rapidId} status=${lastVal} (last write)`
  );

  assert(afterRapid !== null,
    `MongoDB shows last value '${lastVal}' for ${rapidId} — last write wins`);

  const rapidCount = await col.countDocuments({ customer_id: rapidId });
  assert(rapidCount === 1,
    `Still exactly 1 document for ${rapidId} after 5 rapid updates (got ${rapidCount})`);

  // ── Summary ───────────────────────────────────────────────────────────────
  head("IDEMPOTENCY TEST SUMMARY");

  console.log(`  ${G("PASS")} : ${passed}`);
  console.log(`  ${failed > 0 ? R("FAIL") : G("FAIL")} : ${failed}`);
  console.log();

  if (failed === 0) {
    ok(B("All idempotency checks passed — ReplaceOne + KTable semantics are correct"));
  } else {
    fail(B(`${failed} check(s) failed — review output above`));
  }

  await mysqlConn.end();
  await mongoClient.close();
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });

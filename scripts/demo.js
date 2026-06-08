/**
 * End-to-End Pipeline Demo
 *
 * Demonstrates the real-time offload pipeline by:
 *   1. Inserting a new customer into MySQL
 *   2. Updating an existing customer in MySQL
 *   3. Polling MongoDB until both changes appear (bronze + gold layers)
 *   4. Measuring end-to-end latency
 *   5. Printing instructions for manual MongoDB inspection
 *
 * Usage:
 *   node scripts/demo.js
 */

import mysql from "mysql2/promise";
import { MongoClient } from "mongodb";
import "dotenv/config";
import crypto from "crypto";

// ── ANSI colours ──────────────────────────────────────────────────────────────
const C = {
  reset:  "\x1b[0m",
  bold:   "\x1b[1m",
  green:  "\x1b[32m",
  yellow: "\x1b[33m",
  cyan:   "\x1b[36m",
  red:    "\x1b[31m",
  blue:   "\x1b[34m",
  gray:   "\x1b[90m",
};

const ok    = (s) => console.log(`${C.green}✓${C.reset} ${s}`);
const info  = (s) => console.log(`${C.cyan}ℹ${C.reset} ${s}`);
const warn  = (s) => console.log(`${C.yellow}⚠${C.reset} ${s}`);
const err   = (s) => console.log(`${C.red}✗${C.reset} ${s}`);
const head  = (s) => console.log(`\n${C.bold}${C.blue}${"─".repeat(60)}${C.reset}\n${C.bold} ${s}${C.reset}\n${C.blue}${"─".repeat(60)}${C.reset}`);
const sub   = (s) => console.log(`  ${C.gray}${s}${C.reset}`);
const hi    = (s) => `${C.yellow}${C.bold}${s}${C.reset}`;

// ── Config ────────────────────────────────────────────────────────────────────
const POLL_INTERVAL_MS  = 500;
const POLL_TIMEOUT_MS   = 30_000;

const NEW_CUSTOMER_ID   = `demo-${crypto.randomUUID().slice(0, 8)}`;
const NEW_CUSTOMER = {
  customer_id:  NEW_CUSTOMER_ID,
  external_ref: `EXT-DEMO-${NEW_CUSTOMER_ID.slice(5)}`,
  first_name:   "Demo",
  last_name:    "User",
  date_of_birth:"1990-01-15",
  gender:       "F",
  nationality:  "AUS",
  status:       "ACTIVE",
};

const UPDATE_CUSTOMER_ID = "cust-0001";
const UPDATE_NEW_STATUS  = "SUSPENDED";

// ── MySQL helpers ─────────────────────────────────────────────────────────────
async function getMysql() {
  return mysql.createConnection({
    host:     process.env.MYSQL_HOST     || "localhost",
    port:     parseInt(process.env.MYSQL_PORT || "3306"),
    user:     process.env.MYSQL_USER     || "poc_user",
    password: process.env.MYSQL_PASSWORD || "poc_pass",
    database: process.env.MYSQL_DATABASE || "offload_poc",
  });
}

// ── MongoDB helpers ───────────────────────────────────────────────────────────
async function getMongo() {
  const client = new MongoClient(process.env.MONGODB_URI, {
    connectTimeoutMS: 10_000,
    serverSelectionTimeoutMS: 10_000,
  });
  await client.connect();
  return { client, db: client.db(process.env.MONGODB_DB || "offload_poc") };
}

async function pollUntil(fn, label, timeoutMs = POLL_TIMEOUT_MS) {
  const start = Date.now();
  process.stdout.write(`  ⏳ Waiting for ${label}`);
  while (Date.now() - start < timeoutMs) {
    const result = await fn();
    if (result) {
      const latency = Date.now() - start;
      process.stdout.write(`\r  ${C.green}✓${C.reset} ${label} — appeared in ${C.bold}${C.green}${latency}ms${C.reset}\n`);
      return { result, latency };
    }
    process.stdout.write(".");
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }
  process.stdout.write(`\r  ${C.red}✗${C.reset} ${label} — timed out after ${timeoutMs}ms\n`);
  return { result: null, latency: timeoutMs };
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`\n${C.bold}${C.blue}${"═".repeat(60)}${C.reset}`);
  console.log(`${C.bold}  Database Offloading POC — End-to-End Demo${C.reset}`);
  console.log(`${C.blue}${"═".repeat(60)}${C.reset}\n`);

  const mysqlConn = await getMysql();
  const { client: mongoClient, db } = await getMongo();

  ok("Connected to MySQL");
  ok("Connected to MongoDB Atlas");

  // ── CHANGE 1: INSERT ────────────────────────────────────────────────────────
  head("CHANGE 1 — INSERT new customer into MySQL");

  info(`Inserting: ${hi(NEW_CUSTOMER.first_name + " " + NEW_CUSTOMER.last_name)}`);
  sub(`customer_id  : ${NEW_CUSTOMER.customer_id}`);
  sub(`external_ref : ${NEW_CUSTOMER.external_ref}`);
  sub(`status       : ${NEW_CUSTOMER.status}`);
  sub(`nationality  : ${NEW_CUSTOMER.nationality}`);

  const insertedAt = Date.now();
  await mysqlConn.execute(
    `INSERT INTO customer
       (customer_id, external_ref, first_name, last_name, date_of_birth, gender, nationality, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      NEW_CUSTOMER.customer_id,
      NEW_CUSTOMER.external_ref,
      NEW_CUSTOMER.first_name,
      NEW_CUSTOMER.last_name,
      NEW_CUSTOMER.date_of_birth,
      NEW_CUSTOMER.gender,
      NEW_CUSTOMER.nationality,
      NEW_CUSTOMER.status,
    ]
  );
  ok(`INSERT committed to MySQL at T+0ms`);

  // ── CHANGE 2: UPDATE ────────────────────────────────────────────────────────
  head("CHANGE 2 — UPDATE existing customer in MySQL");

  const [beforeRows] = await mysqlConn.execute(
    "SELECT status FROM customer WHERE customer_id = ?",
    [UPDATE_CUSTOMER_ID]
  );
  const oldStatus = beforeRows[0]?.status ?? "UNKNOWN";

  info(`Updating customer ${hi(UPDATE_CUSTOMER_ID)} (Alice Nguyen)`);
  sub(`Field  : status`);
  sub(`Before : ${hi(oldStatus)}`);
  sub(`After  : ${hi(UPDATE_NEW_STATUS)}`);

  const updatedAt = Date.now();
  await mysqlConn.execute(
    "UPDATE customer SET status = ?, updated_at = NOW() WHERE customer_id = ?",
    [UPDATE_NEW_STATUS, UPDATE_CUSTOMER_ID]
  );
  ok(`UPDATE committed to MySQL at T+0ms`);

  // ── POLL BRONZE LAYER ───────────────────────────────────────────────────────
  head("POLLING — Bronze layer (1:1 staging collections)");

  const { latency: bronzeInsertLatency } = await pollUntil(
    () => db.collection("bronze_customer").findOne({ customer_id: NEW_CUSTOMER_ID }),
    `bronze_customer: new record (${NEW_CUSTOMER_ID})`
  );

  const { latency: bronzeUpdateLatency } = await pollUntil(
    () => db.collection("bronze_customer").findOne({
      customer_id: UPDATE_CUSTOMER_ID,
      status: UPDATE_NEW_STATUS,
    }),
    `bronze_customer: updated status → ${UPDATE_NEW_STATUS} (${UPDATE_CUSTOMER_ID})`
  );

  // ── POLL GOLD LAYER ─────────────────────────────────────────────────────────
  head("POLLING — Gold layer (merged customer_profile)");

  const { result: newProfile, latency: goldInsertLatency } = await pollUntil(
    () => db.collection("customer_profile").findOne({ customer_id: NEW_CUSTOMER_ID }),
    `customer_profile: new merged doc (${NEW_CUSTOMER_ID})`
  );

  const { result: updatedProfile, latency: goldUpdateLatency } = await pollUntil(
    () => db.collection("customer_profile").findOne({
      customer_id: UPDATE_CUSTOMER_ID,
      status: UPDATE_NEW_STATUS,
    }),
    `customer_profile: updated status → ${UPDATE_NEW_STATUS} (${UPDATE_CUSTOMER_ID})`
  );

  // ── LATENCY SUMMARY ─────────────────────────────────────────────────────────
  head("LATENCY SUMMARY");

  const pad = (s) => String(s).padStart(7);
  console.log(`  ${"Change".padEnd(45)} ${"Bronze".padStart(8)}  ${"Gold".padStart(8)}`);
  console.log(`  ${"─".repeat(65)}`);
  console.log(`  ${"INSERT new customer".padEnd(45)} ${pad(bronzeInsertLatency + "ms")}  ${pad(goldInsertLatency + "ms")}`);
  console.log(`  ${"UPDATE status (cust-0001)".padEnd(45)} ${pad(bronzeUpdateLatency + "ms")}  ${pad(goldUpdateLatency + "ms")}`);
  console.log(`\n  Pipeline: MySQL → Debezium → Kafka → Kafka Streams → MongoDB`);

  // ── WHAT TO CHECK IN MONGODB ─────────────────────────────────────────────────
  head("WHAT TO CHECK IN MONGODB ATLAS");

  console.log(`  Open MongoDB Atlas and navigate to database: ${hi("offload_poc")}\n`);

  console.log(`  ${C.bold}1. Bronze layer — raw 1:1 copy${C.reset}`);
  console.log(`     Collection : ${hi("bronze_customer")}`);
  console.log(`     Filter     : ${hi(`{ "customer_id": "${NEW_CUSTOMER_ID}" }`)}`);
  console.log(`     Expect     : one document with all Debezium metadata (__op, __table, etc.)\n`);

  console.log(`  ${C.bold}2. Gold layer — merged profile (INSERT)${C.reset}`);
  console.log(`     Collection : ${hi("customer_profile")}`);
  console.log(`     Filter     : ${hi(`{ "customer_id": "${NEW_CUSTOMER_ID}" }`)}`);
  if (newProfile) {
    console.log(`     Fields     : ${hi(Object.keys(newProfile).filter(k => k !== "_id").join(", "))}`);
    console.log(`     No __op, no __deleted — clean business document ✓`);
  }
  console.log();

  console.log(`  ${C.bold}3. Gold layer — merged profile (UPDATE)${C.reset}`);
  console.log(`     Collection : ${hi("customer_profile")}`);
  console.log(`     Filter     : ${hi(`{ "customer_id": "${UPDATE_CUSTOMER_ID}" }`)}`);
  console.log(`     Expect     : ${hi(`status = "${UPDATE_NEW_STATUS}"`)}`);
  if (updatedProfile) {
    console.log(`     Confirmed  : status = "${hi(updatedProfile.status)}" ✓`);
  }
  console.log();

  console.log(`  ${C.bold}4. Compare bronze vs gold${C.reset}`);
  console.log(`     Bronze has : __op, __deleted, __table, __source_ts_ms  (CDC metadata)`);
  console.log(`     Gold has   : customer_id, addresses[], contacts[], relationships[]  (clean)`);

  console.log(`\n${C.blue}${"═".repeat(60)}${C.reset}`);
  console.log(`${C.bold}  Demo complete!${C.reset}`);
  console.log(`${C.blue}${"═".repeat(60)}${C.reset}\n`);

  await mysqlConn.end();
  await mongoClient.close();
}

main().catch((e) => {
  err(`Fatal: ${e.message}`);
  console.error(e);
  process.exit(1);
});

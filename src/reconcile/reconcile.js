/**
 * Reconciliation check — MySQL source truth vs MongoDB gold layer.
 *
 * For each customer in MySQL, fetches the gold customer_profile document
 * and checks:
 *   - MISSING   : document does not exist in MongoDB yet
 *   - STALE     : core fields (status, first_name, last_name) differ
 *   - OK        : document matches source
 *
 * Usage:
 *   node src/reconcile/reconcile.js [--limit=200] [--verbose]
 *
 * Exit codes:
 *   0 — all checked records match
 *   1 — at least one mismatch or missing document found
 */
import "dotenv/config";
import { getRdsPool } from "../db/rds.js";
import { getMongoDb, closeMongoDb } from "../db/mongo.js";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => a.replace(/^--/, "").split("="))
);
const LIMIT   = parseInt(args.limit   || "200");
const VERBOSE = "verbose" in args;

// ANSI helpers
const G = (s) => `\x1b[32m${s}\x1b[0m`;
const R = (s) => `\x1b[31m${s}\x1b[0m`;
const Y = (s) => `\x1b[33m${s}\x1b[0m`;
const B = (s) => `\x1b[1m${s}\x1b[0m`;

async function getMysqlCustomers(pool, limit) {
  const [rows] = await pool.execute(
    `SELECT customer_id, first_name, last_name, status, updated_at
     FROM customer
     ORDER BY customer_id
     LIMIT ?`,
    [limit]
  );
  return rows;
}

async function getMongoProfiles(db, customerIds) {
  const docs = await db
    .collection("customer_profile")
    .find({ customer_id: { $in: customerIds } }, {
      projection: { customer_id: 1, first_name: 1, last_name: 1, status: 1, _id: 0 },
    })
    .toArray();
  return new Map(docs.map((d) => [d.customer_id, d]));
}

function checkRecord(mysqlRow, mongoDoc) {
  if (!mongoDoc) return { verdict: "MISSING", diffs: [] };

  const diffs = [];
  for (const field of ["first_name", "last_name", "status"]) {
    const src = mysqlRow[field];
    const tgt = mongoDoc[field];
    if (src !== tgt) diffs.push({ field, mysql: src, mongo: tgt });
  }
  return { verdict: diffs.length ? "STALE" : "OK", diffs };
}

async function run() {
  const pool = getRdsPool();
  const db   = await getMongoDb();

  console.log(B(`\nReconciliation — MySQL vs MongoDB Gold Layer`));
  console.log(`Sampling up to ${LIMIT} customers from MySQL source...\n`);

  const mysqlRows  = await getMysqlCustomers(pool, LIMIT);
  const ids        = mysqlRows.map((r) => r.customer_id);
  const mongoIndex = await getMongoProfiles(db, ids);

  let ok = 0, missing = 0, stale = 0;
  const issues = [];

  for (const row of mysqlRows) {
    const mongoDoc = mongoIndex.get(row.customer_id);
    const { verdict, diffs } = checkRecord(row, mongoDoc);

    if (verdict === "OK") {
      ok++;
      if (VERBOSE) console.log(G(`  OK      ${row.customer_id}`));
    } else {
      if (verdict === "MISSING") missing++;
      else stale++;
      issues.push({ customer_id: row.customer_id, verdict, diffs });
    }
  }

  // Print issues
  for (const issue of issues) {
    if (issue.verdict === "MISSING") {
      console.log(R(`  MISSING  ${issue.customer_id}`));
    } else {
      console.log(Y(`  STALE    ${issue.customer_id}`));
      for (const d of issue.diffs) {
        console.log(`           ${d.field}: mysql=${B(d.mysql)}  mongo=${B(d.mongo ?? "null")}`);
      }
    }
  }

  // Summary
  const total = mysqlRows.length;
  console.log(`\n${"─".repeat(50)}`);
  console.log(B(`Reconciliation summary  (sample: ${total} customers)`));
  console.log(`  ${G("OK")}      : ${ok}  (${pct(ok, total)}%)`);
  console.log(`  ${Y("STALE")}   : ${stale}  (${pct(stale, total)}%)`);
  console.log(`  ${R("MISSING")} : ${missing}  (${pct(missing, total)}%)`);
  console.log(`${"─".repeat(50)}\n`);

  await closeMongoDb();
  await pool.end();

  process.exit(missing + stale > 0 ? 1 : 0);
}

function pct(n, total) {
  return total ? ((n / total) * 100).toFixed(1) : "0.0";
}

run().catch((e) => { console.error(e); process.exit(1); });

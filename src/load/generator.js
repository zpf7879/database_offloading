/**
 * Simple load generator — fires concurrent GET /customer/:id requests
 * and reports p50/p95/p99 latency + throughput.
 *
 * Usage:
 *   node src/load/generator.js [--mode mongo|baseline] [--rps 50] [--duration 30]
 */
import "dotenv/config";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => a.replace(/^--/, "").split("="))
);

const MODE     = args.mode     || "mongo";
const RPS      = parseInt(args.rps      || "50");
const DURATION = parseInt(args.duration || "30"); // seconds
const BASE_URL = process.env.API_BASE_URL || "http://localhost:3000";

const CUSTOMER_IDS = [
  "cust-0001","cust-0002","cust-0003","cust-0004","cust-0005",
  "cust-0006","cust-0007","cust-0008","cust-0009","cust-0010",
];

const latencies = [];
let errors = 0;

async function fireRequest() {
  const id  = CUSTOMER_IDS[Math.floor(Math.random() * CUSTOMER_IDS.length)];
  const url = MODE === "baseline"
    ? `${BASE_URL}/customer/${id}/baseline`
    : `${BASE_URL}/customer/${id}`;
  const t0 = Date.now();
  try {
    const res = await fetch(url);
    if (!res.ok) errors++;
    latencies.push(Date.now() - t0);
  } catch {
    errors++;
    latencies.push(Date.now() - t0);
  }
}

function percentile(sorted, p) {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

async function run() {
  console.log(`Load test: mode=${MODE} rps=${RPS} duration=${DURATION}s`);
  const intervalMs = 1000 / RPS;
  const endTime    = Date.now() + DURATION * 1000;
  const pending    = [];

  while (Date.now() < endTime) {
    pending.push(fireRequest());
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  await Promise.allSettled(pending);

  const sorted = [...latencies].sort((a, b) => a - b);
  const total  = latencies.length;
  console.log("\n--- Results ---");
  console.log(`Total requests : ${total}`);
  console.log(`Errors         : ${errors}`);
  console.log(`Throughput     : ${(total / DURATION).toFixed(1)} req/s`);
  console.log(`p50 latency    : ${percentile(sorted, 50)} ms`);
  console.log(`p95 latency    : ${percentile(sorted, 95)} ms`);
  console.log(`p99 latency    : ${percentile(sorted, 99)} ms`);
  console.log(`Max latency    : ${sorted[sorted.length - 1]} ms`);
}

run().catch(console.error);

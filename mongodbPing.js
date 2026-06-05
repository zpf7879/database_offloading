/**
 * MongoDB Atlas Connectivity Check
 *
 * Install dependencies:
 *   npm install mongodb dotenv
 *
 * Set your connection string (choose one):
 *   Option A — environment variable:
 *     Windows (cmd):   set MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
 *     Windows (PS):    $env:MONGODB_URI="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/"
 *     macOS / Linux:   export MONGODB_URI="mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/"
 *   Option B — create a .env file in this directory:
 *     MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/
 *
 * Run:
 *   node mongodbPing.js
 */

// dotenv loads key=value pairs from a .env file into process.env.
// { override: false } means an already-set env var always wins.
import "dotenv/config";

import { MongoClient } from "mongodb";

// ---------------------------------------------------------------------------
// Resolve the connection URI
// ---------------------------------------------------------------------------

const uri = process.env.MONGODB_URI;

if (!uri) {
  // Bail early with a clear message rather than letting the driver throw a
  // cryptic error about a missing connection string.
  console.error(
    "ERROR: MONGODB_URI is not set.\n" +
      "  Set it as an environment variable or add it to a .env file.\n" +
      "  See the comment at the top of this file for instructions."
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Connect and ping
// ---------------------------------------------------------------------------

// A single MongoClient manages the connection pool for your whole application.
// In a real app you would create it once and reuse it — never create one per
// request, because each instantiation opens its own pool of TCP connections.
const client = new MongoClient(uri, {
  // Fail fast if Atlas is unreachable, so the user gets a clear error quickly.
  connectTimeoutMS: 10_000,
  // How long the driver will wait to find an available server before giving up.
  serverSelectionTimeoutMS: 10_000,
});

async function run() {
  try {
    console.log("Connecting to MongoDB Atlas…");
    await client.connect();

    // The ping command sends a minimal { ping: 1 } document to the server.
    // It is the lightest possible way to confirm the cluster is reachable and
    // that the credentials in the URI are valid — no data is read or written.
    const result = await client.db("admin").command({ ping: 1 });

    if (result?.ok === 1) {
      console.log("SUCCESS: Pinged your MongoDB Atlas cluster.");
      console.log("  The connection is working and your credentials are valid.");
    } else {
      // The server responded, but the reply was unexpected.
      console.warn("WARNING: Ping returned an unexpected response:", result);
    }
  } catch (err) {
    // Common causes:
    //   - Wrong URI (typo in hostname, user, or password)
    //   - Your IP address is not on the Atlas network access list
    //   - The cluster is paused or the network is unreachable
    console.error("ERROR: Could not connect to MongoDB Atlas.");
    console.error("  Reason:", err.message);
    console.error("\nTroubleshooting tips:");
    console.error(
      "  1. Check your connection string — user, password, and cluster hostname."
    );
    console.error(
      "  2. In Atlas → Network Access, ensure your current IP is allowed."
    );
    console.error("  3. Make sure the cluster is not paused.");
    process.exit(1);
  } finally {
    // Always close the client so the process exits cleanly.
    // In a long-running server you would keep the client open for the app's
    // lifetime and only close it on graceful shutdown.
    await client.close();
    console.log("Connection closed.");
  }
}

run();

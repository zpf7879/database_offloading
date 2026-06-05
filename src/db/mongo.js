import { MongoClient } from "mongodb";
import "dotenv/config";

let client;
let db;

export async function getMongoDb() {
  if (!client) {
    client = new MongoClient(process.env.MONGODB_URI, {
      connectTimeoutMS: 10_000,
      serverSelectionTimeoutMS: 10_000,
    });
    await client.connect();
    db = client.db(process.env.MONGODB_DB || "offload_poc");
    await ensureIndexes(db);
  }
  return db;
}

async function ensureIndexes(db) {
  const col = db.collection("customer_profile");
  await col.createIndexes([
    { key: { customer_id: 1 },          unique: true },
    { key: { external_ref: 1 },         sparse: true },
    { key: { "relationships.party_id_to": 1 } },
    { key: { updated_at: -1 } },
    { key: { status: 1 } },
  ]);
}

export async function closeMongoDb() {
  if (client) {
    await client.close();
    client = null;
    db = null;
  }
}

/**
 * Initial sync — bulk loads all customers from MySQL into the MongoDB
 * gold layer (customer_profile collection).
 *
 * Run this once to populate MongoDB before or after CDC is active.
 * The upsert is idempotent — safe to re-run at any time without creating
 * duplicates. CDC will continue to maintain documents after this completes.
 *
 * Usage:
 *   node scripts/initial-sync.js [--batch=500]
 */
import "dotenv/config";
import { getRdsPool } from "../src/db/rds.js";
import { getMongoDb, closeMongoDb } from "../src/db/mongo.js";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => a.replace(/^--/, "").split("="))
);
const BATCH_SIZE = parseInt(args.batch || "500");

// ANSI helpers
const G  = (s) => `\x1b[32m${s}\x1b[0m`;
const B  = (s) => `\x1b[1m${s}\x1b[0m`;
const C  = (s) => `\x1b[36m${s}\x1b[0m`;

async function fetchAllCustomerIds(pool) {
  const [rows] = await pool.query("SELECT customer_id FROM customer ORDER BY customer_id");
  return rows.map((r) => r.customer_id);
}

async function fetchProfileBatch(pool, ids) {
  // Same full-join query as the baseline API path — assembles one row per
  // child entity so we can reconstruct the embedded arrays.
  const placeholders = ids.map(() => "?").join(",");
  const [rows] = await pool.query(
    `SELECT
       c.customer_id, c.external_ref, c.first_name, c.last_name,
       c.date_of_birth, c.gender, c.nationality, c.preferred_locale,
       c.status, c.created_at, c.updated_at,
       ca.address_id, ca.address_type, ca.line1, ca.line2,
       ca.city, ca.state, ca.postcode, ca.country AS addr_country,
       ca.is_primary AS addr_primary,
       cc.contact_id, cc.contact_type, cc.contact_value, cc.is_primary AS contact_primary, cc.is_verified,
       ci.id_record_id, ci.id_type, ci.id_number, ci.expiry_date,
       ct.tax_record_id, ct.tax_country, ct.tax_id, ct.tin_type,
       r.relationship_id, r.party_id_from, r.party_id_to,
       r.relationship_type, r.status AS rel_status
     FROM customer c
     LEFT JOIN customer_address        ca ON ca.customer_id = c.customer_id
     LEFT JOIN customer_contact        cc ON cc.customer_id = c.customer_id
     LEFT JOIN customer_identification ci ON ci.customer_id = c.customer_id
     LEFT JOIN customer_tax            ct ON ct.customer_id = c.customer_id
     LEFT JOIN relationship             r ON r.party_id_from = c.customer_id
                                          OR r.party_id_to   = c.customer_id
     WHERE c.customer_id IN (${placeholders})`,
    ids
  );
  return rows;
}

function assembleProfiles(rows) {
  const profileMap = new Map();

  for (const r of rows) {
    if (!profileMap.has(r.customer_id)) {
      profileMap.set(r.customer_id, {
        customer_id:      r.customer_id,
        external_ref:     r.external_ref,
        first_name:       r.first_name,
        last_name:        r.last_name,
        date_of_birth:    r.date_of_birth,
        gender:           r.gender,
        nationality:      r.nationality,
        preferred_locale: r.preferred_locale,
        status:           r.status,
        created_at:       r.created_at,
        updated_at:       r.updated_at,
        addresses:        [],
        contacts:         [],
        identifications:  [],
        tax_records:      [],
        relationships:    [],
        _seen: {
          addr: new Set(), contact: new Set(),
          ident: new Set(), tax: new Set(), rel: new Set(),
        },
      });
    }

    const p = profileMap.get(r.customer_id);

    if (r.address_id && !p._seen.addr.has(r.address_id)) {
      p._seen.addr.add(r.address_id);
      p.addresses.push({
        address_id:   r.address_id,
        address_type: r.address_type,
        line1:        r.line1,
        line2:        r.line2,
        city:         r.city,
        state:        r.state,
        postcode:     r.postcode,
        country:      r.addr_country,
        is_primary:   !!r.addr_primary,
      });
    }
    if (r.contact_id && !p._seen.contact.has(r.contact_id)) {
      p._seen.contact.add(r.contact_id);
      p.contacts.push({
        contact_id:    r.contact_id,
        contact_type:  r.contact_type,
        contact_value: r.contact_value,
        is_primary:    !!r.contact_primary,
        is_verified:   !!r.is_verified,
      });
    }
    if (r.id_record_id && !p._seen.ident.has(r.id_record_id)) {
      p._seen.ident.add(r.id_record_id);
      p.identifications.push({
        id_type:     r.id_type,
        id_number:   r.id_number,
        expiry_date: r.expiry_date,
      });
    }
    if (r.tax_record_id && !p._seen.tax.has(r.tax_record_id)) {
      p._seen.tax.add(r.tax_record_id);
      p.tax_records.push({
        tax_country: r.tax_country,
        tax_id:      r.tax_id,
        tin_type:    r.tin_type,
      });
    }
    if (r.relationship_id && !p._seen.rel.has(r.relationship_id)) {
      p._seen.rel.add(r.relationship_id);
      p.relationships.push({
        relationship_id:   r.relationship_id,
        party_id_from:     r.party_id_from,
        party_id_to:       r.party_id_to,
        relationship_type: r.relationship_type,
        rel_status:        r.rel_status,
      });
    }
  }

  // Strip internal tracking sets before writing to Mongo
  return Array.from(profileMap.values()).map(({ _seen, ...doc }) => doc);
}

async function upsertBatch(col, profiles) {
  if (!profiles.length) return;
  const ops = profiles.map((p) => ({
    replaceOne: {
      filter:      { customer_id: p.customer_id },
      replacement: p,
      upsert:      true,
    },
  }));
  const result = await col.bulkWrite(ops, { ordered: false });
  return result.upsertedCount + result.modifiedCount + result.matchedCount;
}

async function run() {
  const pool = getRdsPool();
  const db   = await getMongoDb();
  const col  = db.collection("customer_profile");

  console.log(B("\nInitial Sync — MySQL → MongoDB customer_profile\n"));

  const allIds = await fetchAllCustomerIds(pool);
  const total  = allIds.length;
  console.log(C(`Found ${total} customers in MySQL. Syncing in batches of ${BATCH_SIZE}…\n`));

  let synced = 0;
  const startMs = Date.now();

  for (let i = 0; i < total; i += BATCH_SIZE) {
    const batchIds = allIds.slice(i, i + BATCH_SIZE);
    const rows     = await fetchProfileBatch(pool, batchIds);
    const profiles = assembleProfiles(rows);
    await upsertBatch(col, profiles);
    synced += profiles.length;
    process.stdout.write(`\r  ${synced}/${total} upserted…`);
  }

  const elapsed = ((Date.now() - startMs) / 1000).toFixed(1);
  console.log(`\r  ${G(`✓ ${synced}/${total} documents upserted in ${elapsed}s`)}`);
  console.log(`\n${B("Done.")} Run ${C("npm run reconcile")} to verify.\n`);

  await closeMongoDb();
  await pool.end();
}

run().catch((e) => { console.error(e); process.exit(1); });

/**
 * Customer Profile Merger
 *
 * Watches MongoDB change streams on the 6 staging collections written by the
 * MongoDB Kafka sink connector and merges every change into a single
 * customer_profile document.
 *
 * Architecture:
 *   Kafka → MongoDB Kafka sink → staging collections
 *                                       ↓  (this process)
 *                                 customer_profile   ← merged document
 *
 * Usage:
 *   node src/merger/merger.js
 *   npm run merger
 */

import { getMongoDb, closeMongoDb } from "../db/mongo.js";

const STAGING_DB = process.env.MONGODB_DB || "offload_poc";

async function run() {
  const db      = await getMongoDb();
  const profile = db.collection("customer_profile");

  console.log("Merger started — watching staging collections for changes...");

  // Open a change stream on every staging collection concurrently
  await Promise.all([
    watchCustomer(db, profile),
    watchAddresses(db, profile),
    watchContacts(db, profile),
    watchIdentifications(db, profile),
    watchTax(db, profile),
    watchRelationships(db, profile),
  ]);
}

// ─── helpers ──────────────────────────────────────────────────────────────────

function openStream(db, collectionName) {
  return db.collection(collectionName).watch(
    [{ $match: { operationType: { $in: ["insert", "update", "replace", "delete"] } } }],
    { fullDocument: "updateLookup" }
  );
}

function logChange(collection, operationType, id) {
  console.log(`[${collection}] ${operationType} → customer_profile _id: ${id}`);
}

// ─── customer (root document) ─────────────────────────────────────────────────

async function watchCustomer(db, profile) {
  const stream = openStream(db, "customer");
  for await (const change of stream) {
    const doc = change.fullDocument;
    if (!doc) continue;  // delete — keep profile but mark deleted

    const customerId = doc.customer_id;
    await profile.updateOne(
      { customer_id: customerId },
      {
        $set: {
          customer_id:      doc.customer_id,
          external_ref:     doc.external_ref,
          first_name:       doc.first_name,
          last_name:        doc.last_name,
          date_of_birth:    doc.date_of_birth,
          gender:           doc.gender,
          nationality:      doc.nationality,
          preferred_locale: doc.preferred_locale,
          status:           doc.status,
          updated_at:       doc.updated_at,
        },
        $setOnInsert: { created_at: doc.created_at },
      },
      { upsert: true }
    );
    logChange("customer", change.operationType, customerId);
  }
}

// ─── customer_address ─────────────────────────────────────────────────────────

async function watchAddresses(db, profile) {
  const stream = openStream(db, "customer_address");
  for await (const change of stream) {
    const doc        = change.fullDocument;
    const customerId = doc?.customer_id;
    if (!customerId) continue;

    // Pull the old version then push the new one (idempotent replace)
    const addressId = doc.address_id;
    await profile.updateOne(
      { customer_id: customerId },
      { $pull: { addresses: { address_id: addressId } } }
    );
    if (change.operationType !== "delete") {
      await profile.updateOne(
        { customer_id: customerId },
        {
          $push: {
            addresses: {
              address_id:   doc.address_id,
              address_type: doc.address_type,
              line1:        doc.line1,
              line2:        doc.line2,
              city:         doc.city,
              state:        doc.state,
              postcode:     doc.postcode,
              country:      doc.country,
              is_primary:   doc.is_primary === 1 || doc.is_primary === true,
            },
          },
        }
      );
    }
    logChange("customer_address", change.operationType, customerId);
  }
}

// ─── customer_contact ─────────────────────────────────────────────────────────

async function watchContacts(db, profile) {
  const stream = openStream(db, "customer_contact");
  for await (const change of stream) {
    const doc        = change.fullDocument;
    const customerId = doc?.customer_id;
    if (!customerId) continue;

    const contactId = doc.contact_id;
    await profile.updateOne(
      { customer_id: customerId },
      { $pull: { contacts: { contact_id: contactId } } }
    );
    if (change.operationType !== "delete") {
      await profile.updateOne(
        { customer_id: customerId },
        {
          $push: {
            contacts: {
              contact_id:    doc.contact_id,
              contact_type:  doc.contact_type,
              contact_value: doc.contact_value,
              is_primary:    doc.is_primary === 1 || doc.is_primary === true,
              is_verified:   doc.is_verified === 1 || doc.is_verified === true,
            },
          },
        }
      );
    }
    logChange("customer_contact", change.operationType, customerId);
  }
}

// ─── customer_identification ──────────────────────────────────────────────────

async function watchIdentifications(db, profile) {
  const stream = openStream(db, "customer_identification");
  for await (const change of stream) {
    const doc        = change.fullDocument;
    const customerId = doc?.customer_id;
    if (!customerId) continue;

    const idRecordId = doc.id_record_id;
    await profile.updateOne(
      { customer_id: customerId },
      { $pull: { identifications: { id_record_id: idRecordId } } }
    );
    if (change.operationType !== "delete") {
      await profile.updateOne(
        { customer_id: customerId },
        {
          $push: {
            identifications: {
              id_record_id:      doc.id_record_id,
              id_type:           doc.id_type,
              id_number:         doc.id_number,
              issuing_authority: doc.issuing_authority,
              issue_date:        doc.issue_date,
              expiry_date:       doc.expiry_date,
            },
          },
        }
      );
    }
    logChange("customer_identification", change.operationType, customerId);
  }
}

// ─── customer_tax ─────────────────────────────────────────────────────────────

async function watchTax(db, profile) {
  const stream = openStream(db, "customer_tax");
  for await (const change of stream) {
    const doc        = change.fullDocument;
    const customerId = doc?.customer_id;
    if (!customerId) continue;

    const taxRecordId = doc.tax_record_id;
    await profile.updateOne(
      { customer_id: customerId },
      { $pull: { tax_records: { tax_record_id: taxRecordId } } }
    );
    if (change.operationType !== "delete") {
      await profile.updateOne(
        { customer_id: customerId },
        {
          $push: {
            tax_records: {
              tax_record_id: doc.tax_record_id,
              tax_country:   doc.tax_country,
              tax_id:        doc.tax_id,
              tin_type:      doc.tin_type,
            },
          },
        }
      );
    }
    logChange("customer_tax", change.operationType, customerId);
  }
}

// ─── relationship ─────────────────────────────────────────────────────────────

async function watchRelationships(db, profile) {
  const stream = openStream(db, "relationship");
  for await (const change of stream) {
    const doc        = change.fullDocument;
    const customerId = doc?.party_id_from;
    if (!customerId) continue;

    const relId = doc.relationship_id;
    await profile.updateOne(
      { customer_id: customerId },
      { $pull: { relationships: { relationship_id: relId } } }
    );
    if (change.operationType !== "delete") {
      await profile.updateOne(
        { customer_id: customerId },
        {
          $push: {
            relationships: {
              relationship_id:   doc.relationship_id,
              party_id_to:       doc.party_id_to,
              relationship_type: doc.relationship_type,
              valid_from:        doc.valid_from,
              valid_to:          doc.valid_to,
              status:            doc.status,
            },
          },
        }
      );
    }
    logChange("relationship", change.operationType, customerId);
  }
}

// ─── entrypoint ───────────────────────────────────────────────────────────────

run().catch((err) => {
  console.error("Merger fatal error:", err);
  closeMongoDb().finally(() => process.exit(1));
});

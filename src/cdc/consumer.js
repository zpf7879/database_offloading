import { Kafka } from "kafkajs";
import { getMongoDb } from "../db/mongo.js";
import "dotenv/config";

const kafka = new Kafka({
  clientId: "mongo-offload-consumer",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
});

const TOPICS = [
  "poc.offload_poc.customer",
  "poc.offload_poc.customer_address",
  "poc.offload_poc.customer_contact",
  "poc.offload_poc.customer_identification",
  "poc.offload_poc.customer_tax",
  "poc.offload_poc.relationship",
];

const consumer = kafka.consumer({ groupId: "mongo-offload-group" });

export async function startConsumer() {
  const db = await getMongoDb();
  const rawCol     = db.collection("cdc_raw_events");
  const profileCol = db.collection("customer_profile");

  await consumer.connect();
  await consumer.subscribe({ topics: TOPICS, fromBeginning: true });

  console.log("CDC consumer started, subscribed to:", TOPICS);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) return; // tombstone — skip (delete handled via __deleted flag)

      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch {
        console.error("Unparseable message on", topic, message.value.toString());
        return;
      }

      const ingestedAt = new Date();

      // Always persist raw event for audit / reconciliation
      await rawCol.insertOne({
        topic,
        partition,
        offset: message.offset,
        event,
        ingested_at: ingestedAt,
      });

      const op    = event.__op;           // c=create, u=update, d=delete, r=snapshot
      const table = topic.split(".").pop(); // e.g. "customer"

      try {
        await upsertProfile(profileCol, table, op, event);
      } catch (err) {
        console.error(`Failed to upsert profile for table=${table} op=${op}:`, err.message);
      }
    },
  });
}

// ---------------------------------------------------------------------------
// Idempotent merge into customer_profile document
// ---------------------------------------------------------------------------

async function upsertProfile(col, table, op, event) {
  if (table === "customer") {
    if (op === "d" || event.__deleted === "true") {
      await col.updateOne(
        { customer_id: event.customer_id },
        { $set: { status: "DELETED", updated_at: new Date(event.updated_at) } }
      );
      return;
    }
    await col.updateOne(
      { customer_id: event.customer_id },
      {
        $set: {
          customer_id:      event.customer_id,
          external_ref:     event.external_ref,
          first_name:       event.first_name,
          last_name:        event.last_name,
          date_of_birth:    event.date_of_birth,
          gender:           event.gender,
          nationality:      event.nationality,
          preferred_locale: event.preferred_locale,
          status:           event.status,
          updated_at:       new Date(event.updated_at),
        },
        $setOnInsert: { created_at: new Date(event.created_at) },
      },
      { upsert: true }
    );
    return;
  }

  if (table === "customer_address") {
    if (op === "d" || event.__deleted === "true") {
      await col.updateOne(
        { customer_id: event.customer_id },
        { $pull: { addresses: { address_id: event.address_id } } }
      );
      return;
    }
    await col.updateOne(
      { customer_id: event.customer_id },
      {
        $set: { updated_at: new Date(event.updated_at) },
        $pull: { addresses: { address_id: event.address_id } },
      }
    );
    await col.updateOne(
      { customer_id: event.customer_id },
      {
        $push: {
          addresses: {
            address_id:   event.address_id,
            address_type: event.address_type,
            line1:        event.line1,
            line2:        event.line2,
            city:         event.city,
            state:        event.state,
            postcode:     event.postcode,
            country:      event.country,
            is_primary:   event.is_primary === 1,
          },
        },
      }
    );
    return;
  }

  if (table === "customer_contact") {
    if (op === "d" || event.__deleted === "true") {
      await col.updateOne(
        { customer_id: event.customer_id },
        { $pull: { contacts: { contact_id: event.contact_id } } }
      );
      return;
    }
    await col.updateOne(
      { customer_id: event.customer_id },
      { $pull: { contacts: { contact_id: event.contact_id } } }
    );
    await col.updateOne(
      { customer_id: event.customer_id },
      {
        $push: {
          contacts: {
            contact_id:    event.contact_id,
            contact_type:  event.contact_type,
            contact_value: event.contact_value,
            is_primary:    event.is_primary === 1,
            is_verified:   event.is_verified === 1,
          },
        },
      }
    );
    return;
  }

  if (table === "customer_identification") {
    if (op === "d" || event.__deleted === "true") {
      await col.updateOne(
        { customer_id: event.customer_id },
        { $pull: { identifications: { id_record_id: event.id_record_id } } }
      );
      return;
    }
    await col.updateOne(
      { customer_id: event.customer_id },
      { $pull: { identifications: { id_record_id: event.id_record_id } } }
    );
    await col.updateOne(
      { customer_id: event.customer_id },
      {
        $push: {
          identifications: {
            id_record_id:      event.id_record_id,
            id_type:           event.id_type,
            id_number:         event.id_number,
            issuing_authority: event.issuing_authority,
            issue_date:        event.issue_date,
            expiry_date:       event.expiry_date,
          },
        },
      }
    );
    return;
  }

  if (table === "customer_tax") {
    if (op === "d" || event.__deleted === "true") {
      await col.updateOne(
        { customer_id: event.customer_id },
        { $pull: { tax_records: { tax_record_id: event.tax_record_id } } }
      );
      return;
    }
    await col.updateOne(
      { customer_id: event.customer_id },
      { $pull: { tax_records: { tax_record_id: event.tax_record_id } } }
    );
    await col.updateOne(
      { customer_id: event.customer_id },
      {
        $push: {
          tax_records: {
            tax_record_id: event.tax_record_id,
            tax_country:   event.tax_country,
            tax_id:        event.tax_id,
            tin_type:      event.tin_type,
          },
        },
      }
    );
    return;
  }

  if (table === "relationship") {
    if (op === "d" || event.__deleted === "true") {
      await col.updateOne(
        { customer_id: event.party_id_from },
        { $pull: { relationships: { relationship_id: event.relationship_id } } }
      );
      return;
    }
    const rel = {
      relationship_id:   event.relationship_id,
      party_id_to:       event.party_id_to,
      relationship_type: event.relationship_type,
      valid_from:        event.valid_from,
      valid_to:          event.valid_to,
      status:            event.status,
    };
    await col.updateOne(
      { customer_id: event.party_id_from },
      { $pull: { relationships: { relationship_id: event.relationship_id } } }
    );
    await col.updateOne(
      { customer_id: event.party_id_from },
      { $push: { relationships: rel } }
    );
  }
}

// Allow running standalone: node src/cdc/consumer.js
startConsumer().catch((err) => {
  console.error("Consumer fatal error:", err);
  process.exit(1);
});

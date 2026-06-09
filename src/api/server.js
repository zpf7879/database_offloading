import express from "express";
import "dotenv/config";
import { getMongoDb } from "../db/mongo.js";
import { getCustomerRelationalFull } from "../db/rds.js";

const app  = express();
const PORT = process.env.API_PORT || 3000;

app.use(express.json());

// ---------------------------------------------------------------------------
// GET /customer/:id
// Returns the customer profile from MongoDB (offload path)
// ---------------------------------------------------------------------------
app.get("/customer/:id", async (req, res) => {
  const t0 = Date.now();
  try {
    const db  = await getMongoDb();
    const doc = await db.collection("customer_profile").findOne(
      { customer_id: req.params.id },
      { projection: { _id: 0 } }
    );
    if (!doc) return res.status(404).json({ error: "not found" });
    res.json({ source: "mongodb", latency_ms: Date.now() - t0, data: doc });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /customer/:id/baseline
// Returns the same profile assembled from relational joins (baseline path)
// ---------------------------------------------------------------------------
app.get("/customer/:id/baseline", async (req, res) => {
  const t0 = Date.now();
  try {
    const rows = await getCustomerRelationalFull(req.params.id);
    if (!rows.length) return res.status(404).json({ error: "not found" });

    // Assemble a comparable shape from flat join rows
    const first = rows[0];
    const profile = {
      customer_id:  first.customer_id,
      external_ref: first.external_ref,
      first_name:   first.first_name,
      last_name:    first.last_name,
      status:       first.status,
      updated_at:   first.updated_at,
      addresses:    [],
      contacts:     [],
      identifications: [],
      tax_records:  [],
      relationships: [],
    };

    const seen = { addr: new Set(), contact: new Set(), ident: new Set(), tax: new Set(), rel: new Set() };
    for (const r of rows) {
      if (r.address_id && !seen.addr.has(r.address_id)) {
        seen.addr.add(r.address_id);
        profile.addresses.push({ address_id: r.address_id, address_type: r.address_type, line1: r.line1, city: r.city, state: r.state, postcode: r.postcode });
      }
      if (r.contact_id && !seen.contact.has(r.contact_id)) {
        seen.contact.add(r.contact_id);
        profile.contacts.push({ contact_id: r.contact_id, contact_type: r.contact_type, contact_value: r.contact_value });
      }
      if (r.id_type && !seen.ident.has(r.id_type)) {
        seen.ident.add(r.id_type);
        profile.identifications.push({ id_type: r.id_type, id_number: r.id_number });
      }
      if (r.tax_country && !seen.tax.has(r.tax_country)) {
        seen.tax.add(r.tax_country);
        profile.tax_records.push({ tax_country: r.tax_country, tax_id: r.tax_id });
      }
      if (r.relationship_id && !seen.rel.has(r.relationship_id)) {
        seen.rel.add(r.relationship_id);
        profile.relationships.push({ relationship_id: r.relationship_id, relationship_type: r.relationship_type, rel_status: r.rel_status });
      }
    }

    res.json({ source: "mysql", latency_ms: Date.now() - t0, rows_scanned: rows.length, data: profile });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /galaxy/customer/:id
// Galaxy consumer path — returns the slim summary shape that Galaxy's
// formerly-batch inquiry screen requires. Reads from the same MongoDB gold
// layer but projects only the fields Galaxy needs, demonstrating how a
// second downstream consumer can read the same offloaded data without
// touching the source system.
// ---------------------------------------------------------------------------
app.get("/galaxy/customer/:id", async (req, res) => {
  const t0 = Date.now();
  try {
    const db  = await getMongoDb();
    const doc = await db.collection("customer_profile").findOne(
      { customer_id: req.params.id },
      {
        projection: {
          _id: 0,
          customer_id: 1,
          external_ref: 1,
          first_name: 1,
          last_name: 1,
          status: 1,
          // Primary contact only
          contacts: { $slice: 1 },
          // Primary address only
          addresses: { $slice: 1 },
          // Active relationships count — Galaxy shows a badge, not full list
          relationships: 1,
        },
      }
    );
    if (!doc) return res.status(404).json({ error: "not found" });

    // Shape the response to match what Galaxy's inquiry screen consumes
    const primaryContact = doc.contacts?.[0] ?? null;
    const primaryAddress = doc.addresses?.[0] ?? null;

    res.json({
      source: "mongodb_galaxy",
      latency_ms: Date.now() - t0,
      data: {
        customer_id:        doc.customer_id,
        external_ref:       doc.external_ref,
        display_name:       `${doc.first_name} ${doc.last_name}`,
        status:             doc.status,
        primary_contact:    primaryContact
          ? { type: primaryContact.contact_type, value: primaryContact.contact_value }
          : null,
        primary_address:    primaryAddress
          ? { city: primaryAddress.city, state: primaryAddress.state, postcode: primaryAddress.postcode }
          : null,
        relationship_count: doc.relationships?.length ?? 0,
      },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------
app.get("/health", (_req, res) => res.json({ status: "ok" }));

app.listen(PORT, () => console.log(`API listening on http://localhost:${PORT}`));

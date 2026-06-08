package com.poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams application — Customer Profile Aggregator
 *
 * Reads from 6 Debezium CDC topics, re-keys child tables by customer_id,
 * aggregates all fields into a single merged JSON document per customer,
 * and writes to poc.customer_profile topic.
 *
 * Pipeline:
 *   6 Kafka topics → Kafka Streams (this app) → poc.customer_profile topic
 *                                                        ↓
 *                                          MongoDB Kafka sink connector
 *                                                        ↓
 *                                              customer_profile collection
 */
public class CustomerProfileStreams {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Input topics (Debezium with ExtractNewRecordState / unwrap transform)
    static final String TOPIC_CUSTOMER        = "poc.offload_poc.customer";
    static final String TOPIC_ADDRESS         = "poc.offload_poc.customer_address";
    static final String TOPIC_CONTACT         = "poc.offload_poc.customer_contact";
    static final String TOPIC_IDENTIFICATION  = "poc.offload_poc.customer_identification";
    static final String TOPIC_TAX             = "poc.offload_poc.customer_tax";
    static final String TOPIC_RELATIONSHIP    = "poc.offload_poc.relationship";

    // Output topic consumed by MongoDB Kafka sink connector
    static final String TOPIC_PROFILE         = "poc.customer_profile";

    public static void main(String[] args) {
        Properties props = buildProperties();
        Topology topology = buildTopology();

        System.out.println("Starting Customer Profile Streams...");

        // Verify Kafka connectivity before starting
        String brokers = props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        System.out.println("Bootstrap servers: " + brokers);
        for (String broker : brokers.split(",")) {
            String[] parts = broker.trim().split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), 5000);
                System.out.println("[CONNECTIVITY] OK — reached " + broker);
            } catch (Exception e) {
                System.err.println("[CONNECTIVITY] FAILED — cannot reach " + broker + ": " + e.getMessage());
            }
        }

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.exit(0);
    }

    // ── topology ──────────────────────────────────────────────────────────────

    static Topology buildTopology() {
        final StreamsBuilder builder   = new StreamsBuilder();
        final Serde<String>  strSerde  = Serdes.String();
        final JsonSerde      jsonSerde = new JsonSerde();

        Consumed<String, JsonNode> consumed = Consumed.with(strSerde, jsonSerde);

        // ── customer (already keyed by customer_id) ──────────────────────────
        KTable<String, JsonNode> customerTable = builder
            .stream(TOPIC_CUSTOMER, consumed)
            .peek((k, v) -> System.out.println("[DEBUG] CUSTOMER RAW key=" + k
                + " value=" + (v != null ? v.toString().substring(0, Math.min(120, v.toString().length())) : "null")))
            .selectKey((k, v) -> extractCustomerId(v, "customer_id"))
            .peek((k, v) -> System.out.println("[DEBUG] CUSTOMER REKEYED key=" + k))
            .toTable(Materialized.with(strSerde, jsonSerde));

        // ── child tables: re-key by customer_id, aggregate into maps ─────────
        KTable<String, Map<String, JsonNode>> addressTable =
            buildChildTable(builder, consumed, strSerde, TOPIC_ADDRESS, "address_id", "customer_id");

        KTable<String, Map<String, JsonNode>> contactTable =
            buildChildTable(builder, consumed, strSerde, TOPIC_CONTACT, "contact_id", "customer_id");

        KTable<String, Map<String, JsonNode>> identTable =
            buildChildTable(builder, consumed, strSerde, TOPIC_IDENTIFICATION, "id_record_id", "customer_id");

        KTable<String, Map<String, JsonNode>> taxTable =
            buildChildTable(builder, consumed, strSerde, TOPIC_TAX, "tax_record_id", "customer_id");

        KTable<String, Map<String, JsonNode>> relTable =
            buildChildTable(builder, consumed, strSerde, TOPIC_RELATIONSHIP, "relationship_id", "party_id_from");

        // ── join all KTables into one merged profile ───────────────────────
        KTable<String, JsonNode> profileTable = customerTable
            .leftJoin(addressTable,       (cust, addrs)   -> merge(cust, "addresses",      addrs))
            .leftJoin(contactTable,       (prof, contacts) -> merge(prof, "contacts",       contacts))
            .leftJoin(identTable,         (prof, idents)   -> merge(prof, "identifications",idents))
            .leftJoin(taxTable,           (prof, taxes)    -> merge(prof, "tax_records",    taxes))
            .leftJoin(relTable,           (prof, rels)     -> merge(prof, "relationships",  rels));

        // ── write to output topic ─────────────────────────────────────────────
        profileTable.toStream()
            .peek((k, v) -> System.out.println("[DEBUG] PROFILE EMITTED key=" + k
                + " fields=" + (v != null ? v.fieldNames().toString() : "null")))
            .to(TOPIC_PROFILE, Produced.with(strSerde, jsonSerde));

        return builder.build();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Builds a KTable<customerId, Map<pkValue, record>> for a child topic.
     * Re-keys by customer_id and aggregates records by their own PK so that
     * updates replace rather than append.
     */
    private static KTable<String, Map<String, JsonNode>> buildChildTable(
            StreamsBuilder builder,
            Consumed<String, JsonNode> consumed,
            Serde<String> strSerde,
            String topic,
            String pkField,
            String fkField) {

        MapSerde mapSerde = new MapSerde();

        return builder
            .stream(topic, consumed)
            // re-key by customer_id (or party_id_from for relationship)
            .selectKey((k, v) -> extractCustomerId(v, fkField))
            // group all child records for the same customer
            .groupByKey(Grouped.with(strSerde, new JsonSerde()))
            // aggregate: Map<pkValue, record> — upsert semantics
            .aggregate(
                HashMap::new,
                (customerId, record, map) -> {
                    if (record == null || isDeleted(record)) {
                        // tombstone or __deleted=true: remove from map
                        String pk = extractCustomerId(record, pkField);
                        if (pk != null) map.remove(pk);
                    } else {
                        String pk = extractCustomerId(record, pkField);
                        if (pk != null) map.put(pk, record);
                    }
                    return map;
                },
                Materialized.<String, Map<String, JsonNode>, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>
                    as("store-" + topic.replace(".", "-"))
                    .withKeySerde(strSerde)
                    .withValueSerde(mapSerde)
            );
    }

    /**
     * Merges a child map into the parent profile document as an array.
     * Unwraps schema+payload envelope from both parent and children if present.
     */
    private static JsonNode merge(JsonNode profile, String arrayField, Map<String, JsonNode> children) {
        if (profile == null) return null;
        // Unwrap parent if schema-wrapped
        JsonNode base = profile.has("payload") ? profile.get("payload") : profile;
        ObjectNode out = base.deepCopy();
        ArrayNode arr = MAPPER.createArrayNode();
        if (children != null) {
            for (JsonNode child : children.values()) {
                // Unwrap child if schema-wrapped
                arr.add(child.has("payload") ? child.get("payload") : child);
            }
        }
        out.set(arrayField, arr);
        return out;
    }

    private static String extractCustomerId(JsonNode node, String field) {
        if (node == null) return null;
        // Unwrap schema+payload envelope if present (schemas.enable=true format)
        // e.g. { "schema": {...}, "payload": { "customer_id": "..." } }
        if (node.has("payload")) node = node.get("payload");
        JsonNode val = node.get(field);
        return (val == null || val.isNull()) ? null : val.asText();
    }

    private static boolean isDeleted(JsonNode node) {
        if (node == null) return true;
        if (node.has("payload")) node = node.get("payload");
        JsonNode del = node.get("__deleted");
        return del != null && "true".equalsIgnoreCase(del.asText());
    }

    // ── Kafka Streams config ──────────────────────────────────────────────────

    private static Properties buildProperties() {
        String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "localhost:9092");
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,    "customer-profile-aggregator-v2");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
        // start from the beginning so initial snapshot data is processed
        p.put("auto.offset.reset", "earliest");
        return p;
    }
}

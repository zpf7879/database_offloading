package com.poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka Serde for Jackson JsonNode — used for all Kafka Streams value operations.
 */
public class JsonSerde implements Serde<JsonNode> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Serializer<JsonNode> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize JsonNode", e);
            }
        };
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readTree(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize JsonNode", e);
            }
        };
    }
}

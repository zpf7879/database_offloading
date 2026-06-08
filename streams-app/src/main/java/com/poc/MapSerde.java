package com.poc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Serde for Map<String, JsonNode> — used for child-table aggregation state stores.
 */
public class MapSerde implements Serde<Map<String, JsonNode>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<HashMap<String, JsonNode>> TYPE_REF =
        new TypeReference<>() {};

    @Override
    public Serializer<Map<String, JsonNode>> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize Map<String, JsonNode>", e);
            }
        };
    }

    @Override
    public Deserializer<Map<String, JsonNode>> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, TYPE_REF);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize Map<String, JsonNode>", e);
            }
        };
    }
}

package com.qinematos.core.arrow;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ArrowContextSerializer.
 *
 * Tests the "Vulgate" component of the RVP framework - Apache Arrow
 * serialization for cross-language, zero-copy data transfer.
 */
@QuarkusTest
class ArrowContextSerializerTest {

    @Inject
    ArrowContextSerializer serializer;

    @Test
    void testStandardSchemasAreRegistered() {
        // The serializer should register standard schemas on initialization
        assertTrue(serializer.getSchema("agent.context").isPresent(),
                "agent.context schema should be registered");
        assertTrue(serializer.getSchema("lineage.trace").isPresent(),
                "lineage.trace schema should be registered");
        assertTrue(serializer.getSchema("finance.market_data").isPresent(),
                "finance.market_data schema should be registered");
        assertTrue(serializer.getSchema("generic.keyvalue").isPresent(),
                "generic.keyvalue schema should be registered");
    }

    @Test
    void testAgentContextSchemaHasExpectedFields() {
        Schema schema = serializer.getSchema("agent.context").orElseThrow();

        List<String> fieldNames = schema.getFields().stream()
                .map(f -> f.getName())
                .toList();

        assertTrue(fieldNames.contains("agent_id"), "Should have agent_id field");
        assertTrue(fieldNames.contains("context_key"), "Should have context_key field");
        assertTrue(fieldNames.contains("context_value"), "Should have context_value field");
        assertTrue(fieldNames.contains("timestamp_nanos"), "Should have timestamp_nanos field");
        assertTrue(fieldNames.contains("version"), "Should have version field");
        assertTrue(fieldNames.contains("lineage_id"), "Should have lineage_id field");
    }

    @Test
    void testSerializeAndDeserializeRoundTrip() {
        // Create test records
        List<Map<String, Object>> records = List.of(
                Map.of(
                        "agent_id", "test-agent-1",
                        "context_key", "key1",
                        "context_value", "value1",
                        "data_type", "string",
                        "timestamp_nanos", 1234567890000000L,
                        "version", 1L,
                        "lineage_id", "lin_123",
                        "source_system", "test"),
                Map.of(
                        "agent_id", "test-agent-2",
                        "context_key", "key2",
                        "context_value", "value2",
                        "data_type", "string",
                        "timestamp_nanos", 1234567891000000L,
                        "version", 2L,
                        "lineage_id", "lin_124",
                        "source_system", "test"));

        // Serialize to Arrow format
        byte[] arrowBytes = serializer.serializeToArrow("agent.context", records);

        assertNotNull(arrowBytes, "Arrow bytes should not be null");
        assertTrue(arrowBytes.length > 0, "Arrow bytes should have content");

        // Deserialize back
        List<Map<String, Object>> deserialized = serializer.deserializeFromArrow(arrowBytes);

        assertEquals(2, deserialized.size(), "Should deserialize 2 records");

        // Verify first record
        Map<String, Object> first = deserialized.get(0);
        assertEquals("test-agent-1", first.get("agent_id"));
        assertEquals("key1", first.get("context_key"));
        assertEquals("value1", first.get("context_value"));
    }

    @Test
    void testSerializeMarketData() {
        List<Map<String, Object>> records = List.of(
                Map.of(
                        "symbol", "AAPL",
                        "price", 185.50,
                        "volume", 1000000L,
                        "bid", 185.45,
                        "ask", 185.55,
                        "timestamp_nanos", System.nanoTime(),
                        "exchange", "NASDAQ",
                        "trade_id", "TRD001"));

        byte[] arrowBytes = serializer.serializeToArrow("finance.market_data", records);

        assertNotNull(arrowBytes);
        assertTrue(arrowBytes.length > 0);

        List<Map<String, Object>> deserialized = serializer.deserializeFromArrow(arrowBytes);
        assertEquals(1, deserialized.size());
        assertEquals("AAPL", deserialized.get(0).get("symbol"));
    }

    @Test
    void testFallbackToGenericSchema() {
        // Using a non-registered topic should fall back to generic.keyvalue
        List<Map<String, Object>> records = List.of(
                Map.of(
                        "key", "test-key",
                        "value", "test-value".getBytes(),
                        "value_type", "bytes",
                        "timestamp_nanos", System.nanoTime()));

        byte[] arrowBytes = serializer.serializeToArrow("unknown.topic", records);

        assertNotNull(arrowBytes);
        assertTrue(arrowBytes.length > 0);
    }

    @Test
    void testSchemaSerializationRoundTrip() {
        Schema original = serializer.getSchema("agent.context").orElseThrow();

        // Serialize schema
        byte[] schemaBytes = serializer.serializeSchema(original);
        assertNotNull(schemaBytes);
        assertTrue(schemaBytes.length > 0);

        // Deserialize schema
        Schema deserialized = serializer.deserializeSchema(schemaBytes);
        assertNotNull(deserialized);
        assertEquals(original.getFields().size(), deserialized.getFields().size());
    }

    @Test
    void testGetAllSchemas() {
        Map<String, Schema> allSchemas = serializer.getAllSchemas();

        assertNotNull(allSchemas);
        assertTrue(allSchemas.size() >= 4, "Should have at least 4 standard schemas");
        assertTrue(allSchemas.containsKey("agent.context"));
        assertTrue(allSchemas.containsKey("lineage.trace"));
    }
}

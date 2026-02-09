package com.qinematos.core.arrow;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jboss.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;

@ApplicationScoped
@Startup
@RegisterForReflection(targets = {
        Schema.class,
        Field.class,
        FieldType.class,
        ArrowType.Utf8.class,
        ArrowType.Int.class,
        ArrowType.FloatingPoint.class,
        ArrowType.Binary.class
})
public class ArrowContextSerializer {

    private static final Logger LOG = Logger.getLogger(ArrowContextSerializer.class);
    private BufferAllocator allocator;
    private final Map<String, Schema> schemaCache = new HashMap<>();

    @PostConstruct
    void initialize() {
        this.allocator = new RootAllocator(1024 * 1024 * 1024);
        registerStandardSchemas();
        LOG.info("ArrowContextSerializer initialized with 1GB allocator");
    }

    @PreDestroy
    void shutdown() {
        if (allocator != null) {
            allocator.close();
        }
        LOG.info("ArrowContextSerializer shutdown complete");
    }

    private void registerStandardSchemas() {
        Schema agentContextSchema = createAgentContextSchema();
        registerSchema("agent.context", agentContextSchema);
        Schema lineageSchema = createLineageSchema();
        registerSchema("lineage.trace", lineageSchema);
        registerSchema("finance.market_data", createMarketDataSchema());
        registerSchema("generic.keyvalue", createKeyValueSchema());
        registerSchema("session.knowledge_base", agentContextSchema);
        registerSchema("session.communication", agentContextSchema);
        registerSchema("session.external_provider", createKeyValueSchema());
        registerSchema("session.discovery", agentContextSchema);
        registerSchema("system.lineage", lineageSchema);
        registerSchema("system.agents", agentContextSchema);
        registerSchema("system.health", createKeyValueSchema());
    }

    private Schema createAgentContextSchema() {
        return new Schema(Arrays.asList(
                Field.nullable("agent_id", new ArrowType.Utf8()),
                Field.nullable("context_key", new ArrowType.Utf8()),
                Field.nullable("context_value", new ArrowType.Utf8()),
                Field.nullable("data_type", new ArrowType.Utf8()),
                Field.nullable("timestamp_nanos", new ArrowType.Int(64, true)),
                Field.nullable("version", new ArrowType.Int(64, true)),
                Field.nullable("lineage_id", new ArrowType.Utf8()),
                Field.nullable("source_system", new ArrowType.Utf8())));
    }

    private Schema createLineageSchema() {
        return new Schema(Arrays.asList(
                Field.nullable("trace_id", new ArrowType.Utf8()),
                Field.nullable("parent_trace_id", new ArrowType.Utf8()),
                Field.nullable("operation", new ArrowType.Utf8()),
                Field.nullable("agent_id", new ArrowType.Utf8()),
                Field.nullable("input_hash", new ArrowType.Utf8()),
                Field.nullable("output_hash", new ArrowType.Utf8()),
                Field.nullable("start_time_nanos", new ArrowType.Int(64, true)),
                Field.nullable("end_time_nanos", new ArrowType.Int(64, true)),
                Field.nullable("status", new ArrowType.Utf8()),
                Field.nullable("ebpf_correlation_id", new ArrowType.Utf8())));
    }

    private Schema createMarketDataSchema() {
        return new Schema(Arrays.asList(
                Field.nullable("symbol", new ArrowType.Utf8()),
                Field.nullable("price", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("volume", new ArrowType.Int(64, true)),
                Field.nullable("bid", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("ask", new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                Field.nullable("timestamp_nanos", new ArrowType.Int(64, true)),
                Field.nullable("exchange", new ArrowType.Utf8()),
                Field.nullable("trade_id", new ArrowType.Utf8())));
    }

    private Schema createKeyValueSchema() {
        return new Schema(Arrays.asList(
                Field.nullable("key", new ArrowType.Utf8()),
                Field.nullable("value", new ArrowType.LargeBinary()),
                Field.nullable("value_type", new ArrowType.Utf8()),
                Field.nullable("timestamp_nanos", new ArrowType.Int(64, true))));
    }

    public void registerSchema(String topic, Schema schema) {
        schemaCache.put(topic, schema);
        LOG.debugf("Registered schema for topic: %s", topic);
    }

    public Optional<Schema> getSchema(String topic) {
        return Optional.ofNullable(schemaCache.get(topic));
    }

    public byte[] serializeToArrow(String topic, List<Map<String, Object>> records) {
        Schema schema = schemaCache.get(topic);
        if (schema == null) {
            schema = schemaCache.get("generic.keyvalue");
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            root.allocateNew();
            populateVectors(root, records);
            root.setRowCount(records.size());
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            byte[] result = outputStream.toByteArray();
            LOG.debugf("Serialized %d records to %d bytes for topic: %s", records.size(), result.length, topic);
            return result;
        } catch (IOException e) {
            LOG.errorf("Failed to serialize Arrow payload: %s", e.getMessage());
            throw new RuntimeException("Arrow serialization failed", e);
        }
    }

    public List<Map<String, Object>> deserializeFromArrow(byte[] arrowBytes) {
        List<Map<String, Object>> records = new ArrayList<>();
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(arrowBytes);
                ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Schema schema = root.getSchema();
            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                for (int row = 0; row < rowCount; row++) {
                    Map<String, Object> record = new LinkedHashMap<>();
                    for (Field field : schema.getFields()) {
                        FieldVector vector = root.getVector(field.getName());
                        Object value = extractValue(vector, row);
                        record.put(field.getName(), value);
                    }
                    records.add(record);
                }
            }
            LOG.debugf("Deserialized %d records from Arrow payload", records.size());
            return records;
        } catch (IOException e) {
            LOG.errorf("Failed to deserialize Arrow payload: %s", e.getMessage());
            throw new RuntimeException("Arrow deserialization failed", e);
        }
    }

    public byte[] serializeSchema(Schema schema) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
            writer.start();
            writer.end();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Schema serialization failed", e);
        }
    }

    public Schema deserializeSchema(byte[] schemaBytes) {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(schemaBytes);
                ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {
            return reader.getVectorSchemaRoot().getSchema();
        } catch (IOException e) {
            throw new RuntimeException("Schema deserialization failed", e);
        }
    }

    private void populateVectors(VectorSchemaRoot root, List<Map<String, Object>> records) {
        Schema schema = root.getSchema();
        for (int row = 0; row < records.size(); row++) {
            Map<String, Object> record = records.get(row);
            for (Field field : schema.getFields()) {
                FieldVector vector = root.getVector(field.getName());
                Object value = record.get(field.getName());
                if (value == null) {
                    setNull(vector, row);
                } else {
                    setValue(vector, row, value);
                }
            }
        }
    }

    private void setValue(FieldVector vector, int row, Object value) {
        if (vector instanceof VarCharVector varCharVector) {
            varCharVector.setSafe(row, value.toString().getBytes());
        } else if (vector instanceof BigIntVector bigIntVector) {
            bigIntVector.setSafe(row, ((Number) value).longValue());
        } else if (vector instanceof Float8Vector float8Vector) {
            float8Vector.setSafe(row, ((Number) value).doubleValue());
        } else if (vector instanceof IntVector intVector) {
            intVector.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof LargeVarBinaryVector binaryVector) {
            if (value instanceof byte[] bytes) {
                binaryVector.setSafe(row, bytes);
            } else {
                binaryVector.setSafe(row, value.toString().getBytes());
            }
        }
    }

    private void setNull(FieldVector vector, int row) {
        if (vector instanceof VarCharVector v) { v.setNull(row); }
        else if (vector instanceof BigIntVector v) { v.setNull(row); }
        else if (vector instanceof Float8Vector v) { v.setNull(row); }
        else if (vector instanceof IntVector v) { v.setNull(row); }
        else if (vector instanceof LargeVarBinaryVector v) { v.setNull(row); }
    }

    private Object extractValue(FieldVector vector, int row) {
        if (vector.isNull(row)) return null;
        if (vector instanceof VarCharVector v) {
            byte[] bytes = v.get(row);
            return bytes != null ? new String(bytes) : null;
        } else if (vector instanceof BigIntVector v) { return v.get(row); }
        else if (vector instanceof Float8Vector v) { return v.get(row); }
        else if (vector instanceof IntVector v) { return v.get(row); }
        else if (vector instanceof LargeVarBinaryVector v) { return v.get(row); }
        return vector.getObject(row);
    }

    public BufferAllocator getAllocator() { return allocator; }

    public Map<String, Schema> getAllSchemas() { return Collections.unmodifiableMap(schemaCache); }
}

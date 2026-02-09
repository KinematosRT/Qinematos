package com.qinematos.api;

import com.qinematos.core.arrow.ArrowContextSerializer;
import com.qinematos.core.ipc.ZeroCopyIPCEngine;
import com.qinematos.core.topic.ContextTopicManager;
import com.qinematos.core.topic.ContextTopicManager.*;
import com.qinematos.persistence.XodusContextStore;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ContextOrchestratorResource {

    private static final Logger LOG = Logger.getLogger(ContextOrchestratorResource.class);

    @Inject
    ContextTopicManager topicManager;

    @Inject
    ZeroCopyIPCEngine ipcEngine;

    @Inject
    ArrowContextSerializer arrowSerializer;

    @Inject
    XodusContextStore contextStore;

    @GET
    @Path("/health")
    public Response health() {
        var ipcStats = ipcEngine.getStats();
        var storageStats = contextStore.getStats();
        var topics = topicManager.listTopics(null);
        return Response.ok(Map.of(
                "status", "healthy",
                "timestamp", Instant.now().toString(),
                "version", "1.0.0",
                "rvp_framework", Map.of(
                        "recent", "Live streaming via gRPC Watch API",
                        "vulgate", "Apache Arrow columnar format",
                        "plain", "Zero-Copy IPC via Memory-Mapped Files"),
                "stats", Map.of(
                        "topics", topics.size(),
                        "active_ipc_segments", ipcStats.activeSegments(),
                        "ipc_mapped_bytes", ipcStats.totalMappedBytes(),
                        "storage_topics", storageStats.topicCount(),
                        "storage_contexts", storageStats.contextCount()))).build();
    }

    @GET
    @Path("/info")
    public Response info() {
        return Response.ok(Map.of(
                "name", "Qinematos Data Base Plane",
                "description", "Deterministic Data Base Plane for Agentic AI",
                "version", "1.0.0-SNAPSHOT",
                "rvp_framework", Map.of(
                        "R_ecent", "High-velocity, real-time inference using live streams",
                        "V_ulgate", "Standardized, auditable communication using MCP and Arrow",
                        "P_lain", "Zero-copy, zero-serialization overhead using FFM and shared memory"),
                "endpoints", Map.of("rest", "/api/v1", "grpc", "localhost:9000", "mcp", "/mcp"),
                "technologies", Map.of(
                        "runtime", "Quarkus + GraalVM",
                        "data_format", "Apache Arrow",
                        "persistence", "JetBrains Xodus",
                        "ipc", "Java 25 FFM + Memory-Mapped Files"))).build();
    }

    @GET
    @Path("/topics")
    public Response listTopics(@QueryParam("pattern") String pattern) {
        var topics = topicManager.listTopics(pattern);
        return Response.ok(Map.of("topics", topics.stream().map(this::topicToMap).toList(),
                "count", topics.size())).build();
    }

    @POST
    @Path("/topics")
    public Response createTopic(CreateTopicRequest request) {
        LOG.infof("Creating topic: %s", request.name());
        try {
            var schema = request.schemaType() != null
                    ? arrowSerializer.getSchema(request.schemaType()).orElse(null) : null;
            var config = TopicConfig.builder()
                    .maxVersions(request.maxVersions() > 0 ? request.maxVersions() : 100)
                    .enableLineageTracking(request.enableLineageTracking())
                    .isPublic(request.isPublic()).build();
            var metadata = topicManager.createTopic(TopicDefinition.builder()
                    .name(request.name()).description(request.description())
                    .schema(schema).config(config).build());
            return Response.status(Response.Status.CREATED)
                    .entity(Map.of("success", true, "topic", topicToMap(metadata))).build();
        } catch (IllegalStateException e) {
            return Response.status(Response.Status.CONFLICT).entity(Map.of("error", e.getMessage())).build();
        }
    }

    @GET
    @Path("/topics/{topic}")
    public Response getTopic(@PathParam("topic") String topic) {
        return topicManager.getTopic(topic)
                .map(m -> Response.ok(topicToMap(m)).build())
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Topic not found: " + topic)).build());
    }

    @POST
    @Path("/topics/{topic}/publish")
    public Response publish(@PathParam("topic") String topic, PublishRequest request) {
        LOG.debugf("Publishing to topic: %s, records: %d", topic, request.data().size());
        try {
            var result = topicManager.publish(topic, request.data(), request.lineageId());
            byte[] arrowPayload = arrowSerializer.serializeToArrow(topic, request.data());
            contextStore.storeContext(topic, result.version(), arrowPayload,
                    request.metadata() != null ? request.metadata() : Map.of());
            return Response.ok(Map.of("success", true, "version", result.version(),
                    "ipc_handle", result.ipcHandle(),
                    "subscribers_notified", result.subscribersNotified(),
                    "payload_size_bytes", arrowPayload.length)).build();
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.NOT_FOUND).entity(Map.of("error", e.getMessage())).build();
        }
    }

    @GET
    @Path("/topics/{topic}/versions")
    public Response getVersions(@PathParam("topic") String topic,
            @QueryParam("limit") @DefaultValue("10") int limit) {
        var versions = contextStore.getVersionHistory(topic, limit);
        return Response.ok(Map.of("topic", topic, "versions",
                versions.stream().map(v -> Map.of("version", v.version(),
                        "size_bytes", v.sizeBytes(), "timestamp", v.timestampNanos())).toList())).build();
    }

    @GET
    @Path("/topics/{topic}/consume")
    public Response consume(@PathParam("topic") String topic, @QueryParam("version") Long version) {
        LOG.debugf("Consuming from topic: %s, version: %s", topic, version);
        try {
            var contextOpt = contextStore.getContext(topic, version);
            if (contextOpt.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "No context found for topic: " + topic)).build();
            }
            var ctx = contextOpt.get();
            var records = arrowSerializer.deserializeFromArrow(ctx.payload());
            return Response.ok(Map.of("topic", topic, "version", ctx.version().version(),
                    "record_count", records.size(), "records", records)).build();
        } catch (Exception e) {
            LOG.errorf("Error consuming from topic %s: %s", topic, e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", "Failed to retrieve context: " + e.getMessage())).build();
        }
    }

    @GET
    @Path("/topics/{topic}/latest")
    public Response getLatestContext(@PathParam("topic") String topic) {
        return consume(topic, null);
    }

    @GET
    @Path("/ipc/stats")
    public Response getIPCStats() {
        var stats = ipcEngine.getStats();
        return Response.ok(Map.of("active_segments", stats.activeSegments(),
                "total_mapped_bytes", stats.totalMappedBytes(),
                "current_version", stats.currentVersion())).build();
    }

    @GET
    @Path("/schemas")
    public Response listSchemas() {
        var schemas = arrowSerializer.getAllSchemas();
        return Response.ok(Map.of("schemas",
                schemas.entrySet().stream().map(e -> Map.of("name", e.getKey(),
                        "fields", e.getValue().getFields().stream().map(f -> Map.of(
                                "name", f.getName(), "type", f.getType().toString(),
                                "nullable", f.isNullable())).toList())).toList())).build();
    }

    @GET
    @Path("/schemas/{name}")
    public Response getSchema(@PathParam("name") String name) {
        return arrowSerializer.getSchema(name)
                .map(schema -> Response.ok(Map.of("name", name,
                        "fields", schema.getFields().stream().map(f -> Map.of(
                                "name", f.getName(), "type", f.getType().toString(),
                                "nullable", f.isNullable())).toList())).build())
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "Schema not found: " + name)).build());
    }

    @GET
    @Path("/storage/stats")
    public Response getStorageStats() {
        var stats = contextStore.getStats();
        return Response.ok(Map.of("topic_count", stats.topicCount(),
                "context_count", stats.contextCount(), "lineage_count", stats.lineageCount(),
                "bytes_written", stats.bytesWritten(), "bytes_read", stats.bytesRead())).build();
    }

    private Map<String, Object> topicToMap(TopicMetadata m) {
        return Map.of("name", m.name(),
                "description", m.description() != null ? m.description() : "",
                "current_version", m.currentVersion(),
                "subscriber_count", m.subscriberCount(),
                "created_at", m.createdAt().toString());
    }

    public record CreateTopicRequest(String name, String description, String schemaType,
            int maxVersions, boolean enableLineageTracking, boolean isPublic) {}
    public record PublishRequest(List<Map<String, Object>> data, String lineageId,
            Map<String, String> metadata) {}
}

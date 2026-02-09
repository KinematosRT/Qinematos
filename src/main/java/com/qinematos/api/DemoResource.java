package com.qinematos.api;

import com.qinematos.ContextGraphService;
import com.qinematos.InteropService;
import com.qinematos.TrackingService;
import com.qinematos.core.arrow.ArrowContextSerializer;
import com.qinematos.core.ipc.ZeroCopyIPCEngine;
import com.qinematos.core.topic.ContextTopicManager;
import com.qinematos.lineage.LineageTracker;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DemoResource - Interactive Demonstration Endpoints
 *
 * Provides interactive endpoints to demonstrate the Qinematos
 * Data Base Plane capabilities, specifically:
 * - Zero-Copy IPC with Java 25 FFM
 * - Apache Arrow serialization
 * - Reactive streaming
 * - Lineage tracking
 */
@Path("/demo")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DemoResource {

    private static final Logger LOG = Logger.getLogger(DemoResource.class);

    @Inject
    ZeroCopyIPCEngine ipcEngine;

    @Inject
    ArrowContextSerializer arrowSerializer;

    @Inject
    ContextTopicManager topicManager;

    @Inject
    ContextGraphService contextGraphService;

    @Inject
    LineageTracker lineageTracker;

    @Inject
    TrackingService trackingService;

    @Inject
    InteropService interopService;

    /**
     * Run the complete zero-copy demonstration.
     *
     * This endpoint demonstrates the full RVP workflow:
     * 1. Serialize data to Arrow format (Vulgate)
     * 2. Write to shared memory (Plain - Zero-Copy)
     * 3. Publish to topic (Recent - Real-time)
     * 4. Track lineage (Verifiability)
     */
    @POST
    @Path("/zero-copy")
    public Response demonstrateZeroCopy(@QueryParam("records") @DefaultValue("100") int recordCount) {
        LOG.info("=== Starting Zero-Copy Demonstration ===");

        long startTime = System.nanoTime();
        String demoId = UUID.randomUUID().toString().substring(0, 8);

        try {
            // Step 1: Start lineage trace
            String lineageId = lineageTracker.startTrace("demo-source", "demo.zerocopy");
            LOG.infof("Step 1: Started lineage trace: %s", lineageId);

            // Step 2: Create sample data
            List<Map<String, Object>> records = generateSampleRecords(recordCount, demoId);
            LOG.infof("Step 2: Generated %d sample records", records.size());

            // Step 3: Serialize to Arrow format
            long arrowStartTime = System.nanoTime();
            byte[] arrowPayload = arrowSerializer.serializeToArrow("agent.context", records);
            long arrowTime = System.nanoTime() - arrowStartTime;
            LOG.infof("Step 3: Serialized to Arrow: %d bytes in %d µs",
                    arrowPayload.length, arrowTime / 1000);

            // Step 4: Write to zero-copy IPC
            long ipcStartTime = System.nanoTime();
            var ipcHandle = ipcEngine.writeArrowPayload("demo.zerocopy", arrowPayload, lineageId);
            long ipcTime = System.nanoTime() - ipcStartTime;
            LOG.infof("Step 4: Written to IPC: %s in %d µs", ipcHandle.handleId(), ipcTime / 1000);

            // Step 5: Complete lineage
            lineageTracker.recordTransformation(lineageId, "arrow_serialization", "1.0", List.of());
            var lineageNode = lineageTracker.completeTrace(
                    lineageId,
                    ipcHandle.handleId(),
                    new byte[0] // Would be actual hash in production
            );
            LOG.infof("Step 5: Completed lineage trace");

            long totalTime = System.nanoTime() - startTime;

            // Build response
            Map<String, Object> result = Map.of(
                    "success", true,
                    "demo_id", demoId,
                    "rvp_workflow", Map.of(
                            "vulgate", Map.of(
                                    "format", "Apache Arrow IPC",
                                    "bytes", arrowPayload.length,
                                    "serialization_time_us", arrowTime / 1000),
                            "plain", Map.of(
                                    "method", "Zero-Copy Memory-Mapped File",
                                    "ipc_handle", ipcHandle.filePath(),
                                    "write_time_us", ipcTime / 1000),
                            "recent", Map.of(
                                    "topic", "demo.zerocopy",
                                    "version", ipcHandle.version()),
                            "verifiable", Map.of(
                                    "lineage_id", lineageId,
                                    "transformations", 1)),
                    "performance", Map.of(
                            "records", recordCount,
                            "total_time_us", totalTime / 1000,
                            "throughput_records_per_sec", recordCount * 1_000_000_000L / totalTime),
                    "read_instructions", Map.of(
                            "python", String.format(
                                    "import pyarrow as pa\n" +
                                            "source = pa.memory_map('%s', 'r')\n" +
                                            "# Skip 32-byte Qinematos header\n" +
                                            "source.seek(32)\n" +
                                            "reader = pa.ipc.open_stream(source)\n" +
                                            "table = reader.read_all()",
                                    ipcHandle.filePath().replace("\\", "/")),
                            "java", String.format(
                                    "var handle = new IPCHandle(\"%s\", ...);\n" +
                                            "ByteBuffer buffer = ipcEngine.readArrowPayload(handle);",
                                    ipcHandle.handleId())));

            LOG.info("=== Zero-Copy Demonstration Complete ===");

            return Response.ok(result).build();

        } catch (Exception e) {
            LOG.errorf("Demo failed: %s", e.getMessage());
            return Response.serverError()
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    /**
     * Demonstrate native memory operations with FFM.
     */
    @GET
    @Path("/ffm")
    public Response demonstrateFFM(@QueryParam("size") @DefaultValue("1024") int sizeBytes) {
        LOG.info("=== Demonstrating Java 25 FFM API ===");

        var allocationResult = trackingService.allocateNativeMemory(sizeBytes);
        var segmentDemo = trackingService.demonstrateSegmentOperations();

        return Response.ok(Map.of(
                "java_version", System.getProperty("java.version"),
                "ffm_api", "Java 25 Foreign Function & Memory API",
                "allocation_test", Map.of(
                        "success", allocationResult.success(),
                        "size_bytes", allocationResult.sizeBytes(),
                        "time_ns", allocationResult.elapsedNanos(),
                        "address", String.format("0x%X", allocationResult.address()),
                        "test_value", allocationResult.testValue()),
                "segment_operations", Map.of(
                        "success", segmentDemo.success(),
                        "magic_number", segmentDemo.magic(),
                        "version", segmentDemo.version(),
                        "header_size", segmentDemo.headerSize(),
                        "data_size", segmentDemo.dataSize())))
                .build();
    }

    /**
     * Demonstrate agent registration and context graph.
     */
    @POST
    @Path("/agents")
    public Response demonstrateAgents(@QueryParam("count") @DefaultValue("5") int agentCount) {
        LOG.info("=== Demonstrating Agent Context Graph ===");

        List<String> agentIds = new java.util.ArrayList<>();

        // Register agents
        for (int i = 0; i < agentCount; i++) {
            String agentId = "demo-agent-" + i;
            contextGraphService.registerAgent(agentId, "demo", Map.of(
                    "created_by", "demo",
                    "index", String.valueOf(i)));
            agentIds.add(agentId);
        }

        // Create some connections
        for (int i = 0; i < agentCount - 1; i++) {
            contextGraphService.linkAgents(agentIds.get(i), agentIds.get(i + 1));
        }

        // Add some traces
        for (String agentId : agentIds) {
            contextGraphService.addTrace(agentId, "Demo trace content", "demo-location", null);
        }

        var stats = contextGraphService.getStats();

        return Response.ok(Map.of(
                "agents_created", agentCount,
                "connections_created", agentCount - 1,
                "traces_added", agentCount,
                "graph_stats", Map.of(
                        "total_agents", stats.agentCount(),
                        "total_edges", stats.edgeCount(),
                        "total_traces", stats.traceCount())))
                .build();
    }

    /**
     * Get complete system status.
     */
    @GET
    @Path("/status")
    public Response getSystemStatus() {
        var ipcStats = ipcEngine.getStats();
        var lineageStats = lineageTracker.getStats();
        var graphStats = contextGraphService.getStats();
        var topics = topicManager.listTopics(null);

        return Response.ok(Map.of(
                "timestamp", Instant.now().toString(),
                "system", "Qinematos Data Base Plane",
                "version", "1.0.0-SNAPSHOT",
                "rvp_framework", Map.of(
                        "recent", "gRPC Watch API on port 9000",
                        "vulgate", "Apache Arrow 18.0",
                        "plain", "Java 25 FFM + Memory-Mapped Files"),
                "statistics", Map.of(
                        "ipc", Map.of(
                                "active_segments", ipcStats.activeSegments(),
                                "mapped_bytes", ipcStats.totalMappedBytes(),
                                "current_version", ipcStats.currentVersion()),
                        "lineage", Map.of(
                                "completed_traces", lineageStats.completedTraces(),
                                "active_traces", lineageStats.activeTraces(),
                                "total_created", lineageStats.totalTracesCreated()),
                        "agents", Map.of(
                                "count", graphStats.agentCount(),
                                "edges", graphStats.edgeCount(),
                                "traces", graphStats.traceCount()),
                        "topics", Map.of(
                                "count", topics.size(),
                                "names", topics.stream().map(t -> t.name()).toList()))))
                .build();
    }

    /**
     * Demonstrate framework integration.
     */
    @POST
    @Path("/integrate/{framework}")
    public Response integrateFramework(@PathParam("framework") String framework) {
        var result = interopService.connectToFramework(framework, Map.of());

        return Response.ok(Map.of(
                "success", result.success(),
                "framework", result.frameworkName(),
                "topic", result.topicName(),
                "endpoint", result.endpoint(),
                "usage", Map.of(
                        "subscribe", "Use gRPC Watch API with topic pattern: " + result.topicName(),
                        "publish", "POST /api/v1/topics/" + result.topicName() + "/publish")))
                .build();
    }

    // --------------------------
    // Helpers
    // --------------------------

    private List<Map<String, Object>> generateSampleRecords(int count, String demoId) {
        List<Map<String, Object>> records = new java.util.ArrayList<>();
        long now = Instant.now().toEpochMilli() * 1_000_000;

        for (int i = 0; i < count; i++) {
            records.add(Map.of(
                    "agent_id", "demo-agent-" + (i % 10),
                    "context_key", "key_" + i,
                    "context_value", "value_" + demoId + "_" + i,
                    "data_type", "string",
                    "timestamp_nanos", now + i,
                    "version", i + 1L,
                    "lineage_id", "demo_" + demoId,
                    "source_system", "qinematos-demo"));
        }

        return records;
    }
}

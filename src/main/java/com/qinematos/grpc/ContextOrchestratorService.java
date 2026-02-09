package com.qinematos.grpc;

import com.google.protobuf.ByteString;
import com.qinematos.core.arrow.ArrowContextSerializer;
import com.qinematos.core.topic.ContextTopicManager;
import com.qinematos.core.topic.ContextTopicManager.TopicDefinition;
import com.qinematos.core.topic.ContextTopicManager.TopicMetadata;
import com.qinematos.core.topic.ContextTopicManager.TopicConfig;
import com.qinematos.core.topic.ContextTopicManager.ContextUpdate;
import com.qinematos.core.topic.ContextTopicManager.UpdateType;
import com.qinematos.core.topic.ContextTopicManager.PublishResult;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import jakarta.inject.Inject;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ContextOrchestratorService - gRPC Service Implementation
 *
 * Implements the Context Orchestrator gRPC service defined in
 * context_orchestrator.proto.
 * This is the primary API for agents to subscribe to and receive context
 * updates.
 *
 * Key Responsibilities:
 * - Handle Watch requests for streaming context updates
 * - Manage subscriptions with delivery options
 * - Publish context to topics
 * - Topic lifecycle management
 */
@GrpcService
public class ContextOrchestratorService {

    private static final Logger LOG = Logger.getLogger(ContextOrchestratorService.class);

    @Inject
    ContextTopicManager topicManager;

    @Inject
    ArrowContextSerializer arrowSerializer;

    // Active watch streams by agent
    private final Map<String, WatchSession> activeSessions = new ConcurrentHashMap<>();

    // Metrics
    private final AtomicLong totalWatchRequests = new AtomicLong(0);
    private final AtomicLong totalPublishRequests = new AtomicLong(0);
    private final Instant startTime = Instant.now();

    /**
     * Watch - Server-streaming RPC for context subscriptions.
     *
     * This is the core API for agents to receive real-time context updates.
     * Returns IPC handles (file paths) for zero-copy data access.
     */
    public Multi<SubscriptionUpdateProto> watch(WatchRequestProto request) {
        totalWatchRequests.incrementAndGet();

        String agentId = request.getAgentId();
        List<String> patterns = request.getTopicPatternsList();
        Long fromVersion = request.hasFromVersion() ? request.getFromVersion() : null;

        LOG.infof("Watch request from agent %s for patterns: %s", agentId, patterns);

        // Create watch session
        WatchSession session = new WatchSession(
                agentId,
                patterns,
                request.getDeliveryOptions(),
                request.getCapabilities(),
                Instant.now());
        activeSessions.put(agentId, session);

        // Subscribe to all requested patterns and merge streams
        List<Multi<ContextUpdate>> streams = patterns.stream()
                .map(pattern -> topicManager.subscribe(agentId, pattern, fromVersion))
                .toList();

        // Merge all pattern streams
        Multi<ContextUpdate> mergedStream = streams.isEmpty()
                ? Multi.createFrom().empty()
                : Multi.createBy().merging().streams(streams);

        // Transform to proto messages
        return mergedStream
                .map(update -> toSubscriptionUpdateProto(update, request.getDeliveryOptions()))
                .onTermination().invoke(() -> {
                    activeSessions.remove(agentId);
                    LOG.infof("Watch session ended for agent %s", agentId);
                });
    }

    /**
     * Publish - Publish context to a topic.
     */
    public PublishResponseProto publish(PublishRequestProto request) {
        totalPublishRequests.incrementAndGet();

        String topic = request.getTopic();
        byte[] arrowPayload = request.getArrowPayload().toByteArray();
        String lineageId = request.hasLineage() ? request.getLineage().getContextVersionId() : null;

        LOG.debugf("Publish request for topic %s: %d bytes", topic, arrowPayload.length);

        try {
            // Deserialize to records for validation
            List<Map<String, Object>> records = arrowSerializer.deserializeFromArrow(arrowPayload);

            // Publish to topic
            PublishResult result = topicManager.publish(topic, records, lineageId);

            return PublishResponseProto.newBuilder()
                    .setVersion(result.version())
                    .setIpcHandle(result.ipcHandle())
                    .setSubscribersNotified(result.subscribersNotified())
                    .build();

        } catch (Exception e) {
            LOG.errorf("Publish failed for topic %s: %s", topic, e.getMessage());
            throw new RuntimeException("Publish failed: " + e.getMessage(), e);
        }
    }

    /**
     * CreateTopic - Create a new context topic.
     */
    public CreateTopicResponseProto createTopic(CreateTopicRequestProto request) {
        String name = request.getName();
        String description = request.getDescription();

        LOG.infof("CreateTopic request: %s", name);

        try {
            // Parse schema if provided
            Schema schema = null;
            if (!request.getArrowSchema().isEmpty()) {
                schema = arrowSerializer.deserializeSchema(request.getArrowSchema().toByteArray());
            }

            // Parse config
            TopicConfig config = toTopicConfig(request.getConfig());

            // Create topic
            TopicMetadata metadata = topicManager.createTopic(
                    TopicDefinition.builder()
                            .name(name)
                            .description(description)
                            .schema(schema)
                            .config(config)
                            .build());

            return CreateTopicResponseProto.newBuilder()
                    .setSuccess(true)
                    .setMetadata(toTopicMetadataProto(metadata))
                    .build();

        } catch (Exception e) {
            LOG.errorf("CreateTopic failed for %s: %s", name, e.getMessage());
            return CreateTopicResponseProto.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
        }
    }

    /**
     * ListTopics - List all available topics.
     */
    public ListTopicsResponseProto listTopics(ListTopicsRequestProto request) {
        String pattern = request.getPattern();
        int limit = request.getLimit() > 0 ? request.getLimit() : 100;

        List<TopicMetadata> topics = topicManager.listTopics(pattern);

        ListTopicsResponseProto.Builder responseBuilder = ListTopicsResponseProto.newBuilder();

        topics.stream()
                .limit(limit)
                .map(this::toTopicMetadataProto)
                .forEach(responseBuilder::addTopics);

        return responseBuilder.build();
    }

    /**
     * GetTopic - Get metadata for a specific topic.
     */
    public TopicMetadataProto getTopic(GetTopicRequestProto request) {
        String name = request.getName();

        return topicManager.getTopic(name)
                .map(this::toTopicMetadataProto)
                .orElseThrow(() -> new RuntimeException("Topic not found: " + name));
    }

    /**
     * Acknowledge - Acknowledge receipt of updates.
     */
    public AcknowledgeResponseProto acknowledge(AcknowledgeRequestProto request) {
        String agentId = request.getAgentId();
        List<String> updateIds = request.getUpdateIdsList();

        LOG.debugf("Acknowledge from agent %s: %d updates", agentId, updateIds.size());

        // In a production system, this would update delivery tracking
        // For now, just acknowledge success
        return AcknowledgeResponseProto.newBuilder()
                .setSuccess(true)
                .setAcknowledgedCount(updateIds.size())
                .build();
    }

    /**
     * HealthCheck - Check orchestrator health status.
     */
    public HealthCheckResponseProto healthCheck(HealthCheckRequestProto request) {
        long uptimeSeconds = Instant.now().getEpochSecond() - startTime.getEpochSecond();

        HealthCheckResponseProto.Builder responseBuilder = HealthCheckResponseProto.newBuilder()
                .setStatus(HealthStatusProto.HEALTH_STATUS_HEALTHY)
                .setUptimeSeconds(uptimeSeconds)
                .setActiveSubscriptions(activeSessions.size())
                .setTopicCount(topicManager.listTopics(null).size())
                .setMemoryUsage(MemoryUsageProto.newBuilder()
                        .setHeapMemoryBytes(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
                        .build());

        if (request.getIncludeDiagnostics()) {
            responseBuilder
                    .putDiagnostics("total_watch_requests", String.valueOf(totalWatchRequests.get()))
                    .putDiagnostics("total_publish_requests", String.valueOf(totalPublishRequests.get()))
                    .putDiagnostics("java_version", System.getProperty("java.version"))
                    .putDiagnostics("os_name", System.getProperty("os.name"));
        }

        return responseBuilder.build();
    }

    // --------------------------
    // Conversion Helpers
    // --------------------------

    private SubscriptionUpdateProto toSubscriptionUpdateProto(ContextUpdate update, DeliveryOptionsProto options) {
        SubscriptionUpdateProto.Builder builder = SubscriptionUpdateProto.newBuilder()
                .setHeader(UpdateHeaderProto.newBuilder()
                        .setTopic(update.topic())
                        .setVersion(update.version())
                        .setTimestampNanos(update.timestampNanos())
                        .setUpdateType(toUpdateTypeProto(update.updateType()))
                        .setUpdateId(update.handleId())
                        .build())
                .setPayloadSizeBytes(update.payloadSize());

        // Set IPC handle for zero-copy delivery
        if (update.ipcHandle() != null) {
            builder.setIpcHandle(update.ipcHandle());
        }

        // Include schema if requested
        if (options != null && options.getIncludeSchema() && update.schemaBytes() != null) {
            builder.setArrowSchema(ByteString.copyFrom(update.schemaBytes()));
        }

        // Include lineage if available
        if (update.lineageId() != null) {
            builder.setLineage(LineageMetadataProto.newBuilder()
                    .setContextVersionId(update.lineageId())
                    .build());
        }

        return builder.build();
    }

    private UpdateTypeProto toUpdateTypeProto(UpdateType type) {
        return switch (type) {
            case SNAPSHOT -> UpdateTypeProto.UPDATE_TYPE_SNAPSHOT;
            case DELTA -> UpdateTypeProto.UPDATE_TYPE_DELTA;
            case DELETE -> UpdateTypeProto.UPDATE_TYPE_DELETE;
            case SCHEMA_CHANGE -> UpdateTypeProto.UPDATE_TYPE_SCHEMA_CHANGE;
        };
    }

    private TopicConfig toTopicConfig(TopicConfigProto proto) {
        if (proto == null) {
            return TopicConfig.defaultConfig();
        }

        return TopicConfig.builder()
                .maxVersions(proto.hasRetention() ? proto.getRetention().getMaxVersions() : 100)
                .maxAgeSecs(proto.hasRetention() ? proto.getRetention().getMaxAgeSeconds() : 86400)
                .minVersions(proto.hasRetention() ? proto.getRetention().getMinVersions() : 1)
                .maxPayloadSize(proto.getMaxPayloadSize() > 0 ? proto.getMaxPayloadSize() : 100 * 1024 * 1024)
                .enableLineageTracking(proto.getEnableLineageTracking())
                .isPublic(proto.hasAccessControl() && proto.getAccessControl().getIsPublic())
                .build();
    }

    private TopicMetadataProto toTopicMetadataProto(TopicMetadata metadata) {
        TopicMetadataProto.Builder builder = TopicMetadataProto.newBuilder()
                .setName(metadata.name())
                .setCurrentVersion(metadata.currentVersion())
                .setSubscriberCount(metadata.subscriberCount())
                .setCreatedAtNanos(metadata.createdAt().toEpochMilli() * 1_000_000);

        if (metadata.description() != null) {
            builder.setDescription(metadata.description());
        }

        if (metadata.schemaBytes() != null) {
            builder.setArrowSchema(ByteString.copyFrom(metadata.schemaBytes()));
        }

        return builder.build();
    }

    // --------------------------
    // Session Tracking
    // --------------------------

    private record WatchSession(
            String agentId,
            List<String> patterns,
            DeliveryOptionsProto deliveryOptions,
            AgentCapabilitiesProto capabilities,
            Instant createdAt) {
    }
}

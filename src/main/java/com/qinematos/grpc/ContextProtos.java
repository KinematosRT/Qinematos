package com.qinematos.grpc;

/**
 * Proto Message Stubs for gRPC Service
 *
 * These classes represent the generated protobuf message types.
 * In production, these would be generated from context_orchestrator.proto
 * using the protobuf-maven-plugin.
 *
 * Generated with: mvn quarkus:generate-code
 */

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;

// =============================================================================
// Watch Request/Response
// =============================================================================

record WatchRequestProto(String agentId, List<String> topicPatternsList, boolean hasFromVersion, Long fromVersion,
        DeliveryOptionsProto deliveryOptions, AgentCapabilitiesProto capabilities) {
    public String getAgentId() {
        return agentId;
    }

    public List<String> getTopicPatternsList() {
        return topicPatternsList;
    }

    public boolean hasFromVersion() {
        return hasFromVersion;
    }

    public Long getFromVersion() {
        return fromVersion;
    }

    public DeliveryOptionsProto getDeliveryOptions() {
        return deliveryOptions;
    }

    public AgentCapabilitiesProto getCapabilities() {
        return capabilities;
    }
}

// =============================================================================
// Subscription Update
// =============================================================================

class SubscriptionUpdateProto {
    private UpdateHeaderProto header;
    private String ipcHandle;
    private ByteString arrowSchema;
    private long payloadSizeBytes;
    private LineageMetadataProto lineage;
    private ByteString inlinePayload;

    private SubscriptionUpdateProto(Builder builder) {
        this.header = builder.header;
        this.ipcHandle = builder.ipcHandle;
        this.arrowSchema = builder.arrowSchema;
        this.payloadSizeBytes = builder.payloadSizeBytes;
        this.lineage = builder.lineage;
        this.inlinePayload = builder.inlinePayload;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private UpdateHeaderProto header;
        private String ipcHandle;
        private ByteString arrowSchema;
        private long payloadSizeBytes;
        private LineageMetadataProto lineage;
        private ByteString inlinePayload;

        public Builder setHeader(UpdateHeaderProto header) {
            this.header = header;
            return this;
        }

        public Builder setIpcHandle(String ipcHandle) {
            this.ipcHandle = ipcHandle;
            return this;
        }

        public Builder setArrowSchema(ByteString arrowSchema) {
            this.arrowSchema = arrowSchema;
            return this;
        }

        public Builder setPayloadSizeBytes(long payloadSizeBytes) {
            this.payloadSizeBytes = payloadSizeBytes;
            return this;
        }

        public Builder setLineage(LineageMetadataProto lineage) {
            this.lineage = lineage;
            return this;
        }

        public Builder setInlinePayload(ByteString inlinePayload) {
            this.inlinePayload = inlinePayload;
            return this;
        }

        public SubscriptionUpdateProto build() {
            return new SubscriptionUpdateProto(this);
        }
    }
}

// =============================================================================
// Update Header
// =============================================================================

class UpdateHeaderProto {
    private String topic;
    private long version;
    private long timestampNanos;
    private UpdateTypeProto updateType;
    private String updateId;

    private UpdateHeaderProto(Builder builder) {
        this.topic = builder.topic;
        this.version = builder.version;
        this.timestampNanos = builder.timestampNanos;
        this.updateType = builder.updateType;
        this.updateId = builder.updateId;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String topic;
        private long version;
        private long timestampNanos;
        private UpdateTypeProto updateType;
        private String updateId;

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder setTimestampNanos(long timestampNanos) {
            this.timestampNanos = timestampNanos;
            return this;
        }

        public Builder setUpdateType(UpdateTypeProto updateType) {
            this.updateType = updateType;
            return this;
        }

        public Builder setUpdateId(String updateId) {
            this.updateId = updateId;
            return this;
        }

        public UpdateHeaderProto build() {
            return new UpdateHeaderProto(this);
        }
    }
}

// =============================================================================
// Enums
// =============================================================================

enum UpdateTypeProto {
    UPDATE_TYPE_SNAPSHOT,
    UPDATE_TYPE_DELTA,
    UPDATE_TYPE_DELETE,
    UPDATE_TYPE_SCHEMA_CHANGE
}

enum DeliveryMethodProto {
    DELIVERY_METHOD_IPC,
    DELIVERY_METHOD_ARROW_FLIGHT,
    DELIVERY_METHOD_INLINE
}

enum HealthStatusProto {
    HEALTH_STATUS_HEALTHY,
    HEALTH_STATUS_DEGRADED,
    HEALTH_STATUS_UNHEALTHY

}

// =============================================================================
// Delivery Options
// =============================================================================

record DeliveryOptionsProto(
        DeliveryMethodProto method,
        int maxBatchSize,
        boolean includeSchema) {

    public DeliveryMethodProto getMethod() {
        return method;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public boolean getIncludeSchema() {
        return includeSchema;
    }

}

// =============================================================================
// Agent Capabilities
// =============================================================================

record AgentCapabilitiesProto(
        boolean supportsMmap,
        boolean supportsArrow,
        long maxMemoryBytes,
        int processId) {
}

// =============================================================================
// Lineage Metadata
// =============================================================================

class LineageMetadataProto {
    private String contextVersionId;
    private String sourceSystem;

    private LineageMetadataProto(Builder builder) {
        this.contextVersionId = builder.contextVersionId;
        this.sourceSystem = builder.sourceSystem;
    }

    public String getContextVersionId() {
        return contextVersionId;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String contextVersionId;
        private String sourceSystem;

        public Builder setContextVersionId(String id) {
            this.contextVersionId = id;
            return this;
        }

        public Builder setSourceSystem(String system) {
            this.sourceSystem = system;
            return this;
        }

        public LineageMetadataProto build() {
            return new LineageMetadataProto(this);
        }
    }
}

// =============================================================================
// Publish Request/Response
// =============================================================================

record PublishRequestProto(
        String topic,
        ByteString arrowPayload,
        LineageMetadataProto lineage,
        UpdateTypeProto updateType) {

    public String getTopic() {
        return topic;
    }

    public ByteString getArrowPayload() {
        return arrowPayload;
    }

    public boolean hasLineage() {
        return lineage != null;
    }

    public LineageMetadataProto getLineage() {
        return lineage;
    }

    public UpdateTypeProto getUpdateType() {
        return updateType;
    }
}

class PublishResponseProto {
    private long version;
    private String ipcHandle;
    private int subscribersNotified;

    private PublishResponseProto(Builder builder) {
        this.version = builder.version;
        this.ipcHandle = builder.ipcHandle;
        this.subscribersNotified = builder.subscribersNotified;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long version;
        private String ipcHandle;
        private int subscribersNotified;

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder setIpcHandle(String ipcHandle) {
            this.ipcHandle = ipcHandle;
            return this;
        }

        public Builder setSubscribersNotified(int subscribers) {
            this.subscribersNotified = subscribers;
            return this;
        }

        public PublishResponseProto build() {
            return new PublishResponseProto(this);
        }
    }

}

// =============================================================================
// Topic Management
// =============================================================================

record CreateTopicRequestProto(
        String name,
        ByteString arrowSchema,
        TopicConfigProto config,
        String description) {

    public String getName() {
        return name;
    }

    public ByteString getArrowSchema() {
        return arrowSchema;
    }

    public TopicConfigProto getConfig() {
        return config;
    }

    public String getDescription() {
        return description;
    }
}

class CreateTopicResponseProto {
    private boolean success;
    private String errorMessage;
    private TopicMetadataProto metadata;

    private CreateTopicResponseProto(Builder builder) {
        this.success = builder.success;
        this.errorMessage = builder.errorMessage;
        this.metadata = builder.metadata;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private boolean success;
        private String errorMessage;
        private TopicMetadataProto metadata;

        public Builder setSuccess(boolean success) {
            this.success = success;
            return this;
        }

        public Builder setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder setMetadata(TopicMetadataProto metadata) {
            this.metadata = metadata;
            return this;
        }

        public CreateTopicResponseProto build() {
            return new CreateTopicResponseProto(this);
        }
    }

}

record ListTopicsRequestProto(String pattern, int limit) {

    public String getPattern() {
        return pattern;
    }

    public int getLimit() {
        return limit;
    }
}

class ListTopicsResponseProto {
    private final java.util.List<TopicMetadataProto> topics = new java.util.ArrayList<>();

    public void addTopics(TopicMetadataProto topic) {
        topics.add(topic);
    }

    public java.util.List<TopicMetadataProto> getTopicsList() {
        return topics;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final ListTopicsResponseProto proto = new ListTopicsResponseProto();

        public Builder addTopics(TopicMetadataProto topic) {
            proto.addTopics(topic);
            return this;
        }

        public ListTopicsResponseProto build() {
            return proto;
        }
    }

}

record GetTopicRequestProto(String name) {

    public String getName() {
        return name;
    }
}

class TopicMetadataProto {
    private String name;
    private ByteString arrowSchema;
    private long currentVersion;
    private int subscriberCount;
    private long createdAtNanos;
    private String description;

    private TopicMetadataProto(Builder builder) {
        this.name = builder.name;
        this.arrowSchema = builder.arrowSchema;
        this.currentVersion = builder.currentVersion;
        this.subscriberCount = builder.subscriberCount;
        this.createdAtNanos = builder.createdAtNanos;
        this.description = builder.description;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private ByteString arrowSchema;
        private long currentVersion;
        private int subscriberCount;
        private long createdAtNanos;
        private String description;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setArrowSchema(ByteString arrowSchema) {
            this.arrowSchema = arrowSchema;
            return this;
        }

        public Builder setCurrentVersion(long currentVersion) {
            this.currentVersion = currentVersion;
            return this;
        }

        public Builder setSubscriberCount(int subscriberCount) {
            this.subscriberCount = subscriberCount;
            return this;
        }

        public Builder setCreatedAtNanos(long createdAtNanos) {
            this.createdAtNanos = createdAtNanos;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public TopicMetadataProto build() {
            return new TopicMetadataProto(this);
        }
    }

}

record TopicConfigProto(
        RetentionPolicyProto retention,
        AccessControlProto accessControl,
        long maxPayloadSize,
        boolean enableLineageTracking) {

    public boolean hasRetention() {
        return retention != null;
    }

    public RetentionPolicyProto getRetention() {
        return retention;
    }

    public boolean hasAccessControl() {
        return accessControl != null;
    }

    public AccessControlProto getAccessControl() {
        return accessControl;
    }

    public long getMaxPayloadSize() {
        return maxPayloadSize;
    }

    public boolean getEnableLineageTracking() {
        return enableLineageTracking;
    }

}

record RetentionPolicyProto(int maxVersions, long maxAgeSeconds, int minVersions) {

    public int getMaxVersions() {
        return maxVersions;
    }

    public long getMaxAgeSeconds() {
        return maxAgeSeconds;
    }

    public int getMinVersions() {
        return minVersions;
    }

}

record AccessControlProto(List<String> readAgents, List<String> writeAgents, boolean isPublic) {

    public boolean getIsPublic() {
        return isPublic;
    }

}

// =============================================================================
// Acknowledgment
// =============================================================================

record AcknowledgeRequestProto(String agentId, List<String> updateIdsList) {

    public String getAgentId() {
        return agentId;
    }

    public List<String> getUpdateIdsList() {
        return updateIdsList;
    }
}

class AcknowledgeResponseProto {
    private boolean success;
    private int acknowledgedCount;

    private AcknowledgeResponseProto(Builder builder) {
        this.success = builder.success;
        this.acknowledgedCount = builder.acknowledgedCount;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private boolean success;
        private int acknowledgedCount;

        public Builder setSuccess(boolean success) {
            this.success = success;
            return this;
        }

        public Builder setAcknowledgedCount(int count) {
            this.acknowledgedCount = count;
            return this;
        }

        public AcknowledgeResponseProto build() {
            return new AcknowledgeResponseProto(this);
        }
    }

}

// =============================================================================
// Health Check
// =============================================================================

record HealthCheckRequestProto(boolean includeDiagnostics) {

    public boolean getIncludeDiagnostics() {
        return includeDiagnostics;
    }
}

class HealthCheckResponseProto {
    private HealthStatusProto status;
    private long uptimeSeconds;
    private int activeSubscriptions;
    private int topicCount;
    private MemoryUsageProto memoryUsage;
    private final java.util.Map<String, String> diagnostics = new java.util.HashMap<>();

    private HealthCheckResponseProto(Builder builder) {
        this.status = builder.status;
        this.uptimeSeconds = builder.uptimeSeconds;
        this.activeSubscriptions = builder.activeSubscriptions;
        this.topicCount = builder.topicCount;
        this.memoryUsage = builder.memoryUsage;
        this.diagnostics.putAll(builder.diagnostics);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private HealthStatusProto status;
        private long uptimeSeconds;
        private int activeSubscriptions;
        private int topicCount;
        private MemoryUsageProto memoryUsage;
        private final java.util.Map<String, String> diagnostics = new java.util.HashMap<>();

        public Builder setStatus(HealthStatusProto status) {
            this.status = status;
            return this;
        }

        public Builder setUptimeSeconds(long uptimeSeconds) {
            this.uptimeSeconds = uptimeSeconds;
            return this;
        }

        public Builder setActiveSubscriptions(int activeSubscriptions) {
            this.activeSubscriptions = activeSubscriptions;
            return this;
        }

        public Builder setTopicCount(int topicCount) {
            this.topicCount = topicCount;
            return this;
        }

        public Builder setMemoryUsage(MemoryUsageProto memoryUsage) {
            this.memoryUsage = memoryUsage;
            return this;
        }

        public Builder putDiagnostics(String key, String value) {
            this.diagnostics.put(key, value);
            return this;
        }

        public HealthCheckResponseProto build() {
            return new HealthCheckResponseProto(this);
        }
    }
}

class MemoryUsageProto {
    private long contextMemoryBytes;
    private long mmapMemoryBytes;
    private long heapMemoryBytes;

    private MemoryUsageProto(Builder builder) {
        this.contextMemoryBytes = builder.contextMemoryBytes;
        this.mmapMemoryBytes = builder.mmapMemoryBytes;
        this.heapMemoryBytes = builder.heapMemoryBytes;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long contextMemoryBytes;
        private long mmapMemoryBytes;
        private long heapMemoryBytes;

        public Builder setContextMemoryBytes(long bytes) {
            this.contextMemoryBytes = bytes;
            return this;
        }

        public Builder setMmapMemoryBytes(long bytes) {
            this.mmapMemoryBytes = bytes;
            return this;
        }

        public Builder setHeapMemoryBytes(long bytes) {
            this.heapMemoryBytes = bytes;
            return this;
        }

        public MemoryUsageProto build() {
            return new MemoryUsageProto(this);
        }
    }
}

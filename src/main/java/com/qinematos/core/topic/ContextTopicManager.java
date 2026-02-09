package com.qinematos.core.topic;

import com.qinematos.core.arrow.ArrowContextSerializer;
import com.qinematos.core.ipc.ZeroCopyIPCEngine;
import com.qinematos.core.ipc.ZeroCopyIPCEngine.IPCHandle;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@ApplicationScoped
public class ContextTopicManager {

    private static final Logger LOG = Logger.getLogger(ContextTopicManager.class);

    @Inject
    ZeroCopyIPCEngine ipcEngine;

    @Inject
    ArrowContextSerializer arrowSerializer;

    private final Map<String, ContextTopic> topics = new ConcurrentHashMap<>();
    private final Map<String, Set<Subscription>> subscriptionsByAgent = new ConcurrentHashMap<>();
    private final AtomicLong subscriptionCounter = new AtomicLong(0);

    @PostConstruct
    void initialize() {
        LOG.info("ContextTopicManager initialized");
        createSystemTopics();
    }

    @PreDestroy
    void shutdown() {
        topics.values().forEach(topic -> {
            if (topic.broadcaster() != null) {
                topic.broadcaster().onComplete();
            }
        });
        LOG.info("ContextTopicManager shutdown complete");
    }

    private void createSystemTopics() {
        createTopic(TopicDefinition.builder().name("system.health")
                .description("System health and status updates")
                .schema(arrowSerializer.getSchema("generic.keyvalue").orElse(null))
                .config(TopicConfig.defaultConfig()).build());
        createTopic(TopicDefinition.builder().name("system.agents")
                .description("Agent registration and status")
                .schema(arrowSerializer.getSchema("agent.context").orElse(null))
                .config(TopicConfig.defaultConfig()).build());
        createTopic(TopicDefinition.builder().name("system.lineage")
                .description("Context lineage and eBPF traces")
                .schema(arrowSerializer.getSchema("lineage.trace").orElse(null))
                .config(TopicConfig.builder().maxVersions(1000).enableLineageTracking(true).build()).build());
        createTopic(TopicDefinition.builder().name("session.knowledge_base")
                .description("Past implementations, reusable patterns, and accumulated knowledge for task discovery")
                .schema(arrowSerializer.getSchema("agent.context").orElse(null))
                .config(TopicConfig.builder().maxVersions(10000).maxAgeSecs(30 * 24 * 3600)
                        .enableLineageTracking(true).isPublic(true).build()).build());
        createTopic(TopicDefinition.builder().name("session.communication")
                .description("Cross-session communication: findings, pending tasks, and knowledge traces for future sessions")
                .schema(arrowSerializer.getSchema("agent.context").orElse(null))
                .config(TopicConfig.builder().maxVersions(1000).enableLineageTracking(true).isPublic(true).build()).build());
        createTopic(TopicDefinition.builder().name("session.external_provider")
                .description("Integration with external providers for maintaining common knowledge database")
                .schema(arrowSerializer.getSchema("generic.keyvalue").orElse(null))
                .config(TopicConfig.builder().maxVersions(500).enableLineageTracking(true).isPublic(true).build()).build());
        createTopic(TopicDefinition.builder().name("session.discovery")
                .description("Agent session capabilities and specializations for task routing")
                .schema(arrowSerializer.getSchema("agent.context").orElse(null))
                .config(TopicConfig.builder().maxVersions(100).enableLineageTracking(false).isPublic(true).build()).build());
    }

    public TopicMetadata createTopic(TopicDefinition definition) {
        if (topics.containsKey(definition.name())) {
            throw new IllegalStateException("Topic already exists: " + definition.name());
        }
        BroadcastProcessor<ContextUpdate> broadcaster = BroadcastProcessor.create();
        ContextTopic topic = new ContextTopic(definition.name(), definition.description(),
                definition.schema(), definition.config(), broadcaster, new AtomicLong(0),
                Instant.now(), new ArrayList<>());
        topics.put(definition.name(), topic);
        LOG.infof("Created topic: %s", definition.name());
        return toMetadata(topic);
    }

    public PublishResult publish(String topicName, List<Map<String, Object>> records, String lineageId) {
        ContextTopic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }
        byte[] arrowPayload = arrowSerializer.serializeToArrow(topicName, records);
        IPCHandle handle = ipcEngine.writeArrowPayload(topicName, arrowPayload, lineageId);
        long version = topic.currentVersion().incrementAndGet();
        ContextUpdate update = new ContextUpdate(topicName, version, handle.handleId(),
                handle.filePath(), arrowPayload.length,
                Instant.now().toEpochMilli() * 1_000_000, handle.lineageId(),
                UpdateType.SNAPSHOT,
                topic.schema() != null ? arrowSerializer.serializeSchema(topic.schema()) : null);
        topic.versionHistory().add(new VersionInfo(version, handle.handleId(), Instant.now()));
        topic.broadcaster().onNext(update);
        int subscriberCount = (int) subscriptionsByAgent.values().stream()
                .flatMap(Set::stream)
                .filter(sub -> matchesTopic(sub.topicPattern(), topicName))
                .count();
        LOG.debugf("Published to topic %s: version=%d, subscribers=%d, size=%d bytes",
                topicName, version, subscriberCount, arrowPayload.length);
        return new PublishResult(version, handle.filePath(), subscriberCount);
    }

    public Multi<ContextUpdate> subscribe(String agentId, String topicPattern, Long fromVersion) {
        long subscriptionId = subscriptionCounter.incrementAndGet();
        Subscription subscription = new Subscription(subscriptionId, agentId, topicPattern, fromVersion, Instant.now());
        subscriptionsByAgent.computeIfAbsent(agentId, k -> ConcurrentHashMap.newKeySet()).add(subscription);
        LOG.infof("Agent %s subscribed to pattern: %s (subscription #%d)", agentId, topicPattern, subscriptionId);
        Multi<ContextUpdate> replayStream = buildReplayStream(topicPattern, fromVersion);
        Multi<ContextUpdate> liveStream = buildLiveStream(topicPattern);
        return Multi.createBy().concatenating().streams(replayStream, liveStream)
                .onTermination().invoke(() -> {
                    Set<Subscription> agentSubs = subscriptionsByAgent.get(agentId);
                    if (agentSubs != null) { agentSubs.remove(subscription); }
                    LOG.debugf("Subscription #%d terminated for agent %s", subscriptionId, agentId);
                });
    }

    private Multi<ContextUpdate> buildReplayStream(String topicPattern, Long fromVersion) {
        if (fromVersion == null) { return Multi.createFrom().empty(); }
        List<ContextTopic> matchingTopics = topics.values().stream()
                .filter(t -> matchesTopic(topicPattern, t.name())).toList();
        List<ContextUpdate> replayUpdates = new ArrayList<>();
        for (ContextTopic topic : matchingTopics) {
            for (VersionInfo vi : topic.versionHistory()) {
                if (vi.version() >= fromVersion) {
                    replayUpdates.add(new ContextUpdate(topic.name(), vi.version(), vi.handleId(),
                            null, 0, vi.timestamp().toEpochMilli() * 1_000_000, null, UpdateType.SNAPSHOT, null));
                }
            }
        }
        replayUpdates.sort(Comparator.comparingLong(ContextUpdate::version));
        return Multi.createFrom().iterable(replayUpdates);
    }

    private Multi<ContextUpdate> buildLiveStream(String topicPattern) {
        List<BroadcastProcessor<ContextUpdate>> broadcasters = topics.values().stream()
                .filter(t -> matchesTopic(topicPattern, t.name()))
                .map(ContextTopic::broadcaster).toList();
        if (broadcasters.isEmpty()) {
            return Multi.createBy().merging().streams(
                    topics.values().stream().map(t -> (Multi<ContextUpdate>) t.broadcaster())
                            .collect(Collectors.toList()))
                    .filter(update -> matchesTopic(topicPattern, update.topic()));
        }
        return Multi.createBy().merging().streams(
                broadcasters.stream().map(bp -> (Multi<ContextUpdate>) bp).collect(Collectors.toList()));
    }

    private boolean matchesTopic(String pattern, String topicName) {
        String regex = pattern.replace(".", "\\.").replace("**", "@@DOUBLE@@")
                .replace("*", "[^.]+").replace("@@DOUBLE@@", ".+");
        return Pattern.matches(regex, topicName);
    }

    public Optional<TopicMetadata> getTopic(String topicName) {
        ContextTopic topic = topics.get(topicName);
        return topic != null ? Optional.of(toMetadata(topic)) : Optional.empty();
    }

    public List<TopicMetadata> listTopics(String pattern) {
        return topics.values().stream()
                .filter(t -> pattern == null || pattern.isEmpty() || matchesTopic(pattern, t.name()))
                .map(this::toMetadata).toList();
    }

    public int getSubscriberCount(String topicName) {
        return (int) subscriptionsByAgent.values().stream().flatMap(Set::stream)
                .filter(sub -> matchesTopic(sub.topicPattern(), topicName)).count();
    }

    private TopicMetadata toMetadata(ContextTopic topic) {
        return new TopicMetadata(topic.name(), topic.description(), topic.currentVersion().get(),
                getSubscriberCount(topic.name()), topic.createdAt(),
                topic.schema() != null ? arrowSerializer.serializeSchema(topic.schema()) : null,
                topic.config());
    }

    public record ContextTopic(String name, String description, Schema schema,
            TopicConfig config, BroadcastProcessor<ContextUpdate> broadcaster,
            AtomicLong currentVersion, Instant createdAt, List<VersionInfo> versionHistory) {}

    public record VersionInfo(long version, String handleId, Instant timestamp) {}

    public record TopicDefinition(String name, String description, Schema schema, TopicConfig config) {
        public static Builder builder() { return new Builder(); }
        public static class Builder {
            private String name; private String description; private Schema schema; private TopicConfig config;
            public Builder name(String n) { this.name = n; return this; }
            public Builder description(String d) { this.description = d; return this; }
            public Builder schema(Schema s) { this.schema = s; return this; }
            public Builder config(TopicConfig c) { this.config = c; return this; }
            public TopicDefinition build() { return new TopicDefinition(name, description, schema, config); }
        }
    }

    public record TopicConfig(int maxVersions, long maxAgeSecs, int minVersions,
            long maxPayloadSize, boolean enableLineageTracking, boolean isPublic) {
        public static TopicConfig defaultConfig() {
            return new TopicConfig(100, 86400, 1, 100 * 1024 * 1024, true, true);
        }
        public static Builder builder() { return new Builder(); }
        public static class Builder {
            private int maxVersions = 100; private long maxAgeSecs = 86400; private int minVersions = 1;
            private long maxPayloadSize = 100 * 1024 * 1024; private boolean enableLineageTracking = true;
            private boolean isPublic = true;
            public Builder maxVersions(int v) { this.maxVersions = v; return this; }
            public Builder maxAgeSecs(long v) { this.maxAgeSecs = v; return this; }
            public Builder minVersions(int v) { this.minVersions = v; return this; }
            public Builder maxPayloadSize(long v) { this.maxPayloadSize = v; return this; }
            public Builder enableLineageTracking(boolean v) { this.enableLineageTracking = v; return this; }
            public Builder isPublic(boolean v) { this.isPublic = v; return this; }
            public TopicConfig build() {
                return new TopicConfig(maxVersions, maxAgeSecs, minVersions, maxPayloadSize, enableLineageTracking, isPublic);
            }
        }
    }

    public record TopicMetadata(String name, String description, long currentVersion,
            int subscriberCount, Instant createdAt, byte[] schemaBytes, TopicConfig config) {}

    public record ContextUpdate(String topic, long version, String handleId, String ipcHandle,
            long payloadSize, long timestampNanos, String lineageId, UpdateType updateType, byte[] schemaBytes) {}

    public enum UpdateType { SNAPSHOT, DELTA, DELETE, SCHEMA_CHANGE }

    public record Subscription(long id, String agentId, String topicPattern,
            Long fromVersion, Instant createdAt) {}

    public record PublishResult(long version, String ipcHandle, int subscribersNotified) {}
}

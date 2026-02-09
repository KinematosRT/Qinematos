package com.qinematos.persistence;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class XodusContextStore {

    private static final Logger LOG = Logger.getLogger(XodusContextStore.class);

    private static final String CONTEXTS_STORE = "contexts";
    private static final String METADATA_STORE = "metadata";
    private static final String LINEAGE_STORE = "lineage";
    private static final String TOPICS_STORE = "topics";
    private static final String VERSIONS_STORE = "versions";

    @ConfigProperty(name = "qinematos.xodus.data-dir", defaultValue = "./data/xodus")
    String dataDir;

    @ConfigProperty(name = "qinematos.xodus.memory-usage-percentage", defaultValue = "50")
    int memoryUsagePercentage;

    private Environment environment;
    private final Map<String, TopicInfo> topicCache = new ConcurrentHashMap<>();

    @PostConstruct
    void initialize() {
        try {
            Path dataPath = Path.of(dataDir);
            Files.createDirectories(dataPath);
            EnvironmentConfig config = new EnvironmentConfig()
                    .setLogDurableWrite(true)
                    .setMemoryUsagePercentage(memoryUsagePercentage)
                    .setGcEnabled(true)
                    .setGcMinUtilization(50);
            environment = Environments.newInstance(dataPath.toFile(), config);
            environment.executeInTransaction(txn -> {
                environment.openStore(CONTEXTS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
                environment.openStore(METADATA_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
                environment.openStore(LINEAGE_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
                environment.openStore(TOPICS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
                environment.openStore(VERSIONS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            });
            loadTopicCache();
            LOG.infof("XodusContextStore initialized at: %s", dataPath);
        } catch (IOException e) {
            LOG.errorf("Failed to initialize Xodus: %s", e.getMessage());
            throw new RuntimeException("Xodus initialization failed", e);
        }
    }

    @PreDestroy
    void shutdown() {
        if (environment != null) { environment.close(); }
        LOG.info("XodusContextStore shutdown complete");
    }

    public ContextVersion storeContext(String topic, long version, byte[] arrowPayload, Map<String, String> metadata) {
        String key = buildContextKey(topic, version);
        long timestampNanos = Instant.now().toEpochMilli() * 1_000_000;
        environment.executeInTransaction(txn -> {
            Store contextStore = environment.openStore(CONTEXTS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            contextStore.put(txn, StringBinding.stringToEntry(key), new ArrayByteIterable(arrowPayload));
            Store metadataStore = environment.openStore(METADATA_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            String metadataJson = encodeMetadata(topic, version, arrowPayload.length, timestampNanos, metadata);
            metadataStore.put(txn, StringBinding.stringToEntry(key), StringBinding.stringToEntry(metadataJson));
            Store topicStore = environment.openStore(TOPICS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            TopicInfo topicInfo = topicCache.getOrDefault(topic, new TopicInfo(topic, 0, 0, Instant.now()));
            topicInfo = new TopicInfo(topic, version, topicInfo.contextCount() + 1, topicInfo.createdAt());
            topicCache.put(topic, topicInfo);
            String topicJson = encodeTopicInfo(topicInfo);
            topicStore.put(txn, StringBinding.stringToEntry(topic), StringBinding.stringToEntry(topicJson));
            Store versionsStore = environment.openStore(VERSIONS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            String versionKey = topic + ":latest";
            versionsStore.put(txn, StringBinding.stringToEntry(versionKey), StringBinding.stringToEntry(String.valueOf(version)));
        });
        LOG.debugf("Stored context: topic=%s, version=%d, size=%d bytes", topic, version, arrowPayload.length);
        return new ContextVersion(topic, version, arrowPayload.length, timestampNanos, metadata);
    }

    public Optional<ContextWithPayload> getContext(String topic, Long version) {
        long targetVersion = version != null ? version : getLatestVersion(topic).orElse(-1L);
        if (targetVersion < 0) { return Optional.empty(); }
        String key = buildContextKey(topic, targetVersion);
        return environment.computeInReadonlyTransaction(txn -> {
            Store contextStore = environment.openStore(CONTEXTS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            ByteIterable payloadBytes = contextStore.get(txn, StringBinding.stringToEntry(key));
            if (payloadBytes == null) { return Optional.empty(); }
            Store metadataStore = environment.openStore(METADATA_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            ByteIterable metadataBytes = metadataStore.get(txn, StringBinding.stringToEntry(key));
            Map<String, String> metadata = metadataBytes != null
                    ? decodeMetadata(StringBinding.entryToString(metadataBytes)) : Map.of();
            byte[] payload = toByteArray(payloadBytes);
            return Optional.of(new ContextWithPayload(
                    new ContextVersion(topic, targetVersion, payload.length,
                            Instant.now().toEpochMilli() * 1_000_000, metadata), payload));
        });
    }

    public Optional<Long> getLatestVersion(String topic) {
        return environment.computeInReadonlyTransaction(txn -> {
            Store versionsStore = environment.openStore(VERSIONS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            String versionKey = topic + ":latest";
            ByteIterable versionBytes = versionsStore.get(txn, StringBinding.stringToEntry(versionKey));
            if (versionBytes == null) { return Optional.empty(); }
            return Optional.of(Long.parseLong(StringBinding.entryToString(versionBytes)));
        });
    }

    public List<ContextVersion> getVersionHistory(String topic, int limit) {
        return environment.computeInReadonlyTransaction(txn -> {
            Store metadataStore = environment.openStore(METADATA_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            List<ContextVersion> versions = new ArrayList<>();
            String prefix = topic + ":";
            try (Cursor cursor = metadataStore.openCursor(txn)) {
                ByteIterable startKey = StringBinding.stringToEntry(prefix);
                if (cursor.getSearchKeyRange(startKey) != null) {
                    do {
                        String key = StringBinding.entryToString(cursor.getKey());
                        if (!key.startsWith(prefix)) break;
                        String metadataJson = StringBinding.entryToString(cursor.getValue());
                        versions.add(decodeContextVersion(metadataJson));
                        if (versions.size() >= limit) break;
                    } while (cursor.getNext());
                }
            }
            versions.sort((a, b) -> Long.compare(b.version(), a.version()));
            return versions;
        });
    }

    public void storeLineage(String lineageId, String sourceSystem, String parentLineageId,
            String contextVersionId, Map<String, String> metadata) {
        environment.executeInTransaction(txn -> {
            Store lineageStore = environment.openStore(LINEAGE_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            String lineageJson = encodeLineage(lineageId, sourceSystem, parentLineageId, contextVersionId, metadata);
            lineageStore.put(txn, StringBinding.stringToEntry(lineageId), StringBinding.stringToEntry(lineageJson));
        });
        LOG.debugf("Stored lineage: %s -> %s", lineageId, contextVersionId);
    }

    public Optional<LineageInfo> getLineage(String lineageId) {
        return environment.computeInReadonlyTransaction(txn -> {
            Store lineageStore = environment.openStore(LINEAGE_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            ByteIterable lineageBytes = lineageStore.get(txn, StringBinding.stringToEntry(lineageId));
            if (lineageBytes == null) { return Optional.empty(); }
            return Optional.of(decodeLineage(StringBinding.entryToString(lineageBytes)));
        });
    }

    public List<TopicInfo> getAllTopics() { return new ArrayList<>(topicCache.values()); }

    public StorageStats getStats() {
        return environment.computeInReadonlyTransaction(txn -> {
            Store contextStore = environment.openStore(CONTEXTS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            Store lineageStore = environment.openStore(LINEAGE_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            return new StorageStats(topicCache.size(), contextStore.count(txn), lineageStore.count(txn), 0L, 0L);
        });
    }

    public int deleteOldVersions(String topic, int keepCount) {
        return environment.computeInTransaction(txn -> {
            Store contextStore = environment.openStore(CONTEXTS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            Store metadataStore = environment.openStore(METADATA_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            List<String> keysToDelete = new ArrayList<>();
            String prefix = topic + ":";
            List<Long> versions = new ArrayList<>();
            try (Cursor cursor = metadataStore.openCursor(txn)) {
                ByteIterable startKey = StringBinding.stringToEntry(prefix);
                if (cursor.getSearchKeyRange(startKey) != null) {
                    do {
                        String key = StringBinding.entryToString(cursor.getKey());
                        if (!key.startsWith(prefix)) break;
                        versions.add(Long.parseLong(key.substring(prefix.length())));
                    } while (cursor.getNext());
                }
            }
            versions.sort(Collections.reverseOrder());
            if (versions.size() > keepCount) {
                for (int i = keepCount; i < versions.size(); i++) {
                    keysToDelete.add(buildContextKey(topic, versions.get(i)));
                }
            }
            for (String key : keysToDelete) {
                contextStore.delete(txn, StringBinding.stringToEntry(key));
                metadataStore.delete(txn, StringBinding.stringToEntry(key));
            }
            return keysToDelete.size();
        });
    }

    private void loadTopicCache() {
        environment.executeInReadonlyTransaction(txn -> {
            Store topicStore = environment.openStore(TOPICS_STORE, StoreConfig.WITHOUT_DUPLICATES, txn);
            try (Cursor cursor = topicStore.openCursor(txn)) {
                while (cursor.getNext()) {
                    String topic = StringBinding.entryToString(cursor.getKey());
                    String topicJson = StringBinding.entryToString(cursor.getValue());
                    TopicInfo topicInfo = decodeTopicInfo(topicJson);
                    topicCache.put(topic, topicInfo);
                }
            }
        });
        LOG.infof("Loaded %d topics into cache", topicCache.size());
    }

    private String buildContextKey(String topic, long version) {
        return String.format("%s:%012d", topic, version);
    }

    private byte[] toByteArray(ByteIterable iterable) {
        byte[] unsafe = iterable.getBytesUnsafe();
        if (unsafe.length == iterable.getLength()) return unsafe;
        byte[] result = new byte[iterable.getLength()];
        System.arraycopy(unsafe, 0, result, 0, iterable.getLength());
        return result;
    }

    private String encodeMetadata(String topic, long version, int size, long timestamp, Map<String, String> metadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"topic\":\"").append(topic).append("\",");
        sb.append("\"version\":").append(version).append(",");
        sb.append("\"size\":").append(size).append(",");
        sb.append("\"timestamp\":").append(timestamp);
        if (metadata != null && !metadata.isEmpty()) {
            sb.append(",\"metadata\":{");
            boolean first = true;
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
                first = false;
            }
            sb.append("}");
        }
        sb.append("}");
        return sb.toString();
    }

    private Map<String, String> decodeMetadata(String json) {
        Map<String, String> result = new HashMap<>();
        int metadataStart = json.indexOf("\"metadata\":{");
        if (metadataStart >= 0) {
            int blockStart = json.indexOf("{", metadataStart + 10);
            int blockEnd = json.indexOf("}", blockStart);
            if (blockStart >= 0 && blockEnd > blockStart) {
                String metadataBlock = json.substring(blockStart + 1, blockEnd);
                String[] pairs = metadataBlock.split(",");
                for (String pair : pairs) {
                    String[] kv = pair.split(":");
                    if (kv.length == 2) {
                        String key = kv[0].trim().replace("\"", "");
                        String value = kv[1].trim().replace("\"", "");
                        if (!key.isEmpty()) result.put(key, value);
                    }
                }
            }
        }
        return result;
    }

    private ContextVersion decodeContextVersion(String json) {
        String topic = extractStringValue(json, "topic");
        long version = extractLongValue(json, "version");
        long size = extractLongValue(json, "size");
        long timestamp = extractLongValue(json, "timestamp");
        Map<String, String> metadata = decodeMetadata(json);
        return new ContextVersion(topic, version, size, timestamp, metadata);
    }

    private String encodeTopicInfo(TopicInfo info) {
        return String.format("{\"topic\":\"%s\",\"latestVersion\":%d,\"contextCount\":%d,\"createdAt\":\"%s\"}",
                info.topic(), info.latestVersion(), info.contextCount(), info.createdAt().toString());
    }

    private TopicInfo decodeTopicInfo(String json) {
        String topic = extractStringValue(json, "topic");
        long latestVersion = extractLongValue(json, "latestVersion");
        long contextCount = extractLongValue(json, "contextCount");
        String createdAtStr = extractStringValue(json, "createdAt");
        Instant createdAt = createdAtStr.isEmpty() ? Instant.now() : Instant.parse(createdAtStr);
        return new TopicInfo(topic, latestVersion, contextCount, createdAt);
    }

    private String encodeLineage(String lineageId, String sourceSystem, String parentLineageId,
            String contextVersionId, Map<String, String> metadata) {
        return String.format("{\"id\":\"%s\",\"source\":\"%s\",\"parent\":\"%s\",\"context\":\"%s\"}",
                lineageId, sourceSystem, parentLineageId != null ? parentLineageId : "", contextVersionId);
    }

    private LineageInfo decodeLineage(String json) {
        String lineageId = extractStringValue(json, "id");
        String sourceSystem = extractStringValue(json, "source");
        String parentLineageId = extractStringValue(json, "parent");
        String contextVersionId = extractStringValue(json, "context");
        return new LineageInfo(lineageId, sourceSystem,
                parentLineageId.isEmpty() ? null : parentLineageId, contextVersionId, Instant.now(), Map.of());
    }

    private String extractStringValue(String json, String key) {
        String pattern = "\"" + key + "\":\"";
        int start = json.indexOf(pattern);
        if (start < 0) return "";
        start += pattern.length();
        int end = json.indexOf("\"", start);
        if (end < 0) return "";
        return json.substring(start, end);
    }

    private long extractLongValue(String json, String key) {
        String pattern = "\"" + key + "\":";
        int start = json.indexOf(pattern);
        if (start < 0) return 0;
        start += pattern.length();
        int end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) end++;
        if (end == start) return 0;
        try { return Long.parseLong(json.substring(start, end)); }
        catch (NumberFormatException e) { return 0; }
    }

    public record ContextVersion(String topic, long version, long sizeBytes,
            long timestampNanos, Map<String, String> metadata) {}
    public record ContextWithPayload(ContextVersion version, byte[] payload) {}
    public record TopicInfo(String topic, long latestVersion, long contextCount, Instant createdAt) {}
    public record LineageInfo(String lineageId, String sourceSystem, String parentLineageId,
            String contextVersionId, Instant timestamp, Map<String, String> metadata) {}
    public record StorageStats(int topicCount, long contextCount, long lineageCount,
            long bytesWritten, long bytesRead) {}
}

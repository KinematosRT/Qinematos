package com.qinematos.core.ipc;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ZeroCopyIPCEngine - The "Plain" Component of the RVP Framework
 *
 * Uses Java FFM API to achieve zero-copy data transfer between
 * the Orchestrator and Agents via memory-mapped files.
 *
 * Performance Target: less than 5ms latency for zero-copy context delivery
 */
@ApplicationScoped
public class ZeroCopyIPCEngine {

    private static final Logger LOG = Logger.getLogger(ZeroCopyIPCEngine.class);

    private static final long HEADER_SIZE = 32;
    private static final long MAGIC_NUMBER = 0x5152494E454D4154L; // "QRINEMAT"

    @ConfigProperty(name = "qinematos.ipc.base-path", defaultValue = "/dev/shm/qinematos")
    String basePath;

    @ConfigProperty(name = "qinematos.ipc.max-segment-size", defaultValue = "1073741824")
    long maxSegmentSize;

    @ConfigProperty(name = "qinematos.ipc.enable-lineage", defaultValue = "true")
    boolean enableLineage;

    private final Map<String, MappedContext> activeSegments = new ConcurrentHashMap<>();
    private final AtomicLong globalVersion = new AtomicLong(0);
    private Arena sharedArena;

    @PostConstruct
    void initialize() {
        this.sharedArena = Arena.ofShared();
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            this.basePath = System.getProperty("java.io.tmpdir") + "/qinematos";
        }
        try {
            Path baseDir = Path.of(basePath, "contexts");
            Files.createDirectories(baseDir);
            LOG.infof("ZeroCopyIPCEngine initialized at: %s", baseDir);
        } catch (IOException e) {
            LOG.errorf("Failed to initialize IPC base directory: %s", e.getMessage());
            throw new RuntimeException("IPC initialization failed", e);
        }
    }

    @PreDestroy
    void shutdown() {
        LOG.info("Shutting down ZeroCopyIPCEngine...");
        activeSegments.values().forEach(this::releaseSegment);
        activeSegments.clear();
        if (sharedArena != null) {
            sharedArena.close();
        }
        LOG.info("ZeroCopyIPCEngine shutdown complete");
    }

    public IPCHandle writeArrowPayload(String topic, byte[] arrowPayload, String lineageId) {
        long version = globalVersion.incrementAndGet();
        String handleId = generateHandleId(topic, version);
        try {
            Path topicDir = Path.of(basePath, "contexts", sanitizeTopic(topic));
            Files.createDirectories(topicDir);
            Path filePath = topicDir.resolve(version + ".arrow");
            long totalSize = HEADER_SIZE + arrowPayload.length;
            if (totalSize > maxSegmentSize) {
                throw new IllegalArgumentException("Payload size " + totalSize + " exceeds maximum " + maxSegmentSize);
            }
            try (FileChannel channel = FileChannel.open(filePath,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment segment = channel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize, sharedArena);
                writeHeader(segment, version, arrowPayload.length);
                MemorySegment payloadSegment = segment.asSlice(HEADER_SIZE, arrowPayload.length);
                payloadSegment.copyFrom(MemorySegment.ofArray(arrowPayload));
                segment.force();
                IPCHandle handle = new IPCHandle(handleId, filePath.toString(), version, topic,
                        arrowPayload.length, Instant.now().toEpochMilli() * 1_000_000,
                        lineageId != null ? lineageId : generateLineageId());
                activeSegments.put(handleId, new MappedContext(segment, handle, filePath));
                LOG.debugf("Written Arrow payload: topic=%s, version=%d, size=%d bytes, path=%s",
                        topic, version, arrowPayload.length, filePath);
                return handle;
            }
        } catch (IOException e) {
            LOG.errorf("Failed to write Arrow payload: %s", e.getMessage());
            throw new RuntimeException("IPC write failed", e);
        }
    }

    public ByteBuffer readArrowPayload(IPCHandle handle) {
        MappedContext context = activeSegments.get(handle.handleId());
        if (context == null) {
            return mapExistingFile(handle);
        }
        MemorySegment payloadSegment = context.segment().asSlice(HEADER_SIZE, handle.payloadSize());
        return payloadSegment.asByteBuffer();
    }

    private ByteBuffer mapExistingFile(IPCHandle handle) {
        try {
            Path filePath = Path.of(handle.filePath());
            if (!Files.exists(filePath)) {
                throw new IllegalStateException("IPC file not found: " + handle.filePath());
            }
            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                long totalSize = HEADER_SIZE + handle.payloadSize();
                MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, totalSize, sharedArena);
                validateHeader(segment, handle.version());
                activeSegments.put(handle.handleId(), new MappedContext(segment, handle, filePath));
                MemorySegment payloadSegment = segment.asSlice(HEADER_SIZE, handle.payloadSize());
                return payloadSegment.asByteBuffer();
            }
        } catch (IOException e) {
            LOG.errorf("Failed to map Arrow file: %s", e.getMessage());
            throw new RuntimeException("IPC read failed", e);
        }
    }

    private void writeHeader(MemorySegment segment, long version, long payloadSize) {
        segment.set(ValueLayout.JAVA_LONG, 0, MAGIC_NUMBER);
        segment.set(ValueLayout.JAVA_LONG, 8, version);
        segment.set(ValueLayout.JAVA_LONG, 16, HEADER_SIZE);
        segment.set(ValueLayout.JAVA_LONG, 24, payloadSize);
    }

    private void validateHeader(MemorySegment segment, long expectedVersion) {
        long magic = segment.get(ValueLayout.JAVA_LONG, 0);
        long version = segment.get(ValueLayout.JAVA_LONG, 8);
        if (magic != MAGIC_NUMBER) {
            throw new IllegalStateException("Invalid IPC file: wrong magic number");
        }
        if (version != expectedVersion) {
            throw new IllegalStateException("Version mismatch: expected " + expectedVersion + ", got " + version);
        }
    }

    public void releaseSegment(String handleId, boolean deleteFile) {
        MappedContext context = activeSegments.remove(handleId);
        if (context != null) {
            releaseSegment(context);
            if (deleteFile) {
                try {
                    Files.deleteIfExists(context.filePath());
                } catch (IOException e) {
                    LOG.warnf("Failed to delete IPC file: %s", context.filePath());
                }
            }
        }
    }

    private void releaseSegment(MappedContext context) {
        LOG.debugf("Released segment: %s", context.handle().handleId());
    }

    public IPCStats getStats() {
        long totalMappedBytes = activeSegments.values().stream()
                .mapToLong(ctx -> ctx.handle().payloadSize()).sum();
        return new IPCStats(activeSegments.size(), totalMappedBytes, globalVersion.get());
    }

    private String generateHandleId(String topic, long version) {
        return String.format("%s_%d_%s", sanitizeTopic(topic), version, UUID.randomUUID().toString().substring(0, 8));
    }

    private String generateLineageId() {
        return "lin_" + UUID.randomUUID().toString();
    }

    private String sanitizeTopic(String topic) {
        return topic.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    public record IPCHandle(String handleId, String filePath, long version, String topic,
            long payloadSize, long timestampNanos, String lineageId) {}

    private record MappedContext(MemorySegment segment, IPCHandle handle, Path filePath) {}

    public record IPCStats(int activeSegments, long totalMappedBytes, long currentVersion) {}
}

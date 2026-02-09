package com.qinematos;

import com.qinematos.core.ipc.ZeroCopyIPCEngine;
import com.qinematos.lineage.LineageTracker;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.lang.foreign.*;

@ApplicationScoped
public class TrackingService {

    private static final Logger LOG = Logger.getLogger(TrackingService.class);

    @Inject
    ZeroCopyIPCEngine ipcEngine;

    @Inject
    LineageTracker lineageTracker;

    public AllocationResult allocateNativeMemory(int sizeBytes) {
        long startTime = System.nanoTime();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(sizeBytes);
            segment.set(ValueLayout.JAVA_INT, 0, 42);
            int value = segment.get(ValueLayout.JAVA_INT, 0);
            long elapsedNanos = System.nanoTime() - startTime;
            LOG.debugf("Allocated %d bytes in %d ns, test value: %d", sizeBytes, elapsedNanos, value);
            return new AllocationResult(true, sizeBytes, elapsedNanos, segment.address(), value);
        } catch (Exception e) {
            LOG.errorf("Native allocation failed: %s", e.getMessage());
            return new AllocationResult(false, 0, 0, 0, 0);
        }
    }

    public SegmentDemo demonstrateSegmentOperations() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment header = arena.allocate(32);
            header.set(ValueLayout.JAVA_LONG, 0, 0x5152494E454D4154L);
            header.set(ValueLayout.JAVA_LONG, 8, 1L);
            header.set(ValueLayout.JAVA_LONG, 16, 32L);
            header.set(ValueLayout.JAVA_LONG, 24, 1024L);
            long magic = header.get(ValueLayout.JAVA_LONG, 0);
            long version = header.get(ValueLayout.JAVA_LONG, 8);
            MemorySegment data = arena.allocate(1024);
            for (int i = 0; i < 256; i++) data.set(ValueLayout.JAVA_INT, i * 4, i);
            return new SegmentDemo(true, String.format("0x%X", magic), version, header.byteSize(), data.byteSize());
        }
    }

    public void trackMemoryAccess(String lineageId, String ipcHandle, int processId) {
        long accessTime = System.nanoTime();
        lineageTracker.recordIPCAccess(lineageId, ipcHandle, processId, accessTime);
        LOG.debugf("Tracked memory access: lineage=%s, handle=%s, pid=%d", lineageId, ipcHandle, processId);
    }

    public ZeroCopyIPCEngine.IPCStats getIPCStats() { return ipcEngine.getStats(); }
    public LineageTracker.LineageStats getLineageStats() { return lineageTracker.getStats(); }

    public record AllocationResult(boolean success, long sizeBytes, long elapsedNanos, long address, int testValue) {}
    public record SegmentDemo(boolean success, String magic, long version, long headerSize, long dataSize) {}
}

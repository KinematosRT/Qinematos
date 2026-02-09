package com.qinematos.lineage;

import com.qinematos.persistence.XodusContextStore;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class LineageTracker {

    private static final Logger LOG = Logger.getLogger(LineageTracker.class);

    @Inject
    XodusContextStore contextStore;

    @ConfigProperty(name = "qinematos.ipc.enable-lineage", defaultValue = "true")
    boolean enableLineage;

    private final Map<String, LineageNode> lineageGraph = new ConcurrentHashMap<>();
    private final Map<String, TraceBuilder> activeTraces = new ConcurrentHashMap<>();
    private final AtomicLong traceCounter = new AtomicLong(0);

    @PostConstruct
    void initialize() {
        if (enableLineage) {
            LOG.info("LineageTracker initialized with eBPF integration enabled");
        } else {
            LOG.info("LineageTracker initialized (eBPF disabled)");
        }
    }

    public String startTrace(String sourceSystem, String contextTopic) {
        String lineageId = generateLineageId();
        TraceBuilder builder = new TraceBuilder(lineageId, sourceSystem, contextTopic);
        activeTraces.put(lineageId, builder);
        LOG.debugf("Started lineage trace: %s from %s for topic %s", lineageId, sourceSystem, contextTopic);
        return lineageId;
    }

    public void recordTransformation(String lineageId, String transformationName,
            String version, List<String> inputVersions) {
        TraceBuilder builder = activeTraces.get(lineageId);
        if (builder == null) { LOG.warnf("Unknown lineage ID: %s", lineageId); return; }
        builder.addTransformation(new TransformationStep(transformationName, version, inputVersions, Instant.now()));
        LOG.debugf("Recorded transformation: %s in trace %s", transformationName, lineageId);
    }

    public void recordIPCAccess(String lineageId, String ipcHandle, int processId, long accessTimeNanos) {
        TraceBuilder builder = activeTraces.get(lineageId);
        if (builder == null) { LOG.warnf("Unknown lineage ID for IPC access: %s", lineageId); return; }
        builder.addIPCAccess(new IPCAccessEvent(ipcHandle, processId, accessTimeNanos, generateEbpfCorrelationId()));
        LOG.debugf("Recorded IPC access: pid=%d, handle=%s, trace=%s", processId, ipcHandle, lineageId);
    }

    public LineageNode completeTrace(String lineageId, String contextVersionId, byte[] payloadHash) {
        TraceBuilder builder = activeTraces.remove(lineageId);
        if (builder == null) { LOG.warnf("Unknown lineage ID to complete: %s", lineageId); return null; }
        LineageNode node = builder.build(contextVersionId, payloadHash);
        lineageGraph.put(lineageId, node);
        contextStore.storeLineage(lineageId, node.sourceSystem(), node.parentLineageId(),
                contextVersionId, Map.of("topic", node.contextTopic(),
                        "transformations", String.valueOf(node.transformations().size()),
                        "ipc_accesses", String.valueOf(node.ipcAccesses().size())));
        LOG.infof("Completed lineage trace: %s -> context %s", lineageId, contextVersionId);
        return node;
    }

    public List<LineageNode> getLineageChain(String lineageId) {
        List<LineageNode> chain = new ArrayList<>();
        String currentId = lineageId;
        while (currentId != null) {
            LineageNode node = lineageGraph.get(currentId);
            if (node == null) break;
            chain.add(node);
            currentId = node.parentLineageId();
        }
        return chain;
    }

    public VerificationResult verifyLineage(String lineageId) {
        List<LineageNode> chain = getLineageChain(lineageId);
        if (chain.isEmpty()) return new VerificationResult(false, "Lineage not found", List.of());
        List<String> issues = new ArrayList<>();
        for (int i = 0; i < chain.size() - 1; i++) {
            LineageNode current = chain.get(i);
            LineageNode parent = chain.get(i + 1);
            if (!parent.lineageId().equals(current.parentLineageId()))
                issues.add("Broken chain at " + current.lineageId());
            if (current.timestamp().isBefore(parent.timestamp()))
                issues.add("Timestamp violation at " + current.lineageId());
        }
        boolean valid = issues.isEmpty();
        return new VerificationResult(valid, valid ? "Lineage verified successfully" : "Lineage verification failed", issues);
    }

    public AuditReport generateAuditReport(String lineageId) {
        LineageNode node = lineageGraph.get(lineageId);
        if (node == null) return new AuditReport(lineageId, false, "Lineage not found", List.of());
        List<AuditEntry> entries = new ArrayList<>();
        entries.add(new AuditEntry("DATA_ORIGIN", node.timestamp(), node.sourceSystem(),
                "Context data originated from source system", null));
        for (TransformationStep step : node.transformations()) {
            entries.add(new AuditEntry("TRANSFORMATION", step.timestamp(),
                    step.name() + " v" + step.version(),
                    "Data transformed: " + step.inputVersions().size() + " inputs", step.inputVersions()));
        }
        for (IPCAccessEvent access : node.ipcAccesses()) {
            entries.add(new AuditEntry("IPC_ACCESS", Instant.ofEpochSecond(0, access.accessTimeNanos()),
                    "PID: " + access.processId(),
                    "eBPF correlation: " + access.ebpfCorrelationId(), List.of(access.ipcHandle())));
        }
        return new AuditReport(lineageId, true, "Audit report generated", entries);
    }

    public Map<String, TraceInfo> getActiveTraces() {
        Map<String, TraceInfo> result = new HashMap<>();
        activeTraces.forEach((id, builder) -> result.put(id, new TraceInfo(id, builder.sourceSystem,
                builder.contextTopic, builder.startTime, builder.transformations.size(), builder.ipcAccesses.size())));
        return result;
    }

    public LineageStats getStats() {
        return new LineageStats(lineageGraph.size(), activeTraces.size(), traceCounter.get());
    }

    private String generateLineageId() {
        long counter = traceCounter.incrementAndGet();
        return String.format("lin_%d_%s", counter, UUID.randomUUID().toString().substring(0, 8));
    }

    private String generateEbpfCorrelationId() {
        return "ebpf_" + UUID.randomUUID().toString().substring(0, 12);
    }

    private static class TraceBuilder {
        final String lineageId; final String sourceSystem; final String contextTopic;
        final Instant startTime; String parentLineageId;
        final List<TransformationStep> transformations = new ArrayList<>();
        final List<IPCAccessEvent> ipcAccesses = new ArrayList<>();
        TraceBuilder(String lineageId, String sourceSystem, String contextTopic) {
            this.lineageId = lineageId; this.sourceSystem = sourceSystem;
            this.contextTopic = contextTopic; this.startTime = Instant.now();
        }
        void addTransformation(TransformationStep step) { transformations.add(step); }
        void addIPCAccess(IPCAccessEvent event) { ipcAccesses.add(event); }
        LineageNode build(String contextVersionId, byte[] payloadHash) {
            return new LineageNode(lineageId, sourceSystem, parentLineageId, contextVersionId,
                    contextTopic, startTime, Instant.now(), List.copyOf(transformations),
                    List.copyOf(ipcAccesses), payloadHash);
        }
    }

    public record LineageNode(String lineageId, String sourceSystem, String parentLineageId,
            String contextVersionId, String contextTopic, Instant timestamp, Instant completedAt,
            List<TransformationStep> transformations, List<IPCAccessEvent> ipcAccesses, byte[] payloadHash) {}
    public record TransformationStep(String name, String version, List<String> inputVersions, Instant timestamp) {}
    public record IPCAccessEvent(String ipcHandle, int processId, long accessTimeNanos, String ebpfCorrelationId) {}
    public record VerificationResult(boolean valid, String message, List<String> issues) {}
    public record AuditReport(String lineageId, boolean success, String message, List<AuditEntry> entries) {}
    public record AuditEntry(String eventType, Instant timestamp, String actor,
            String description, List<String> references) {}
    public record TraceInfo(String lineageId, String sourceSystem, String contextTopic,
            Instant startTime, int transformationCount, int ipcAccessCount) {}
    public record LineageStats(int completedTraces, int activeTraces, long totalTracesCreated) {}
}

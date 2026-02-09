package com.qinematos;

import com.qinematos.core.topic.ContextTopicManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.smallrye.mutiny.Multi;
import org.jboss.logging.Logger;

import java.util.*;

@ApplicationScoped
public class PropagationService {

    private static final Logger LOG = Logger.getLogger(PropagationService.class);

    @Inject
    ContextGraphService contextGraphService;

    @Inject
    ContextTopicManager topicManager;

    public void propagateContext(String sourceAgentId, Map<String, Object> contextData) {
        Set<String> relatedAgents = contextGraphService.getRelatedAgents(sourceAgentId);
        if (relatedAgents.isEmpty()) {
            LOG.debugf("No related agents for %s, skipping propagation", sourceAgentId);
            return;
        }
        LOG.infof("Propagating context from %s to %d related agents", sourceAgentId, relatedAgents.size());
        List<Map<String, Object>> records = new ArrayList<>();
        records.add(Map.of("source_agent_id", sourceAgentId, "context_data", contextData.toString(),
                "target_agents", String.join(",", relatedAgents), "timestamp_nanos", System.nanoTime()));
        try {
            topicManager.publish("system.propagation", records, null);
        } catch (Exception e) {
            LOG.debugf("Propagation topic not available: %s", e.getMessage());
        }
        LOG.infof("Context propagated from %s to %d agents", sourceAgentId, relatedAgents.size());
    }

    public Multi<PropagationUpdate> subscribeToPropagations(String agentId) {
        return topicManager.subscribe(agentId, "system.propagation", null)
                .map(update -> new PropagationUpdate(update.topic(), update.version(),
                        update.handleId(), update.timestampNanos()));
    }

    public PropagationStats getStats() {
        var graphStats = contextGraphService.getStats();
        return new PropagationStats(graphStats.agentCount(), graphStats.edgeCount(), graphStats.traceCount());
    }

    public record PropagationUpdate(String topic, long version, String handleId, long timestampNanos) {}
    public record PropagationStats(int agentCount, int edgeCount, int traceCount) {}
}

package com.qinematos;

import com.qinematos.core.topic.ContextTopicManager;
import com.qinematos.lineage.LineageTracker;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class ContextGraphService {

    private static final Logger LOG = Logger.getLogger(ContextGraphService.class);

    @Inject
    ContextTopicManager topicManager;

    @Inject
    LineageTracker lineageTracker;

    private final Map<String, Agent> agents = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> contextGraph = new ConcurrentHashMap<>();

    public Agent registerAgent(String agentId, String agentType, Map<String, String> metadata) {
        Agent agent = new Agent(agentId, agentType, metadata != null ? metadata : Map.of(),
                Instant.now(), AgentStatus.ACTIVE, new ArrayList<>());
        agents.put(agentId, agent);
        contextGraph.put(agentId, ConcurrentHashMap.newKeySet());
        try {
            topicManager.publish("system.agents", List.of(Map.of("agent_id", agentId,
                    "agent_type", agentType, "status", "REGISTERED",
                    "timestamp_nanos", Instant.now().toEpochMilli() * 1_000_000)), null);
        } catch (Exception e) { LOG.warnf("Failed to publish agent registration: %s", e.getMessage()); }
        LOG.infof("Registered agent: %s (type: %s)", agentId, agentType);
        return agent;
    }

    public void addTrace(String agentId, String content, String location, String lineageId) {
        Agent agent = agents.get(agentId);
        if (agent == null) { LOG.warnf("Unknown agent: %s", agentId); return; }
        String traceLineageId = lineageId;
        if (traceLineageId == null) {
            traceLineageId = lineageTracker.startTrace("agent:" + agentId, "agent.context");
        }
        Trace trace = new Trace(UUID.randomUUID().toString(), content, Instant.now(), location, traceLineageId);
        agent.traces().add(trace);
        LOG.debugf("Added trace to agent %s: %s", agentId, content);
    }

    public void linkAgents(String sourceAgentId, String targetAgentId) {
        if (!agents.containsKey(sourceAgentId) || !agents.containsKey(targetAgentId)) {
            LOG.warnf("Cannot link agents: one or both not found"); return;
        }
        contextGraph.computeIfAbsent(sourceAgentId, k -> ConcurrentHashMap.newKeySet()).add(targetAgentId);
        LOG.debugf("Linked agents: %s -> %s", sourceAgentId, targetAgentId);
    }

    public Set<String> getRelatedAgents(String agentId) {
        return contextGraph.getOrDefault(agentId, Set.of());
    }

    public Optional<Agent> getAgent(String agentId) { return Optional.ofNullable(agents.get(agentId)); }
    public Collection<Agent> getAllAgents() { return agents.values(); }

    public void updateAgentStatus(String agentId, AgentStatus status) {
        Agent agent = agents.get(agentId);
        if (agent != null) {
            agents.put(agentId, new Agent(agent.id(), agent.type(), agent.metadata(),
                    agent.registeredAt(), status, agent.traces()));
            LOG.debugf("Updated agent %s status to %s", agentId, status);
        }
    }

    public void removeAgent(String agentId) {
        agents.remove(agentId);
        contextGraph.remove(agentId);
        contextGraph.values().forEach(set -> set.remove(agentId));
        LOG.infof("Removed agent: %s", agentId);
    }

    public GraphStats getStats() {
        int totalEdges = contextGraph.values().stream().mapToInt(Set::size).sum();
        int totalTraces = agents.values().stream().mapToInt(a -> a.traces().size()).sum();
        return new GraphStats(agents.size(), totalEdges, totalTraces);
    }

    public record Agent(String id, String type, Map<String, String> metadata,
            Instant registeredAt, AgentStatus status, List<Trace> traces) {}
    public record Trace(String id, String content, Instant timestamp, String location, String lineageId) {}
    public enum AgentStatus { ACTIVE, IDLE, SUSPENDED, TERMINATED }
    public record GraphStats(int agentCount, int edgeCount, int traceCount) {}
}

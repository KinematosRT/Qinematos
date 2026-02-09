package com.qinematos;

import com.qinematos.core.arrow.ArrowContextSerializer;
import com.qinematos.core.ipc.ZeroCopyIPCEngine;
import com.qinematos.core.topic.ContextTopicManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class InteropService {

    private static final Logger LOG = Logger.getLogger(InteropService.class);

    @Inject
    ContextTopicManager topicManager;

    @Inject
    ArrowContextSerializer arrowSerializer;

    @Inject
    ZeroCopyIPCEngine ipcEngine;

    public ConnectionResult connectToFramework(String frameworkName, Map<String, String> config) {
        LOG.infof("Connecting to external framework: %s", frameworkName);
        String topicName = "framework." + frameworkName.toLowerCase();
        try {
            topicManager.createTopic(ContextTopicManager.TopicDefinition.builder()
                    .name(topicName).description("Integration topic for " + frameworkName)
                    .schema(arrowSerializer.getSchema("agent.context").orElse(null))
                    .config(ContextTopicManager.TopicConfig.defaultConfig()).build());
            LOG.infof("Created integration topic: %s", topicName);
        } catch (IllegalStateException e) {
            LOG.debugf("Topic already exists: %s", topicName);
        }
        return new ConnectionResult(true, frameworkName, topicName, getConnectionEndpoint(frameworkName));
    }

    private String getConnectionEndpoint(String frameworkName) {
        return switch (frameworkName.toLowerCase()) {
            case "langchain" -> "grpc://localhost:9000/watch?topic=framework.langchain";
            case "autogpt" -> "grpc://localhost:9000/watch?topic=framework.autogpt";
            case "crewai" -> "grpc://localhost:9000/watch?topic=framework.crewai";
            default -> "grpc://localhost:9000/watch?topic=framework." + frameworkName.toLowerCase();
        };
    }

    public PushResult pushContext(String frameworkName, List<Map<String, Object>> contextData) {
        String topicName = "framework." + frameworkName.toLowerCase();
        try {
            var result = topicManager.publish(topicName, contextData, null);
            return new PushResult(true, result.version(), result.ipcHandle(), result.subscribersNotified());
        } catch (Exception e) {
            LOG.errorf("Failed to push context to %s: %s", frameworkName, e.getMessage());
            return new PushResult(false, 0, null, 0);
        }
    }

    public Map<String, IntegrationStatus> getIntegrationStatus() {
        var topics = topicManager.listTopics("framework.*");
        return topics.stream().collect(java.util.stream.Collectors.toMap(
                t -> t.name().replace("framework.", ""),
                t -> new IntegrationStatus(t.name(), t.currentVersion(), t.subscriberCount(), t.subscriberCount() > 0)));
    }

    public record ConnectionResult(boolean success, String frameworkName, String topicName, String endpoint) {}
    public record PushResult(boolean success, long version, String ipcHandle, int subscribersNotified) {}
    public record IntegrationStatus(String topicName, long currentVersion, int subscriberCount, boolean active) {}
}

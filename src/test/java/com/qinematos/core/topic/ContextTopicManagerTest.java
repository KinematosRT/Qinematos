package com.qinematos.core.topic;

import com.qinematos.core.topic.ContextTopicManager.*;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ContextTopicManager - the "Recent" component of RVP framework.
 *
 * Validates topic-based pub/sub for deterministic context distribution
 * between agent sessions.
 */
@QuarkusTest
class ContextTopicManagerTest {

    @Inject
    ContextTopicManager topicManager;

    @Test
    void testSystemTopicsAreCreated() {
        // Verify standard system topics exist
        assertTrue(topicManager.getTopic("system.health").isPresent(),
                "system.health topic should exist");
        assertTrue(topicManager.getTopic("system.agents").isPresent(),
                "system.agents topic should exist");
        assertTrue(topicManager.getTopic("system.lineage").isPresent(),
                "system.lineage topic should exist");
    }

    @Test
    void testContextDistributionTopicsExist() {
        // Verify context distribution topics for agent sessions
        assertTrue(topicManager.getTopic("session.knowledge_base").isPresent(),
                "session.knowledge_base topic should exist for past implementations");
        assertTrue(topicManager.getTopic("session.communication").isPresent(),
                "session.communication topic should exist for future session guidance");
        assertTrue(topicManager.getTopic("session.external_provider").isPresent(),
                "session.external_provider topic should exist for common database");
        assertTrue(topicManager.getTopic("session.discovery").isPresent(),
                "session.discovery topic should exist for agent capabilities");
    }

    @Test
    void testKnowledgeBaseTopicConfiguration() {
        TopicMetadata knowledgeBase = topicManager.getTopic("session.knowledge_base").orElseThrow();

        assertTrue(knowledgeBase.config().enableLineageTracking(),
                "Knowledge base should have lineage tracking enabled");
        assertEquals(10000, knowledgeBase.config().maxVersions(),
                "Knowledge base should have extensive version history");
        assertTrue(knowledgeBase.config().isPublic(),
                "Knowledge base should be public for all agents");
    }

    @Test
    void testListTopicsWithPattern() {
        // List all system topics
        List<TopicMetadata> systemTopics = topicManager.listTopics("system.*");
        assertTrue(systemTopics.size() >= 3,
                "Should have at least 3 system topics");
        assertTrue(systemTopics.stream().allMatch(t -> t.name().startsWith("system.")),
                "All topics should match system.* pattern");

        // List all session topics
        List<TopicMetadata> sessionTopics = topicManager.listTopics("session.*");
        assertTrue(sessionTopics.size() >= 4,
                "Should have at least 4 session topics");
        assertTrue(sessionTopics.stream().allMatch(t -> t.name().startsWith("session.")),
                "All topics should match session.* pattern");
    }

    @Test
    void testPublishToTopic() {
        // Publish test data to system.agents topic
        List<Map<String, Object>> records = List.of(
                Map.of(
                        "agent_id", "test-publisher",
                        "context_key", "capability",
                        "context_value", "data-processing",
                        "data_type", "string",
                        "timestamp_nanos", System.nanoTime(),
                        "version", 1L,
                        "lineage_id", "test-lineage-001",
                        "source_system", "test"));

        PublishResult result = topicManager.publish("system.agents", records, "test-lineage-001");

        assertTrue(result.version() > 0, "Version should be positive");
        assertNotNull(result.ipcHandle(), "IPC handle should be set");
    }

    @Test
    void testSubscribeToTopic() {
        String agentId = "test-subscriber-" + System.currentTimeMillis();

        // Subscribe to system.agents topic
        Multi<ContextUpdate> subscription = topicManager.subscribe(agentId, "system.agents", null);

        assertNotNull(subscription, "Subscription should not be null");

        // Verify subscriber count is tracked
        int subscriberCount = topicManager.getSubscriberCount("system.agents");
        assertTrue(subscriberCount >= 0, "Subscriber count should be non-negative");

        // Start listening (don't block)
        subscription.subscribe().with(
                update -> {
                    /* process update */ },
                error -> fail("Subscription error: " + error.getMessage()));
    }

    @Test
    void testWildcardPatternMatching() {
        // Test multi-level wildcard
        List<TopicMetadata> allTopics = topicManager.listTopics("**");
        assertTrue(allTopics.size() >= 7,
                "Should have at least 7 topics (3 system + 4 session)");

        // Test single-level wildcard
        List<TopicMetadata> sessionTopics = topicManager.listTopics("session.*");
        assertEquals(4, sessionTopics.size(),
                "Should have exactly 4 session topics with single-level wildcard");
    }

    @Test
    void testPublishToKnowledgeBase() {
        // Simulate publishing a past implementation finding
        List<Map<String, Object>> knowledgeRecord = List.of(
                Map.of(
                        "agent_id", "code-analysis-agent",
                        "context_key", "implementation.pattern",
                        "context_value", "{\"pattern\":\"repository-pattern\",\"files\":[\"UserRepository.java\"]}",
                        "data_type", "json",
                        "timestamp_nanos", System.nanoTime(),
                        "version", 1L,
                        "lineage_id", "kb-001",
                        "source_system", "code-analyzer"));

        PublishResult result = topicManager.publish("session.knowledge_base", knowledgeRecord, "kb-001");

        assertTrue(result.version() > 0, "Should successfully publish to knowledge base");
    }

    @Test
    void testPublishSessionCommunication() {
        // Simulate publishing guidance for future sessions
        List<Map<String, Object>> communicationRecord = List.of(
                Map.of(
                        "agent_id", "planning-agent",
                        "context_key", "pending.task",
                        "context_value",
                        "{\"task\":\"implement-caching\",\"priority\":\"high\",\"context\":\"performance issue found\"}",
                        "data_type", "json",
                        "timestamp_nanos", System.nanoTime(),
                        "version", 1L,
                        "lineage_id", "comm-001",
                        "source_system", "planner"));

        PublishResult result = topicManager.publish("session.communication", communicationRecord, "comm-001");

        assertTrue(result.version() > 0, "Should successfully publish session communication");
    }

    @Test
    void testCreateCustomTopic() {
        String customTopicName = "custom.test.topic." + System.currentTimeMillis();

        TopicMetadata created = topicManager.createTopic(TopicDefinition.builder()
                .name(customTopicName)
                .description("Custom test topic")
                .config(TopicConfig.builder()
                        .maxVersions(50)
                        .enableLineageTracking(true)
                        .build())
                .build());

        assertEquals(customTopicName, created.name());
        assertTrue(topicManager.getTopic(customTopicName).isPresent());
    }
}

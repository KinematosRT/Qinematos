package com.qinematos.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;

/**
 * Integration tests for ContextOrchestratorResource REST API.
 *
 * Tests the main API endpoints for the Data Base Plane.
 */
@QuarkusTest
class ContextOrchestratorResourceTest {

    @Test
    void testHealthEndpoint() {
        given()
                .when().get("/api/v1/health")
                .then()
                .statusCode(200)
                .body("status", equalTo("healthy"))
                .body("version", notNullValue())
                .body("rvp_framework.recent", notNullValue())
                .body("rvp_framework.vulgate", notNullValue())
                .body("rvp_framework.plain", notNullValue());
    }

    @Test
    void testInfoEndpoint() {
        given()
                .when().get("/api/v1/info")
                .then()
                .statusCode(200)
                .body("name", equalTo("Qinematos Data Base Plane"))
                .body("version", notNullValue())
                .body("rvp_framework", notNullValue())
                .body("endpoints.rest", equalTo("/api/v1"))
                .body("endpoints.grpc", notNullValue());
    }

    @Test
    void testListTopics() {
        given()
                .when().get("/api/v1/topics")
                .then()
                .statusCode(200)
                .body("topics", notNullValue())
                .body("count", notNullValue());
    }

    @Test
    void testListSchemas() {
        given()
                .when().get("/api/v1/schemas")
                .then()
                .statusCode(200)
                .body("schemas", notNullValue());
    }

    @Test
    void testGetIPCStats() {
        given()
                .when().get("/api/v1/ipc/stats")
                .then()
                .statusCode(200)
                .body("active_segments", notNullValue())
                .body("total_mapped_bytes", notNullValue())
                .body("current_version", notNullValue());
    }

    @Test
    void testGetStorageStats() {
        given()
                .when().get("/api/v1/storage/stats")
                .then()
                .statusCode(200)
                .body("topic_count", notNullValue())
                .body("context_count", notNullValue());
    }

    @Test
    void testGetNonExistentTopic() {
        given()
                .when().get("/api/v1/topics/nonexistent.topic")
                .then()
                .statusCode(404)
                .body("error", containsString("not found"));
    }

    @Test
    void testCreateTopic() {
        given()
                .contentType("application/json")
                .body("""
                        {
                            "name": "test.integration.topic",
                            "description": "Integration test topic",
                            "schemaType": "generic.keyvalue",
                            "maxVersions": 10,
                            "enableLineageTracking": true,
                            "isPublic": true
                        }
                        """)
                .when().post("/api/v1/topics")
                .then()
                .statusCode(201)
                .body("success", equalTo(true))
                .body("topic.name", equalTo("test.integration.topic"));
    }

    @Test
    void testGetSchema() {
        given()
                .when().get("/api/v1/schemas/agent.context")
                .then()
                .statusCode(200)
                .body("name", equalTo("agent.context"))
                .body("fields", notNullValue());
    }

    @Test
    void testGetNonExistentSchema() {
        given()
                .when().get("/api/v1/schemas/nonexistent.schema")
                .then()
                .statusCode(404)
                .body("error", containsString("not found"));
    }

    // ==========================================================
    // Context Propagation Tests - Pub/Sub Round-Trip
    // ==========================================================

    @Test
    void testConsumeNoContext() {
        // Consume from a topic with no published context should return 404
        given()
                .when().get("/api/v1/topics/session.communication/consume")
                .then()
                .statusCode(404)
                .body("error", containsString("No context found"));
    }

    @Test
    void testLatestEndpointNoContext() {
        // Latest endpoint for empty topic should return 404
        given()
                .when().get("/api/v1/topics/session.discovery/latest")
                .then()
                .statusCode(404)
                .body("error", containsString("No context found"));
    }

    @Test
    void testPublishAndConsumeRoundTrip() {
        // Publish context to session.knowledge_base with agent.context schema fields
        String publishBody = """
                {
                    "data": [{
                        "agent_id": "test-agent-001",
                        "context_key": "test.context",
                        "context_value": "Test context value for round-trip verification",
                        "data_type": "text",
                        "timestamp_nanos": 1234567890000,
                        "version": 1,
                        "lineage_id": "test-lineage-001",
                        "source_system": "quarkus-test"
                    }],
                    "lineageId": "test-lineage-001",
                    "metadata": {"test": "true"}
                }
                """;

        // Publish
        given()
                .contentType("application/json")
                .body(publishBody)
                .when().post("/api/v1/topics/session.knowledge_base/publish")
                .then()
                .statusCode(200)
                .body("success", equalTo(true))
                .body("version", notNullValue())
                .body("ipc_handle", notNullValue())
                .body("payload_size_bytes", notNullValue());

        // Consume and verify round-trip
        given()
                .when().get("/api/v1/topics/session.knowledge_base/consume")
                .then()
                .statusCode(200)
                .body("topic", equalTo("session.knowledge_base"))
                .body("version", notNullValue())
                .body("record_count", equalTo(1))
                .body("records[0].agent_id", equalTo("test-agent-001"))
                .body("records[0].context_key", equalTo("test.context"))
                .body("records[0].context_value", equalTo("Test context value for round-trip verification"))
                .body("records[0].source_system", equalTo("quarkus-test"));
    }

    // ==========================================================
    // Schema Registration Tests - Session Topic Aliases
    // ==========================================================

    @Test
    void testSessionKnowledgeBaseSchemaRegistered() {
        // session.knowledge_base should use agent.context schema
        given()
                .when().get("/api/v1/schemas/session.knowledge_base")
                .then()
                .statusCode(200)
                .body("name", equalTo("session.knowledge_base"))
                .body("fields.find { it.name == 'agent_id' }", notNullValue())
                .body("fields.find { it.name == 'context_key' }", notNullValue())
                .body("fields.find { it.name == 'context_value' }", notNullValue());
    }

    @Test
    void testSessionCommunicationSchemaRegistered() {
        // session.communication should use agent.context schema
        given()
                .when().get("/api/v1/schemas/session.communication")
                .then()
                .statusCode(200)
                .body("name", equalTo("session.communication"))
                .body("fields.find { it.name == 'agent_id' }", notNullValue());
    }

    @Test
    void testSystemAgentsSchemaRegistered() {
        // system.agents should use agent.context schema
        given()
                .when().get("/api/v1/schemas/system.agents")
                .then()
                .statusCode(200)
                .body("name", equalTo("system.agents"))
                .body("fields.find { it.name == 'agent_id' }", notNullValue());
    }

    @Test
    void testAllRegisteredSchemasCount() {
        // Should have at least 11 schemas registered (4 base + 7 topic aliases)
        given()
                .when().get("/api/v1/schemas")
                .then()
                .statusCode(200)
                .body("schemas.size()", org.hamcrest.Matchers.greaterThanOrEqualTo(11));
    }
}

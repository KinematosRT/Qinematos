package com.qinematos.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;

/**
 * Integration tests for DemoResource endpoints.
 *
 * Tests the demonstration endpoints that showcase the RVP framework.
 */
@QuarkusTest
class DemoResourceTest {

    @Test
    void testSystemStatus() {
        given()
                .when().get("/demo/status")
                .then()
                .statusCode(200)
                .body("system", equalTo("Qinematos Data Base Plane"))
                .body("version", notNullValue())
                .body("rvp_framework", notNullValue())
                .body("statistics.ipc", notNullValue())
                .body("statistics.lineage", notNullValue())
                .body("statistics.agents", notNullValue())
                .body("statistics.topics", notNullValue());
    }

    @Test
    void testFFMDemonstration() {
        given()
                .when().get("/demo/ffm?size=1024")
                .then()
                .statusCode(200)
                .body("ffm_api", containsString("Foreign Function"))
                .body("allocation_test.success", equalTo(true))
                .body("segment_operations.success", equalTo(true));
    }

    @Test
    void testZeroCopyDemonstration() {
        given()
                .contentType("application/json")
                .when().post("/demo/zero-copy?records=10")
                .then()
                .statusCode(200)
                .body("success", equalTo(true))
                .body("demo_id", notNullValue())
                .body("rvp_workflow.vulgate", notNullValue())
                .body("rvp_workflow.plain", notNullValue())
                .body("rvp_workflow.recent", notNullValue())
                .body("performance.records", equalTo(10));
    }

    @Test
    void testAgentsDemonstration() {
        given()
                .contentType("application/json")
                .when().post("/demo/agents?count=3")
                .then()
                .statusCode(200)
                .body("agents_created", equalTo(3))
                .body("connections_created", equalTo(2))
                .body("graph_stats", notNullValue());
    }

    @Test
    void testFrameworkIntegration() {
        given()
                .contentType("application/json")
                .when().post("/demo/integrate/langchain")
                .then()
                .statusCode(200)
                .body("success", equalTo(true))
                .body("framework", equalTo("langchain"))
                .body("topic", containsString("framework.langchain"))
                .body("endpoint", notNullValue());
    }
}

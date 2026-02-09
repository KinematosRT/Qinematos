package com.qinematos.mcp;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests for MCPResourceServer (Model Context Protocol).
 *
 * Tests MCP endpoints for AI agent standardized resource access.
 */
@QuarkusTest
class MCPResourceServerTest {

    @Test
    void testMCPInitialize() {
        given()
                .contentType("application/json")
                .body("""
                        {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {},
                            "clientInfo": {
                                "name": "test-agent",
                                "version": "1.0.0"
                            }
                        }
                        """)
                .when().post("/mcp/initialize")
                .then()
                .statusCode(200)
                .body("protocolVersion", notNullValue())
                .body("capabilities.resources", notNullValue())
                .body("capabilities.tools", notNullValue())
                .body("serverInfo.name", equalTo("qinematos-data-plane"));
    }

    @Test
    void testMCPPing() {
        given()
                .contentType("application/json")
                .when().post("/mcp/ping")
                .then()
                .statusCode(200)
                .body("status", equalTo("ok"))
                .body("timestamp", notNullValue());
    }

    @Test
    void testMCPListResources() {
        given()
                .when().get("/mcp/resources/list")
                .then()
                .statusCode(200)
                .body("resources", notNullValue());
    }

    @Test
    void testMCPListTools() {
        given()
                .when().get("/mcp/tools/list")
                .then()
                .statusCode(200)
                .body("tools", notNullValue())
                .body("tools.size()", greaterThan(0));
    }

    @Test
    void testMCPListPrompts() {
        given()
                .when().get("/mcp/prompts/list")
                .then()
                .statusCode(200)
                .body("prompts", notNullValue());
    }

    @Test
    void testMCPCallToolGetTopicInfo() {
        given()
                .contentType("application/json")
                .body("""
                        {
                            "name": "get_topic_info",
                            "arguments": {
                                "topic": "system.health"
                            }
                        }
                        """)
                .when().post("/mcp/tools/call")
                .then()
                .statusCode(200)
                .body("content", notNullValue())
                .body("isError", is(false));
    }
}

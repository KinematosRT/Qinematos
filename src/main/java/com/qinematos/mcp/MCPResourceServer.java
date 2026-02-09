package com.qinematos.mcp;

import com.qinematos.core.arrow.ArrowContextSerializer;
import com.qinematos.core.topic.ContextTopicManager;
import com.qinematos.core.topic.ContextTopicManager.TopicMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * MCPResourceServer - Model Context Protocol Implementation
 *
 * Implements the Anthropic Model Context Protocol (MCP) for standardized
 * resource access and tool calls to the Qinematos Data Base Plane.
 *
 * MCP provides the "Vulgate" (common language) interface that allows any
 * MCP-compliant AI agent to interact with Qinematos resources.
 *
 * Reference: https://modelcontextprotocol.io/
 */
@Path("/mcp")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MCPResourceServer {

    private static final Logger LOG = Logger.getLogger(MCPResourceServer.class);

    private static final String MCP_VERSION = "2024-11-05";
    private static final String SERVER_NAME = "qinematos-data-plane";
    private static final String SERVER_VERSION = "1.0.0";

    @Inject
    ContextTopicManager topicManager;

    @Inject
    ArrowContextSerializer arrowSerializer;

    // --------------------------
    // MCP Initialization
    // --------------------------

    /**
     * Initialize MCP session.
     * Corresponds to: initialize request
     */
    @POST
    @Path("/initialize")
    public Response initialize(InitializeRequest request) {
        LOG.infof("MCP initialize request from: %s", request.clientInfo().name());

        InitializeResponse response = new InitializeResponse(
                MCP_VERSION,
                new ServerCapabilities(
                        new ResourcesCapability(true, true), // subscribe, listChanged
                        new ToolsCapability(true), // listChanged
                        new PromptsCapability(true), // listChanged
                        null // logging (not implemented)
                ),
                new ServerInfo(SERVER_NAME, SERVER_VERSION),
                "MCP server for Qinematos Data Base Plane - Zero-Copy Context Delivery");

        return Response.ok(response).build();
    }

    /**
     * Ping endpoint for connection verification.
     */
    @POST
    @Path("/ping")
    public Response ping() {
        return Response.ok(Map.of("status", "ok", "timestamp", Instant.now().toString())).build();
    }

    // --------------------------
    // MCP Resources (Context Topics)
    // --------------------------

    /**
     * List available resources (Context Topics).
     * Corresponds to: resources/list request
     */
    @GET
    @Path("/resources/list")
    public Response listResources(@QueryParam("cursor") String cursor) {
        LOG.debug("MCP resources/list request");

        List<TopicMetadata> topics = topicManager.listTopics(null);

        List<MCPResource> resources = topics.stream()
                .map(this::topicToResource)
                .collect(Collectors.toList());

        // Add static resources
        resources.add(new MCPResource(
                "qinematos://schema/all",
                "All registered Arrow schemas",
                "Provides access to all registered Arrow schemas for context topics",
                "application/json",
                null));

        return Response.ok(new ListResourcesResponse(resources, null)).build();
    }

    /**
     * Read a specific resource.
     * Corresponds to: resources/read request
     */
    @POST
    @Path("/resources/read")
    public Response readResource(ReadResourceRequest request) {
        String uri = request.uri();
        LOG.debugf("MCP resources/read request: %s", uri);

        try {
            // Parse URI
            if (uri.startsWith("qinematos://topic/")) {
                return readTopicResource(uri.substring("qinematos://topic/".length()));
            } else if (uri.equals("qinematos://schema/all")) {
                return readAllSchemas();
            } else {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new MCPError("ResourceNotFound", "Unknown resource: " + uri))
                        .build();
            }
        } catch (Exception e) {
            LOG.errorf("Error reading resource %s: %s", uri, e.getMessage());
            return Response.serverError()
                    .entity(new MCPError("InternalError", e.getMessage()))
                    .build();
        }
    }

    /**
     * Subscribe to resource updates.
     * Corresponds to: resources/subscribe request
     */
    @POST
    @Path("/resources/subscribe")
    public Response subscribeResource(SubscribeRequest request) {
        String uri = request.uri();
        LOG.infof("MCP resources/subscribe request: %s", uri);

        // In a full implementation, this would set up a subscription
        // and send notifications via the MCP notification mechanism

        return Response.ok(Map.of(
                "subscribed", true,
                "uri", uri,
                "message", "Use gRPC Watch API for streaming updates")).build();
    }

    // --------------------------
    // MCP Tools
    // --------------------------

    /**
     * List available tools.
     * Corresponds to: tools/list request
     */
    @GET
    @Path("/tools/list")
    public Response listTools(@QueryParam("cursor") String cursor) {
        LOG.debug("MCP tools/list request");

        List<MCPTool> tools = Arrays.asList(
                new MCPTool(
                        "publish_context",
                        "Publish data to a Qinematos Context Topic",
                        new ToolInputSchema("object", Map.of(
                                "topic", Map.of("type", "string", "description", "Target context topic name"),
                                "data", Map.of("type", "array", "description", "Array of record objects to publish"),
                                "lineage_id",
                                Map.of("type", "string", "description", "Optional lineage identifier for tracing")),
                                List.of("topic", "data"))),
                new MCPTool(
                        "create_topic",
                        "Create a new Context Topic",
                        new ToolInputSchema("object", Map.of(
                                "name",
                                Map.of("type", "string", "description",
                                        "Topic name (hierarchical, e.g., 'finance.trades')"),
                                "description", Map.of("type", "string", "description", "Topic description"),
                                "schema_type",
                                Map.of("type", "string", "description",
                                        "Schema type: agent.context, lineage.trace, finance.market_data, or generic.keyvalue")),
                                List.of("name"))),
                new MCPTool(
                        "get_topic_info",
                        "Get metadata about a Context Topic",
                        new ToolInputSchema("object", Map.of(
                                "topic", Map.of("type", "string", "description", "Topic name")), List.of("topic"))),
                new MCPTool(
                        "subscribe_topic",
                        "Subscribe to a Context Topic for zero-copy updates",
                        new ToolInputSchema("object", Map.of(
                                "topic_pattern",
                                Map.of("type", "string", "description", "Topic pattern (supports wildcards: *, **)"),
                                "agent_id", Map.of("type", "string", "description", "Subscribing agent identifier"),
                                "from_version",
                                Map.of("type", "integer", "description", "Optional starting version for replay")),
                                List.of("topic_pattern", "agent_id"))),
                new MCPTool(
                        "get_ipc_handle",
                        "Get an IPC handle for zero-copy access to context data",
                        new ToolInputSchema("object", Map.of(
                                "topic", Map.of("type", "string", "description", "Topic name"),
                                "version", Map.of("type", "integer", "description",
                                        "Optional version number (latest if not specified)")),
                                List.of("topic"))));

        return Response.ok(new ListToolsResponse(tools, null)).build();
    }

    /**
     * Call a tool.
     * Corresponds to: tools/call request
     */
    @POST
    @Path("/tools/call")
    public Response callTool(CallToolRequest request) {
        String toolName = request.name();
        Map<String, Object> arguments = request.arguments();

        LOG.infof("MCP tools/call request: %s", toolName);

        try {
            Object result = switch (toolName) {
                case "publish_context" -> executePublishContext(arguments);
                case "create_topic" -> executeCreateTopic(arguments);
                case "get_topic_info" -> executeGetTopicInfo(arguments);
                case "subscribe_topic" -> executeSubscribeTopic(arguments);
                case "get_ipc_handle" -> executeGetIpcHandle(arguments);
                default -> throw new IllegalArgumentException("Unknown tool: " + toolName);
            };

            return Response.ok(new CallToolResponse(
                    List.of(new ToolContent("text", result.toString(), null)),
                    false)).build();

        } catch (Exception e) {
            LOG.errorf("Tool execution failed: %s - %s", toolName, e.getMessage());
            return Response.ok(new CallToolResponse(
                    List.of(new ToolContent("text", "Error: " + e.getMessage(), null)),
                    true)).build();
        }
    }

    // --------------------------
    // MCP Prompts
    // --------------------------

    /**
     * List available prompts.
     * Corresponds to: prompts/list request
     */
    @GET
    @Path("/prompts/list")
    public Response listPrompts(@QueryParam("cursor") String cursor) {
        LOG.debug("MCP prompts/list request");

        List<MCPPrompt> prompts = Arrays.asList(
                new MCPPrompt(
                        "context_query",
                        "Query context from a topic",
                        List.of(
                                new PromptArgument("topic", "The context topic to query", true),
                                new PromptArgument("query", "Natural language query about the context", true))),
                new MCPPrompt(
                        "analyze_lineage",
                        "Analyze the lineage of a context version",
                        List.of(
                                new PromptArgument("lineage_id", "The lineage identifier to analyze", true))));

        return Response.ok(new ListPromptsResponse(prompts, null)).build();
    }

    /**
     * Get a prompt with arguments filled in.
     * Corresponds to: prompts/get request
     */
    @POST
    @Path("/prompts/get")
    public Response getPrompt(GetPromptRequest request) {
        String promptName = request.name();
        Map<String, String> arguments = request.arguments();

        LOG.debugf("MCP prompts/get request: %s", promptName);

        List<PromptMessage> messages = switch (promptName) {
            case "context_query" -> buildContextQueryPrompt(arguments);
            case "analyze_lineage" -> buildLineageAnalysisPrompt(arguments);
            default -> throw new IllegalArgumentException("Unknown prompt: " + promptName);
        };

        return Response.ok(new GetPromptResponse(
                "Prompt for " + promptName,
                messages)).build();
    }

    // --------------------------
    // Tool Implementations
    // --------------------------

    @SuppressWarnings("unchecked")
    private Object executePublishContext(Map<String, Object> args) {
        String topic = (String) args.get("topic");
        List<Map<String, Object>> data = (List<Map<String, Object>>) args.get("data");
        String lineageId = (String) args.get("lineage_id");

        var result = topicManager.publish(topic, data, lineageId);

        return Map.of(
                "success", true,
                "version", result.version(),
                "ipc_handle", result.ipcHandle(),
                "subscribers_notified", result.subscribersNotified());
    }

    private Object executeCreateTopic(Map<String, Object> args) {
        String name = (String) args.get("name");
        String description = (String) args.getOrDefault("description", "");
        String schemaType = (String) args.getOrDefault("schema_type", "generic.keyvalue");

        var schema = arrowSerializer.getSchema(schemaType).orElse(null);

        var metadata = topicManager.createTopic(
                ContextTopicManager.TopicDefinition.builder()
                        .name(name)
                        .description(description)
                        .schema(schema)
                        .config(ContextTopicManager.TopicConfig.defaultConfig())
                        .build());

        return Map.of(
                "success", true,
                "topic", metadata.name(),
                "version", metadata.currentVersion());
    }

    private Object executeGetTopicInfo(Map<String, Object> args) {
        String topic = (String) args.get("topic");

        return topicManager.getTopic(topic)
                .map(m -> Map.of(
                        "name", m.name(),
                        "description", m.description() != null ? m.description() : "",
                        "current_version", m.currentVersion(),
                        "subscriber_count", m.subscriberCount(),
                        "created_at", m.createdAt().toString()))
                .orElseThrow(() -> new IllegalArgumentException("Topic not found: " + topic));
    }

    private Object executeSubscribeTopic(Map<String, Object> args) {
        String pattern = (String) args.get("topic_pattern");
        String agentId = (String) args.get("agent_id");

        // For REST, we can't do streaming - direct to gRPC
        return Map.of(
                "message", "Use gRPC Watch API for streaming subscriptions",
                "grpc_endpoint", "localhost:9000",
                "topic_pattern", pattern,
                "agent_id", agentId);
    }

    private Object executeGetIpcHandle(Map<String, Object> args) {
        String topic = (String) args.get("topic");
        Long version = args.containsKey("version") ? ((Number) args.get("version")).longValue() : null;

        // This would return the IPC handle for a published context
        return Map.of(
                "topic", topic,
                "message", "Use gRPC Watch API to receive IPC handles for zero-copy access",
                "hint", "IPC handles are returned in SubscriptionUpdate.ipc_handle field");
    }

    // --------------------------
    // Helper Methods
    // --------------------------

    private MCPResource topicToResource(TopicMetadata topic) {
        return new MCPResource(
                "qinematos://topic/" + topic.name(),
                "Context Topic: " + topic.name(),
                topic.description() != null ? topic.description()
                        : "Context topic with " + topic.currentVersion() + " versions",
                "application/vnd.apache.arrow.stream",
                Map.of(
                        "current_version", String.valueOf(topic.currentVersion()),
                        "subscriber_count", String.valueOf(topic.subscriberCount())));
    }

    private Response readTopicResource(String topicName) {
        var topic = topicManager.getTopic(topicName);

        if (topic.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new MCPError("ResourceNotFound", "Topic not found: " + topicName))
                    .build();
        }

        var metadata = topic.get();

        return Response.ok(new ReadResourceResponse(List.of(
                new ResourceContent(
                        "qinematos://topic/" + topicName,
                        "application/json",
                        Map.of(
                                "name", metadata.name(),
                                "current_version", metadata.currentVersion(),
                                "subscriber_count", metadata.subscriberCount(),
                                "access_method", "Use gRPC Watch API for zero-copy IPC access").toString(),
                        null))))
                .build();
    }

    private Response readAllSchemas() {
        var schemas = arrowSerializer.getAllSchemas();

        Map<String, Object> schemaInfo = new HashMap<>();
        schemas.forEach((name, schema) -> {
            schemaInfo.put(name, schema.getFields().stream()
                    .map(f -> Map.of("name", f.getName(), "type", f.getType().toString()))
                    .collect(Collectors.toList()));
        });

        return Response.ok(new ReadResourceResponse(List.of(
                new ResourceContent(
                        "qinematos://schema/all",
                        "application/json",
                        schemaInfo.toString(),
                        null))))
                .build();
    }

    private List<PromptMessage> buildContextQueryPrompt(Map<String, String> args) {
        String topic = args.get("topic");
        String query = args.get("query");

        return List.of(
                new PromptMessage("user", new TextContent("text",
                        String.format("Query the Qinematos context topic '%s': %s\n\n" +
                                "Use the get_topic_info tool to understand the topic structure, " +
                                "then use subscribe_topic or get_ipc_handle to access the data.",
                                topic, query))));
    }

    private List<PromptMessage> buildLineageAnalysisPrompt(Map<String, String> args) {
        String lineageId = args.get("lineage_id");

        return List.of(
                new PromptMessage("user", new TextContent("text",
                        String.format("Analyze the lineage trace for context version '%s'.\n\n" +
                                "Explain the data provenance, transformation steps, and any eBPF traces available.",
                                lineageId))));
    }

    // --------------------------
    // MCP Protocol Types
    // --------------------------

    // Request/Response records
    public record InitializeRequest(String protocolVersion, ClientCapabilities capabilities, ClientInfo clientInfo) {
    }

    public record InitializeResponse(String protocolVersion, ServerCapabilities capabilities, ServerInfo serverInfo,
            String instructions) {
    }

    public record ClientInfo(String name, String version) {
    }

    public record ServerInfo(String name, String version) {
    }

    public record ClientCapabilities(Object roots, Object sampling) {
    }

    public record ServerCapabilities(ResourcesCapability resources, ToolsCapability tools, PromptsCapability prompts,
            Object logging) {
    }

    public record ResourcesCapability(boolean subscribe, boolean listChanged) {
    }

    public record ToolsCapability(boolean listChanged) {
    }

    public record PromptsCapability(boolean listChanged) {
    }

    // Resources
    public record MCPResource(String uri, String name, String description, String mimeType,
            Map<String, String> annotations) {
    }

    public record ListResourcesResponse(List<MCPResource> resources, String nextCursor) {
    }

    public record ReadResourceRequest(String uri) {
    }

    public record ReadResourceResponse(List<ResourceContent> contents) {
    }

    public record ResourceContent(String uri, String mimeType, String text, byte[] blob) {
    }

    public record SubscribeRequest(String uri) {
    }

    // Tools
    public record MCPTool(String name, String description, ToolInputSchema inputSchema) {
    }

    public record ToolInputSchema(String type, Map<String, Object> properties, List<String> required) {
    }

    public record ListToolsResponse(List<MCPTool> tools, String nextCursor) {
    }

    public record CallToolRequest(String name, Map<String, Object> arguments) {
    }

    public record CallToolResponse(List<ToolContent> content, boolean isError) {
    }

    public record ToolContent(String type, String text, byte[] data) {
    }

    // Prompts
    public record MCPPrompt(String name, String description, List<PromptArgument> arguments) {
    }

    public record PromptArgument(String name, String description, boolean required) {
    }

    public record ListPromptsResponse(List<MCPPrompt> prompts, String nextCursor) {
    }

    public record GetPromptRequest(String name, Map<String, String> arguments) {
    }

    public record GetPromptResponse(String description, List<PromptMessage> messages) {
    }

    public record PromptMessage(String role, TextContent content) {
    }

    public record TextContent(String type, String text) {
    }

    // Errors
    public record MCPError(String code, String message) {
    }
}

# Qinematos

**The Deterministic Data Base Plane for Agentic AI**

Qinematos replaces probabilistic RAG pipelines with a deterministic, zero-copy context delivery system built on the **RVP Framework**:

| Pillar | Technology | What It Does |
|--------|-----------|--------------|
| **R**ecent | gRPC streaming + Mutiny | Real-time context push (no stale caches) |
| **V**ulgate | Apache Arrow IPC | One columnar format readable by Java, Python, Rust |
| **P**lain | Java FFM + mmap files | Zero-copy, zero-serialization shared memory |

## Quick Start

### Prerequisites

- Java 21+ (with `--enable-preview` for Foreign Function & Memory API)
- Maven 3.9+

### Build & Run

```bash
mvn quarkus:dev -Djvm.args="--enable-preview"
```

### Endpoints

| Interface | URL | Purpose |
|-----------|-----|---------|
| REST API | `http://localhost:8080/api/v1` | Topics, publish, consume, schemas |
| gRPC | `localhost:9000` | Streaming Watch subscriptions |
| MCP | `http://localhost:8080/mcp` | Model Context Protocol for AI agents |
| Demo | `http://localhost:8080/demo` | Interactive demonstrations |
| Health | `http://localhost:8080/api/v1/health` | System health check |

### Try It

```bash
# Check health
curl http://localhost:8080/api/v1/health | jq

# List topics
curl http://localhost:8080/api/v1/topics | jq

# Publish context
curl -X POST http://localhost:8080/api/v1/topics/session.knowledge_base/publish \
  -H "Content-Type: application/json" \
  -d '{"data":[{"agent_id":"agent-1","context_key":"finding","context_value":"pattern detected","data_type":"text","timestamp_nanos":0,"version":1,"lineage_id":"lin-1","source_system":"test"}]}'

# Run zero-copy demo
curl -X POST http://localhost:8080/demo/zero-copy?records=100 | jq
```

## Architecture

```
                    Agent Layer
         Python (PyArrow)  |  Java (FFM)  |  Rust (Arrow2)
                           |
              +---------------------------+
              |     Context Orchestrator   |
              |  (Quarkus + gRPC + REST)   |
              +---------------------------+
              |  Arrow    |  Topic  | IPC  |
              | Serializer| Manager |Engine|
              +---------------------------+
              |     Xodus    |   Lineage   |
              |   (ACID)     |  (eBPF)     |
              +---------------------------+
                           |
              REST :8080 | gRPC :9000 | MCP /mcp
```

### Core Components

- **ZeroCopyIPCEngine** - Memory-mapped file IPC via Java FFM API
- **ArrowContextSerializer** - Apache Arrow columnar serialization
- **ContextTopicManager** - Reactive pub/sub with Mutiny BroadcastProcessor
- **XodusContextStore** - ACID persistence with JetBrains Xodus
- **LineageTracker** - Verifiable audit trails with eBPF correlation IDs
- **MCPResourceServer** - Anthropic Model Context Protocol implementation

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Runtime | Quarkus | 3.17 |
| Language | Java | 21+ (FFM preview) |
| Data Format | Apache Arrow | 18.1 |
| Streaming | gRPC + Mutiny | - |
| Persistence | JetBrains Xodus | 2.0 |
| AI Protocol | MCP (Anthropic) | 2024-11-05 |

## License

Apache License 2.0

# Qinematos

**The Deterministic Data Base Plane for Agentic AI**

Qinematos replaces probabilistic RAG pipelines with a deterministic, zero-copy context delivery system built on the **RVP Framework**:

| Pillar | Technology | What It Does |
|--------|-----------|--------------|
| **R**ecent | gRPC streaming + Mutiny | Real-time context push via pub/sub topics |
| **V**ulgate | Apache Arrow IPC | One columnar format readable by Java, Python, Rust |
| **P**lain | Java 25 FFM + mmap files | Zero-copy, zero-serialization shared memory |

## Quick Start

### Prerequisites

- **Java 25** (with Foreign Function & Memory API support)
- **Maven 3.9+**
- **Docker** (optional, for containerized deployment)

### Verify Java Version

```bash
java -version
# Should show: openjdk version "25" or higher
```

### Build & Run

```bash
# Clone the repository
git clone https://github.com/qinematos/qinematos.git
cd qinematos

# Build
mvn clean package -DskipTests

# Run in dev mode (hot reload)
mvn quarkus:dev
```

### Endpoints

| Interface | URL | Purpose |
|-----------|-----|---------|
| REST API | `http://localhost:8080/api/v1` | Topics, publish, consume, schemas |
| gRPC | `localhost:9000` | Streaming Watch subscriptions |
| MCP | `http://localhost:8080/mcp` | Model Context Protocol for AI agents |
| Demo | `http://localhost:8080/demo` | Interactive demonstrations |
| Health | `http://localhost:8080/api/v1/health` | System health check |
| Metrics | `http://localhost:8080/q/metrics` | Prometheus metrics |

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
curl -X POST "http://localhost:8080/demo/zero-copy?records=1000" | jq
```

### Read from Python (Zero-Copy)

```python
import pyarrow as pa

# Memory-map the Arrow IPC file (zero-copy)
source = pa.memory_map("/dev/shm/qinematos/contexts/demo.context/1.arrow", "r")
source.seek(32)  # Skip 32-byte Qinematos header
table = pa.ipc.open_stream(source).read_all()
print(table.to_pandas())
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

- **ZeroCopyIPCEngine** - Memory-mapped file IPC via Java 25 FFM API (`Arena`, `MemorySegment`, `FileChannel.map`)
- **ArrowContextSerializer** - Apache Arrow columnar serialization with pre-defined schemas
- **ContextTopicManager** - Reactive pub/sub with Mutiny `BroadcastProcessor`, wildcard matching
- **XodusContextStore** - ACID persistence with JetBrains Xodus embedded store
- **LineageTracker** - Verifiable audit trails with eBPF-correlated lineage IDs
- **MCPResourceServer** - Anthropic Model Context Protocol (2024-11-05) implementation
- **ContextOrchestratorService** - gRPC server-streaming Watch API

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Runtime | Quarkus | 3.30.2 |
| Language | Java | 25 (FFM API) |
| Data Format | Apache Arrow | 18.0.0 |
| Streaming | gRPC + Mutiny | - |
| Persistence | JetBrains Xodus | 2.0.1 |
| AI Protocol | MCP (Anthropic) | 2024-11-05 |
| Observability | OpenTelemetry + Prometheus | - |

## Docker

```bash
docker build -t qinematos .
docker run -p 8080:8080 -p 9000:9000 qinematos
```

## License

Apache License 2.0

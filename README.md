# 🏖️ WaveHouse

[![CI](https://github.com/Wave-RF/WaveHouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Wave-RF/WaveHouse/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Wave-RF/WaveHouse)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The Open-Source Real-Time API Gateway for ClickHouse.**

WaveHouse is a high-performance, Go-based gateway designed to sit entirely in front of ClickHouse. It acts as the exclusive entry and exit point for your analytics data, solving the hardest parts of high-scale data ingestion and real-time querying so ClickHouse can focus on what it does best: fast analytics.

If you are building user-facing analytics, **WaveHouse acts like Supabase for ClickHouse**—or an **open-source Tinybird** that pushes data to your frontend in real time over SSE and WebSockets, not just via pull-based REST queries.

## ✨ Why WaveHouse?

ClickHouse is a phenomenal OLAP database, but directly exposing it to frontend applications comes with sharp edges. You typically have to build custom APIs, manage Kafka queues to prevent "too many parts" errors during insertion, and write complex replacing logic for deduplication. WaveHouse abstracts all of this away into a single, deployable binary so you stop interacting with ClickHouse directly.

* **📋 Schema-Aware Validation:** WaveHouse discovers your ClickHouse table schemas automatically via `system.columns` and validates every ingest payload against the real schema — unknown fields are rejected, types are checked, and nullable constraints are enforced. Bring Your Own Schema: you define tables in ClickHouse, WaveHouse enforces them.
* **📥 Asynchronous Buffered Ingestion:** Never drop a packet. WaveHouse writes incoming data to a highly durable Write-Ahead Log (WAL) and returns `200 OK` instantly, batching inserts to ClickHouse in the background.
* **👯 Optional Exact-Once Deduplication:** Built-in exact-match deduplication ensures duplicate payloads are dropped *before* they ever reach ClickHouse, saving expensive merge operations. Enable it when you need it, skip it when you don't.
* **⚡ Two-Tier Query Caching:** An ultra-fast local memory cache (L1) and a shared distributed cache (L2) coalesce identical queries, protecting ClickHouse from dashboard "thundering herds."
* **🌊 Zero-Latency Real-Time Push:** When data is pushed via the WaveHouse API, it is immediately broadcast to SSE/WebSocket listeners—even before it gets flushed to ClickHouse. This ensures instant perceived ingestion, with seamless gap-fill from NATS JetStream history for clients that connect late.
* **🛡️ Dead Letter Queue:** Failed batch inserts are routed to a DLQ (backed by a separate NATS stream) so no data is silently lost. Inspect failures via the DLQ stats API.

## 🚀 Deployment Modes

WaveHouse is designed using a clean architecture that allows it to run anywhere, from a laptop to a multi-region cloud.

| Mode | Binaries | External Dependencies | Use Case |
| ---- | -------- | --------------------- | -------- |
| **Standalone** | `wavehouse` | ClickHouse only | Local dev, single-server |
| **Clustered** | `wavehouse-api` + `wavehouse-worker` | ClickHouse, NATS, Redis, ScyllaDB | Horizontal scaling, production |

## 🛠️ Quick Start (Standalone)

The easiest way to see WaveHouse in action. Requires Docker.

```bash
# Clone the repository
git clone https://github.com/Wave-RF/WaveHouse.git
cd WaveHouse

# Start ClickHouse and WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d

# Create a table in ClickHouse (Bring Your Own Schema)
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS clicks (
      page String,
      button String,
      score Float64,
      received_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
    ) ENGINE = MergeTree()
    ORDER BY (page)
  "

# Ingest data (no auth required by default)
curl -s -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}

# Check discovered schemas
curl -s http://localhost:8080/v1/schema | jq

# Query data (wait ~5s for the batch flush to ClickHouse)
curl -s -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'

# Open a real-time SSE stream (Ctrl+C to stop)
curl -N http://localhost:8080/v1/stream/sse
```

WaveHouse is now accepting API requests on `http://localhost:8080`.

## 🚀 Quick Start (Clustered)

The full distributed stack with load balancing, NATS, Redis, and ScyllaDB:

```bash
# Start everything: Caddy LB, ClickHouse, NATS, Redis, ScyllaDB, 2x API, 2x worker
docker compose -f deployments/compose/cluster.yaml up -d

# Create your tables in ClickHouse, then WaveHouse discovers them automatically.
# See docs/deployment.md for full setup instructions.
```

The API is available behind Caddy at `http://localhost` (port 80).

## 💻 Local Development

For building and testing WaveHouse locally with hot-reload:

```bash
# Install dependencies
go mod download

# Start ClickHouse (standalone mode needs nothing else)
docker compose -f deployments/compose/dependencies.yaml up -d clickhouse

# Create your tables in ClickHouse, then:
make dev
```

WaveHouse will automatically recompile and restart whenever you save a `.go` file.

For clustered local development (run API + worker against external deps), see [docs/development.md](docs/development.md).

## 🤝 Contributing

We welcome issues, pull requests, and feedback! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to structure your code and run the integration test suites.

## 📖 Documentation

* [Architecture](docs/architecture.md) — System design, data flows, and package overview
* [API Reference](docs/api.md) — All endpoints, authentication, request/response formats
* [Configuration](docs/configuration.md) — Full config reference (YAML + environment variables)
* [Deployment](docs/deployment.md) — Standalone, clustered, Docker, and release guide
* [Development](docs/development.md) — Building, testing, linting, and project structure

## 📜 License

WaveHouse is open source under the [MIT License](LICENSE).

# 🏖️ BeachHouse

[![CI](https://github.com/Wave-RF/BeachHouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Wave-RF/BeachHouse/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Wave-RF/BeachHouse)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The Open-Source Real-Time API Gateway for ClickHouse.**

BeachHouse is a high-performance, Go-based gateway designed to sit entirely in front of ClickHouse. It acts as the exclusive entry and exit point for your analytics data, solving the hardest parts of high-scale data ingestion and real-time querying so ClickHouse can focus on what it does best: fast analytics.

If you are building user-facing analytics, **BeachHouse acts like Supabase for ClickHouse**—or an **open-source Tinybird** that pushes data to your frontend in real time over SSE and WebSockets, not just via pull-based REST queries.

## ✨ Why BeachHouse?

ClickHouse is a phenomenal OLAP database, but directly exposing it to frontend applications comes with sharp edges. You typically have to build custom APIs, manage Kafka queues to prevent "too many parts" errors during insertion, and write complex replacing logic for deduplication. BeachHouse abstracts all of this away into a single, deployable binary so you stop interacting with ClickHouse directly.

* **🛡️ Multi-Tenant Access Control:** Enforces strict Row-Level Security (RLS) via JWTs, ensuring tenants only ever read and write their own data.
* **📥 Asynchronous Buffered Ingestion:** Never drop a packet. BeachHouse writes incoming data to a highly durable Write-Ahead Log (WAL) and returns `200 OK` instantly, batching inserts to ClickHouse in the background.
* **👯 Exact-Once Deduplication:** Built-in exact-match deduplication ensures duplicate payloads are dropped *before* they ever reach ClickHouse, saving expensive merge operations.
* **⚡ Two-Tier Query Caching:** An ultra-fast local memory cache (L1) and a shared distributed cache (L2) coalesce identical queries, protecting ClickHouse from dashboard "thundering herds."
* **🌊 Zero-Latency Real-Time Push:** When data is pushed via the BeachHouse API, it is immediately broadcast to authorized SSE/WebSocket listeners—even before it gets flushed to ClickHouse. This ensures instant perceived ingestion for your users, with seamless gap-fill from NATS JetStream history for clients that connect late.
* **🗂️ Dynamic Schema Normalization:** Accept arbitrary, nested JSON payloads from clients. BeachHouse flattens them into an optimized Entity-Attribute-Value (EAV) Map schema, eliminating the need for constant ClickHouse `ALTER TABLE` migrations.

## 🚀 Deployment Modes

BeachHouse is designed using a clean architecture that allows it to run anywhere, from a laptop to a multi-region cloud.

| Mode | Binaries | External Dependencies | Use Case |
| ---- | -------- | --------------------- | -------- |
| **Standalone** | `beachhouse` | ClickHouse only | Local dev, single-server |
| **Clustered** | `beachhouse-api` + `beachhouse-worker` | ClickHouse, NATS, Redis, ScyllaDB | Horizontal scaling, production |

## 🛠️ Quick Start (Standalone)

The easiest way to see BeachHouse in action. Requires Docker.

```bash
# Clone the repository
git clone https://github.com/Wave-RF/BeachHouse.git
cd BeachHouse

# Start ClickHouse and BeachHouse
docker compose -f deployments/compose/standalone.yaml up -d

# Generate a test JWT (requires jwt-cli: https://github.com/mike-engel/jwt-cli)
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"tenant_id": "test-tenant", "exp": 9999999999}')

# Ingest an event
curl -s -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "evt-001", "type": "click", "data": {"page": "/home", "button": "signup"}}'
# → {"ok":true}

# Query events (wait ~5s for the batch flush to ClickHouse)
curl -s -X POST http://localhost:8080/v1/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM events WHERE tenant_id = ? LIMIT 10"}'

# Open a real-time SSE stream (Ctrl+C to stop)
curl -N http://localhost:8080/v1/stream/sse \
  -H "Authorization: Bearer $TOKEN"
```

BeachHouse is now accepting API requests on `http://localhost:8080`.

## 🚀 Quick Start (Clustered)

The full distributed stack with load balancing, NATS, Redis, and ScyllaDB:

```bash
# Start everything: Caddy LB, ClickHouse, NATS, Redis, ScyllaDB, 2x API, 2x worker
docker compose -f deployments/compose/cluster.yaml up -d

# Create ClickHouse table + ScyllaDB keyspace/table
# (auto_migrate defaults to false in clustered mode — see docs/deployment.md for
# manual steps, or set BH_CH_AUTO_MIGRATE=true and BH_DEDUPE_AUTO_MIGRATE=true)
```

The API is available behind Caddy at `http://localhost` (port 80).

## 💻 Local Development

For building and testing BeachHouse locally with hot-reload:

```bash
# Install dependencies
go mod download

# Start ClickHouse (standalone mode needs nothing else)
docker compose -f deployments/compose/dependencies.yaml up -d clickhouse

# Run with hot-reload (requires air: https://github.com/air-verse/air)
# The events table is auto-created at startup.
make dev
```

BeachHouse will automatically recompile and restart whenever you save a `.go` file.

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

BeachHouse is open source under the [MIT License](LICENSE).

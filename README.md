# 🏖️ WaveHouse

[![CI](https://github.com/Wave-RF/WaveHouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Wave-RF/WaveHouse/actions/workflows/ci.yml)
[![Coverage](https://github.com/Wave-RF/WaveHouse/blob/badges/.badges/main/coverage.svg?raw=true)](https://github.com/Wave-RF/WaveHouse/actions)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Wave-RF/WaveHouse)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The open-source real-time API gateway for ClickHouse.**

WaveHouse is a high-performance Go gateway that sits in front of ClickHouse and acts as the exclusive entry and exit point for your analytics data. It solves the hardest parts of high-scale data ingestion and real-time querying so ClickHouse can focus on what it does best — fast analytics.

If you're building user-facing analytics, **WaveHouse is like Supabase for ClickHouse** — or an open-source Tinybird that pushes data to the frontend in real time over SSE and WebSockets, not just via pull-based REST.

📖 **[Read the full documentation at docs.wavehouse.dev](https://docs.wavehouse.dev)**

## ✨ Features

- **📋 Schema-aware validation** — Discovers your ClickHouse table schemas via `system.columns` and validates every ingest payload: unknown fields rejected, types checked, null constraints enforced.
- **📥 Async buffered ingest** — Never drop a packet. Writes land in a durable NATS JetStream WAL and return `200 OK` instantly; a background worker batch-flushes to ClickHouse.
- **👯 Optional exact-once dedup** — Drop duplicate payloads before they reach ClickHouse, saving expensive merges. Enable it when you need it.
- **⚡ Two-tier query caching** — L1 Ristretto (in-process) + L2 Redis (shared) with singleflight coalescing — dashboards survive thundering herds.
- **🌊 Zero-latency real-time push** — Events are broadcast to SSE/WebSocket subscribers *before* they're flushed to ClickHouse. Gap-fill from JetStream history for late-connecting clients.
- **🛡️ Dead Letter Queue** — Failed batch inserts go to a dedicated DLQ stream instead of blocking retries. Inspect depth via `GET /v1/dlq/stats`.
- **🔐 Hasura-style access control** — Per-table, per-role column and row-level policies with JWT claim templating.
- **🔍 Structured queries** — Type-safe query AST at `POST /v1/tables/{table}/query` — schema validation, permission enforcement, timestamp bucketing, aggregations.
- **🔗 Named pipes** — Pre-defined SQL templates (Tinybird-style) with parameter binding, role restrictions, and caching.
- **📦 TypeScript SDK** — `@wavehouse/sdk`: zero-dependency client with type-safe query builder, live queries, real-time SSE streaming, and codegen from ClickHouse schemas.

## 🚀 Deployment modes

WaveHouse runs anywhere — from a laptop to a multi-region cloud.

| Mode | Binaries | External dependencies | Use case |
| ---- | -------- | --------------------- | -------- |
| **Standalone** | `wavehouse` | ClickHouse only | Local dev, single-server |
| **Clustered** | `wavehouse-api` + `wavehouse-worker` | ClickHouse, NATS, Redis, ScyllaDB | Horizontal scaling, production |

## 🛠️ 30-second quickstart

```bash
git clone https://github.com/Wave-RF/WaveHouse.git
cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d

# Create a table in ClickHouse — WaveHouse discovers it automatically
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS clicks (
      page String, button String, score Float64,
      received_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
    ) ENGINE = MergeTree() ORDER BY (page)
  "

# Ingest → query → stream
curl -s -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'

curl -s -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'

curl -N http://localhost:8080/v1/stream/sse
```

Full walkthrough: **[Getting Started](https://docs.wavehouse.dev/getting-started/)**.

For the clustered stack (Caddy LB + 2× API + 2× worker + NATS + Redis + ScyllaDB), see the [Deployment guide](https://docs.wavehouse.dev/deployment/#clustered-distributed).

## 📖 Documentation

All docs live at **[docs.wavehouse.dev](https://docs.wavehouse.dev)**. Source files live in [`docs/`](docs/).

- [Getting Started](docs/getting-started.md) — five-minute quickstart
- [Architecture](docs/architecture.md) — system design, data flows, package overview
- [API Reference](docs/api.md) — endpoints, auth, request/response formats
- [TypeScript SDK](docs/sdk.md) — client library and codegen
- [Configuration](docs/configuration.md) — YAML + `WH_*` environment variables
- [Deployment](docs/deployment.md) — standalone, clustered, Docker, releases
- [Development](docs/development.md) — building, testing, linting, project structure

## 🤝 Contributing

Issues, pull requests, and feedback are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and the [Development guide](docs/development.md) for local setup. Security reports: see [SECURITY.md](SECURITY.md).

## 📜 License

WaveHouse is open source under the [MIT License](LICENSE).

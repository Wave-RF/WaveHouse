---
title: "Getting Started"
description: "Run WaveHouse locally in five minutes — ingest, query, and subscribe to real-time events."
sidebar:
  order: 2
---

Run WaveHouse locally in under five minutes. WaveHouse ships as a single binary with ClickHouse as the only external dependency; this walkthrough covers ingest, query, and real-time streaming.

## Prerequisites

- **Docker** — for running ClickHouse (and optionally WaveHouse itself).
- **curl** and **jq** (optional) — for poking the API.
- **Go 1.26+** — only required if you want to build from source; skip it for the Docker path below.

## 1. Start WaveHouse

The fastest path uses Docker Compose — it launches ClickHouse and a single `wavehouse` process with no extra configuration.

```bash
git clone https://github.com/Wave-RF/WaveHouse.git
cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d
```

This exposes:

- WaveHouse API on `http://localhost:8080`
- ClickHouse on ports `8123` (HTTP) and `9000` (native)

## 2. Create a ClickHouse table

WaveHouse uses a **Bring Your Own Schema** model — you create tables in ClickHouse, and WaveHouse discovers them automatically via `system.columns`.

```bash
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
```

Schemas refresh every 60 seconds by default, or on demand via `POST /v1/schema/refresh`.

## 3. Ingest an event

Authentication is **disabled by default**, so you can POST straight to `/v1/ingest/{table}`.

```bash
curl -s -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}
```

WaveHouse validates the body against the ClickHouse schema before acknowledging. Unknown fields, type mismatches, and missing required columns are rejected with a `400`.

## 4. Query

Queries are cached in-process (L1 Ristretto) with singleflight coalescing — duplicate concurrent queries hit ClickHouse once.

```bash
# Wait ~5 seconds for the batch flush to ClickHouse, then:
curl -s -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
```

Prefer a type-safe query builder over raw SQL? See the [structured query endpoint](api.md#post-v1tablestablequery--structured-query) or the [TypeScript SDK](sdk.md).

## 5. Subscribe to real-time updates

Every ingested event is broadcast to SSE and WebSocket subscribers **before** it's flushed to ClickHouse, so dashboards see new data with zero perceived lag.

```bash
# All tables
curl -N http://localhost:8080/v1/stream/sse

# Specific table
curl -N "http://localhost:8080/v1/stream/sse?topic=ingest.clicks"

# With historical replay (RFC 3339 timestamp)
curl -N "http://localhost:8080/v1/stream/sse?since=2026-03-24T11:00:00Z"
```

## Next steps

- **[Architecture](architecture.md)** — how ingest, query, cache, and streaming fit together.
- **[API Reference](api.md)** — every endpoint, request/response shape, and error code.
- **[TypeScript SDK](sdk.md)** — zero-dependency client with query builder, live queries, and codegen.
- **[Configuration](configuration.md)** — full YAML + environment variable reference.
- **[Deployment](deployment.md)** — Docker images, releases, health checks.
- **[Development](development.md)** — building from source, running tests, hot-reload workflow.

## Going further

- **Enable JWT auth**: set `WH_AUTH_ENABLED=true` and `WH_AUTH_JWT_SECRET=<secret>` — see [API Reference — Authentication](api.md#authentication).
- **Enable deduplication**: set `WH_DEDUPE_ENABLED=true` and `WH_DEDUPE_ID_FIELD=event_id` — see [Configuration — Deduplication](configuration.md#deduplication).

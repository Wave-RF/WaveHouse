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

The JWT middleware always runs, but with no secret configured (the default) every request resolves to the policy `default_role` — so you can POST straight to `/v1/ingest?table={table}`.

```bash
curl -s -X POST http://localhost:8080/v1/ingest?table=clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}
```

WaveHouse validates the body against the ClickHouse schema before acknowledging. Unknown fields, type mismatches, and missing required columns are rejected with a `400`.

## 4. Query

Queries are cached in-process (L1 Ristretto) with singleflight coalescing — duplicate concurrent queries hit ClickHouse once. Note that `/v1/admin/query` is the exception: it's an admin escape hatch that never caches and emits `Cache-Control: no-store`, so every request goes straight to ClickHouse. The cached read paths are `POST /v1/query?table={table}` and `GET/POST /v1/pipes/{name}`.

```bash
# Wait ~5 seconds for the batch flush to ClickHouse, then:
# `/v1/admin/query` is admin-only: send a valid JWT whose role is the policy
# `admin_role` ("admin" by default) via `Authorization: Bearer <jwt>`.
curl -s -X POST http://localhost:8080/v1/admin/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
```

Prefer a type-safe query builder over raw SQL? See the [structured query endpoint](/api#post-v1querytabletable--structured-query) or the [TypeScript SDK](/sdk).

## 5. Subscribe to real-time updates

Every ingested event is broadcast to SSE subscribers **before** it's flushed to ClickHouse, so dashboards see new data with zero perceived lag.

```bash
# Specific table (?table= is required)
curl -N "http://localhost:8080/v1/stream?table=clicks"

# With historical replay (RFC 3339 timestamp)
curl -N "http://localhost:8080/v1/stream?table=clicks&since=2026-03-24T11:00:00Z"
```

## Next steps

- **[Architecture](/architecture)** — how ingest, query, cache, and streaming fit together.
- **[API Reference](/api)** — every endpoint, request/response shape, and error code.
- **[TypeScript SDK](/sdk)** — zero-dependency client with query builder, live queries, and codegen.
- **[Configuration](/configuration)** — full YAML + environment variable reference.
- **[Deployment](/deployment)** — Docker images, releases, health checks.
- **[Development](/development)** — building from source, running tests, hot-reload workflow.

## Going further

- **Validate JWTs**: set `WH_AUTH_JWT_SECRET=<secret>` (the middleware always runs; without a secret every request is the policy `default_role`) — see [API Reference — Authentication](/api#authentication).
- **Enable deduplication**: set `WH_DEDUPE_ENABLED=true` and `WH_DEDUPE_ID_FIELD=event_id` — see [Configuration — Deduplication](/configuration#deduplication).

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

The fastest path uses Docker Compose — it launches ClickHouse and a single `wavehouse` process.

```bash
git clone https://github.com/Wave-RF/WaveHouse.git
cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d
```

This exposes:

- WaveHouse API on `http://localhost:8080`
- ClickHouse on ports `8123` (HTTP) and `9000` (native)

WaveHouse is **fail-closed** — with no policy loaded, every request is denied. So the standalone stack ships a permissive **trial policy** (`deployments/compose/dev-policy.yaml`, mounted read-only and wired in via `WH_POLICY_FILE_PATH`): a non-admin [`public` role](/access-control#default_role--public-unauthenticated-access) that can read and write the demo tables (`clicks`, `events`) with no token, so the quickstart just works. It's *not* admin — it can't run raw SQL or manage policy/pipes — and it names specific tables, so it grants nothing in a real deployment (those tables won't exist there). It seeds into NATS KV on first boot; after that KV is authoritative (see [Access Control — Bootstrapping](/access-control#bootstrapping-and-the-policy-lifecycle)). It's deliberately lenient for trialing — a real deployment should [tune it](/access-control): your own roles, real tables, scoped columns, and usually tokens instead of a public default.

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

Schemas refresh every 60 seconds by default, or on demand via `POST /v1/schema/refresh` (admin-only). If the first ingest below returns `404 unknown table: clicks`, the refresh simply hasn't picked the new table up yet — wait a few seconds and retry.

## 3. Ingest an event

The JWT middleware always runs, but with no secret configured (the default) every request resolves to the policy `default_role` — which the trial policy from step 1 maps to the `public` role (granted insert on the demo tables). So you can POST straight to `/v1/ingest?table=clicks` with no token:

```bash
curl -s -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}
```

WaveHouse validates the body against the ClickHouse schema before acknowledging. Unknown fields, type mismatches, and missing required columns are rejected with a `400`.

## 4. Query

The trial `public` role can read the demo tables, so query `clicks` with the structured-query endpoint — no token needed:

```bash
# Wait ~5 seconds for the batch flush to ClickHouse, then query:
curl -s -X POST "http://localhost:8080/v1/query?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"columns": ["page", "button", "score"], "limit": 10}'
```

`POST /v1/query?table={table}` and `GET/POST /v1/pipes/{name}` are cached in-process (L1 Ristretto) with singleflight coalescing — duplicate concurrent queries hit ClickHouse once. For raw SQL there's `POST /v1/admin/query` (an admin escape hatch that never caches, emitting `Cache-Control: no-store`), but it's **admin-only** — the trial `public` role can't reach it. To use it, swap the public default for real auth: configure a JWT secret and present a token whose role is the policy [`admin_role`](/access-control#admin_role--the-privileged-role).

Prefer a type-safe query builder? See the [structured query endpoint](/api#post-v1querytabletable--structured-query) or the [TypeScript SDK](/sdk).

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

- **Validate JWTs**: set `WH_AUTH_JWT_SECRET=<secret>` (the middleware always runs; without a secret every request is the policy `default_role`) and replace the shipped trial policy (`deployments/compose/dev-policy.yaml`) with a least-privilege one — see [API Reference — Authentication](/api#authentication) and [Access Control](/access-control).
- **Enable deduplication**: set `WH_DEDUPE_ENABLED=true` and `WH_DEDUPE_ID_FIELD=event_id` — see [Configuration — Deduplication](/configuration#deduplication).

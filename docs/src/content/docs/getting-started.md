---
title: "Getting Started"
description: "Run WaveHouse locally in five minutes — ingest, query, and subscribe to real-time events."
sidebar:
  order: 2
---

Run WaveHouse locally in under five minutes. It ships as a single binary with ClickHouse as the only external dependency.

## Prerequisites

- **Docker** — for ClickHouse (and optionally WaveHouse).
- **curl** and **jq** (optional) — for API testing.
- **Go 1.26+** — only if building from source.

## 1. Start WaveHouse

Use Docker Compose to launch ClickHouse and `wavehouse`:

```bash
git clone https://github.com/Wave-RF/WaveHouse.git
cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d
```

Exposed ports:

- WaveHouse API: `http://localhost:8080`
- ClickHouse: `8123` (HTTP) and `9000` (native)

WaveHouse is **fail-closed** — with no policy loaded, every request is denied. So the standalone stack ships a permissive **trial policy** (`deployments/compose/dev-policy.yaml`, mounted read-only, wired via `WH_POLICY_FILE_PATH`): a non-admin [`public` role](/access-control#default_role--public-unauthenticated-access) with read/write on the demo tables (`clicks`, `events`), no token needed. It's *not* admin — it can't run raw SQL or manage policy/pipes — and it names specific tables, so it grants nothing in a real deployment. It seeds into NATS KV on first boot; thereafter KV is authoritative ([Access Control — Bootstrapping](/access-control#bootstrapping-and-the-policy-lifecycle)). For production, [tune it](/access-control): your own roles, real tables, scoped columns, and tokens instead of a public default.

## 2. Create a ClickHouse table

WaveHouse uses **Bring Your Own Schema**; it discovers tables via `system.columns`.

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

Schemas refresh every 60 seconds or via `POST /v1/schema/refresh` (admin-only). If ingest returns `404 unknown table: clicks`, wait for the next refresh.

## 3. Ingest an event

Without a configured secret, requests resolve to `default_role`, which the trial policy maps to `public`. POST directly to `/v1/ingest?table=clicks`:

```bash
curl -s -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}
```

WaveHouse validates the body against the ClickHouse schema; unknown fields, type mismatches, and missing required columns return `400`.

## 4. Query

Query `clicks` using the structured-query endpoint:

```bash
# Wait ~5 seconds for the batch flush to ClickHouse, then query:
curl -s -X POST "http://localhost:8080/v1/query?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"columns": ["page", "button", "score"], "limit": 10}'
```

`POST /v1/query?table={table}` and `GET/POST /v1/pipes/{name}` are cached in-process (L1 Ristretto) with singleflight coalescing — duplicate concurrent queries hit ClickHouse once. Raw SQL goes through `POST /v1/admin/query`, an admin escape hatch that never caches and emits `Cache-Control: no-store`; the trial `public` role can't reach it. To use it, configure a JWT secret and present a token whose role is the policy [`admin_role`](/access-control#admin_role--the-privileged-role).

:::tip[Prefer a type-safe client?]
The [TypeScript SDK](/sdk) provides a chainable query builder with autocomplete. See the [structured query reference](/api#post-v1querytabletable--structured-query) for raw shapes.
:::

## 5. Subscribe to real-time updates

Events are broadcast via SSE **before** flushing to ClickHouse:

```bash
# Specific table (?table= is required)
curl -N "http://localhost:8080/v1/stream?table=clicks"

# With historical replay (RFC 3339 timestamp)
curl -N "http://localhost:8080/v1/stream?table=clicks&since=2026-03-24T11:00:00Z"
```

## Troubleshooting first runs

- **`404 unknown table: clicks`**: Schema discovery refreshes every 60s (`WH_SCHEMA_REFRESH_INTERVAL`). Wait and retry.
- **Query returns `[]` after ingest**: Ingest is durable in the WAL immediately, but batch workers flush to ClickHouse every few seconds. Re-query after ~5 seconds or use [SSE stream](#5-subscribe-to-real-time-updates).
- **`403` on custom tables**: The trial policy only grants access to `clicks` and `events`. See [Access Control](/access-control) to grant permissions for new tables.
- **Port conflict**: Stop processes using `8080`, `8123`, or `9000`, or edit `deployments/compose/standalone.yaml`.
- **Boot errors**: `docker compose -f deployments/compose/standalone.yaml ps` should show both services up; ClickHouse takes a few seconds to initialize on a cold start.

## Next steps

- **[Architecture](/architecture)** — Ingest, query, cache, and streaming flow.
- **[API Reference](/api)** — Endpoints, shapes, and error codes.
- **[TypeScript SDK](/sdk)** — Client with query builder and codegen.
- **[Configuration](/configuration)** — YAML and environment variables.
- **[Deployment](/deployment)** — Images, releases, and health checks.
- **[Development](/development)** — Building from source and testing.

## Going further

- **Validate JWTs**: Set `WH_AUTH_JWT_SECRET=<secret>` and replace the trial policy with a least-privilege one (see [Authentication](/api#authentication) and [Access Control](/access-control)).
- **Enable deduplication**: Set `WH_DEDUPE_ENABLED=true` and `WH_DEDUPE_ID_FIELD=event_id` ([Configuration — Deduplication](/configuration#deduplication)).

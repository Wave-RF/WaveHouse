# Architecture

This document describes the internal architecture of BeachHouse, the real-time API gateway and sidecar for ClickHouse.

## Overview

BeachHouse is a Go-based gateway that sits in front of ClickHouse, acting as the exclusive entry and exit point for analytics data. It handles ingestion, deduplication, caching, real-time streaming, and multi-tenant access control so ClickHouse can focus on fast analytics.

```text
┌─────────────────────────────────────────────────────────────────┐
│                         Clients                                 │
│              (REST API, SSE, WebSocket)                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BeachHouse API Layer                         │
│  ┌──────────┐   ┌──────────┐ ┌──────────┐  ┌──────────────┐     │
│  │  Ingest  │   │  Query   │ │   SSE    │  │  WebSocket   │     │
│  │  Handler │   │  Handler │ │  Handler │  │   Handler    │     │
│  └────┬─────┘   └────┬─────┘ └────┬─────┘  └──────┬───────┘     │
│       │              │            │               │             │
│  ┌────▼────┐    ┌────▼────┐   ┌───▼───┐           │             │
│  │ Dedupe  │    │ Cache   │   │  Hub  │  (broadcast fan-out)    │
│  └────┬────┘    │ (Tiered)│   └───┬───┘                         │
│       │         └────┬────┘       │                             │
│       ▼              │            │                             │
│  ┌─────────┐         │       ┌────▼──────┐                      │
│  │   MQ    │─────────┼──────▶│  Replay   │                      │
│  │ (NATS)  │         │       │  Buffer   │                      │
│  └────┬────┘         │       └───────────┘                      │
│       │              │                                          │
│  ┌────▼──────────┐   │                                          │
│  │ Buffer        │   │                                          │
│  │ Consumer      │   │                                          │
│  │ (batch flush) │   │                                          │
│  └────┬──────────┘   │                                          │
│       │              │                                          │
└───────┼──────────────┼──────────────────────────────────────────┘
        │              │
        ▼              ▼
┌─────────────────────────────┐   ┌───────────┐
│        ClickHouse           │   │   Redis   │  (L2 cache,
│   (analytics storage)       │   │           │   clustered only)
└─────────────────────────────┘   └───────────┘
```

## Binaries

BeachHouse ships three binaries for different deployment modes:

| Binary | Mode | Purpose |
| ------ | ---- | ------- |
| `beachhouse` | Standalone | All-in-one: API + worker + embedded NATS + embedded Pebble dedup. Zero external deps beyond ClickHouse. |
| `beachhouse-api` | Clustered | Stateless API server. Handles ingest/query/streaming. Connects to external NATS, Redis, ScyllaDB. Horizontally scalable. |
| `beachhouse-worker` | Clustered | Background worker. Consumes from NATS, batch-flushes to ClickHouse, maintains replay buffer. |

## Internal Packages

```text
internal/
├── api/        HTTP layer (Chi router, handlers, middleware, Hub)
├── cache/      Two-tier caching (L1 Ristretto + L2 Redis)
├── config/     YAML + env var configuration loading
├── dedupe/     Exact-once deduplication (Pebble or ScyllaDB)
├── ingest/     Batch buffering and replay buffer
├── mq/         Message queue abstraction (embedded or remote NATS)
└── schema/     JSON flattening to EAV format
```

### `api/` — HTTP Layer

The API layer uses [Chi](https://github.com/go-chi/chi) for routing with standard middleware (RequestID, RealIP, Recoverer).

- **router.go** — Route definitions. Public: `/health`, `/ready`. Protected (JWT): `/v1/ingest`, `/v1/query`, `/v1/stream/sse`, `/v1/stream/ws`.
- **middleware.go** — JWT Bearer token validation. Extracts `tenant_id` from the token claims and injects it into the request context. Tenant ID is **never** sourced from user input.
- **ingest.go** — Accepts JSON events, deduplicates, flattens to EAV schema, publishes to MQ.
- **query.go** — Executes SQL queries against ClickHouse with tenant RLS. Cache key = `sha256(tenant_id:sql)`.
- **stream_sse.go** / **stream_ws.go** — Real-time streaming via SSE and WebSocket. Supports gap-fill from the replay buffer using a `since` timestamp parameter.
- **hub.go** — In-process pub/sub for broadcasting MQ messages to connected streaming clients.
- **health.go** — Liveness (`/health`) and readiness (`/ready`) probes. Readiness checks ClickHouse connectivity.

### `cache/` — Two-Tier Caching

- **cache.go** — `Cache` interface: `Get`, `Set`, `Close`.
- **local.go** — L1 in-process cache using [Ristretto](https://github.com/dgraph-io/ristretto) with `sync.Map` TTL tracking.
- **shared.go** — L2 distributed cache backed by Redis.
- **tiered.go** — Combines L1 + L2 with [singleflight](https://pkg.go.dev/golang.org/x/sync/singleflight) to prevent cache stampede on concurrent misses.

### `config/` — Configuration

- **config.go** — Loads configuration from YAML file with environment variable overrides (using [cleanenv](https://github.com/ilyakaznacheev/cleanenv)). All settings use `BH_` prefixed env vars. See [Configuration Reference](configuration.md).

### `dedupe/` — Deduplication

- **dedupe.go** — `Deduplicator` interface: `CheckAndMark(ctx, tenant_id, event_id) (bool, error)`.
- **embedded.go** — Standalone mode: uses [Pebble](https://github.com/cockroachdb/pebble) (embedded key-value store). Composite key = `tenant_id:event_id`.
- **distributed.go** — Clustered mode: uses ScyllaDB with `INSERT IF NOT EXISTS` for atomic check-and-mark.

### `ingest/` — Buffering & Replay

- **buffer.go** — `BufferConsumer` subscribes to `ingest.>`, accumulates events (batch size 1000 or 5-second flush interval), and performs batch inserts to the ClickHouse `events` table.
- **replay.go** — `ReplayBuffer` is a fixed-size ring buffer (10,000 events) used for SSE/WebSocket gap-fill. Clients can request events `since` a timestamp to catch up before transitioning to live data.

### `mq/` — Message Queue

- **mq.go** — `Publisher` and `Subscriber` interfaces. `Message` struct with `Ack()`/`Nak()`.
- **embedded.go** — Standalone mode: in-process NATS server with JetStream. Creates stream `BEACHHOUSE` with subjects `ingest.>`.
- **remote.go** — Clustered mode: connects to an external NATS cluster with the same stream/subject configuration.

### `schema/` — JSON Flattening

- **flatten.go** — Converts arbitrary nested JSON into parallel `[]string` arrays of dot-notation keys and string values.  Handles objects (recursive), arrays (numeric indices), and primitives. Output is compatible with ClickHouse `Map(String, String)` columns.
- **flatten_test.go** — Unit tests covering simple objects, nested objects, arrays, booleans, nulls, and invalid JSON.

## Data Flows

### Ingest Path

```text
Client POST /v1/ingest
  → JWT auth middleware (extract tenant_id)
  → Deduplication check (tenant_id:event_id)
  → Schema flattening (nested JSON → EAV)
  → Publish to NATS JetStream (ingest.events)
  → 200 OK returned immediately

BufferConsumer (async goroutine):
  ← Subscribe to ingest.>
  → Accumulate events (1000 events or 5s timeout)
  → Batch INSERT into ClickHouse events table
  → Ack messages

ReplayBuffer (async goroutine):
  ← Subscribe to ingest.>
  → Append to ring buffer (10K capacity)
  → Used for SSE/WS gap-fill
```

### Query Path

```text
Client POST /v1/query
  → JWT auth middleware (extract tenant_id)
  → Check tiered cache (L1 → L2)
  → Cache HIT: return cached result (X-Cache: HIT header)
  → Cache MISS:
    → Execute query on ClickHouse with tenant_id RLS
    → Store result in L1 + L2
    → Return result (X-Cache: MISS header)
```

### Streaming Path

```text
Client GET /v1/stream/sse or /v1/stream/ws
  → JWT auth middleware
  → If ?since= parameter provided:
    → Query replay buffer for events after timestamp
    → Send historical events first
  → Subscribe to Hub (in-process pub/sub)
  → Stream live events as they arrive via MQ → Hub → client
```

## Standalone vs. Clustered

| Aspect | Standalone | Clustered |
| ------ | ---------- | --------- |
| Binaries | Single `beachhouse` binary | `beachhouse-api` + `beachhouse-worker` |
| Message Queue | Embedded NATS (in-process) | External NATS cluster |
| Deduplication | Pebble (embedded KV) | ScyllaDB (distributed) |
| Cache | L1 only (Ristretto) | L1 (Ristretto) + L2 (Redis) |
| Replay Buffer | In the single process | In the worker process(es) |
| Scaling | Vertical only | Horizontal (add API/worker nodes) |
| External Dependencies | ClickHouse only | ClickHouse, NATS, Redis, ScyllaDB |

## Technology Stack

| Component | Technology | Purpose |
| --------- | ---------- | ------- |
| Language | Go 1.25 | Core runtime |
| HTTP Router | Chi v5 | Request routing and middleware |
| Authentication | golang-jwt v5 | JWT parsing and validation |
| Analytics DB | ClickHouse | Primary data store |
| Message Queue | NATS + JetStream | Durable event streaming |
| L1 Cache | Ristretto v2 | In-process memory cache |
| L2 Cache | Redis 7 | Distributed shared cache |
| Embedded KV | Pebble | Standalone deduplication |
| Distributed KV | ScyllaDB | Clustered deduplication |
| WebSocket | coder/websocket | WebSocket protocol support |
| Config | cleanenv | YAML + env var config loading |
| Release | GoReleaser | Cross-platform binary builds |
| Containers | Docker (distroless) | Minimal production images |

# Architecture

This document describes the internal architecture of WaveHouse, a schema-aware ClickHouse proxy.

## Overview

WaveHouse is a Go-based gateway that sits in front of ClickHouse, acting as the entry and exit point for data. It discovers your real ClickHouse table schemas, validates data at ingest time, batches inserts asynchronously, and provides real-time streaming and query caching.

```text
┌─────────────────────────────────────────────────────────────────┐
│                         Clients                                 │
│              (REST API, SSE, WebSocket)                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    WaveHouse API Layer                         │
│  ┌──────────┐   ┌──────────┐ ┌──────────┐  ┌──────────────┐     │
│  │  Ingest  │   │  Query   │ │   SSE    │  │  WebSocket   │     │
│  │  Handler │   │  Handler │ │  Handler │  │   Handler    │     │
│  └────┬─────┘   └────┬─────┘ └────┬─────┘  └──────┬───────┘     │
│       │              │            │               │             │
│  ┌────▼────┐    ┌────▼────┐   ┌───▼───┐           │             │
│  │ Schema  │    │ Cache   │   │  Hub  │  (broadcast fan-out)    │
│  │Registry │    │ (Tiered)│   └───┬───┘                         │
│  └────┬────┘    └────┬────┘       │                             │
│       │              │            │                             │
│  ┌────▼────┐         │                                          │
│  │ Dedupe  │         │       (NATS JetStream retains messages   │
│  │(optional)│        │        for SSE/WS gap-fill via           │
│  └────┬────┘         │        DeliverByStartTime)               │
│       │              │                                          │
│       ▼              │                                          │
│  ┌─────────┐         │                                          │
│  │   MQ    │         │                                          │
│  │ (NATS)  │         │                                          │
│  └────┬────┘         │                                          │
│       │              │                                          │
│  ┌────▼──────────┐   │                                          │
│  │ Buffer        │   │       ┌───────────┐                      │
│  │ Consumer      │   │       │  Active   │                      │
│  │ (batch flush) │   │       │  Sweeper  │ (purges old msgs)    │
│  └────┬──────────┘   │       └───────────┘                      │
│       │              │                                          │
│  ┌────▼──────┐       │                                          │
│  │   DLQ     │       │       (failed inserts → WAVEHOUSE_DLQ)  │
│  └───────────┘       │                                          │
│                      │                                          │
└───────┼──────────────┼──────────────────────────────────────────┘
        │              │
        ▼              ▼
┌─────────────────────────────┐   ┌───────────┐
│        ClickHouse           │   │   Redis   │  (L2 cache,
│   (analytics storage)       │   │           │   clustered only)
└─────────────────────────────┘   └───────────┘
```

## Binaries

WaveHouse ships three binaries for different deployment modes:

| Binary | Mode | Purpose |
| ------ | ---- | ------- |
| `wavehouse` | Standalone | All-in-one: API + worker + embedded NATS + optional embedded Pebble dedup. Zero external deps beyond ClickHouse. |
| `wavehouse-api` | Clustered | Stateless API server. Handles ingest/query/streaming. Connects to external NATS, Redis, optional ScyllaDB. Horizontally scalable. |
| `wavehouse-worker` | Clustered | Background worker. Consumes from NATS, batch-flushes to ClickHouse, runs Active Sweeper for message lifecycle. |

## Internal Packages

```text
internal/
├── api/         HTTP layer (Chi router, handlers, middleware, Hub)
├── cache/       Two-tier caching (L1 Ristretto + L2 Redis)
├── config/      YAML + env var configuration loading
├── dedupe/      Optional deduplication (Pebble or ScyllaDB)
├── discovery/   ClickHouse schema introspection and validation
├── ingest/      Batch buffering, DLQ, and Active Sweeper
├── mq/          Message queue abstraction (embedded or remote NATS)
├── pipes/       Named query pipes (NATS KV store + SQL file bootstrap)
├── policy/      Hasura-style access control (policy types, evaluation, NATS KV store)
└── query/       Structured query AST, SQL builder, and timestamp bucketing
```

### `api/` — HTTP Layer

The API layer uses [Chi](https://github.com/go-chi/chi) for routing with standard middleware (RequestID, RealIP, Recoverer).

- **router.go** — Route definitions. Public: `/health`, `/ready`. Protected: `/v1/ingest/{table}`, `/v1/query`, `/v1/tables/{table}/query` (structured), `/v1/pipes/{name}` (named pipes), `/v1/stream/sse`, `/v1/stream/ws`, `/v1/schema/*`, `/v1/dlq/stats`. Admin: `/v1/admin/policy`, `/v1/admin/pipes/*`.
- **middleware.go** — JWT auth middleware supporting HMAC and JWKS validation, role extraction from configurable claim path, and dev mode bypass. Controlled by `auth.enabled`.
- **policy.go** — CRUD handler for access control policies (`/v1/admin/policy`).
- **pipes.go** — Named query pipe handlers: admin CRUD and execution with parameter binding.
- **structured_query.go** — Handler for `POST /v1/tables/{table}/query`: validates query AST, enforces permissions, builds and executes SQL.
- **ingest.go** — Accepts flat JSON body for `POST /v1/ingest/{table}`, validates against discovered schema, optional dedup, publishes to NATS subject `ingest.{table}`.
- **query.go** — Executes SQL queries directly against ClickHouse. Results are cached. UUID/DateTime columns are converted to strings.
- **stream_sse.go** / **stream_ws.go** — Real-time streaming via SSE and WebSocket. Default topic is `ingest.>` (all tables). Supports gap-fill from NATS JetStream using `DeliverByStartTime`.
- **transform.go** — Shared `transformForClient` function: passes through `table_name`, `received_timestamp`, and `data` from the wire format.
- **schema.go** — Schema discovery API: list all schemas, get one table, trigger refresh.
- **dlq.go** — DLQ stats endpoint and `EnsureDLQStream` helper for creating the `WAVEHOUSE_DLQ` NATS stream.
- **hub.go** — In-process pub/sub for broadcasting MQ messages to connected streaming clients.
- **health.go** — Liveness (`/health`) and readiness (`/ready`) probes.

### `cache/` — Two-Tier Caching

- **cache.go** — `Cache` interface: `Get`, `Set`, `Close`.
- **local.go** — L1 in-process cache using [Ristretto](https://github.com/dgraph-io/ristretto) with `sync.Map` TTL tracking.
- **shared.go** — L2 distributed cache backed by Redis.
- **tiered.go** — Combines L1 + L2 with [singleflight](https://pkg.go.dev/golang.org/x/sync/singleflight) to prevent cache stampede on concurrent misses.

### `config/` — Configuration

- **config.go** — Loads configuration from YAML file with environment variable overrides (using [cleanenv](https://github.com/ilyakaznacheev/cleanenv)). All settings use `WH_` prefixed env vars. See [Configuration Reference](configuration.md).

### `dedupe/` — Deduplication (Optional)

- **dedupe.go** — `Deduplicator` interface: `CheckAndMark(ctx, eventID) (bool, error)`.
- **embedded.go** — Standalone mode: uses [Pebble](https://github.com/cockroachdb/pebble) (embedded key-value store). Key = event ID.
- **distributed.go** — Clustered mode: uses ScyllaDB with `INSERT IF NOT EXISTS` for atomic check-and-mark.
- **schema.go** — ScyllaDB DDL for the dedup table (`PRIMARY KEY (event_hash)`).

### `discovery/` — Schema Discovery & Validation

- **discovery.go** — `SchemaRegistry` queries `system.columns` to discover ClickHouse table schemas. Supports periodic auto-refresh and on-demand refresh. Thread-safe via `sync.RWMutex`.
- **validation.go** — `Validate(schema, data)` checks incoming JSON against the discovered schema: unknown fields, type compatibility, missing required columns, null handling.
- **discovery_test.go** — Unit tests for validation logic.

### `ingest/` — Bento Pipeline, DLQ & Sweeping

- **bento.go** — `StartIngestWorker` launches a Bento-based ingest pipeline: a JetStream input (`jsInput`) reads from the `WAVEHOUSE` stream via a durable `buffer-consumer` pull consumer, batches events per table, and performs bulk INSERTs to ClickHouse. Delete actions (`action: "delete"`) are handled inline via `chConn.Exec`. Failed batches are routed to a DLQ output (`dlqOutput`) which publishes to `dlq.{table}` NATS subjects when DLQ is enabled. Unsafe table names are rejected via `safeIdentifierRe`.
- **types.go** — `EventMessage` struct (TableName, ReceivedTimestamp, Data) and `BufferConsumerName` constant, shared across API handlers and the ingest pipeline.
- **sweeper.go** — `Sweeper` implements the Active Sweeper pattern. It runs every minute and purges NATS JetStream messages that are **both** ACKed by the buffer consumer (written to ClickHouse) **and** older than the configurable gap window.

### `mq/` — Message Queue

- **mq.go** — `Publisher` and `Subscriber` interfaces. `Message` struct with `Ack()`/`Nak()`.
- **embedded.go** — Standalone mode: in-process NATS server with JetStream. Creates stream `WAVEHOUSE` with subjects `ingest.>`.
- **remote.go** — Clustered mode: connects to an external NATS cluster with the same stream/subject configuration.

### `policy/` — Access Control (New)

- **policy.go** — Hasura-style policy types (`Policy`, `TablePolicy`, `RolePermissions`, `Filter`), `Evaluate()` function that resolves permissions against JWT claims (including `{{ jwt.claim.path }}` template resolution), `IsColumnAllowed()`, `IsAggregationAllowed()`, `Validate()`.
- **store.go** — `Store` backed by NATS KV bucket `WAVEHOUSE_POLICY`. Supports file-based bootstrap (YAML/JSON), cluster-wide sync via KV Watch, local caching.

### `pipes/` — Named Query Pipes (New)

- **pipes.go** — `NamedQuery` type with SQL template and parameter definitions, `Store` backed by NATS KV bucket `WAVEHOUSE_PIPES`. Supports `.sql` file directory bootstrap. `BindParams()` replaces `{{param}}` placeholders with positional parameters.

### `query/` — Structured Query Engine (New)

- **ast.go** — `StructuredQuery` AST types: columns, aggregations, filters, group by, order by, limit, time range.
- **builder.go** — `Build()` converts AST to parameterized SQL. Validates all identifiers against schema (SQL injection prevention). `InjectPermissionFilters()` adds row-level security. `ApplyMaxRows()` enforces limits. Timestamp bucketing for cache optimization.

## Data Flows

### Ingest Path

```text
Client POST /v1/ingest/{table}
  → Optional JWT auth middleware
  → Look up table schema from SchemaRegistry
  → Validate JSON body against schema (type checks, required columns)
  → Optional deduplication check (configurable ID field)
  → Publish to NATS JetStream (ingest.{table})
  → 200 OK returned immediately
  → (If NATS stream is full: 503 + Retry-After header)

Bento ingest pipeline (StartIngestWorker):
  ← JetStream pull consumer (buffer-consumer) on ingest.>
  → Validate table name against safeIdentifierRe
  → Batch events per table, bulk INSERT to ClickHouse
  → Delete actions handled inline (chConn.Exec)
  → On success: Ack messages
  → On failure: route to DLQ output (dlq.{table}), then Ack to prevent infinite retry

Active Sweeper (async goroutine, every 60s):
  → Read buffer consumer's AckFloor (highest contiguous ACKed seq)
  → Binary search for first message within the gap window
  → Purge target = MIN(ack_floor + 1, gap_window_seq)
  → Purge all messages below target from JetStream
```

### Query Path

```text
Client POST /v1/query
  → Optional JWT auth middleware
  → Check tiered cache (L1 → L2)
  → Cache HIT: return cached result (X-Cache: HIT header)
  → Cache MISS:
    → Execute query directly on ClickHouse
    → Convert UUID/DateTime types to strings
    → Store result in L1 + L2
    → Return result (X-Cache: MISS header)
```

### Streaming Path

```text
Client GET /v1/stream/sse or /v1/stream/ws
  → Optional JWT auth middleware
  → If ?since= parameter provided:
    → Create ephemeral NATS consumer with DeliverByStartTime
    → Send historical events from JetStream first
  → Subscribe to Hub (in-process pub/sub)
  → Stream live events as they arrive via MQ → Hub → client
```

## Standalone vs. Clustered

| Aspect | Standalone | Clustered |
| ------ | ---------- | --------- |
| Binaries | Single `wavehouse` binary | `wavehouse-api` + `wavehouse-worker` |
| Message Queue | Embedded NATS (in-process) | External NATS cluster |
| Deduplication | Optional — Pebble (embedded KV) | Optional — ScyllaDB (distributed) |
| Cache | L1 only (Ristretto) | L1 (Ristretto) + L2 (Redis) |
| Schema Discovery | On boot + periodic refresh | On boot + periodic refresh |
| DLQ | NATS stream `WAVEHOUSE_DLQ` | NATS stream `WAVEHOUSE_DLQ` |
| Scaling | Vertical only | Horizontal (add API/worker nodes) |
| External Dependencies | ClickHouse only | ClickHouse, NATS, Redis, (ScyllaDB if dedup enabled) |

## Technology Stack

| Component | Technology | Purpose |
| --------- | ---------- | ------- |
| Language | Go 1.25 | Core runtime |
| HTTP Router | Chi v5 | Request routing and middleware |
| Authentication | golang-jwt v5 + keyfunc v3 | JWT (HMAC + JWKS) parsing and validation |
| Analytics DB | ClickHouse | Primary data store + schema source of truth |
| Message Queue | NATS + JetStream | Durable event streaming |
| L1 Cache | Ristretto v2 | In-process memory cache |
| L2 Cache | Redis 7 | Distributed shared cache |
| Embedded KV | Pebble | Optional standalone deduplication |
| Distributed KV | ScyllaDB | Optional clustered deduplication |
| WebSocket | coder/websocket | WebSocket protocol support |
| Config | cleanenv | YAML + env var config loading |
| Release | GoReleaser | Cross-platform binary builds |
| Containers | Docker (distroless) | Minimal production images |

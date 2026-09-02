---
title: "Architecture"
description: "System design, data flows, internal packages, and technology stack."
cloudCta:
  body: "Every box in these diagrams is something somebody has to run, watch, and upgrade. On WaveHouse Cloud that somebody is us — the architecture is identical, which is why your queries and SDK code do not change when you move."
sidebar:
  order: 4
---

This document describes the internal architecture of WaveHouse, a schema-aware ClickHouse proxy.

## Overview

WaveHouse is a Go-based gateway that sits in front of ClickHouse, acting as the entry and exit point for data. It discovers your real ClickHouse table schemas, validates data at ingest time, batches inserts asynchronously, and provides real-time streaming and query caching.

```mermaid
flowchart TD
    Clients["Clients<br/>(REST API, SSE)"]:::client

    Clients --> IH
    Clients --> QH
    Clients --> SSH

    subgraph api["WaveHouse API Layer"]
        IH["Ingest Handler"] --> SR["Schema Registry"]
        SR --> DD["Dedupe (optional)"]
        DD --> MQ["MQ (NATS)"]
        MQ --> BC["Buffer Consumer<br/>(batch flush)"]
        BC -.->|failed inserts| DLQ["DLQ"]:::fail

        QH["Query Handler"] --> Cache["Cache<br/>(Ristretto + singleflight)"]

        SSH["SSE Handler"] --> Hub["Stream Hub<br/>(project once per role)"]

        SW["Active Sweeper"] -.->|purges old msgs| MQ

        NATS["NATS JetStream retains messages<br/>for SSE gap-fill<br/>via DeliverByStartTime"]
    end

    BC --> CH[("ClickHouse<br/>(analytics storage)")]:::store
    Cache --> CH

    style NATS fill:none,stroke-dasharray:5 5,stroke:#888,color:#888
```

## Binaries

WaveHouse ships a single binary, `wavehouse`: an all-in-one process running the API, batch worker, embedded NATS JetStream, and optional embedded Pebble dedup. The only external dependency is ClickHouse.

## Internal Packages

```text
internal/
├── api/         HTTP layer (Chi router, handlers, middleware)
├── auth/        JWT/JWKS authentication middleware (HMAC or JWKS, role extraction)
├── cache/       In-process Ristretto cache with singleflight coalescing
├── chconn/      The one ClickHouse driver.Conn every consumer holds; reload swaps the connection behind it
├── chsql/       Shared ClickHouse SQL helpers (identifier quoting, bind-safety)
├── config/      YAML + env var configuration loading
├── dedupe/      Optional deduplication (Pebble)
├── discovery/   ClickHouse schema introspection and validation
├── ingest/      Batch buffering, DLQ, and Active Sweeper
├── mq/          Message queue abstraction (embedded NATS)
├── observability/ OpenTelemetry pipeline (traces/metrics/logs + Prometheus exposition)
├── pipes/       Named query pipes (NamedQuery type, parameter binding, Source)
├── policy/      Hasura-style access control (policy types, evaluation, Source)
├── query/       Structured query AST, SQL builder, and timestamp bucketing
├── settings/    Settings directory: validate the JSON files, hold the adopted snapshot, reload on watch / SIGHUP / API
└── stream/      SSE fan-out: event Hub (project once per role), Subscriber queue, Bucket fan-out, keepalive Heartbeater wheel
```

### `api/` — HTTP Layer

The API layer uses [Chi](https://github.com/go-chi/chi) for routing with RequestID, a CORS middleware, and a custom JSON recoverer (`jsonRecoverer`) that emits a JSON `500` on panic instead of chi's plain-text `middleware.Recoverer`.

- **router.go** — Route definitions. Public: `/livez`, `/readyz`, and the content-free `/v1/health` SDK ping (plus the permanent `/healthz` alias and the deprecated `/health`, `/ready` aliases). Policy-gated: `/v1/ingest?table={table}`, `/v1/query?table={table}` (structured), `/v1/pipes/{name}` (named pipes), `/v1/stream`. Admin-only (`RequireAdmin` — role == `policy.admin_role`, or a request bearing the operator key's operator bit, which passes even under a nil policy): `/v1/ops/schema/*`, `/v1/ops/dlq/stats`, `GET /v1/ops/pipes[/{name}]`, `/v1/ops/settings/reload`, `/v1/ops/query` (raw SQL — same gate as the rest of `/v1/ops/*`).
- **auth middleware** — the JWT/JWKS authentication middleware is its own package, [`auth/`](#auth--authentication); the router runs it on every `/v1/*` route.
- **pipes.go** — Named query pipe handlers: admin listing (`GET /v1/ops/pipes[/{name}]`, read per request from its `pipes.Source`) and execution with parameter binding. `pipes.json` is the only write path.
- **structured_query.go** — Handler for `POST /v1/query?table={table}`: validates query AST, enforces permissions, builds and executes SQL.
- **ingest.go** — Accepts `POST /v1/ingest?table={table}` in three body shapes: one flat JSON object, a JSON array of them, or NDJSON. The **required** `Content-Type` chooses the format *family* — `application/json` versus the four NDJSON spellings — and within the JSON family the body's first non-whitespace byte picks array versus single object; the bytes never choose the family. An absent, unsupported, or comma-bearing type is a `415`, as are two `Content-Type` headers that disagree — all decided before the body is read; parameters are otherwise ignored. Then it validates each record against the discovered schema, optional dedup, publishes to NATS subject `ingest.{table}`. When dedup is on, a row missing the configured `id_field` can't be deduped: it is logged at `WARN` and counted by `wavehouse_ingest_dedupe_missing_id_total` (labeled by `table`), then published un-deduped — or rejected when `dedupe.require_id` is set ([#219](https://github.com/Wave-RF/WaveHouse/issues/219)).
- **query.go** — Proxies raw SQL for `POST /v1/ops/query` straight to ClickHouse's HTTP interface. **Not cached** — sets `Cache-Control: no-store` so every request hits ClickHouse; DateTime is rendered ISO-8601 via `date_time_output_format=iso` (the Go-side type conversion lives in the structured-query / pipes path, not here).
- **stream.go** — Real-time streaming via SSE. Callers select a table with the `?table=` query parameter. Each connection registers one `Subscriber` (the `stream/` package) with both the event `Hub` (under its `(topic, role)`) and the shared keepalive wheel, then drains both from a single byte-pump — so idle streams keep emitting `:` keepalive comments (surviving reverse-proxy idle timeouts) while live events arrive already projected and serialized. Per-event projection/serialization happens **once per role** in the `Hub`, not once per subscriber ([#294](https://github.com/Wave-RF/WaveHouse/issues/294)); the handler also snapshots the connection's JWT claims onto the `Subscriber`, which the `Hub` evaluates per subscriber when the role carries a row-level `filter` ([#319](https://github.com/Wave-RF/WaveHouse/issues/319)). Gap-fill replay from NATS JetStream (`DeliverByStartTime`) stays per-connection (low-volume, one-time on connect).
- **schema.go** — Schema discovery API: list all schemas, get one table, trigger refresh.
- **dlq.go** — DLQ stats endpoint and `EnsureDLQStream` helper for creating the `WAVEHOUSE_DLQ` NATS stream.
- **health.go** — Liveness (`/livez`), readiness (`/readyz`), and a content-free `Online` ping (`/v1/health`, the SDK's public liveness check); `/healthz` is a permanent alias of `/livez`, and `/health`/`/ready` are deprecated aliases. All three consult an optional `BootState` so they can return 503 while boot-time schema discovery is still failing in the retry loop (see `cmd/wavehouse/main.go`); once `BootState.Set(nil)` fires, `/livez` returns 200 and stays there. `/readyz` additionally pings ClickHouse each call; `/v1/health` deliberately does not.

### `stream/` — SSE keepalive & fan-out

The SSE fan-out, factored out of `api/` so the delivery hot path ([#294](https://github.com/Wave-RF/WaveHouse/issues/294)) lives next to the keepalive primitives it shares. One abstraction per file.

- **hub.go** — `Hub`, the event fan-out. Subscribers register under `(topic, role)`; `Broadcast` decodes each event once, applies each subscribed role's column policy once, builds one SSE frame per role, and fans it to every member of that role's `Bucket` — collapsing the prior per-subscriber `unmarshal → evaluate → filter → marshal` into one pass per distinct `(role, table)` output shape (the [#294](https://github.com/Wave-RF/WaveHouse/issues/294) lever; the measured ceiling was ~2 270 deliveries/s from re-projecting per subscriber). The column projection is claims-independent, so it is shared across a role's whole bucket; the role's row-level `filter` predicate is not — it is resolved against each subscriber's JWT claims, so for a role that carries a filter `Broadcast` keeps the shared column projection but delivers it only to the subscribers whose claims admit each row (`ResolvedPermissions.RowVisible`, evaluated against the full event via the type-aware comparison seeded from the schema registry — `policy.ColumnSpec`: numeric columns compare numerically, `String` bytewise, `DateTime`/`DateTime64` as instants through the same parser ingest canonicalization uses (`discovery.Column.TimeParser` — one grammar, so filter constants and canonicalized payloads can't disagree on the instant), everything else admits byte-equality only and fails ordering/`!=` closed, so a missing schema can never downgrade the comparison to a leak). Each row withheld this way increments `wavehouse_sse_rows_withheld_total`. This is the [#319](https://github.com/Wave-RF/WaveHouse/issues/319) fix that closes the query/stream row-level-security drift; roles without a filter keep the pure once-per-role fast path. `ReplayProjector` shares the same projection and per-connection row check for the handler's gap-fill, holding one policy snapshot per gap-fill and caching the per-table column-kind lookup across the replay loop.
- **subscriber.go** — `Subscriber`, the per-connection handle. It carries the connection's JWT claims, fixed at construction (`NewSubscriber(claims, metrics)`, no setter) — the claims the `Hub` resolves a role's row-level `filter` against, and immutability is what makes the fan-out's unsynchronized claims read race-free structurally. It owns a single ready-to-write outbound queue of `Frame`s (each tagged with its `kind`, so the handler labels the write where it happens): producers — the keepalive wheel and the event `Hub` — fan frames in with `Send` (non-blocking; a full queue drops, and `Send` itself counts the drop by frame kind, so no producer can forget to), and the handler drains `Frames()` to the client verbatim. The queue is sized for buffering live events (cap 64, up from the keepalive-only cap 1; #152 will make it a knob), and an `Evicted()` channel is the seam the slow-consumer follow-up closes to disconnect a wedged consumer.
- **bucket.go** — `Bucket`, the reusable fan-out primitive: a concurrency-safe set of subscribers. `Push` delivers a shared `Frame` to each fire-and-forget (the keepalive wheel's ring, and the `Hub`'s no-row-filter fast path); `Snapshot` exposes the members so the event `Hub` can evaluate row visibility per subscriber before sending (drop counting lives in `Send` itself). The `Hub` holds one `Bucket` per `(topic, role)` so a projected frame is built once and sent to every member instead of re-projected per subscriber.
- **heartbeat.go** — The keepalive wheel (`Heartbeater`). A single process-wide ticker fans a minimal `:` comment across the ring of `Bucket`s, waking ~1/N of live streams per tick so the writes don't synchronize. The effective per-connection keepalive period is `stream.keepalive_interval` in the settings directory (the wheel ticks every `keepalive_interval ÷ keepalive_buckets`, so one rotation spans the interval; a reload calls `Reconfigure`, which rebuilds the ring in place with every live subscriber carried over); the owning handler goroutine does the actual write, so the shared ticker never touches a `ResponseWriter` directly.
- **metrics.go** — `Metrics`, the SSE instrument set: `wavehouse_sse_active_streams` (open streams), `wavehouse_sse_stream_duration_seconds` (lifetime), `wavehouse_sse_frames_sent_total` / `wavehouse_sse_bytes_sent_total` (labeled by `kind`: `keepalive`, `event`, `replay`), `wavehouse_sse_dropped_frames_total` (frames dropped to a full subscriber queue — the slow-consumer signal that was silent before #294), and `wavehouse_sse_rows_withheld_total` (rows withheld from a subscriber by the row-level-security filter, labeled by table and role — the signal that separates "no matching rows" from "a fail-closed filter is withholding everything"). Nil-safe, so the handler holds one unconditionally and tests skip wiring it; one shared instance records the handler's write sites, each `Subscriber`'s queue-full drops (counted inside `Send`, by frame kind), and the `Hub`'s row-withheld counts. Separate from `observability.RegisterSystemMetrics`, which covers only the NATS/Pebble system gauges. Streams are observed through these metrics rather than per-event traces (the router excludes `/v1/stream` from the HTTP tracer).

### `auth/` — Authentication

- **auth.go** — `NewAuthenticator(cfg, policySource, logger)` owns the verifier; its `Middleware()` reads the current one per request, and `Reconfigure(cfg)` swaps the whole verifier — key source plus its pinned `alg` allowlist — atomically after a settings reload (`auth.jwks_url` / `auth.role_claim`; see [Settings Directory — Authentication](/settings-directory#authentication)). Verifies JWT tokens with HMAC **or** JWKS (never both), with the accepted `alg` pinned to the active verifier and checked before any key is consulted (rejects `alg: none` and cross-family confusion). Extracts the caller's role from a configurable dot-path claim (`auth.role_claim`, default `role`). Claims parse with `jwt.WithJSONNumber()`, so a numeric claim reaches the policy engine as its exact digits (`json.Number`, never a rounded float64) — part of the row-visibility guarantee (AGENTS.md invariant 12). It always runs and never rejects — a missing/invalid/expired token yields an empty role (resolved to `default_role` downstream), with the token error stashed in context so a denying gate can fail loud (`401`, not a bare `403`). Before the Bearer token it checks a non-JWT operator key (`auth.operator_key`): a constant-time match on the presented credential — an `Authorization: Operator <key>` header, or the `X-Operator-Key` alias — stamps the live admin role plus an operator bit (`auth.WithOperator`) that `RequireAdmin` honors even under a nil policy — a full-access break-glass credential, audit-logged at Info with no client IP (`store`/`logger` back this path). A presented-but-wrong operator key is logged at `WARN` and counted by `wavehouse_auth_operator_key_failures_total` — a probing signal on the most privileged credential — then falls through like any unauthenticated request (the middleware never rejects).
- **context.go** — request-context accessors and their setters for the role, claims, and token error (`RoleFromContext`, `ClaimsFromContext`, `AuthErrorFromContext`, and the matching `With*` helpers).

### `cache/` — Query Cache

- **cache.go** — `Cache` interface: `Get`, `Set`, `Close`.
- **local.go** — In-process cache using [Ristretto](https://github.com/dgraph-io/ristretto) with `sync.Map` TTL tracking.
- **tiered.go** — Wraps the local cache with [singleflight](https://pkg.go.dev/golang.org/x/sync/singleflight) to prevent cache stampede on concurrent misses. The tiered interface accepts an optional second cache slot for future shared-cache backends, but ships with the slot empty.

### `config/` — Configuration

- **config.go** — Loads *boot* configuration from a YAML file with environment variable overrides (using [cleanenv](https://github.com/ilyakaznacheev/cleanenv)); every key has a `WH_`-prefixed env var. Boot config is only what can't change under a running process — resource sizing, listeners, observability exporters, the settings-directory path, and the secrets (`clickhouse.password`, `auth.jwt_secret`, `auth.operator_key`). Everything tenant-tunable lives in the settings directory (`settings/`). The YAML is strict: `rejectUnknownKeys` refuses to boot naming every key the struct doesn't declare, so a tunable that moved to the settings directory can't be read, ignored, and believed. See [Configuration Reference](/configuration).

### `dedupe/` — Deduplication (Optional)

- **dedupe.go** — `Deduplicator` interface: `CheckAndMark(ctx, eventID) (bool, error)`.
- **embedded.go** — Uses [Pebble](https://github.com/cockroachdb/pebble) (embedded key-value store). Key = event ID.
- **managed.go** — `Managed` wraps the Pebble store behind the hot-reloadable `dedupe.enabled` switch: a `settings.Store.AfterAdopt` hook opens or closes the store after every adoption, so flipping the key is a reload, not a restart. `CheckAndMark` returns `ErrDisabled` while switched off (the ingest handler publishes un-deduped and counts it — a reload-window race, not a mode).

### `discovery/` — Schema Discovery & Validation

- **discovery.go** — `SchemaRegistry` queries `system.columns` to discover ClickHouse table schemas. Each refresh also records the server version (`SELECT version()`), joins `system.tables` for each table's `create_table_query` (kept in-process as `TableSchema.DDL` and marked `json:"-"` — an external-engine table renders its wiring in that statement — endpoint, bucket/host, database, username, access key id — so it must never reach `/v1/ops/schema`; ClickHouse masks the password as `[HIDDEN]` from ~23.9, so what is withheld here is the topology), reads each column's `default_expression` and 1-based `position` alongside its type, discovers the server's default time zone (`SELECT timezone()`) and bakes every `DateTime`/`DateTime64` column's canonicalization spec (precision + resolved zone) into the cached schema, so the per-record ingest path parses no type strings and loads no zones ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)). Supports periodic auto-refresh, on-demand refresh, and `RetryRefresh` (boot-time exponential backoff loop used by `cmd/wavehouse` so a transiently unreachable ClickHouse doesn't crash-loop the binary). Thread-safe via `sync.RWMutex`.
- **timestamp.go** — `CanonicalizeTimestamps(schema, data)` rewrites every parseable value in a top-level `DateTime`/`DateTime64` column to the canonical RFC 3339 UTC wire form before the event is published ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)): zone-less values are interpreted in the column's declared zone, else the discovered server default — ClickHouse's own rule, so the spelling changes but never the instant. Fail-open: an unparseable value or unresolvable zone passes through verbatim for ClickHouse's own parser to judge; ingest never rejects a record over its timestamp spelling. `Column.TimeParser()` exposes the same grammar as a value→instant parser (nil for a column with no resolved timestamp spec — a non-timestamp column, or one whose declared zone couldn't be loaded), which the stream row-filter uses so filter constants and canonicalized payloads can't disagree on the instant ([#381](https://github.com/Wave-RF/WaveHouse/issues/381)).
- **validation.go** — `Validate(schema, data)` checks incoming JSON against the discovered schema: unknown fields, type compatibility, missing required columns, null handling. Also exports the type classifiers `IsNumericType` / `IsStringType` and the storage-model classifier `NumericStorageOf` (all unwrapping `Nullable`/`LowCardinality`; the latter yields a numeric column's float width, `Decimal` scale, or integer exactness), which — together with `Column.TimeParser` from timestamp.go — seed the stream row-filter's `policy.ColumnSpec` comparison.
- **discovery_test.go** — Unit tests for validation logic.

### `ingest/` — Ingest Pipeline, DLQ & Sweeping

- **worker.go** — `StartIngestWorker` launches an ingest pipeline: a JetStream consumer reads from the `WAVEHOUSE` stream via a durable `buffer-consumer` pull subscription, batches events per table, and performs bulk INSERTs to ClickHouse. The pipeline is **insert-only**. The wire format `EventMessage` carries `{table_name, received_timestamp, data}` and nothing else; the worker accepts any table name now (the table name in the NATS subject is `query.SafeEncodeNATS(rawUnsafeTableName)`), then bulk-INSERTs. The embedded NATS server runs with `DontListen: true` (`internal/mq/embedded.go`), so the only Publishers reachable on the `ingest.>` subjects are in-process Go code — today, only the HTTP `/v1/ingest?table={table}` handler. Non-insert mutations (`DELETE`/`UPDATE`/`TRUNCATE`/…) must go through `POST /v1/ops/query` under the admin role (`policy.admin_role`) — see the Query Path section below; the `/v1/ops/*` `RequireAdmin` middleware enforces the check at the API layer, so a no/invalid-token request (resolved to `default_role`, not admin in a production config) never reaches the proxy. On a bulk-insert failure the batch is re-inserted row by row; rows that succeed are acked, and only the rows that fail again are routed to the DLQ (`sendToDLQ`), which republishes the as-published `EventMessage` envelope to `dlq.{table}` NATS subjects with the failure context in `X-DLQ-*` headers when DLQ is enabled — see [Ingest Pipeline](/ingest-pipeline) for the worker internals.
- **types.go** — `EventMessage` struct (TableName, Scope — reserved, always empty today, ReceivedTimestamp, Data) and `BufferConsumerName` constant, shared across API handlers and the ingest pipeline.
- **sweeper.go** — `Sweeper` implements the Active Sweeper pattern. It runs every minute and purges NATS JetStream messages that are **both** ACKed by the buffer consumer (written to ClickHouse) **and** older than the configurable gap window.

### `mq/` — Message Queue

- **mq.go** — `Publisher` and `Subscriber` interfaces. `Message` struct with `DoubleAck(ctx)`, `Ack()`, and `Nak()`.
- **embedded.go** — In-process NATS server with JetStream. Creates stream `WAVEHOUSE` with subjects `ingest.>`, capped at the settings directory's `mq.max_bytes_gb`; `Resize` updates the cap on the live stream after a reload.

### `observability/` — OpenTelemetry Pipeline

- **provider.go** — `InitProvider(ctx, serviceName, ProviderConfig)` wires the OTel pipeline. Each output is independently gated; the W3C TraceContext + Baggage propagator is always installed (cheap, harmless when traces are off). Returns `(shutdown, promHandler http.Handler, err)` — `promHandler` is non-nil only when `PrometheusEnabled` is true and reads from a *private* `prometheus.Registry` to avoid leaking the process/Go collectors that `prometheus.DefaultRegisterer` auto-registers. OTLP-metrics push (`MetricsEnabled`) and Prometheus exposition (`PrometheusEnabled`) are independent: either, both, or neither may be set, and any combination produces a single MeterProvider feeding the active readers. The Endpoint field is only dialed by the OTLP exporters (traces / metrics-OTLP / logs); Prometheus-only operation leaves it untouched. Provider init in `main.go` runs whenever `otel.enabled` OR `prometheus.enabled` is true, so Prometheus-only operation (Alloy/scrape, no collector) is a first-class mode.
- **logger.go** — `NewLogger(component, level, isJSON, otlpSampleRate)` produces a slog logger that fans out to stdout (always 100%) and the OTLP log exporter (DEBUG/INFO sampled at `otlpSampleRate`, WARN/ERROR always 100% as a non-configurable safety floor). `TraceHandler` injects `trace_id`/`span_id` from the active span when one exists. `otlpSamplerFn` is exposed (lowercase) for unit testing the per-level rate logic without driving through the slogmulti middleware.
- **metrics.go** — `RegisterSystemMetrics(natsServer, dedup)` registers observable gauges for embedded NATS connections, in-msgs, and Pebble dedupe storage stats. Wired in `cmd/wavehouse/main.go` after the providers are up.
- **tracer.go** — W3C TraceContext propagation over NATS message headers (`InjectNATS` / `ExtractNATS`) — bridges the API request span into the ingest worker so end-to-end traces survive the queue handoff.

The package's design invariants — stdout always 100%, WARN+ERROR always export at 100%, gRPC exporters dial lazily so unreachable collectors never block startup, private Prometheus registry — are documented in AGENTS.md "Key Design Decisions" #15 and must be preserved by anything touching this package.

### `policy/` — Access Control

- **policy.go** — Hasura-style policy types, now **role-first**: `TablePolicy` is `map[string]RolePermissions`, and a role's grant carries a separate `SelectPermissions` and `InsertPermissions` — so a field only one side ever honored (`filter`, aggregations and the `max_*` limits on select; `check` on insert) does not exist on the other, and a document that puts one there fails the strict decode as an unknown key. `Evaluate()` resolves permissions against JWT claims (including `{{ jwt.claim.path }}` template resolution) for ONE operation, marking the side it did not resolve `unresolved` so every accessor denies on it rather than reading an empty grant as unrestricted. The per-column decision `IsColumnAllowed(col, insert)` takes the side it is being asked about, alongside its batch/projection forms `AllowedProjection()` and `RestrictsColumns()`, `IsAggregationAllowed()`, `resolvePredicates()` — the one resolution both read surfaces render from, so the SQL `WHERE` and the in-memory row check cannot drift — and `Validate()`, split into `validateSelectPerms`/`validateInsertPerms` and run from `settings.Validate` on every adoption, which is where the rules in [Access Control](/access-control) are actually enforced.
- **rowfilter.go** — the in-memory row-visibility twin of the SQL `WHERE`: `HasRowFilter`, `RowVisible` (evaluates the resolved predicates against a decoded event, per subscriber), and `ColumnSpec` — the per-column comparison contract (`ColumnKind` `Numeric`/`Text`/`Time`/`Opaque`, plus each kind's parameters: the caller-supplied instant parser for `Time`, the `NumericSpec` storage model for `Numeric`) whose zero value is the fail-closed floor: numeric columns compare in the column's **storage domain** (operands rendered by canonical.go, compared by numeric.go — next two bullets), `String` bytewise, `DateTime`/`DateTime64` chronologically (both operands through the ingest grammar; either side unreadable ⇒ withheld), and everything else (including any column with no usable schema) admits byte-equality only, failing `!=`/`>`/`<` closed. Both `HasRowFilter` and `RowVisible` fail closed on a denied or unresolved grant: `HasRowFilter` is the gate in front of `RowVisible`, so it must answer *true* there or the whole-bucket fast path skips the check entirely.
- **canonical.go** — the one rendering layer for comparison operands: every value a `filter` or `check` compares — a JWT claim (`CanonicalScalar`), a policy-authored literal (`CanonicalNumericLiteral`), an ingested payload value (`numericCanonical`) — converges on one exact canonical decimal form (positional, digit-bounded, never a float64 round-trip), so what a read filter binds and what the stream compares can't drift; `scalarString` is the deliberate exception, the raw byte rendering that `Text`/`Opaque` equality compares.
- **numeric.go** — compares canonical forms the way the column that stores them would: `compareCanonicalDecimals` orders by exact digit-string arithmetic, and `NumericSpec` first narrows both operands the way ClickHouse narrows the stored value and the bound constant — `Float32`/`Float64` width rounding, `Decimal` scale truncation, integers exact at any width, with an operand outside the column's width or a `Decimal`'s precision budget refused rather than modeled; the `tests/integration` differential oracle holds in-range verdicts equal to a live ClickHouse's and asserts the never-admit-where-SQL-hides direction for the refused out-of-range operands.
- **source.go** — `Source`, a `func() *Policy` every consumer (the auth middleware, ingest, structured query, pipes, the stream hub, the `/v1/ops` gate) reads per call, so a settings reload applies to the very next request. In production it is `settings.Store.Policy`; `Static(p)` fixes one for tests. A `nil` result is a deliberate lockout.

### `pipes/` — Named Query Pipes

- **pipes.go** — `NamedQuery` type with SQL template and parameter definitions, and `Source` (`Pipe(name)` / `Pipes()`), read per request — `settings.Store` in production (`pipes.json`), `Static(q...)` in tests. `BindParams()` resolves `{{param}}` / `{{param:default}}` placeholders by inlining escaped literal values into the SQL (strings single-quote-escaped; arrays rendered as escaped `(…)` `IN`-lists). A non-scalar value with no SQL form (a JSON object, or an empty array) is rejected rather than emitted raw.

### `query/` — Structured Query Engine

- **ast.go** — `StructuredQuery` AST types: columns, aggregations, filters, group by, order by, limit, time range.
- **builder.go** — `Build()` converts AST to parameterized SQL. It is the single chokepoint that validates every referenced identifier against the schema **and** authorizes every column reference — projection, aggregation args, filters, group_by, order_by, time_range — against the role's column allowlist (the [#223](https://github.com/Wave-RF/WaveHouse/issues/223) hard cap). A full-row read is requested with `select_all`, which expands to the role's allowed columns rather than emitting a raw `SELECT *`; an omitted projection selects nothing, and `*` in `columns` is a literal column name. Every identifier is backtick-quoted via `internal/chsql` (`QuoteIdent`) so any ClickHouse-legal name is accepted — a name containing `?` is refused fail-closed ([#279](https://github.com/Wave-RF/WaveHouse/issues/279)). The role's row-level-security predicate and `max_rows` cap are emitted by `Build()` itself, as part of the WHERE and LIMIT assembly — policy SQL is never spliced into rendered text ([#322](https://github.com/Wave-RF/WaveHouse/issues/322)). Timestamp bucketing for cache optimization.

### `settings/` — Settings Directory

The hot-reloadable half of configuration: a directory of four JSON files (`config.json`, `roles.json`, `policies.json`, `pipes.json`) that the server validates at boot and re-adopts while running — `Validate` is the single gate (strict decode, per-file shape rules, and the cross-file check that every role a policy grant or pipe allowlist names is declared in `roles.json`), and `Store` holds the adopted `Document` as one atomic snapshot. `Store` is also the runtime authority for access control and pipes: it implements `policy.Source` (`Store.Policy`, nil when `policies.json` is `{}` — fail closed) and `pipes.Source`, and there is no other copy — the files are the only write path. See [Settings Directory](/settings-directory).

- **validate.go** — `Validate(dir)` reads, decodes, and checks the directory in one pass (strict JSON — unknown fields and duplicate keys are errors; per-file shape rules; cross-file role references) and returns every `Finding` at once. Shared by `wavehouse validate`, boot, and every reload.
- **finding.go** — `Finding` / `Severity`: errors make the directory invalid, warnings don't block adoption. The JSON shape is part of the ops API (`POST /v1/ops/settings/reload` returns them).
- **store.go** — `Store` owns the adopted snapshot. `Open` validates and adopts at boot; `Reload` re-validates and swaps the document atomically when there are no errors (a rejected reload keeps the previous snapshot). Consumers read typed accessors per call (`ClickHouse()`, `Auth()`, `DedupeFor(table)`, `DLQFor(table)`, `Keepalive()`, …) rather than holding values, and `AfterAdopt` registers hooks (dedupe store open/close, keepalive-wheel rebuild) that run after each successful reload.
- **watch.go** — fsnotify on the *directory* (not the files, so atomic-writer replaces and Kubernetes ConfigMap symlink swaps aren't lost), debounced into one reload; reloads once as soon as the watch exists so an edit between the boot read and the watch is never missed. `SIGHUP` and the reload endpoint funnel through the same serialized `Reload`.
- **seed.go** / **seed/** — The `go:embed`ded starter directory with every key at its default. The binary carries no compiled defaults: `wavehouse bootstrap [dir]` writes this seed, and the compose stack and e2e fixture ship copies of it.

### `chconn/` — ClickHouse Connection Manager

- **chconn.go** — `Manager` is a `driver.Conn` whose backing connection is swapped by `Reconfigure(Params)` after a settings reload changes the ClickHouse wiring (`clickhouse.addr` / `http_port` / `http_scheme` / `database` / `username` / `query_timeout`, combined with the boot-config password). Like `clickhouse.Open` it never dials, so boot tolerates an unreachable ClickHouse (schema discovery retries) and a bad address surfaces where reachability is already handled (`/readyz`, query errors). The replaced connection closes after a `query_timeout` grace so in-flight queries finish. `Target()` / `Database()` / `QueryTimeout()` expose the current wiring for the HTTP-interface consumers (ingest INSERTs, raw-SQL proxy).

### `chsql/` — ClickHouse SQL Helpers

- **chsql.go** — Dependency-free ClickHouse SQL helpers shared by `query/` and `policy/`, kept in their own package to break an import cycle. `QuoteIdent` is the single place every identifier — column, table, alias — becomes SQL text: always backtick-quoted and escaped, so any ClickHouse-legal name (dots, spaces, unicode, keywords) is safe. `BindUnsafe` reports whether a name contains a literal `?`, which would desync clickhouse-go's positional binder; such names are rejected fail-closed rather than silently mis-bound.

## Data Flows

### Ingest Path

```text wrap=false
Client POST /v1/ingest?table={table}
  → JWT auth middleware (always runs; token optional)
  → Look up table schema from SchemaRegistry
  → Policy check: role allowed to insert into this table (before the body is parsed)
  → Validate JSON body against schema (type checks, required columns)
  → Policy column rules + check clauses (disallowed columns rejected;
    claim-derived values enforced or injected)
  → Canonicalize top-level DateTime/DateTime64 column values to RFC 3339 UTC
    (rewrites the payload so every consumer shares one spelling; fail-open —
    an unparseable value passes through verbatim for ClickHouse's parser to judge)
  → Optional deduplication check (configurable ID field; a row missing that
    field is published un-deduped + logged/counted, or rejected under require_id)
  → Publish to NATS JetStream (ingest.{table})
  → 200 OK returned immediately
  → (If NATS stream is full: 503 + Retry-After header)

Ingest worker pipeline (StartIngestWorker):
  ← JetStream pull consumer (buffer-consumer) on ingest.>
  → Parse the event envelope (a malformed envelope is the only poison pill: it's acked-and-dropped)
  → Batch events per table, bulk INSERT to ClickHouse
    (INSERTs pin date_time_input_format=best_effort — the server default since
    ClickHouse 26.5; see /ingest-pipeline for the basic-vs-best_effort divergence)
  → On success: DoubleAck messages
  → On failure: re-insert row by row; each row that fails again → DLQ output (dlq.{table}), then Ack to prevent infinite retry

  (Insert-only pipeline. The wire format `EventMessage` carries only
  {table_name, received_timestamp, data}; non-insert mutations
  DELETE/UPDATE/TRUNCATE/DROP/etc. must go through POST /v1/ops/query — the
  /v1/ops/* RequireAdmin gate rejects non-admin callers at the API layer, so
  a no/invalid-token request (resolved to default_role, not admin in a
  production config) cannot reach the proxy.)

Active Sweeper (async goroutine, every 60s):
  → Read buffer consumer's AckFloor (highest contiguous ACKed seq)
  → Binary search for first message within the gap window
  → Purge target = MIN(ack_floor + 1, gap_window_seq)
  → Purge all messages below target from JetStream
```

### Query Path

```text
Client POST /v1/ops/query
  → JWT auth middleware (always runs, never rejects; a bad token yields an
    empty role and stashes its verification error for the denying gate)
  → policy.ResolveRole (empty role → default_role — the one sanctioned
    roleless exception)
  → /v1/ops RequireAdmin (resolved role == policy.admin_role, or the
    operator-key bit) — single gate shared with the rest of /v1/ops/*
    (pipes inspection, settings reload, schema discovery, DLQ stats). A denial is
    401 when a stashed error shows the caller presented an invalid token,
    else 403. Raw SQL has no per-statement scope check (a full SQL parser
    would be needed to authorize predicates), so the role gate is the
    entire authorization story. /v1/ops/query is the only sanctioned
    surface for non-SELECT statements (DELETE/UPDATE/TRUNCATE/DROP/ALTER/…);
    non-admin callers use `POST /v1/ingest?table={table}` for writes and
    the structured query endpoint or named pipes for reads.
  → Decode {"sql": "..."} from the request body.
  → POST the SQL verbatim to ClickHouse's HTTP interface at
    <scheme>://<host>:<httpport>/?default_format=JSON
       &date_time_output_format=iso&database=<db>
    Auth via X-ClickHouse-User / X-ClickHouse-Key headers.
    Bound by a clickhouse.query_timeout context derived from the inbound request — client
    disconnect cancels the upstream call.
  → ClickHouse parses the SQL natively and decides what to do:
    → Read: returns 200 + {"meta":[...], "data":[...], "rows":N,
      "statistics":{...}} as JSON. The handler extracts `data` and
      forwards just that array, preserving the [{...}, {...}] response
      shape callers expect.
    → Mutation/DDL: returns 200 + empty body. The handler emits `[]` so
      response shape stays "always an array."
    → Error: returns 4xx/5xx + plain-text error message. The handler
      maps ClickHouse 4xx → HTTP 400 (caller-fault, bad SQL or missing
      table) and ClickHouse 5xx → HTTP 502 (gateway-fault, upstream
      problem), with the trimmed message inside the JSON error
      envelope — admins see ClickHouse's exact diagnostic.
  → Response carries Cache-Control: no-store so no downstream layer
    (browser, CDN, corp proxy) caches the result.
```

The proxy-pattern wins are: zero classification logic on the WaveHouse side (no isMutation heuristic to maintain), and any ClickHouse statement type — including verbs added in future versions and inline FORMAT overrides — works without WaveHouse code changes. Multi-statement input (`SELECT 1; TRUNCATE t`) is supported when the upstream ClickHouse has multi-query enabled, which is the default on recent versions; older or restrictively-configured servers will return a clear error from ClickHouse itself for the second statement. The proxy buffers the response in memory with a 64 MiB cap (502 with `clickhouse response exceeded N bytes` on overflow, to keep a runaway `SELECT *` from pinning RAM on the API server), and passes ClickHouse's `Content-Type` through when an inline `FORMAT` directive overrides the default JSON envelope. The structured query endpoint and pipes still go through `clickhouse-go`'s native driver (Query/Exec) for performance and to keep the cached row-array shape consistent.

### Streaming Path

```text
Client GET /v1/stream
  → JWT auth middleware (always runs; token optional)
  → Register a Subscriber with the Stream Hub, keyed by (topic, role)
  → If ?since= / Last-Event-ID provided:
    → Create ephemeral NATS consumer with DeliverByStartTime
    → Send historical events (projected per-connection) first
  → Live events: MQ → Hub.Broadcast → projected & serialized ONCE per role
    → fan the finished frame to every Subscriber of that (topic, role);
      a role carrying a row-level filter delivers per subscriber instead:
      the shared frame goes only to subscribers whose JWT claims admit
      the row (RowVisible)
  → Handler drains keepalives + event frames from one byte-pump → client
  → Policy filtering (historical + live): denied tables skipped, denied
    columns stripped, row filter evaluated per subscriber against claims.
    Column projection runs once per role (Hub.Broadcast) — per-subscriber
    work only where a row filter makes visibility per-connection; replay
    shares the same column policy + row check but projects per-connection
```

## Technology Stack

| Component | Technology | Purpose |
| --------- | ---------- | ------- |
| Language | Go 1.26 | Core runtime |
| HTTP Router | Chi v5 | Request routing and middleware |
| Authentication | golang-jwt v5 + keyfunc v3 | JWT (HMAC + JWKS) parsing and validation |
| Analytics DB | ClickHouse | Primary data store + schema source of truth |
| Message Queue | NATS + JetStream | Durable event streaming |
| L1 Cache | Ristretto v2 | In-process memory cache |
| Embedded KV | Pebble | Optional deduplication |
| Config | cleanenv | YAML + env var config loading |
| Release | GoReleaser | Cross-platform binary builds |
| Containers | Docker (distroless) | Minimal production images |

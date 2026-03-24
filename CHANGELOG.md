# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- **BREAKING:** ClickHouse schema now uses `UUID` type for `tenant_id` and `event_id` columns (previously `String`)
- **BREAKING:** ClickHouse schema now uses three typed Map columns (`str_data Map(String, String)`, `num_data Map(String, Float64)`, `bool_data Map(String, Bool)`) replacing `map_keys Array(String)` / `map_values Array(String)`
- **BREAKING:** ClickHouse table engine changed from `MergeTree()` to `ReplacingMergeTree(ingested_timestamp)` with `PARTITION BY toYYYYMM(timestamp)` and `ORDER BY (tenant_id, table_name, toDate(timestamp), event_id)`
- **BREAKING:** Ingest endpoint now requires `table_name` and `data` fields (previously optional)
- **BREAKING:** The `type` column/field has been renamed to `table_name` throughout (ClickHouse DDL, API request/response, wire format, SSE/WS output)
- **BREAKING:** Ingest `id` field must be a valid UUID (previously any string)
- **BREAKING:** JWT `tenant_id` claim must be a valid UUID — non-UUID values are rejected with 403
- **BREAKING:** Query endpoint no longer requires `WHERE tenant_id = ?` — tenant filtering is automatically injected via CTE
- SSE and WebSocket streams now send client-friendly JSON with unflattened `data` object (no more `str_data`/`num_data`/`bool_data` maps or `tenant_id` in output)
- Query results automatically unflatten typed map columns into nested `data` JSON, strip `tenant_id`, and convert UUID/DateTime types to strings

### Added

- `received_timestamp` column in ClickHouse — records when BeachHouse received the event
- `ingested_timestamp` column in ClickHouse — auto-populated by ClickHouse via `DEFAULT now64(3, 'UTC')`, used as the `ReplacingMergeTree` version column
- `table_name` as a pseudo-table concept in ORDER BY for efficient partitioning by event type
- `Unflatten()` function in `schema` package — reconstructs nested JSON from three typed maps
- `transformForClient()` shared function for SSE/WS event transformation
- Automatic CTE-based tenant isolation in query handler (`injectTenantFilter`)
- UUID validation for `tenant_id` in JWT middleware
- "Resetting ClickHouse in Development" section in deployment docs

### Fixed

- Query handler: use typed scan destinations from `ColumnType.ScanType()` instead of `*interface{}`, fixing "converting String to *interface {} is unsupported" error with clickhouse-go v2

### Added

- Auto-migrate ClickHouse schema (`events` table) at startup via `clickhouse.auto_migrate` / `BH_CH_AUTO_MIGRATE` — defaults to `true` in standalone, `false` in clustered
- Auto-migrate ScyllaDB schema (keyspace + `dedupe` table) at startup via `dedupe.auto_migrate` / `BH_DEDUPE_AUTO_MIGRATE` — defaults to `true` in standalone, `false` in clustered
- Multi-tenant JWT authentication with row-level security
- Asynchronous buffered ingestion via NATS JetStream
- Exact-once event deduplication (Pebble for standalone, ScyllaDB for clustered)
- Two-tier query caching (L1 Ristretto + L2 Redis) with singleflight
- Real-time streaming via SSE and WebSocket with NATS-based gap-fill (DeliverByStartTime)
- Active Sweeper pattern for NATS message lifecycle management — purges messages that are both ACKed by the buffer consumer and older than the configurable gap window
- Backpressure mechanism: NATS `DiscardNew` policy returns 503 + `Retry-After` header when the JetStream stream is full
- Configurable NATS gap window (`BH_MQ_GAP_WINDOW_MINUTES`) and max stream size (`BH_MQ_MAX_BYTES_GB`)
- Dynamic JSON schema flattening to three typed ClickHouse Map columns (str_data, num_data, bool_data) with unflattening support
- Standalone deployment mode (single binary, embedded NATS + Pebble)
- Clustered deployment mode (separate API + worker binaries with external NATS, Redis, ScyllaDB)
- Docker Compose configurations for standalone, clustered, and dependencies-only
- Multi-platform Docker images (distroless) via GoReleaser
- Liveness and readiness health check endpoints
- Comprehensive documentation (architecture, API, configuration, deployment, development)
- CI/CD workflows (lint, test, build, release)

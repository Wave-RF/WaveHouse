# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

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
- Dynamic JSON schema flattening to ClickHouse `Array(String)` columns (map_keys/map_values)
- Standalone deployment mode (single binary, embedded NATS + Pebble)
- Clustered deployment mode (separate API + worker binaries with external NATS, Redis, ScyllaDB)
- Docker Compose configurations for standalone, clustered, and dependencies-only
- Multi-platform Docker images (distroless) via GoReleaser
- Liveness and readiness health check endpoints
- Comprehensive documentation (architecture, API, configuration, deployment, development)
- CI/CD workflows (lint, test, build, release)

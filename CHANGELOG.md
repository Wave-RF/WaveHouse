# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- **JWKS authentication**: New `auth.jwks_url` config for public key validation via JWKS endpoint. JWKS is tried first, falling back to HMAC secret. Powered by `keyfunc/v3`.
- **Role-based access control**: JWT role extraction from configurable claim path (`auth.role_claim`). Built-in `admin`/`service` roles with full access; other roles governed by policies.
- **Hasura-style access control policies**: Per-table, per-role column and row-level permissions with JWT claim templating (`{{ jwt.path }}`). Stored in NATS KV (`WAVEHOUSE_POLICY`) with file-based YAML/JSON bootstrap and cluster-wide sync via KV Watch.
- **Policy admin API**: `GET/PUT /v1/admin/policy` for CRUD, `POST /v1/admin/policy/validate` for dry-run validation.
- **Structured query endpoint**: `POST /v1/tables/{table}/query` accepts a type-safe query AST (columns, aggregations, filters, group by, order by, limit, time range). Validated against schema, permissions enforced, converted to parameterized SQL.
- **Timestamp bucketing**: Structured queries truncate time ranges to configurable buckets (`cache.timestamp_bucket_seconds`, default 60s) to improve cache hit rates.
- **Named query pipes**: Pre-defined SQL templates with parameter binding, role restrictions, and caching. `GET/POST /v1/pipes/{name}` for execution. Admin CRUD at `/v1/admin/pipes/*`. Bootstrap from `.sql` files via `pipes.directory`.
- **Ingest permission enforcement**: When policies are active, ingest checks insert permission, validates allowed columns, enforces check rules, and auto-injects claim-derived values.
- **Stream permission filtering**: SSE and WebSocket streams filter events per role — denied columns are removed and unauthorized tables are skipped.
- **Dev mode**: `auth.dev_mode` skips JWT validation and treats all requests as admin (development only).
- **`internal/policy/`** package: Policy types, evaluation engine, and NATS KV store.
- **`internal/pipes/`** package: Named query types and NATS KV store with `.sql` file bootstrap.
- **`internal/query/`** package: Structured query AST types, SQL builder with schema validation, permission injection, and timestamp bucketing.
- **TypeScript SDK** (`clients/ts/`): `@wavehouse/sdk` — zero-dependency client with type-safe query builder, real-time SSE streaming, live queries with smart aggregation updates (incrementable/decomposable/poll), and codegen CLI for generating typed interfaces from ClickHouse schemas.
- **Schema discovery**: New `internal/discovery/` package introspects ClickHouse `system.columns` to build a live schema registry. Schemas are cached and auto-refreshed on a configurable interval (`schema.refresh_interval` / `WH_SCHEMA_REFRESH_INTERVAL`).
- **Schema validation**: Ingest payloads are validated against discovered ClickHouse schemas — unknown fields, type mismatches, and non-nullable violations are rejected with descriptive 400 errors.
- **Schema API endpoints**: `GET /v1/schema` (list all tables), `GET /v1/schema/{table}` (single table), `POST /v1/schema/refresh` (force refresh).
- **Dead Letter Queue (DLQ)**: Failed batch inserts are published to a separate NATS stream (`WAVEHOUSE_DLQ`) instead of being silently lost. Controlled by `dlq.enabled` / `WH_DLQ_ENABLED`.
- **DLQ stats endpoint**: `GET /v1/dlq/stats` returns pending message count and consumer info.
- **Optional authentication**: JWT auth is now opt-in via `auth.enabled` / `WH_AUTH_ENABLED` (defaults to `false`). When disabled, all `/v1/*` routes are open.
- **Optional deduplication**: Dedup is now opt-in via `dedupe.enabled` / `WH_DEDUPE_ENABLED` (defaults to `false`). When enabled, specify the dedup key field with `dedupe.id_field` / `WH_DEDUPE_ID_FIELD`.
- **Table-based ingest routing**: Ingest endpoint is now `POST /v1/ingest/{table}` — the table name comes from the URL path.

### Changed

- **BREAKING: JWT middleware signature**: `JWTAuthMiddleware` now takes `AuthConfig` struct instead of `(secret, enabled)` pair to support JWKS, role claims, and dev mode.
- **Raw SQL restriction**: Non-admin/service roles must have `raw_sql: true` in their policy to use `POST /v1/query`.
- **Bento ingest worker**: Replaced Go channel bridge (`dataChan`) with direct JetStream pull via `consumer.Messages()`. Eliminates the 1000-message buffer, ensures NATS acks happen immediately after ClickHouse writes, and removes all package-level mutable state. The custom Bento input plugin is now registered at runtime with the JetStream consumer captured via closure instead of using `init()` and globals.
- **BREAKING: Dropped multi-tenancy** — Removed `tenant_id` from JWT claims, middleware, ClickHouse schema, dedup keys, query filtering (CTE injection), and all API request/response formats. WaveHouse is now a single-tenant gateway.
- **BREAKING: Dropped schemaless typed maps** — Removed the `str_data`/`num_data`/`bool_data` Map columns, `Flatten()`/`Unflatten()` functions, and the fixed `events` table. WaveHouse now writes to user-defined ClickHouse tables with real columns.
- **BREAKING: New ingest format** — Body is now a flat JSON object (e.g., `{"page": "/home", "score": 42}`) posted to `POST /v1/ingest/{table}`. The old `{"id", "table_name", "data"}` envelope is removed.
- **BREAKING: Bring Your Own Schema** — WaveHouse no longer auto-creates ClickHouse tables. Users must create tables before ingesting. Removed `clickhouse.auto_migrate` / `WH_CH_AUTO_MIGRATE` and `dedupe.auto_migrate` / `WH_DEDUPE_AUTO_MIGRATE` config options.
- **BREAKING: Query endpoint is direct passthrough** — SQL is forwarded to ClickHouse as-is. No CTE injection, no tenant scoping.
- **BREAKING: Auth disabled by default** — Previously JWT was always required. Now it's opt-in.
- **BREAKING: Dedup disabled by default** — Previously dedup was always on. Now it's opt-in with configurable ID field.
- NATS subjects changed from `ingest` to `ingest.<table>` (e.g., `ingest.clicks`).
- DLQ subjects use `dlq.<table>` pattern.
- SSE/WS default topic changed from `ingest` to `ingest.>` (wildcard for all tables).
- BufferConsumer now groups events by table and performs per-table dynamic INSERTs using only provided columns (omitted columns use ClickHouse defaults).
- BufferConsumer coerces JSON `float64` values to the correct Go integer/float types before appending to ClickHouse batches.
- Query cache key now includes both SQL and parameters — different parameter values no longer share caches.
- Dedup `CheckAndMark` signature changed from `(ctx, tenantID, eventID)` to `(ctx, eventID)`.
- ScyllaDB dedup table simplified to `PRIMARY KEY (event_hash)` (removed `tenant_id` column).

### Removed

- `internal/schema/` package (`Flatten`, `Unflatten` functions) — replaced by `internal/discovery/`.
- `internal/ingest/schema.go` (`EnsureSchema` function) — no more auto-migration.
- `TenantIDFromContext()` and tenant middleware context key.
- `injectTenantFilter()` CTE query rewriting.
- `tenant_id` from all ClickHouse INSERT/SELECT operations.
- Fixed `events` table schema and auto-migration DDL.
- `EventMessage.TableName` envelope field (table now comes from URL).
- `clickhouse.auto_migrate` and `dedupe.auto_migrate` config options.

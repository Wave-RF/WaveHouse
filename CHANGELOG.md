# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- **Dependabot**: `.github/dependabot.yml` with weekly grouped PRs for Go modules and GitHub Actions.
- **Vulnerability scanning**: `govulncheck ./...` runs in CI (`check` job) on every push and PR.
- **Air install check**: `make dev` now checks for `air` in `$PATH` and prints install instructions if missing (same pattern as `make lint` for golangci-lint).
- **`.air.toml` modernized**: Added `#:schema` header, replaced deprecated `bin` with `entrypoint`, added `exclude_regex`, `exclude_unchanged`, `stop_on_error`, `send_interrupt`, `[color]`, and `[misc]` sections.
- **Build tags support**: `TAGS` variable in Makefile for conditional compilation (e.g., `make build TAGS="scylla dynamodb"`).
- **VS Code workspace config**: `.vscode/settings.json` with gopls build flags (`tools`, `integration` tags), schema overrides, and search exclusions. `.vscode/extensions.json` with recommended extensions.
- **`.markdownlint.json`**: Disables `MD024` (duplicate headings) and `MD060` (table column style) for CHANGELOG and doc compatibility.
- **DLQ unit tests**: `internal/ingest/buffer_test.go` — tests for `sendToDLQ` publish routing, `flushBatch` DLQ failover on insert failure, DLQ-disabled ack behavior, and mixed-table batching.
- **DLQ handler unit tests**: `internal/api/dlq_test.go` — tests for `Stats` endpoint (empty stream, populated counts, single table) and `EnsureDLQStream` idempotency, using embedded NATS.
- **DLQ integration tests**: `tests/integration_test.go` — end-to-end tests with ClickHouse testcontainer + embedded NATS: DLQ stats empty on fresh start, DLQ populated when Bento insert fails on non-existent table, successful ingest produces no DLQ entries.
- **Shared test utilities**: `internal/testutil/` package with `NopLogger()` for silencing embedded NATS output in tests.

### Changed

- **Pinned dev tools**: `gotestsum`, `gofumpt`, and `goimports` are tracked in `go.mod` via native `tool` directives (Go 1.24+). The Makefile uses `go run` — no manual tool installation needed. Eliminates `@latest` non-determinism.
- **Race detector enabled by default**: All test targets (`make test`, `make test-integration`, `make test-all`, `make ci`) now run with `-race`. Critical for catching data races in WaveHouse's concurrent NATS/cache/streaming code.
- **Integration tests bypass cache**: `-count=1` added to `make test-integration` to ensure tests always run against fresh Docker containers.
- **Makefile overhaul**: Added `make setup`, `make fmt` (gofumpt + goimports), `make ci` (full check: fmt + lint + tests). Replaced `make check` with `make ci`. Verbose output via `V=1 make test`. Colored output with `@`-prefixed commands.
- **CI workflow hardened**: Added formatting check (`gofumpt -l`) and module tidiness check (`go mod tidy` + `git diff --exit-code`). Integration tests now run with `-race -count=1`. All test steps use gotestsum. Go version read from `go.mod` (no hardcoded version). Replaced Codecov with native `go tool cover` + GitHub step summary.
- **`.golangci.yml` v2 format**: Uses `version: "2"` with `default: none` for explicit linter control. Added `goimports` linter.
- **Lint binary check**: `make lint` and `make ci` now check for `golangci-lint` in `$PATH` and print install instructions if missing.
- **Code formatting**: Switched from `gofmt` to `gofumpt` (strict superset). Added `gofumpt` to `.golangci.yml` linters.
- **Embedded NATS logger**: `mq.NewEmbedded()` now accepts an optional `*slog.Logger` parameter to control server log output.
- **Test structure standardized**: Moved `bento_pub` smoke test to `tests/cmd/bento_pub/`. Consolidated Makefile test targets with `ARGS` pass-through.

### Fixed

- **DLQ stats per-subject counts**: `GET /v1/dlq/stats` now passes `WithSubjectFilter(">")` to NATS `Stream.Info()`, fixing empty per-table breakdown in the response.
- **Bento DLQ subject routing**: Bento ingest worker now sets `table_name` metadata on messages so the DLQ output routes to `dlq.<table>` instead of `dlq.unknown`.
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

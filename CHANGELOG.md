# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- **Repository governance files**: `CLAUDE.md` and `.gemini/styleguide.md` pointers to `AGENTS.md` so Claude Code and Gemini Code Assist pick up project conventions automatically. `.github/CODEOWNERS` routes governance-file changes (LICENSE, SECURITY, AGENTS, CI/CD config) to admin reviewers.
- **PR title linting**: `.github/workflows/pr-title.yml` validates that PR titles follow Conventional Commits. Runs informationally on every PR (not yet a required check). CONTRIBUTING.md updated with the full accepted type list (`feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `deps`, `build`, `perf`, `revert`, `style`).

- **Issue triage automation**: `.github/workflows/triage.yml` classifies new/edited issues via GitHub Models (`gpt-4o-mini`) and applies `area/*`, `security`, and `breaking-change` labels. When a `PROJECT_BOARD_TOKEN` secret with project scope is configured, also writes the `Priority` field on Task Board project #7. The step soft-fails if the secret is missing so label-only triage still works out of the box.
- **Claude Code agent workflow**: `.github/workflows/claude-agent.yml` invokes `anthropics/claude-code-action` when an OWNER, MEMBER, or COLLABORATOR mentions `@claude` in an issue / PR / review / comment, or applies the `agent` label to an issue. Requires a `CLAUDE_CODE_OAUTH_TOKEN` secret (generated via `claude setup-token`); without it the workflow exists but cannot authenticate.
- **Claude PR review workflow**: `.github/workflows/claude-review.yml` auto-reviews PRs opened or updated by OWNER/MEMBER/COLLABORATOR authors (Dependabot and drafts skipped). Runs alongside Gemini Code Assist; both are advisory — CODEOWNERS and the `main branch protection` ruleset are the merge-gate. Fork PRs from first-time contributors are not auto-reviewed; a maintainer can invoke Claude by commenting `@claude`.
- **Dependabot auto-merge workflow**: `.github/workflows/dependabot-automerge.yml` auto-approves and enables auto-merge on patch / minor dependency bumps (via `dependabot/fetch-metadata`). Major bumps post a comment and stay held for human review. Action-ecosystem bumps touching `.github/workflows/` still require human codeowner approval per the ruleset; gomod and npm bumps merge fully hands-off after CI passes.
- **Issue template config**: `.github/ISSUE_TEMPLATE/config.yml` disables blank issues and adds a Security Policy contact link that routes vulnerability reports away from public issues.
- **Repo labels — `area/*` rationalization**: Added `area/api`, `area/ingest`, `area/query`, `area/cache`, `area/dedupe`, `area/policy`, `area/pipes`, `area/sdk`, `area/docs`, `area/infra`. Renamed `Observability` → `area/observability` (preserves label on 17 historical issues). Added `agent`, `security`, `breaking-change`. Deleted unused hardware-org leftovers (`Hardware`, `Software`, `firmware`).

### Changed

- **Supply-chain hardening — SHA-pinned GitHub Actions**: All `uses:` references in `ci.yml` and `release.yml` now pin to full-length commit SHAs with version comments (`actions/checkout`, `actions/setup-go`, `golangci/golangci-lint-action`, `vladopajic/go-test-coverage`, `docker/login-action`, `goreleaser/goreleaser-action`). Protects against tag-retargeting attacks on third-party actions. Dependabot tracks updates and will open PRs that bump both the SHA and the comment.
- **SECURITY.md refreshed**: JWT section now documents JWKS support (previously only mentioned HMAC), adds role-based access control detail, distinguishes raw-SQL vs structured-query permissions, and notes the supply-chain posture (SHA-pinned actions, govulncheck, Dependabot).
- **AGENTS.md extended**: New sections for repository automation tiers (triage / Gemini / Claude agent), governance files (CODEOWNERS intent, pointer-file discipline), and the workflow rule that adding an `internal/` package requires adding a matching `area/*` label and extending `triage.yml`'s area enumeration.
- **Hub wildcard support**: `Broadcast()` now matches NATS-style topic wildcards (`*` for one token, `>` for one-or-more remaining tokens) so SSE/WS clients subscribing to patterns like `ingest.>` receive events for all matching subjects. Includes dedup to prevent double-delivery.
- **Auth `?token=` query parameter fallback**: JWT can now be passed via `?token=<jwt>` query parameter for WebSocket and SSE connections where custom headers are not possible. The `Authorization` header takes precedence. Token is stripped from URL after extraction.
- **SSE `id:` field and `Last-Event-ID` reconnection**: SSE events now include an `id:` field set to `received_timestamp`, enabling native `EventSource` automatic reconnection. The `Last-Event-ID` request header overrides `?since=` for seamless gap-fill on reconnect.
- **WebSocket multiplexing**: WebSocket connections now support in-band JSON commands (`{"action":"subscribe","topic":"..."}` / `{"action":"unsubscribe","topic":"..."}`) for dynamic multi-topic subscriptions over a single connection. Outbound messages are wrapped in `{"topic":"...","data":{...}}` envelopes. Backward compatible with `?topic=` query parameter.
- **DLQ stats table filter**: `GET /v1/dlq/stats` now accepts optional `?table=` query parameter to filter stats to a specific table.
- **TypeScript SDK `SharedWSManager`**: Single multiplexed WebSocket per client with ref-counted subscriptions, auto-reconnect with exponential backoff, and client-side NATS-style wildcard dispatch.
- **TypeScript SDK `LiveQuery`**: Stream-first backfill orchestrator — subscribes to the stream immediately, buffers events, fetches historical data, deduplicates by timestamp, then resumes live updates. Available via `queryBuilder.liveQuery(subscriber, opts?)`.
- **TypeScript SDK `FilteredStreamController`**: Streams created from query builders with active filters/columns now apply client-side filtering (eq, neq, gt, gte, lt, lte, in, like, not_like) and column projection.
- **TypeScript SDK `AbortSignal` support**: `stream({ signal })` option wires an `AbortSignal` to close the stream when aborted.
- **TypeScript SDK SSE connection counter**: Warns when opening more than 5 concurrent SSE connections (browser limit).
- **TypeScript SDK default query limit**: `QueryBuilder.DEFAULT_LIMIT = 1000` applied when no explicit limit is set, preventing unbounded result sets.
- **TypeScript SDK unit tests**: Comprehensive Vitest test suite for `@wavehouse/sdk` — `errors.test.ts`, `http.test.ts`, `query-builder.test.ts`, `table.test.ts`, `pipes.test.ts`, `namespaces.test.ts` (sql, schema, policy, DLQ, sys), `client.test.ts` (factory, namespace wiring, transport selection), `stream/controller.test.ts` (subscribe, unsubscribe, async iterator, ref counting).
- **TypeScript SDK codegen CLI**: `npm run codegen -- --url <url> --out <file>` introspects `/v1/schema` and generates a TypeScript `Database` interface with ClickHouse-to-TypeScript type mapping (String, numeric, DateTime, Array, Map, Nullable, LowCardinality, etc.).
- **TypeScript SDK playground**: Three runnable scripts (`playground:public`, `playground:auth`, `playground:admin`) demonstrating unauthenticated queries/SSE, JWT auth/WebSocket streaming, and admin workflows (schema, policy, pipes, DLQ, raw SQL). Includes Docker Compose file and setup/seed script.

### Removed

- **Dead code: `BufferConsumer`** — Removed `BufferConsumer`, `coerceValue`, `insertTableBatch`, `flushBatch`, and `sendToDLQ` from `internal/ingest/buffer.go` (replaced by Bento pipeline in `bento.go`). The `EventMessage` struct and `BufferConsumerName` constant were preserved in a new `types.go`.

### Added

- **Unit tests — Bento pipeline**: `bento_test.go` covering `safeIdentifierRe` (15 subtests), `dlqOutput.WriteBatch` (3 subtests), `jsInput.Read` (6 subtests), and `jsInput.Close`.
- **Unit tests — Sweeper**: `sweeper_test.go` covering `sweep()` (8 subtests), `findGapSequence()` (7 subtests), and `Start()` context cancellation.
- **CORS origin allowlist**: New `server.cors_allowed_origins` config field (`WH_SERVER_CORS_ALLOWED_ORIGINS`) controls which origins receive CORS headers. Defaults to `*` (allow all). When specific origins are listed, only matching requests get CORS headers with `Vary: Origin`.
- **Config validation**: `config.Validate()` runs at startup, catching invalid mode, out-of-range port, negative timeouts, missing auth credentials, zero schema refresh interval, and negative cache/gap-window values.
- **Default query limit**: Structured queries are capped at `DefaultMaxRows` (10,000) to prevent unbounded result sets. Explicit limits exceeding the cap are silently reduced.
- **WebSocket origin enforcement**: WebSocket upgrades respect the same `cors_allowed_origins` config as HTTP CORS middleware.
- **Pipes Store nil-safe Put/Delete**: `pipes.Store.Put()` and `Delete()` gracefully handle nil KV backing (memory-only mode), enabling seamless unit testing with `NewMemoryStore`.
- **Unit tests — security & robustness**: Config validation tests (11 scenarios), coerceValue tests (20 subtests), builder DefaultMaxRows tests, policy Validate/Evaluate edge cases (nil policy, negative MaxRows/MaxExecutionTime, service role, nil claims, multiple templates), ingest policy check clause match/auto-inject/deny-columns/admin bypass, discovery validation (Tuple/Enum16/Decimal/IPv4/IPv6/unknown type, nil data, all-defaults), tiered cache singleflight dedup verification, pipes handler Put/Delete success paths, router wiring verification, middleware default role claim and non-bearer token tests.
- **Test infrastructure**: Expanded `internal/testutil/` with shared mocks (`MockPublisher`, `MockCache`, `MockDeduplicator`, `MockSubscriber` in `mocks.go`), JWT helpers (`MakeJWT`, `MakeExpiredJWT` in `jwt.go`), schema helpers (`NewTestSchemaRegistry`), and response assertions (`AssertJSONResponse`, `AssertJSONContains`).
- **Policy test helper**: `policy.NewMemoryStore(p)` for in-memory policy testing without NATS.
- **Schema test helper**: `discovery.NewSchemaRegistryFromMap(tables)` factory for schema-aware tests without ClickHouse.
- **Unit tests — critical path**: `middleware_test.go` (12 scenarios: auth disabled, dev mode, JWT validation, role extraction, claim extraction), `policy_test.go` (Evaluate, IsColumnAllowed, IsAggregationAllowed, navigateClaims, resolveTemplate), `config_test.go` (defaults, YAML loading, env overrides, invalid YAML), `ingest_test.go` (valid payload, missing/unknown table, invalid JSON, schema validation, dedup, publish errors, policy enforcement).
- **Unit tests — core features**: `hub_test.go`, `health_test.go`, `schema_test.go`, `transform_test.go`, `router_test.go`, `builder_test.go`, `tiered_test.go`, `pipes_test.go`.
- **Unit tests — handlers & cache**: `policy_test.go` (handler: Get nil/populated, Put/Validate invalid JSON, Validate valid), `query_test.go` (missing SQL, invalid JSON, policy forbids/allows raw SQL, cache key determinism), `structured_query_test.go` (missing/unknown table, invalid JSON, policy forbidden/column/aggregation checks), `pipes_test.go` (handler: List, Get found/not-found, Execute not-found/role-forbidden/allowed/wildcard/missing-param/query-params, Put invalid JSON), `stream_test.go` (SSE/WS applyStreamPolicy: no-policy passthrough, column filtering, forbidden table, non-event JSON, invalid JSON), `local_test.go` (Get miss, Set and Get, expired key, overwrite, zero TTL).
- **Pipes test helper**: `pipes.NewMemoryStore(queries...)` for in-memory pipe testing without NATS.
- **Makefile targets**: `tools`, `check-tools`, `build-debug`, `fmt-check`, `lint-fix`, `fix`, `coverage-enforce` (70% threshold), `mod-tidy-check`, `vulncheck`, `security`, `deadcode`, `audit-cgo`, `size-report`, `size-tree`, `size-treemap`, `dep-graph`, `dep-why`, `dep-cut`, `binary-analysis`, `release-test`.
- **Linters**: Added `bodyclose`, `noctx`, `errorlint`, `tparallel` to `.golangci.yml`.
- **Agent instructions**: Testing conventions section in `AGENTS.md`, testing rules (6, 7) in `copilot-instructions.md`.
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

- **Generated artifacts moved to `tmp/`**: Coverage output now goes to `tmp/coverage/`, analysis artifacts (`size-map.*`, `graph.*`) to `tmp/analysis/`. Repo root is no longer cluttered with generated files. Updated Makefile, `.gitignore`, CI workflow, and all docs.
- **CGO scoped to build targets only**: Removed global `export CGO_ENABLED=0` from Makefile. `CGO_ENABLED=0` is now set per-command on `go build` invocations only. Test targets use the system default (`CGO_ENABLED=1`), which is required for `-race` on Linux CI runners. Tests continue to work on macOS (which has a pure-Go race detector) and now also work on Linux CI.
- **CI jobs parallelized**: `lint`, `test`, `build`, and `check` jobs now run fully in parallel. Previously `lint` waited for `check` and `integration` waited for `build`. Integration tests now gate only on `test` (unit tests must pass first).
- **GoReleaser config**: Added `ids` filter to `dockers_v2` (only linux builds go into Docker image) and OCI labels for image metadata.
- **`make vulncheck`**: Default output is now a summary (`-show=summary`). Use `V=1 make vulncheck` for full details.
- **`make clean`**: Simplified to `rm -rf bin/ tmp/ data/ dist/` — all generated files now live under `tmp/`.
- **CI workflow**: Refactored `.github/workflows/ci.yml` to use Makefile targets (`make mod-tidy-check`, `make fmt-check`, `make vulncheck`, `make coverage`, `make test-integration`). Pinned golangci-lint to v2.1.6.
- **`filterEventColumns()`**: Returns filtered copy instead of mutating shared event data (prevents cross-client data corruption during SSE/WS broadcast).
- **Hub bridge subscription**: `cmd/wavehouse-api/main.go` now fails startup on Subscribe error instead of suppressing it.
- **`make ci`**: Expanded to 7 steps: tidy + fmt + lint + vulncheck + build + unit tests + integration tests.
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
- **Analysis Make targets rewritten**: `size-tree` uses `gsa --format text --hide-sections` for a clean package-size table (was broken `goda weight`). `size-treemap` outputs text + SVG + interactive HTML treemap, auto-opens HTML outside CI. `dep-graph` suppresses graphviz cluster warnings, CI-aware auto-open. `dep-cut` filters to InDegree ≤ 3, strips `github.com/` prefix, respects `LIMIT` var (default 30). `audit-cgo` adds informational context about CGO_ENABLED=0. New `LIMIT` variable for analysis row limits. Analysis artifacts added to `make clean` and `.gitignore`.

### Fixed

- **Cursor pagination with DateTime64 filters**: `coerceFilterValue` now returns ClickHouse-compatible formatted strings (e.g. `2026-04-02 16:25:50.297`) preserving sub-second precision, instead of `time.Time` which the driver formats with second-only precision via `toDateTime()`. Fixes cursor pagination returning 0 rows on subsequent pages when DateTime64 columns are used as cursor keys.
- **Pipes template inline substitution**: `BindParams` now inlines parameter values directly into the SQL string (with proper escaping) instead of using `?` positional placeholders. String values that look numeric are inlined bare (for LIMIT etc.), other strings are single-quote escaped. Fixes `{{limit:10}}` templates causing ClickHouse syntax errors.
- **WebSocket double "connecting" status**: `StreamController.onStatus` now deduplicates redundant status transitions and `SharedWSManager.subscribe()` defers initial status notification to `_doConnect()` when opening a new connection. Fixes duplicate `Status: connecting` events.
- **SQL injection in Bento ingest**: Table names are now validated against `^[a-zA-Z_][a-zA-Z0-9_]*$` regex before use in SQL queries, preventing injection via crafted NATS subject metadata.
- **CORS origin reflection**: Replaced permissive origin-echoing CORS middleware with allowlist-based validation. Non-matching origins receive no CORS headers.
- **os.Exit in library code**: `ingest.StartIngestWorker` now returns `(*service.Stream, error)` instead of calling `os.Exit(1)`, enabling proper error handling by callers.
- **Policy nil claim injection**: `resolveTemplate()` returns `""` for unresolvable JWT claim templates instead of leaking raw template text into SQL filters.
- **Data loss on shutdown**: Buffer ticker goroutine now calls `flush()` before returning on context cancellation, ensuring in-flight batches are written to ClickHouse during graceful shutdown.
- **Hub channel leak**: `Hub.Unsubscribe()` now calls `close(ch)` after removing the channel, preventing goroutine leaks in SSE/WS handlers waiting on the channel.
- **Ristretto race on Close**: `LocalCache.Close()` now calls `cache.Wait()` before `cache.Close()`, ensuring all async admission goroutines have finished before teardown. Added `LocalCache.Wait()` for test use.
- **Test race in tiered cache**: `memCache` test helper now uses `sync.RWMutex` to prevent concurrent map read/write during singleflight tests.
- **Docs — configuration**: Added missing `clickhouse.http_port` field, fixed `policy.file_path` default to `policy.yaml`, fixed `pipes.directory` default to `./pipes`.
- **Docs — deployment**: Fixed `cluster.yaml` → `clustered.yaml` Docker Compose file references.
- **Docs — development**: Updated Makefile targets table (removed nonexistent `build-all`/`compose-cluster`, added all new targets), added missing linters to list, added missing packages to project structure, updated vulnerability scanning to use `make vulncheck`.
- **`config.yaml`**: Fixed `policy.file_path` and `pipes.directory` defaults to match `config.go` struct tags.
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

### Changed (breaking)

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

# AGENTS.md — AI Agent Instructions for BeachHouse

This file provides context for AI coding agents (Copilot, Cursor, Cody, Aider, etc.) working on this codebase.

## Project Overview

BeachHouse is a **schema-aware real-time API gateway for ClickHouse**, written in Go. It handles ingestion with schema validation, optional deduplication, caching, real-time streaming, query proxying, and a Dead Letter Queue. It sits entirely in front of ClickHouse as the exclusive data entry/exit point.

## Architecture (Quick Reference)

Three binaries:

- **`cmd/beachhouse/`** — Standalone mode (all-in-one with embedded NATS, optional Pebble dedup)
- **`cmd/beachhouse-api/`** — Clustered API server (stateless, horizontally scalable)
- **`cmd/beachhouse-worker/`** — Clustered background worker (batch consumer + sweeper)

Seven internal packages under `internal/`:

- **`api/`** — Chi HTTP router, optional JWT middleware, ingest/query/SSE/WS/schema/DLQ handlers, Hub
- **`cache/`** — `Cache` interface → `LocalCache` (Ristretto) + `SharedCache` (Redis) + `TieredCache` (singleflight)
- **`config/`** — YAML + env var config loading (cleanenv)
- **`dedupe/`** — `Deduplicator` interface → `Embedded` (Pebble) + `Distributed` (ScyllaDB) — optional, controlled by `dedupe.enabled`
- **`discovery/`** — `SchemaRegistry` that introspects ClickHouse `system.columns` + `Validate()` for ingest payloads
- **`ingest/`** — `BufferConsumer` (per-table batch flush to ClickHouse with DLQ) + `Sweeper` (Active Sweeper for NATS message lifecycle)
- **`mq/`** — `Publisher`/`Subscriber` interfaces → `EmbeddedNATS` + `RemoteNATS`

## Key Design Decisions

1. **Interface-first**: Core behaviors are defined as Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`). Standalone and clustered modes use different implementations.
2. **Bring Your Own Schema (BYOS)**: Users create tables in ClickHouse directly. BeachHouse discovers schemas by querying `system.columns` and validates ingest payloads against real column definitions. No auto-migration, no fixed table schema.
3. **Schema-driven ingest**: `POST /v1/ingest/{table}` accepts a flat JSON body. The table name comes from the URL. The body is validated against the discovered schema (unknown fields rejected, types checked, nullable constraints enforced). No envelope — just data.
4. **Async ingestion**: Ingest returns 200 immediately after optional dedup + MQ publish. ClickHouse writes happen asynchronously via BufferConsumer. If NATS stream is full, returns 503 + Retry-After.
5. **Per-table batching**: BufferConsumer groups events by table name and performs dynamic INSERTs using the schema's column order. Each table's batch is independent.
6. **Dead Letter Queue**: Failed batch inserts are published to a separate NATS stream (`BEACHHOUSE_DLQ`) with subjects `dlq.<table>`. This prevents silent data loss. Controlled by `dlq.enabled`.
7. **Optional auth**: JWT authentication is opt-in via `auth.enabled`. When disabled (default), all `/v1/*` routes are open. When enabled, tokens are validated but no tenant scoping is applied.
8. **Optional dedup**: Deduplication is opt-in via `dedupe.enabled`. When enabled, the `dedupe.id_field` config specifies which JSON field to use as the dedup key.
9. **Singleflight**: TieredCache uses `golang.org/x/sync/singleflight` to prevent cache stampede.
10. **Active Sweeper**: NATS messages are retained for SSE/WS gap-fill. The Sweeper purges messages that are both ACKed (written to ClickHouse) and older than the gap window. Gap-fill uses NATS `DeliverByStartTime` — no in-process ring buffer.

## Code Conventions

- **Go 1.25**, standard formatting (`gofmt`)
- **Structured logging** with `log/slog` (JSON handler)
- **Chi v5** for HTTP routing
- **Error handling**: Return errors, don't panic. Wrap with `fmt.Errorf("context: %w", err)`.
- **No global state**: Dependencies are passed explicitly (constructor injection).
- **Package naming**: Lowercase, single word (or abbreviated). `internal/` enforces module privacy.

## Build & Test Commands

```bash
make build             # Compile all 3 binaries to bin/
make test              # Unit tests: go test ./internal/...
make test-integration  # Integration tests (needs Docker): go test ./tests/... -tags=integration
make lint              # golangci-lint run ./...
make dev               # Hot-reload dev server (air)
make docker            # Build Docker images
make clean             # Remove bin/, tmp/, data/
```

## Documentation & Consistency Sync (MANDATORY)

**This is a hard requirement. Every code change MUST include corresponding updates to all affected files below. Do NOT wait for the user to ask — verify and update these automatically as part of every task. A code change without its documentation counterpart is incomplete.**

### What to check on EVERY change

1. **API docs** (`docs/api.md`) — If you add, modify, or remove an endpoint, request/response field, error code, or query parameter, update the API reference. Ensure JSON field names, HTTP status codes, and curl examples match the actual handler code.
2. **Configuration docs** (`docs/configuration.md`) — If you add or change a field in `internal/config/config.go`, update the config reference table, the example YAML block, and the mode-specific settings section.
3. **Architecture docs** (`docs/architecture.md`) — If you add/rename a package, change a data flow, or modify component wiring, update the architecture overview, package descriptions, data flow diagrams, and the standalone-vs-clustered comparison table.
4. **Deployment docs** (`docs/deployment.md`) — If you change Docker Compose files, environment variables, the ClickHouse schema, or startup behavior, update the deployment guide and quick-start blocks.
5. **Development docs** (`docs/development.md`) — If you change build commands, test procedures, prerequisites, or the project structure, update the development guide.
6. **README.md** — If any user-facing behavior, quick-start steps, or feature descriptions change, update the README.
7. **CHANGELOG.md** — Every notable change gets an entry under `[Unreleased]`. Use Added/Changed/Fixed/Removed subsections.
8. **AGENTS.md** — If you change the architecture, add packages, modify design decisions, or alter conventions described here, update this file so future agents have accurate context.
9. **Docker Compose files** (`deployments/compose/`) — If you add a new env var or dependency, ensure all relevant compose files set it.
10. **Default config** (`config.yaml`) — If you add a config field with a default, ensure `config.yaml` includes it.

### Cross-referencing rules

These representations of the same data MUST always agree:

| Source of truth | Must match in |
| --------------- | ------------- |
| Go struct tags in `config.go` (field name, env var, default) | `docs/configuration.md` tables, `config.yaml`, compose env blocks |
| `EventMessage` struct JSON tags in `buffer.go` | `docs/api.md` event format, SSE/WS examples, ClickHouse `INSERT` columns |
| Route registrations in `router.go` | `docs/api.md` endpoint list |
| Handler error responses in `ingest.go`, `query.go`, etc. | `docs/api.md` error tables |
| Compose env vars in `deployments/compose/*.yaml` | `docs/configuration.md`, `docs/deployment.md` |

### How to verify

Before finishing any task, do a quick search across docs for the identifiers you touched (field names, env var names, endpoint paths, struct names). If anything is stale, fix it in the same change.

### Quick reference table

| Change | Files to update |
| ------ | --------------- |
| Add/modify API endpoint | `docs/api.md`, `README.md` (if user-facing) |
| Add/modify config option | `docs/configuration.md`, `config.yaml`, compose files, `docs/deployment.md` |
| Change architecture/packages | `docs/architecture.md`, `AGENTS.md` |
| Change ingest/event format | `docs/api.md`, `docs/deployment.md` (CH schema) |
| Change deployment/Docker | `docs/deployment.md`, compose files |
| Change build/test process | `docs/development.md`, `Makefile` |
| Any notable change | `CHANGELOG.md` (under `[Unreleased]`) |

## Common Tasks

### Adding a new API endpoint

1. Create or modify a handler in `internal/api/` (follow existing patterns like `ingest.go`).
2. Register the route in `internal/api/router.go`.
3. If it needs new dependencies, add to the `Dependencies` struct in `router.go`.
4. Wire dependencies in the relevant `cmd/*/main.go` file(s).
5. Add tests.
6. Document in `docs/api.md`.

### Adding a new config option

1. Add the field to the appropriate struct in `internal/config/config.go` with `yaml`, `env`, and `env-default` tags.
2. Use the new config value in the relevant `cmd/*/main.go` or internal package.
3. Document in `docs/configuration.md`.

### Adding a new internal package

1. Create the package under `internal/`.
2. Define an interface if there will be multiple implementations.
3. Wire it into the appropriate `cmd/*/main.go`.
4. Document in `docs/architecture.md`.

## File Structure

```text
cmd/                    → Binary entry points (thin — just wiring)
internal/api/           → HTTP layer (handlers, router, middleware, Hub, schema/DLQ endpoints)
internal/cache/         → Caching (interface + L1/L2/tiered implementations)
internal/config/        → Configuration structs + loader
internal/dedupe/        → Optional deduplication (interface + embedded/distributed)
internal/discovery/     → ClickHouse schema introspection + ingest validation
internal/ingest/        → Batch buffer with DLQ + Active Sweeper (NATS message lifecycle)
internal/mq/            → MQ abstraction (interface + embedded/remote NATS)
tests/                  → Integration tests (build tag: integration)
deployments/compose/    → Docker Compose files
deployments/docker/     → Dockerfiles
docs/                   → Project documentation
```

## Security Considerations

- JWT secret must be cryptographically strong when auth is enabled in production
- All `/v1/*` routes are behind optional JWT auth middleware
- Input JSON is validated against ClickHouse schemas before processing
- ClickHouse queries are passed through directly — use appropriate access controls on ClickHouse itself

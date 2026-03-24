# AGENTS.md — AI Agent Instructions for BeachHouse

This file provides context for AI coding agents (Copilot, Cursor, Cody, Aider, etc.) working on this codebase.

## Project Overview

BeachHouse is a **real-time API gateway and sidecar for ClickHouse**, written in Go. It handles multi-tenant ingestion, deduplication, caching, real-time streaming, and query proxying. It sits entirely in front of ClickHouse as the exclusive data entry/exit point.

## Architecture (Quick Reference)

Three binaries:

- **`cmd/beachhouse/`** — Standalone mode (all-in-one with embedded NATS + Pebble)
- **`cmd/beachhouse-api/`** — Clustered API server (stateless, horizontally scalable)
- **`cmd/beachhouse-worker/`** — Clustered background worker (batch consumer + sweeper)

Seven internal packages under `internal/`:

- **`api/`** — Chi HTTP router, JWT middleware, ingest/query/SSE/WS handlers, Hub
- **`cache/`** — `Cache` interface → `LocalCache` (Ristretto) + `SharedCache` (Redis) + `TieredCache` (singleflight)
- **`config/`** — YAML + env var config loading (cleanenv)
- **`dedupe/`** — `Deduplicator` interface → `Embedded` (Pebble) + `Distributed` (ScyllaDB)
- **`ingest/`** — `BufferConsumer` (batch flush to ClickHouse) + `Sweeper` (Active Sweeper for NATS message lifecycle)
- **`mq/`** — `Publisher`/`Subscriber` interfaces → `EmbeddedNATS` + `RemoteNATS`
- **`schema/`** — JSON flattening to typed Maps + unflattening (`Flatten` and `Unflatten` functions)

## Key Design Decisions

1. **Interface-first**: Core behaviors are defined as Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`). Standalone and clustered modes use different implementations.
2. **Tenant isolation**: `tenant_id` is **always** sourced from JWT claims via middleware — never from request bodies, query params, or headers. The middleware validates that `tenant_id` is a valid UUID. Queries are auto-scoped to the tenant via CTE injection — users never write `WHERE tenant_id = ?`. Do not introduce any code path that reads tenant_id from user input.
3. **Typed Map schema**: Arbitrary JSON is flattened to three typed `Map` columns (`str_data Map(String, String)`, `num_data Map(String, Float64)`, `bool_data Map(String, Bool)`) for ClickHouse. No ALTER TABLE migrations needed. The `Unflatten` function reconstructs nested JSON from the three maps.
4. **Async ingestion**: Ingest returns 200 immediately after dedup + MQ publish. ClickHouse writes happen asynchronously via BufferConsumer. If NATS stream is full, returns 503 + Retry-After.
5. **Singleflight**: TieredCache uses `golang.org/x/sync/singleflight` to prevent cache stampede.
6. **Active Sweeper**: NATS messages are retained for SSE/WS gap-fill. The Sweeper purges messages that are both ACKed (written to ClickHouse) and older than the gap window. Gap-fill uses NATS `DeliverByStartTime` — no in-process ring buffer.
7. **Auto-migrate**: ClickHouse and ScyllaDB schemas can be auto-created at startup using `CREATE ... IF NOT EXISTS`. Controlled by `clickhouse.auto_migrate` / `dedupe.auto_migrate` config flags. Defaults to `true` in standalone mode (zero-setup) and `false` in clustered mode (operator-managed). Env var overrides: `BH_CH_AUTO_MIGRATE`, `BH_DEDUPE_AUTO_MIGRATE`.

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
| ClickHouse `INSERT` column list in `buffer.go` | `docs/deployment.md` CREATE TABLE schema, `internal/ingest/schema.go` DDL |
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
internal/api/           → HTTP layer (handlers, router, middleware, Hub)
internal/cache/         → Caching (interface + L1/L2/tiered implementations)
internal/config/        → Configuration structs + loader
internal/dedupe/        → Deduplication (interface + embedded/distributed)
internal/ingest/        → Batch buffer + Active Sweeper (NATS message lifecycle)
internal/mq/            → MQ abstraction (interface + embedded/remote NATS)
internal/schema/        → JSON flattening to typed Maps
tests/                  → Integration tests (build tag: integration)
deployments/compose/    → Docker Compose files
deployments/docker/     → Dockerfiles
docs/                   → Project documentation
```

## Security Considerations

- Never trust user input for tenant identification
- JWT secret must be cryptographically strong in production
- All `/v1/*` routes are behind JWT auth middleware
- ClickHouse queries bind tenant_id as a parameter (not string interpolation)
- Input JSON is validated before processing

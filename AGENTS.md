# AGENTS.md — AI Agent Instructions for BeachHouse

This file provides context for AI coding agents (Copilot, Cursor, Cody, Aider, etc.) working on this codebase.

## Project Overview

BeachHouse is a **real-time API gateway and sidecar for ClickHouse**, written in Go. It handles multi-tenant ingestion, deduplication, caching, real-time streaming, and query proxying. It sits entirely in front of ClickHouse as the exclusive data entry/exit point.

## Architecture (Quick Reference)

Three binaries:

- **`cmd/beachhouse/`** — Standalone mode (all-in-one with embedded NATS + Pebble)
- **`cmd/beachhouse-api/`** — Clustered API server (stateless, horizontally scalable)
- **`cmd/beachhouse-worker/`** — Clustered background worker (batch consumer + replay)

Seven internal packages under `internal/`:

- **`api/`** — Chi HTTP router, JWT middleware, ingest/query/SSE/WS handlers, Hub
- **`cache/`** — `Cache` interface → `LocalCache` (Ristretto) + `SharedCache` (Redis) + `TieredCache` (singleflight)
- **`config/`** — YAML + env var config loading (cleanenv)
- **`dedupe/`** — `Deduplicator` interface → `Embedded` (Pebble) + `Distributed` (ScyllaDB)
- **`ingest/`** — `BufferConsumer` (batch flush to ClickHouse) + `ReplayBuffer` (ring buffer for gap-fill)
- **`mq/`** — `Publisher`/`Subscriber` interfaces → `EmbeddedNATS` + `RemoteNATS`
- **`schema/`** — JSON flattening to dot-notation EAV (`Flatten` function)

## Key Design Decisions

1. **Interface-first**: Core behaviors are defined as Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`). Standalone and clustered modes use different implementations.
2. **Tenant isolation**: `tenant_id` is **always** sourced from JWT claims via middleware — never from request bodies, query params, or headers. Do not introduce any code path that reads tenant_id from user input.
3. **EAV schema**: Arbitrary JSON is flattened to `Map(String, String)` for ClickHouse. No ALTER TABLE migrations needed.
4. **Async ingestion**: Ingest returns 200 immediately after dedup + MQ publish. ClickHouse writes happen asynchronously via BufferConsumer.
5. **Singleflight**: TieredCache uses `golang.org/x/sync/singleflight` to prevent cache stampede.

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

## Documentation Maintenance

**When you modify code, keep documentation in sync:**

| Change | Update |
| ------ | ------ |
| Add/modify API endpoint | `docs/api.md` |
| Add/modify config option | `docs/configuration.md` and `internal/config/config.go` |
| Change architecture/packages | `docs/architecture.md` |
| Change deployment | `docs/deployment.md` |
| Change build/test process | `docs/development.md` |
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
internal/ingest/        → Batch buffer + replay ring buffer
internal/mq/            → MQ abstraction (interface + embedded/remote NATS)
internal/schema/        → JSON flattening to EAV
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

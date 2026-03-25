# Development Guide

## Prerequisites

- **Go 1.25+** — [Install Go](https://go.dev/dl/)
- **Docker** — For running dependencies and integration tests
- **golangci-lint v2** — `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`
- **air** (optional) — For hot-reload during development: [Install air](https://github.com/air-verse/air)

## Quick Start (Standalone Mode)

This is the fastest way to get a fully functional local environment:

```bash
# 1. Clone and install dependencies
git clone https://github.com/Wave-RF/BeachHouse.git
cd BeachHouse
go mod download

# 2. Start ClickHouse (the only external dependency for standalone mode)
make compose-deps

# 3. Create a table in ClickHouse
docker compose -f deployments/compose/dependencies.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS clicks (
      page String,
      button String,
      score Float64,
      received_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
    ) ENGINE = MergeTree()
    ORDER BY (page)
  "

# 4. Run with hot-reload (recompiles on every .go file save)
make dev
```

BeachHouse is now running at `http://localhost:8080` in standalone mode with:

- **Embedded NATS** (JetStream) — no external MQ needed
- **L1 cache only** (Ristretto) — no Redis needed
- **Auth disabled** by default — no JWT needed
- **Dedup disabled** by default — no Pebble needed
- **Schema discovery** — automatically finds your ClickHouse tables

### Test the API

```bash
# Ingest data (no auth required by default)
curl -s -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}

# Check discovered schemas
curl -s http://localhost:8080/v1/schema | jq

# Query events (wait a few seconds for the batch flush)
curl -s -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'

# Open an SSE stream for all tables (Ctrl+C to stop)
curl -N http://localhost:8080/v1/stream/sse

# Open an SSE stream for a specific table
curl -N "http://localhost:8080/v1/stream/sse?topic=ingest.clicks"

# With gap-fill (replays events since the given timestamp, then switches to live)
curl -N "http://localhost:8080/v1/stream/sse?since=2026-03-24T11:00:00Z"

# Health check (no auth required)
curl http://localhost:8080/health
# → {"status":"ok"}

# DLQ stats
curl http://localhost:8080/v1/dlq/stats
```

### Enable Auth (Optional)

Set `BH_AUTH_ENABLED=true` and `BH_AUTH_JWT_SECRET=my-secret` to require JWT tokens:

```bash
BH_AUTH_ENABLED=true BH_AUTH_JWT_SECRET=my-secret make dev
```

Then generate a test token:

```bash
# Using jwt-cli (https://github.com/mike-engel/jwt-cli)
export TOKEN=$(jwt encode --secret "my-secret" '{"exp": 9999999999}')

curl -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup"}'
```

### Enable Dedup (Optional)

Set `BH_DEDUPE_ENABLED=true` and `BH_DEDUPE_ID_FIELD=event_id`:

```bash
BH_DEDUPE_ENABLED=true BH_DEDUPE_ID_FIELD=event_id make dev
```

Then include the dedup field in your ingest body:

```bash
curl -s -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"event_id": "550e8400-e29b-41d4-a716-446655440001", "page": "/home"}'
# → {"ok":true}

# Same event_id again → deduplicated
curl -s -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"event_id": "550e8400-e29b-41d4-a716-446655440001", "page": "/home"}'
# → {"duplicate":true}
```

## Quick Start (Clustered Mode)

To develop against the full clustered infrastructure locally:

```bash
# 1. Start all dependencies (ClickHouse, NATS, Redis, ScyllaDB)
make compose-deps

# 2. Create your tables in ClickHouse
docker compose -f deployments/compose/dependencies.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS clicks (
      page String, button String, score Float64,
      received_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
    ) ENGINE = MergeTree() ORDER BY (page)
  "

# 3. Run the API server in one terminal
BH_MODE=clustered \
BH_MQ_URL=nats://localhost:4222 \
BH_CACHE_REDIS_URL=redis://localhost:6379 \
go run ./cmd/beachhouse-api

# 4. Run the worker in another terminal
BH_MODE=clustered \
BH_MQ_URL=nats://localhost:4222 \
BH_CH_ADDR=localhost:9000 \
go run ./cmd/beachhouse-worker
```

### Using an .env File

```bash
# .env.clustered
export BH_MODE=clustered
export BH_CH_ADDR=localhost:9000
export BH_MQ_URL=nats://localhost:4222
export BH_CACHE_REDIS_URL=redis://localhost:6379
```

Then:

```bash
source .env.clustered
go run ./cmd/beachhouse-api    # terminal 1
go run ./cmd/beachhouse-worker # terminal 2
```

## Building

```bash
# Build all three binaries to bin/
make build

# Build individual binaries
go build -o bin/beachhouse ./cmd/beachhouse
go build -o bin/beachhouse-api ./cmd/beachhouse-api
go build -o bin/beachhouse-worker ./cmd/beachhouse-worker
```

## Running Modes at a Glance

| What you want | Command |
| ------------- | ------- |
| Hot-reload standalone dev server | `make dev` |
| Standalone binary (default config) | `./bin/beachhouse` |
| Standalone via Docker Compose | `make compose-standalone` |
| Clustered API server (local deps) | `source .env.clustered && go run ./cmd/beachhouse-api` |
| Clustered worker (local deps) | `source .env.clustered && go run ./cmd/beachhouse-worker` |
| Full clustered stack via Docker | `make compose-cluster` |
| Infrastructure deps only | `make compose-deps` |

## Testing

### Unit Tests

```bash
make test
# or
go test ./internal/...
```

Unit tests live alongside the code they test (e.g., `internal/discovery/discovery_test.go`).

### Integration Tests

Integration tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up real ClickHouse and NATS containers. They require Docker.

```bash
make test-integration
# or
go test ./tests/... -tags=integration -v -timeout 120s
```

Integration tests use the `//go:build integration` build tag and are located in the `tests/` directory.

### Test Coverage

```bash
go test -coverprofile=coverage.txt -covermode=atomic ./internal/...
go tool cover -html=coverage.txt -o coverage.html
```

## Linting

```bash
make lint
# or
golangci-lint run ./...
```

The linter configuration is in `.golangci.yml`. Enabled linters:

- **errcheck** — Unchecked error returns
- **govet** — Suspicious constructs
- **staticcheck** — Static analysis
- **unused** — Unused code
- **gosec** — Security issues
- **gocritic** — Opinionated style checks
- **revive** — Extensible linter (replaces golint)
- **ineffassign** — Ineffective assignments
- **misspell** — Spelling errors in comments/strings

## Project Structure

```text
BeachHouse/
├── cmd/                    # Binary entry points
│   ├── beachhouse/         # Standalone all-in-one binary
│   ├── beachhouse-api/     # Clustered API server
│   └── beachhouse-worker/  # Clustered background worker
├── internal/               # Private application packages
│   ├── api/                # HTTP handlers, router, middleware
│   ├── cache/              # L1 (Ristretto) + L2 (Redis) caching
│   ├── config/             # YAML + env var configuration
│   ├── dedupe/             # Optional deduplication (Pebble or ScyllaDB)
│   ├── discovery/          # ClickHouse schema introspection + validation
│   ├── ingest/             # Batch buffering + DLQ + Active Sweeper
│   └── mq/                 # NATS message queue abstraction
├── tests/                  # Integration tests
├── deployments/
│   ├── compose/            # Docker Compose files
│   └── docker/             # Dockerfiles
├── docs/                   # Documentation
├── config.yaml             # Default configuration file
├── Makefile                # Build, test, lint, deploy targets
├── .golangci.yml           # Linter configuration
├── .goreleaser.yaml        # Release build configuration
└── .air.toml               # Hot-reload configuration
```

## Code Conventions

- **Standard Go formatting**: Use `gofmt` (enforced by CI).
- **Interface-first design**: Core behaviors (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`) are defined as interfaces with separate implementations for standalone and clustered modes.
- **Package boundaries**: The `internal/` directory ensures packages are private to this module.
- **Error handling**: Return errors to callers. Use `slog` for structured logging.
- **Schema-driven**: ClickHouse is the schema source of truth. BeachHouse discovers and validates against real table schemas.

## Makefile Targets

| Target | Description |
| ------ | ----------- |
| `make build` | Compile all three binaries to `bin/` |
| `make dev` | Hot-reload development server (requires air) |
| `make test` | Run unit tests |
| `make test-integration` | Run integration tests (requires Docker) |
| `make lint` | Run golangci-lint |
| `make docker` | Build all Docker images |
| `make compose-standalone` | Start standalone mode via Docker Compose |
| `make compose-cluster` | Start clustered mode via Docker Compose |
| `make compose-deps` | Start infrastructure dependencies only |
| `make clean` | Remove `bin/`, `tmp/`, and `data/` directories |

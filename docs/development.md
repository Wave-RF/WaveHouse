# Development Guide

## Prerequisites

- **Go 1.25+** — [Install Go](https://go.dev/dl/)
- **Docker** — For running dependencies and integration tests
- **golangci-lint v2** — `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`
- **air** (optional) — For hot-reload during development: [Install air](https://github.com/air-verse/air)
- **jwt-cli** (optional) — For generating test tokens: [Install jwt-cli](https://github.com/mike-engel/jwt-cli)

## Quick Start (Standalone Mode)

This is the fastest way to get a fully functional local environment:

```bash
# 1. Clone and install dependencies
git clone https://github.com/Wave-RF/BeachHouse.git
cd BeachHouse
go mod download

# 2. Start ClickHouse (the only external dependency for standalone mode)
make compose-deps

# 3. Run with hot-reload (recompiles on every .go file save)
# The events table is auto-created at startup (standalone auto_migrate defaults to true).
make dev
```

BeachHouse is now running at `http://localhost:8080` in standalone mode with:
- **Embedded NATS** (JetStream) — no external MQ needed
- **Embedded Pebble** — no external dedup store needed
- **L1 cache only** (Ristretto) — no Redis needed

### Generate a Test JWT

The default `config.yaml` uses `jwt_secret: change-me-in-production`. Generate a token:

```bash
# Option 1: jwt-cli
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"tenant_id": "test-tenant", "exp": 9999999999}')

# Option 2: Python
export TOKEN=$(python3 -c "
import jwt, time
print(jwt.encode({'tenant_id': 'test-tenant', 'exp': int(time.time()) + 86400}, 'change-me-in-production', algorithm='HS256'))
")
```

### Test the API

```bash
# Ingest an event
curl -s -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "evt-001", "type": "page_view", "data": {"url": "/home", "user": "alice"}}'
# → {"ok":true}

# Ingest the same event again (dedup test)
curl -s -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "evt-001", "type": "page_view", "data": {"url": "/home", "user": "alice"}}'
# → {"duplicate":true}

# Query events (wait a few seconds for the batch flush)
curl -s -X POST http://localhost:8080/v1/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM events WHERE tenant_id = ? LIMIT 10"}'

# Open an SSE stream (Ctrl+C to stop)
curl -N http://localhost:8080/v1/stream/sse \
  -H "Authorization: Bearer $TOKEN"

# Open an SSE stream with gap-fill (replays events since the given timestamp, then switches to live)
curl -N "http://localhost:8080/v1/stream/sse?since=2026-03-24T11:00:00Z" \
  -H "Authorization: Bearer $TOKEN"

# Replay only the last 1 minute of gap data
# Set SINCE to 1 minute ago (macOS vs Linux use different date flags):
export SINCE=$(date -u -v-1M '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '1 minute ago' '+%Y-%m-%dT%H:%M:%SZ')
curl -N "http://localhost:8080/v1/stream/sse?since=$SINCE" \
  -H "Authorization: Bearer $TOKEN"

# Health check (no auth required)
curl http://localhost:8080/health
# → {"status":"ok"}
```

## Quick Start (Clustered Mode)

To develop against the full clustered infrastructure locally:

```bash
# 1. Start all dependencies (ClickHouse, NATS, Redis, ScyllaDB)
make compose-deps

# 2. Create the ClickHouse events table (same as standalone)
docker compose -f deployments/compose/dependencies.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS events (
      tenant_id String, event_id String, timestamp DateTime64(3, 'UTC'),
      type String, map_keys Array(String), map_values Array(String)
    ) ENGINE = MergeTree() ORDER BY (tenant_id, timestamp, event_id)
  "

# 3. Create the ScyllaDB keyspace and dedup table
docker compose -f deployments/compose/dependencies.yaml exec scylladb \
  cqlsh -e "
    CREATE KEYSPACE IF NOT EXISTS beachhouse
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    CREATE TABLE IF NOT EXISTS beachhouse.dedupe (
      tenant_id text, event_hash text, created_at timestamp,
      PRIMARY KEY (tenant_id, event_hash)
    );
  "

# 4. Run the API server in one terminal
BH_MODE=clustered \
BH_MQ_URL=nats://localhost:4222 \
BH_CACHE_REDIS_URL=redis://localhost:6379 \
BH_DEDUPE_SCYLLA_HOSTS=localhost:9042 \
BH_AUTH_JWT_SECRET=change-me-in-production \
go run ./cmd/beachhouse-api

# 5. Run the worker in another terminal
BH_MODE=clustered \
BH_MQ_URL=nats://localhost:4222 \
BH_CH_ADDR=localhost:9000 \
go run ./cmd/beachhouse-worker
```

### Using an .env File

For convenience, create a `.env.clustered` file in the project root:

```bash
# .env.clustered — source this before running clustered binaries
export BH_MODE=clustered
export BH_CH_ADDR=localhost:9000
export BH_MQ_URL=nats://localhost:4222
export BH_CACHE_REDIS_URL=redis://localhost:6379
export BH_DEDUPE_SCYLLA_HOSTS=localhost:9042
export BH_DEDUPE_SCYLLA_KEYSPACE=beachhouse
export BH_AUTH_JWT_SECRET=change-me-in-production
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

Unit tests live alongside the code they test (e.g., `internal/schema/flatten_test.go`).

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
│   ├── dedupe/             # Deduplication (Pebble or ScyllaDB)
│   ├── ingest/             # Batch buffering + Active Sweeper
│   ├── mq/                 # NATS message queue abstraction
│   └── schema/             # JSON flattening to EAV
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
- **Tenant isolation**: `tenant_id` is always sourced from JWT claims, never from user input.

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

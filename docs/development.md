# Development Guide

## Prerequisites

- **Go 1.25+** — [Install Go](https://go.dev/dl/)
- **Docker** — For running dependencies and integration tests
- **golangci-lint v2** — `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`
- **air** (optional) — For hot-reload during development: [Install air](https://github.com/air-verse/air)

## Getting Started

```bash
# Clone the repository
git clone https://github.com/Wave-RF/BeachHouse.git
cd BeachHouse

# Install Go dependencies
go mod download

# Start infrastructure dependencies (ClickHouse, NATS, Redis, ScyllaDB)
make compose-deps

# Run development server with hot-reload
make dev
```

BeachHouse will be available at `http://localhost:8080`. It recompiles and restarts automatically when you save a `.go` file.

## Building

```bash
# Build all three binaries to bin/
make build

# Build individual binaries
go build -o bin/beachhouse ./cmd/beachhouse
go build -o bin/beachhouse-api ./cmd/beachhouse-api
go build -o bin/beachhouse-worker ./cmd/beachhouse-worker
```

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
│   ├── ingest/             # Batch buffering + replay buffer
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

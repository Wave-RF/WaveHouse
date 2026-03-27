# Development Guide

## Prerequisites

- **Go 1.25+** — [Install Go](https://go.dev/dl/)
- **Docker** — For running dependencies and integration tests
- **golangci-lint v2** — [Install golangci-lint](https://golangci-lint.run/welcome/install/) (binary install recommended; not in `go.mod` due to dependency tree size)
- **air** — For hot-reload during development: [Install air](https://github.com/air-verse/air) (`brew install air` on macOS)

Other dev tools (`gotestsum`, `gofumpt`, `goimports`) are **pinned in `go.mod`** via native `tool` directives (Go 1.24+) and run automatically through the Makefile — no manual installation needed.

> **Note**: Both `golangci-lint` and `air` are installed as external binaries (not in `go.mod`) because their large dependency trees cause conflicts. If missing, `make lint` and `make dev` print install instructions.

## Quick Start (Standalone Mode)

This is the fastest way to get a fully functional local environment:

```bash
# 1. Clone and install dependencies
git clone https://github.com/Wave-RF/WaveHouse.git
cd WaveHouse
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

WaveHouse is now running at `http://localhost:8080` in standalone mode with:

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

Set `WH_AUTH_ENABLED=true` and `WH_AUTH_JWT_SECRET=my-secret` to require JWT tokens:

```bash
WH_AUTH_ENABLED=true WH_AUTH_JWT_SECRET=my-secret make dev
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

Set `WH_DEDUPE_ENABLED=true` and `WH_DEDUPE_ID_FIELD=event_id`:

```bash
WH_DEDUPE_ENABLED=true WH_DEDUPE_ID_FIELD=event_id make dev
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
WH_MODE=clustered \
WH_MQ_URL=nats://localhost:4222 \
WH_CACHE_REDIS_URL=redis://localhost:6379 \
go run ./cmd/wavehouse-api

# 4. Run the worker in another terminal
WH_MODE=clustered \
WH_MQ_URL=nats://localhost:4222 \
WH_CH_ADDR=localhost:9000 \
go run ./cmd/wavehouse-worker
```

### Using an .env File

```bash
# .env.clustered
export WH_MODE=clustered
export WH_CH_ADDR=localhost:9000
export WH_MQ_URL=nats://localhost:4222
export WH_CACHE_REDIS_URL=redis://localhost:6379
```

Then:

```bash
source .env.clustered
go run ./cmd/wavehouse-api    # terminal 1
go run ./cmd/wavehouse-worker # terminal 2
```

## Building

```bash
# Build all three binaries to bin/
make build

# Build individual binaries
go build -o bin/wavehouse ./cmd/wavehouse
go build -o bin/wavehouse-api ./cmd/wavehouse-api
go build -o bin/wavehouse-worker ./cmd/wavehouse-worker
```

## Running Modes at a Glance

| What you want | Command |
| ------------- | ------- |
| Hot-reload standalone dev server | `make dev` |
| Standalone binary (default config) | `./bin/wavehouse` |
| Standalone via Docker Compose | `make compose-standalone` |
| Clustered API server (local deps) | `source .env.clustered && go run ./cmd/wavehouse-api` |
| Clustered worker (local deps) | `source .env.clustered && go run ./cmd/wavehouse-worker` |
| Full clustered stack via Docker | `make compose-clustered` |
| Infrastructure deps only | `make compose-deps` |

## Testing

### How It Works

All test commands use [gotestsum](https://github.com/gotestyourself/gotestsum) for pytest-style colored output with pass/fail icons, durations, and a summary. Tool versions are pinned in `go.mod` via `tool` directives — the Makefile uses `go run` so no global installation is needed.

All tests run with Go's **race detector** (`-race`) enabled by default. WaveHouse is highly concurrent (NATS consumers, singleflight caching, SSE/WS hubs) — the race detector catches data races that would panic in production.

### Quick Reference

```bash
make test                              # Unit tests (compact output)
V=1 make test                          # Unit tests (verbose output)
make test ARGS="-run TestValidate"     # Run specific test(s)
V=1 make test ARGS="-run TestValidate" # Specific test, verbose
make test-integration                  # Integration tests (requires Docker)
V=1 make test-integration              # Integration tests, verbose
make test-all                          # Unit + integration
make ci                                # Full CI check: fmt + lint + all tests
make coverage                          # Unit test coverage → coverage.html
make smoke-test                        # Manual Bento insert+delete (needs running WaveHouse)
```

**Verbose output**: Use `V=1` to switch from compact `testdox` format to full verbose output. This is a standard Makefile convention (`make test -v` can't work because `-v` is a `make` flag).

**Extra flags**: All test targets accept `ARGS="..."` for additional `go test` flags (e.g., `-run`, `-count`, `-timeout`).

**Note on timing**: gotestsum's `DONE ... in X.XXXs` reports pure test execution time. The total wall time includes Go compiling all packages — the first run compiles everything (~15s), subsequent runs use the build cache (~1s).

### Test Structure

| Category | Location | Docker? | Command |
|----------|----------|---------|---------|
| Unit tests | `internal/*/_test.go` | No | `make test` |
| Integration tests | `tests/integration_test.go` | Yes | `make test-integration` |
| Smoke test (manual) | `tests/cmd/bento_pub/main.go` | External | `make smoke-test` |

- **Unit tests** live beside the code they test (e.g., `internal/discovery/discovery_test.go`). They use mocks or embedded NATS (in-process, no Docker needed).
- **Integration tests** use the `//go:build integration` build tag. The `setupTestEnv` helper starts a ClickHouse testcontainer, embedded NATS, Bento ingest worker, and a full API router via `httptest.Server`. Subtests run sequentially because Bento's global registrations are one-time-per-process. DLQ tests use `assert.Eventually` with a 30-second timeout for the 5-second Bento batch window.
- **Smoke test** (`make smoke-test`) is a standalone binary that publishes insert + delete events to a running NATS (`localhost:4222`) and verifies ClickHouse (`localhost:9000`) processing. Requires a running WaveHouse instance — it is **not** part of `go test` and does not run with `make test-all`.

Shared test utilities live in `internal/testutil/` (e.g., `testutil.NopLogger()` for silencing embedded NATS output).

### Adding New Tests

- **Unit test for `internal/foo/`** → create `internal/foo/foo_test.go` (same package).
- **Integration test needing Docker** → add a subtest inside `tests/integration_test.go` or create a new `tests/*_test.go` file with `//go:build integration`.
- **Test helpers** → add to `internal/testutil/`.

## Linting

```bash
make lint
```

`golangci-lint` is installed separately (not in `go.mod` — its massive dependency tree causes conflicts). If not found, `make lint` prints install instructions.

Install options:

- **macOS**: `brew install golangci-lint`
- **Binary**: See [golangci-lint.run/welcome/install/](https://golangci-lint.run/welcome/install/)
- **Go install**: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`

The configuration is in `.golangci.yml` (v2 format with `default: none` for explicit control). Enabled linters:

- **errcheck** — Unchecked error returns
- **govet** — Suspicious constructs
- **staticcheck** — Static analysis
- **unused** — Unused code
- **gosec** — Security issues
- **gocritic** — Opinionated style checks
- **revive** — Extensible linter (replaces golint)
- **ineffassign** — Ineffective assignments
- **misspell** — Spelling errors in comments/strings
- **gofumpt** — Strict formatting (superset of gofmt)
- **goimports** — Import ordering and grouping
- **bodyclose** — Unclosed HTTP response bodies
- **noctx** — HTTP requests without context
- **errorlint** — Proper error wrapping checks (`%w`, `errors.Is/As`)
- **tparallel** — Missing `t.Parallel()` in test subtests

## Project Structure

```text
WaveHouse/
├── cmd/                    # Binary entry points
│   ├── wavehouse/          # Standalone all-in-one binary
│   ├── wavehouse-api/      # Clustered API server
│   └── wavehouse-worker/   # Clustered background worker
├── internal/               # Private application packages
│   ├── api/                # HTTP handlers, router, middleware
│   ├── cache/              # L1 (Ristretto) + L2 (Redis) caching
│   ├── config/             # YAML + env var configuration
│   ├── dedupe/             # Optional deduplication (Pebble or ScyllaDB)
│   ├── discovery/          # ClickHouse schema introspection + validation
│   ├── ingest/             # Batch buffering + DLQ + Active Sweeper
│   ├── mq/                 # NATS message queue abstraction
│   ├── pipes/              # Named query pipes (NATS KV + .sql bootstrap)
│   ├── policy/             # Access control policies (evaluation + NATS KV store)
│   ├── query/              # Structured query AST + SQL builder
│   └── testutil/           # Shared test helpers and mocks
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

- **Strict Go formatting**: Use `gofumpt` (a stricter superset of `gofmt`, enforced by CI). Run `make fmt` to format.
- **Interface-first design**: Core behaviors (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`) are defined as interfaces with separate implementations for standalone and clustered modes.
- **Package boundaries**: The `internal/` directory ensures packages are private to this module.
- **Error handling**: Return errors to callers. Use `slog` for structured logging.
- **Schema-driven**: ClickHouse is the schema source of truth. WaveHouse discovers and validates against real table schemas.

## Makefile Targets

Run `make help` to see all targets. Key ones:

| Target | Description |
| ------ | ----------- |
| `make help` | Show all targets with descriptions |
| `make setup` | Download Go modules and cache tools |
| `make tools` | Install external dev tools (golangci-lint, air, goreleaser) |
| `make check-tools` | Verify all required tools are installed |
| `make build` | Compile all three binaries to `bin/` |
| `make build-debug` | Compile with debug symbols (for delve/profiling) |
| `make dev` | Hot-reload development server (requires air) |
| `make fmt` | Format code (`gofumpt` + `goimports`) |
| `make fmt-check` | Verify formatting (non-zero exit if unformatted) |
| `make lint` | Run golangci-lint |
| `make lint-fix` | Run golangci-lint with `--fix` |
| `make fix` | Auto-format + auto-fix linters |
| `make test` | Unit tests with race detector |
| `make test-integration` | Integration tests (requires Docker) |
| `make test-all` | Unit + integration tests |
| `make ci` | Full CI check: tidy + fmt + lint + vulncheck + build + tests |
| `make coverage` | Unit test coverage → `coverage.html` |
| `make coverage-enforce` | Fail if coverage is below 70% threshold |
| `make mod-tidy-check` | Verify `go.mod`/`go.sum` are tidy |
| `make smoke-test` | Manual Bento insert+delete (requires running WaveHouse) |
| `make vulncheck` | Run `govulncheck` vulnerability scanner |
| `make security` | Combined scan: vulncheck + gosec via linter |
| `make deadcode` | Find unreachable functions and unused code |
| `make audit-cgo` | Audit dependencies for C code (informational) |
| `make size-report` | Show binary sizes |
| `make size-tree` | Top packages by size in the binary (text table) |
| `make size-treemap` | Full binary analysis → text + SVG + interactive HTML |
| `make dep-graph` | Dependency graph → `graph.svg` (requires graphviz) |
| `make dep-why MOD=...` | Show why a module is included |
| `make dep-cut` | Top cuttable deps by transitive impact (`LIMIT=N`) |
| `make binary-analysis` | Combined: sizes + dead code + CGO audit |
| `make docker` | Build Docker image |
| `make compose-standalone` | Start standalone mode via Docker Compose |
| `make compose-clustered` | Start clustered mode via Docker Compose |
| `make compose-deps` | Start infrastructure dependencies only |
| `make deps-wipe` | Destroy and recreate dependency containers |
| `make release-test` | Test cross-compilation via GoReleaser |
| `make clean` | Remove `bin/`, `tmp/`, `data/`, and coverage files |

All test targets accept `ARGS="..."` for pass-through flags. Build targets accept `TAGS="..."` for build tags (e.g., `make build TAGS="scylla"`).

## Dependency Management

### Updating Dependencies

```bash
go get -u ./...        # Update all direct deps to latest minor/patch
go mod tidy            # Remove unused, add missing
```

### Vulnerability Scanning

`govulncheck` analyzes your actual call graph — not just the module graph — so it only reports vulnerabilities in code paths you use.

```bash
make vulncheck
```

For a combined security scan (vulncheck + gosec):

```bash
make security
```

This also runs automatically in CI on every push and pull request.

### Dependabot

Dependabot is configured in `.github/dependabot.yml` to open weekly grouped PRs for:

- **Go modules** — outdated or vulnerable dependencies
- **GitHub Actions** — outdated action versions

PRs are grouped by ecosystem to reduce noise. Review and merge them regularly.

---
title: "Development"
description: "Building, testing, linting, project structure, and contribution workflow."
sidebar:
  order: 11
---

Everything you need to build, test, lint, and contribute to WaveHouse — from first-clone to hot-reload dev server to full end-to-end SDK tests. If you're only trying the product, start with the [Getting Started](/getting-started) guide instead.

## Prerequisites

Ensure these are on your `PATH` before running `make`:

| Tool | Required version | Why | Install |
| ---- | ---------------- | --- | ------- |
| **Go** | 1.26+ (matches `go.mod`) | Compiles `cmd/wavehouse` and runs pinned `tool` deps (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `deadcode`, `gsa`, `goda`) via `go tool` | [go.dev/dl](https://go.dev/dl/) |
| **GNU Make** | **4.0+** | Required for `--output-sync=target` and bash-pinned recipes. macOS BSD Make 3.81 is incompatible | macOS: `brew install make` (use `gmake` or add `$(brew --prefix make)/libexec/gnubin` to PATH). Linux: usually preinstalled |
| **bash** | 4+ recommended | Recipes and `scripts/` use `set -euo pipefail` and bash arrays | macOS default is 3.2 (`brew install bash` recommended); Linux ships 4+ |
| **Docker** *(or Podman)* | Engine 20.10+ with Compose **v2** plugin | Used for `deployments/compose/` and testcontainers (ClickHouse) in E2E/integration suites | [Docker Desktop](https://docs.docker.com/get-docker/), [colima](https://github.com/abiosoft/colima), or [Podman](https://podman.io). Honors `DOCKER_HOST` for rootless Podman |
| **Node.js** | 22 LTS (via `.nvmrc`) | Runtime for pnpm and Vitest; matches CI to avoid V8 heap-allocation aborts seen in Node 26 | [nodejs.org](https://nodejs.org/) or `nvm`/`fnm`/`volta` |
| **pnpm** | 11.1+ (pinned via `packageManager` in `package.json`) | Manages TypeScript SDK, E2E harness, and docs workspace; used by `make build-ts`, `test-ts`, `test-e2e`, and `*-docs` targets | `corepack enable && corepack prepare pnpm@11.1.3 --activate` or `npm i -g pnpm` |
| **git** + **curl** | any recent | `git` for source/versioning; `curl` fetches `golangci-lint` into `.bin/` | usually preinstalled |

### Auto-installed by `make tools`

Run `make tools` after cloning to install:

- **`golangci-lint` v2.11.4** $\rightarrow$ installed to `.bin/<os>_<arch>/`. Not in `go.mod` due to dependency conflicts.
- **`air` v1.65.1** $\rightarrow$ installed to `.bin/<os>_<arch>/` for hot-reload via `make dev`. Excluded from `go.sum` to avoid bloating with Hugo/Sass deps.
- **Go `tool` deps** (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `go-test-coverage`, `gocover-cobertura`, `deadcode`, `gsa`, `goda`) $\rightarrow$ pinned in `go.mod` (Go 1.24+); cached via `go mod download`.
- **pnpm deps** for `clients/ts/`, `tests/e2e/sdk/`, and `docs/` $\rightarrow$ installed via `pnpm install --frozen-lockfile`. Playwright Chromium (~130 MB) is fetched on-demand by `make build-docs` or `dev-docs` for `rehype-mermaid` (SVG rendering) and the `diagram-png` integration (`docs/src/integrations/diagram-png.mjs`), which rasterizes diagrams to light/dark PNGs at `astro:build:done`; the manual `docs/scripts/screenshot.mjs` QA helper reuses the same Chromium. `--with-deps` (apt-installing `libnspr4`, `libnss3`, etc.) is added only when `$CI` is set, so laptops get no surprise `sudo` prompt; on Linux without those libs, run `pnpm exec playwright install-deps chromium` once. The docs site (`wavehouse-docs`) is a pnpm workspace package driven by the root Makefile via `pnpm --filter`. Since it consumes `@wavehouse/sdk`, `make build-ts` must run before Astro builds. `starlight-links-validator` runs in CI and `build-docs`; skip it in `dev-docs` unless `DOCS_WATCH_STRICT=1` is set.

### Verify your setup

```bash
go version          # go1.26+
make --version      # GNU Make 4.x
docker compose version
node --version      # v22.x (matches .nvmrc and CI)
pnpm --version      # 11.1+
```

Wrong or missing versions produce confusing recipe errors (`--output-sync` unrecognized on Make 3.81; `pnpm: command not found` on `make test-ts`).

### Optional but recommended

| Tool | Why | Install |
| ---- | --- | ------- |
| **[Claude Code](https://claude.com/claude-code)** | Uses repo config in `.claude/` (commands, subagents, hooks). See [Claude Code & AI agents](/claude-code) | `brew install --cask claude-code` or [official install](https://code.claude.com/docs/en/quickstart) |
| **[worktrunk](https://worktrunk.dev)** | Wraps `git worktree`. `.config/wt.toml` auto-runs `make tools` on new worktrees and `make verify` pre-merge | `brew install worktrunk && wt config shell install` |

## Quick Start

Fastest way to get a functional local environment:

```bash
# 1. Clone and bootstrap (Go modules + golangci-lint + pnpm deps)
git clone https://github.com/Wave-RF/WaveHouse.git
cd WaveHouse
make tools

# 2. Start ClickHouse (the only external dependency)
docker compose -f deployments/compose/dependencies.yaml up -d clickhouse

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

WaveHouse runs at `http://localhost:8080` in standalone mode with:

- **Embedded NATS** (JetStream) and **L1 cache** (Ristretto): no external MQ or cache needed.
- **Fail-closed**: `config.yaml` seeds no policy, so requests are denied until you seed one (see [Test the API](#test-the-api)).
- **Dedup disabled**: Pebble not required by default.
- **Schema discovery**: automatically finds ClickHouse tables.

### Test the API

`make dev` is fail-closed. Use the shipped dev policy (`public` trial role: read/write `clicks`/`events`, no token) to enable requests:

```bash
WH_POLICY_FILE_PATH=deployments/compose/dev-policy.yaml make dev
```

Tokenless data-plane calls (ensure `clicks` table exists):

```bash
# Ingest an event
curl -s -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}

# Query it back (wait ~5s for the batch flush)
curl -s -X POST "http://localhost:8080/v1/query?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"columns": ["page", "button", "score"], "limit": 10}'

# Open an SSE stream for a specific table (Ctrl+C to stop)
curl -N "http://localhost:8080/v1/stream?table=clicks"

# With gap-fill (replays events since the given timestamp, then switches to live)
curl -N "http://localhost:8080/v1/stream?table=clicks&since=2026-03-24T11:00:00Z"

# Liveness / readiness (no auth required)
curl http://localhost:8080/livez   # → {"status":"ok"}
curl http://localhost:8080/readyz  # → {"status":"ready"}
```

Admin endpoints (`/v1/schema`, `/v1/admin/query`, `/v1/dlq/stats`) require the **admin** role. Mint an admin JWT (see [Validating tokens](#validating-tokens)) and pass it:

```bash
curl -s http://localhost:8080/v1/schema -H "Authorization: Bearer $TOKEN" | jq
curl -s -X POST http://localhost:8080/v1/admin/query -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
curl -s http://localhost:8080/v1/dlq/stats -H "Authorization: Bearer $TOKEN"
```

### How `make dev` works

`make dev` is a convenience target for backend and frontend development:

```make
dev: deps-up $(AIR)
    air -c .air.toml
```

`deps-up` starts ClickHouse and blocks until the `/ping` healthcheck is healthy. `$(AIR)` installs air to `.bin/<os>_<arch>/`. Air watches `cmd/` and `internal/`, rebuilds `tmp/wavehouse` on change, and restarts the binary.

Config is **not** hot-reloaded: `make dev` uses `WH_CONFIG=.config.local.yaml` (a gitignored copy seeded once from `config.yaml`). To apply config changes, edit `.config.local.yaml` and restart `make dev`. Air is installed via `go install` to avoid bloating `go.sum` with transitive dependencies.

**Features of `make dev`:**

- WaveHouse on `http://localhost:8080` with `cors_allowed_origins: ["*"]`, so any localhost-port browser app can hit the API.
- Placeholder JWT secret `change-me-in-production` in `config.yaml`; override via `WH_AUTH_JWT_SECRET`.
- ClickHouse on `http://localhost:8123` (HTTP) and `localhost:9000` (native), namespaced under `wavehouse-dev`.
- Debounced rebuilds for `.go` files in `cmd/` or `internal/`.

### Dev convenience targets

| Target | What it does |
| ------ | ------------ |
| `make deps-up` | Start ClickHouse and block until healthy. Idempotent. |
| `make deps-down` | Stop ClickHouse; preserves data volume. |
| `make deps-logs` | Stream ClickHouse logs (`docker compose logs -f clickhouse`). |
| `make deps-shell` | Enter `clickhouse-client` REPL on the container. |
| `make deps-wipe` | Stop ClickHouse and destroy its data volume for a clean schema. |
| `make clean-all` | Remove all make artifacts, containers, volumes, and `data/`. |

**Stopping**: `Ctrl+C` stops air and gracefully shuts down WaveHouse (e.g., NATS JetStream flush). ClickHouse remains running; use `make deps-down` or `make deps-wipe` to stop it.

### Running with observability

WaveHouse exports OTLP data to `127.0.0.1:4317`. Run these in a separate terminal alongside `make dev` or `make test-e2e`:

| Target | What it does | UI URL |
| ------ | ------------ | ------ |
| `make obs-aspire` | Aspire dashboard. Fast, in-memory, no login. Ideal for quick debugging. | `http://localhost:18888` |
| `make obs-grafana` | Grafana LGTM (Loki, Grafana, Tempo, Prometheus). Best for charting and correlation. | `http://localhost:3000` |
| `make obs-front` | OTel-Front basic trace viewer. | `http://localhost:8000` |

**Workflow:** Run `make obs-aspire` in Tab 1, then `make dev` in Tab 2 to view traces and metrics instantly.

### Using the SDK against `make dev`

Point the `@wavehouse/sdk` client at `baseURL: "http://localhost:8080"` with a seeded dev policy:

```bash
WH_POLICY_FILE_PATH=deployments/compose/dev-policy.yaml make dev
```

See the [SDK guide](/sdk) for examples. Frontend apps (Vite, Next.js) can use `createClient` directly; permissive CORS allows cross-origin requests.

### Validating tokens

There is no auth on/off switch: the JWT middleware always runs, but authorization is the policy's job — a `nil`/unseeded policy denies every token-based caller, admins included, and only the operator key still reaches the admin surface. To test token auth, seed a policy and set a known secret:

```bash
WH_POLICY_FILE_PATH=deployments/compose/dev-policy.yaml WH_AUTH_JWT_SECRET=my-secret make dev
```

The **operator key** is a non-JWT alternative via `Authorization: Operator <key>` or `X-Operator-Key`. It accesses the admin surface even without a seeded policy (break-glass path). Once a policy is loaded, it resolves to `admin` for full data-plane access.

```bash
WH_AUTH_OPERATOR_KEY=dev-operator-key make dev
# ...then, in another shell — the admin surface works even with no policy seeded:
curl -H "Authorization: Operator dev-operator-key" http://localhost:8080/v1/admin/policy
# the X-Operator-Key alias works too:
curl -H "X-Operator-Key: dev-operator-key" http://localhost:8080/v1/admin/policy
```

Mint a token (role must match the policy `admin_role`):

```bash
# Using jwt-cli (https://github.com/mike-engel/jwt-cli)
export TOKEN=$(jwt encode --secret "my-secret" '{"role": "admin", "exp": 9999999999}')

curl -s -X POST http://localhost:8080/v1/admin/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
```

### Enable Dedup (Optional)

Set `WH_DEDUPE_ENABLED=true` and `WH_DEDUPE_ID_FIELD=event_id`:

```bash
WH_DEDUPE_ENABLED=true WH_DEDUPE_ID_FIELD=event_id make dev
```

Include the dedup field in ingest:

```bash
curl -s -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "550e8400-e29b-41d4-a716-446655440001", "page": "/home"}'
# → {"ok":true}

# Same event_id again → deduplicated
curl -s -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "550e8400-e29b-41d4-a716-446655440001", "page": "/home"}'
# → {"duplicate":true}
```

### Using an .env File

```bash
# .env
export WH_CH_ADDR=localhost:9000
```

Then:

```bash
source .env
go run ./cmd/wavehouse
```

## Building

```bash
# Build the binary to bin/
make build

# Build individual binaries
go build -o bin/wavehouse ./cmd/wavehouse
```

Run `make build` for all binaries or `go build -o bin/wavehouse ./cmd/wavehouse` individually.

## Running Modes at a Glance

| Goal | Command |
| ------------- | ------- |
| Hot-reload dev server | `make dev` |
| Standalone binary (default) | `make build && ./bin/wavehouse` |
| Standalone via Docker Compose | `docker compose -f deployments/compose/standalone.yaml up -d` |
| ClickHouse deps only | `docker compose -f deployments/compose/dependencies.yaml up -d clickhouse` |

## Testing

### How It Works

All test commands use [gotestsum](https://github.com/gotestyourself/gotestsum) for colored output and summaries. Tool versions are pinned in `go.mod` via `tool` directives; the Makefile uses `go run`, removing the need for global installations.

Tests run with Go's **race detector** (`-race`) by default to catch concurrency issues in NATS consumers, singleflight caching, and SSE hubs.

### Quick Reference

```bash
# Prefix any test target with V=1 for verbose output, e.g. `V=1 make test`

# Unit tests (compact output) — alias for `test-unit`
make test

# Run specific test(s)
make test ARGS="-run TestValidate"

# Go integration tests (requires Docker)
make test-integration

# SDK vitest unit tests + coverage + gate against suites.ts-unit
# (`make cov` auto-merges ts-unit + ts-e2e — no separate command)
make test-ts

# E2E SDK suite against bin/wavehouse-cov
make test-e2e

# All four suites sequentially + merged coverage
make test-all

# Full CI: parallel verify + builds (Go + SDK + docs) + test + test-ts,
# then test-integration + test-e2e + cov
make ci

# Merge available covdata + gate against total threshold
make cov
```

Each target writes `covdata` to `tmp/coverage/<suite>/data/`, renders reports, and gates against thresholds in `.testcoverage.yml`. `make cov` merges run suites and gates against the total.

**Verbose output**: Use `V=1` for full output instead of compact `testdox` format.
**Extra flags**: Pass `ARGS="..."` for additional `go test` flags (e.g., `-run`, `-count`).
**Timing**: gotestsum's `DONE ... in X.XXXs` reports pure test execution time; total wall time includes compilation (~15s first run, ~1s cached).

### Test Structure

| Category | Location | Docker? | Command |
| -------- | -------- | ------- | ------- |
| Unit tests | `internal/*/_test.go` | No | `make test` |
| SDK unit tests | `clients/ts/src/**/*.test.ts` | No | `make test-ts` |
| Integration tests (Go) | `tests/integration/*_test.go` | Yes | `make test-integration` |
| E2E tests (SDK) | `tests/e2e/sdk/*.test.ts` | Yes | `make test-e2e` |

- **Unit tests**: beside the code they test (e.g. `internal/discovery/discovery_test.go`); use mocks or embedded NATS.
- **Integration tests**: Use `//go:build integration`. `setupTestEnv` starts a ClickHouse testcontainer, embedded NATS, ingest worker, and API router via `httptest.Server`. DLQ tests use `assert.Eventually` (30s timeout) for the 5s batch window.
- **Utilities**: `internal/testutil/` (Go), e.g. `testutil.NopLogger()` to silence embedded NATS.

### Adding New Tests

- **Unit test (`internal/foo/`)** $\rightarrow$ create `internal/foo/foo_test.go`.
- **Integration test (Docker)** $\rightarrow$ add subtest under `tests/integration/` with `//go:build integration`.
- **E2E SDK test** $\rightarrow$ add `tests/e2e/sdk/*.test.ts` to exercise the full pipeline via the TS SDK. Run with `make test-e2e`.
- **Helpers** $\rightarrow$ add to `internal/testutil/` (Go) or `tests/e2e/sdk/helpers.ts` (E2E).

### E2E Tests via SDK

Located in `tests/e2e/sdk/`, these use the TS SDK as a harness to validate the Go backend and SDK compatibility.

**Architecture**:

- `scripts/orchestrator`: Entrypoint for `make test-e2e`. Starts a ClickHouse testcontainer, launches `wavehouse-cov` on a random port, runs the suite, then SIGINTs the binary to flush coverage. No Compose file is used.
- `tests/e2e/sdk/setup.ts`: `globalSetup`. Probes injected URLs, creates tables, refreshes schema, and bootstraps policy. Fails fast if URLs are unreachable. Warns if local Node major differs from `.nvmrc` to avoid transport bugs (see [#440](https://github.com/Wave-RF/WaveHouse/issues/440)).
- `tests/e2e/sdk/helpers.ts`: JWT factories, typed clients, async wait helpers, and ClickHouse query helpers.

**Running E2E tests**:

```bash
# Build the cover binary, install deps, run all E2E tests
make test-e2e
```

`make test-e2e` builds `bin/wavehouse-cov` (instrumented) and runs the orchestrator. Coverage flushes to `tmp/coverage/e2e/data/`. The orchestrator provisions its own stack, so it won't collide with `make dev`.

To run vitest against a manual stack, start the server from the **repo root** using the E2E fixture:

```bash
WH_CONFIG=tests/e2e/fixtures/config.yaml go run ./cmd/wavehouse
```

The fixture is required: the suite signs tokens with its `sdk-dev-secret` and needs its dedupe, DLQ, and 5s schema-refresh settings. Point it at a default `make dev` server (`jwt_secret: change-me-in-production`) and setup's schema calls are rejected, then global setup dies 30s later on a misleading `schema not refreshed within 30s`. The repo root is required because `policy.file_path` is relative. Use `WH_CH_ADDR` / `WH_CH_HTTP_PORT` if ClickHouse isn't on `localhost:9000`. Note: Prefixing `make dev` with variables fails as it pins `WH_CONFIG=.config.local.yaml` inline.

Set `CLICKHOUSE_URL` / `WAVEHOUSE_URL` and run `pnpm test` from `tests/e2e/sdk/`. A run killed by a harness timeout, stop button, or `SIGKILL` can leave a `wavehouse-cov` behind that shares `tmp/data` and `tmp/wavehouse-cov.log` with the next run and corrupts it, so the orchestrator kills leftovers first and says so.

**Environment knobs**:

| Variable | Effect |
|----------|--------|
| `V=1` | Streams WaveHouse logs live; skips on-failure log excerpt. |
| `E2E_CH_QUERY_TIMEOUT_MS` | Ceiling for direct ClickHouse queries (default `10000`). |
| `E2E_NO_COVERAGE=1` | Skips `--coverage` in vitest. Local debugging only; ignored in `make ci`/`test-all`. |

**Test files** (`tests/e2e/sdk/*.test.ts`): `admin`, `auth`, `batching`, `cache`, `dlq`, `ingest`, `ndjson`, `query`, `streaming`, `stress`, plus `helpers` — a stack-free unit test of the harness's `waitForCondition` poll helper, not a pipeline test.

## Linting

```bash
make lint
```

`golangci-lint` is installed separately to avoid dependency conflicts; `make lint` provides install instructions if missing.

Install options:

- **macOS**: `brew install golangci-lint`
- **Binary**: [golangci-lint.run/welcome/install/](https://golangci-lint.run/welcome/install/)
- **Go install**: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`

`.golangci.yml` (v2 format, `default: none`) is the authoritative list of enabled linters:

- **errcheck**: Unchecked error returns
- **govet**: Suspicious constructs
- **staticcheck**: Static analysis
- **unused**: Unused code
- **gosec**: Security issues
- **gocritic**: Style checks
- **revive**: Extensible linter (replaces golint)
- **ineffassign**: Ineffective assignments
- **misspell**: Spelling errors
- **bodyclose**: Unclosed HTTP bodies
- **noctx**: HTTP requests without context
- **errorlint**: Error wrapping (`%w`, `errors.Is/As`)
- **tparallel**: Missing `t.Parallel()` in subtests

Formatting (**gofumpt** and **goimports**) is enforced via the v2 `formatters:` section.

## Project Structure

```text
WaveHouse/
├── cmd/                    # Binary entry points
│   └── wavehouse/          # Standalone all-in-one binary
├── internal/               # Private application packages
│   ├── api/                # HTTP handlers, router, middleware
│   ├── auth/               # JWT/JWKS authentication middleware
│   ├── cache/              # L1 (Ristretto) + L2 caching
│   ├── chsql/              # Shared ClickHouse SQL helpers (quoting + bind-safety)
│   ├── config/             # YAML + env var configuration
│   ├── dedupe/             # Optional deduplication (Pebble)
│   ├── discovery/          # ClickHouse schema introspection + validation
│   ├── ingest/             # Batch buffering + DLQ + Active Sweeper
│   ├── mq/                 # NATS message queue abstraction
│   ├── observability/      # OpenTelemetry pipeline (traces/metrics/logs + Prometheus)
│   ├── pipes/              # Named query pipes (NATS KV + .sql bootstrap)
│   ├── policy/             # Access control policies (evaluation + NATS KV store)
│   ├── query/              # Structured query AST + SQL builder
│   ├── stream/             # SSE fan-out: Hub, Subscriber queue, Bucket, keepalive wheel
│   └── testutil/           # Shared test helpers and mocks
├── tests/                  # Integration & E2E tests
│   ├── integration/        # Go integration tests (//go:build integration)
│   └── e2e/                # E2E suite (orchestrator + ClickHouse testcontainer)
│       ├── fixtures/       # ClickHouse DDL + config/policy fixtures
│       └── sdk/            # E2E specs driven through the TypeScript SDK (Vitest)
├── clients/                # Client SDKs
│   └── ts/                 # TypeScript SDK (@wavehouse/sdk, pnpm workspace)
├── deployments/
│   ├── compose/            # Docker Compose files (standalone.yaml, dependencies.yaml)
│   ├── Dockerfile          # Runtime image
│   └── Dockerfile.goreleaser  # Release image (built by GoReleaser)
├── scripts/                # E2E orchestrator, cov tool, CI/hook helpers
├── docs/                   # Documentation
├── config.yaml             # Default configuration file
├── Makefile                # Build, test, lint, deploy targets
├── .golangci.yml           # Linter configuration
├── .goreleaser.yaml        # Release build configuration
└── .air.toml               # Hot-reload configuration
```

## Code Conventions

- **Formatting**: Use `gofumpt` (stricter than `gofmt`, CI-enforced). Run `make fmt`.
- **Design**: Core behaviors (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`) use interfaces for swappable implementations.
- **Boundaries**: `internal/` keeps packages private to this module.
- **Errors & Logging**: Return errors; use `slog` for structured logging.
- **Schema**: ClickHouse is the source of truth; WaveHouse validates against real schemas.

## Makefile Targets

Run `make help` to see all targets.

| Target | Description |
| ------ | ----------- |
| `make help` | Show all targets with descriptions (source of truth) |
| `make tools` | Bootstrap: install pinned tools (`golangci-lint`, `air`), Go modules, and pnpm deps |
| **Dev** | |
| `make dev` | Hot-reload server: ClickHouse via Compose + WaveHouse under air on `:8080` |
| `make deps-up` | Start ClickHouse (idempotent; blocks until healthy) |
| `make deps-down` | Stop ClickHouse (preserves data volume) |
| `make deps-logs` | Tail ClickHouse logs |
| `make deps-shell` | `clickhouse-client` REPL on the running container |
| `make deps-wipe` | Stop ClickHouse and destroy its data volume (DESTRUCTIVE) |
| **Observability** | |
| `make obs-aspire` | 0-config o11y UI for WaveHouse metrics, logs, and traces locally |
| `make obs-grafana` | Advanced Grafana alternative to aspire |
| `make obs-front` | Simple custom graphs; easier to configure than grafana |
| **Static checks** | |
| `make fmt` | Check Go (`gofumpt`) and TS (Biome) formatting. Use `make fix` to apply. |
| `make tidy` | Verify `go.mod`/`go.sum` are tidy (use `make fix` to apply) |
| `make lint` | Run linters for Go (`golangci-lint`) and TS (Biome) |
| `make vulncheck` | Run `govulncheck` (`V=1` for full call stacks) |
| `make verify` | Repo-wide checks: Go (tidy, fmt, vulncheck, lint) + TS (Biome, `tsc`). Parallel-safe: `make -j verify` |
| `make fix` | Auto-fix Go (`tidy`, `gofumpt`, `goimports`, `lint --fix`) and TS (Biome `--write`) |
| **Build** | |
| `make build` | Compile `wavehouse` → `bin/wavehouse` (keeps debug symbols) |
| `make build-release` | Stripped release build → `bin/wavehouse-release` |
| `make build-cover` | Coverage-instrumented build → `bin/wavehouse-cov` (used by E2E) |
| `make build-ts` | Build TypeScript SDK → `clients/ts/dist/` |
| **Test** | |
| `make test` | Alias for `test-unit` |
| `make test-unit` | Go unit tests + coverage render + threshold gate |
| `make test-integration` | Go integration tests (requires Docker) + coverage gate |
| `make test-ts` | SDK vitest unit tests + v8 coverage + `suites.ts-unit` gate |
| `make cov` | Merge Go + TS coverage and gate against thresholds. Fails if both are empty; otherwise skips missing data. |
| `make test-e2e` | E2E SDK suite against `bin/wavehouse-cov` + coverage gate |
| `make test-all` | All four suites sequentially + merged coverage gate |
| `make ci` | Pipeline: parallel `verify`, builds, unit/SDK tests, then integration, E2E, and cov |
| **Analysis** (informational) | |
| `make size` | Binary size analysis → `tmp/analysis/` (text, SVG, HTML) |
| `make audit-cgo` | Audit dependency tree for C files (builds use `CGO_ENABLED=0`) |
| `make deadcode` | Find unreachable functions |
| `make dep-cut` | Top cuttable deps by transitive weight (`LIMIT=N` to override) |
| `make binary-analysis` | Combined: `size`, `audit-cgo`, and `deadcode` |
| **Cleanup** | |
| `make clean` | Remove build outputs (`bin/`, `dist/`, `clients/ts/dist/`, `docs/dist/`, `docs/.dev-dist/`) |
| `make clean-test` | Remove test outputs (`tmp/` coverage, logs, NATS state) |
| `make clean-tools` | Remove installed tools and pnpm deps (`.bin/`, `node_modules/`) |
| `make clean-all` | Full reset: all above + `data/` + Docker volumes |

Test targets accept `ARGS="..."` for `go test` flags. Build targets accept `TAGS="..."` for Go build tags. `V=1` enables verbose `gotestsum` output.

## Dependency Management

### Updating Dependencies

```bash
go get -u ./...        # Update all direct deps to latest minor/patch
go mod tidy            # Remove unused, add missing
```

### Vulnerability Scanning

`govulncheck` analyzes the actual call graph, reporting only vulnerabilities in used code paths.

```bash
make vulncheck
```

Run `make verify` for a combined scan; it executes `vulncheck`, `lint`, and `gosec` (via `.golangci.yml`). CI runs this on every push and pull request.

### Dependabot

Configured in `.github/dependabot.yml`, Dependabot opens weekly grouped PRs:

- **Go modules** (root): Outdated or vulnerable dependencies; prefix `deps:`
- **GitHub Actions** (root): Outdated versions against SHA pins in `ci.yml` / `release.yml`; prefix `ci:`
- **npm — pnpm workspace** (root): All three TypeScript packages (docs site, SDK, E2E tests) in one PR; prefix `deps:`

The npm config targets the root (`directory: /`) because Dependabot only updates lockfiles co-located with the target manifest. Previous per-member configs failed CI's `pnpm install --frozen-lockfile` with `ERR_PNPM_OUTDATED_LOCKFILE` as they didn't regenerate the root `pnpm-lock.yaml`. Root targeting allows Dependabot to use `pnpm-workspace.yaml` to update all members and the single lockfile.

**No auto-merge.** All PRs require an approval from `@Wave-RF/wavehouse-admins` (via ruleset `required_reviewers`) and passing checks. The `dependabot-automerge.yml` was removed; every bump now requires human admin review.

## Releasing the SDK

The TypeScript SDK (`@wavehouse/sdk`, in `clients/ts/`) publishes to npm via `.github/workflows/publish-npm.yml` using OIDC trusted publishing (no `NPM_TOKEN`). It is independent of the server's Go/Docker release (`release.yml`); `v*` (server) and `sdk-v*` (SDK) tag globs are disjoint.

Two channels exist:

- **Dev snapshots.** Pushes to `main` publish `0.0.0-dev.<hash>` under the `dev` dist-tag if `dist/` changed. Install via `npm install @wavehouse/sdk@dev`.
- **Tagged releases.** Pushing a `sdk-vX.Y.Z` tag publishes that version and creates a GitHub Release. Stable versions use the `latest` dist-tag; prereleases (e.g., `sdk-v0.2.0-rc.1`) use tags like `alpha`/`beta`/`rc`/`next` based on the suffix and are marked as GitHub pre-releases. The tag **must** match `clients/ts/package.json`'s `version`.

To cut a release:

```bash
# 1. Bump "version" in clients/ts/package.json, commit, and merge to main.
# 2. Tag the release commit and push the tag:
git tag sdk-v0.1.0
git push origin sdk-v0.1.0
```

:::caution[The first tagged release promotes `latest`]
npm sets `latest` on the first publish, even under `--tag dev`. Until the first `sdk-v*` release, `npm install @wavehouse/sdk` and bare CDN URLs resolve to a `0.0.0-dev.*` snapshot. The first stable release fixes this.
:::

## CI & review automation

This repo uses three tiers of AI automation alongside standard CI checks. Full details are in `AGENTS.md`.

### PR title and Conventional Commits

PR titles must follow Conventional Commits format and be $\le$ 72 characters, as they become the squash-merge commit subject. The `PR title` job in `.github/workflows/ci.yml` enforces this; validate locally via `scripts/lint-pr-title.sh "<title>"`.

```text
<type>(optional-scope)(optional-!): <lowercase subject, no trailing period>
```

Allowed types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `deps`, `build`, `perf`, `revert`, `style`. Use `!` before `:` for breaking changes per Conventional Commits 1.0.0 (e.g. `feat!: remove deprecated endpoint`, `refactor(api)!: rename handlers`). Dependabot PRs are exempt from the length cap.

If invalid, the `PR housekeeping` workflow posts a sticky comment explaining the format. Editing the title triggers an automatic re-run of the `PR title` job without requiring a new push.

### Required status checks

The `main branch protection` ruleset requires the `CI` aggregator job (`.github/workflows/ci.yml`) to pass before merging. This DAG runs:

- `lint` (`make verify`)
- `unit` (`make test-unit test-ts`)
- `integration` (`make test-integration`)
- `e2e` (`make -j test-e2e`)
- `coverage` (`make cov`)
- `docs-build` (`make build-docs`)
- `PR title` (Conventional Commits)
- Docs preview/deploy jobs

The aggregator fails if any job fails or is canceled; skipped jobs are treated as passing. Fork PRs skip secret-bearing docs deploys, and docs-only PRs skip Go tests. A non-gating `Timing summary` job provides wall-clock data on the Summary page. See [`.github/workflows/README.md`](https://github.com/Wave-RF/WaveHouse/blob/main/.github/workflows/README.md) for architecture and cache policy.

The ruleset also mandates:

- Approval from `@Wave-RF/wavehouse-admins` (via `required_reviewers`).
- One additional approving review.
- Approval of the most recent push by a non-author.
- Resolution of all review threads.
- Linear history, no force-push, and squash-merge only.

Admins may bypass these for their own PRs but cannot push directly to `main`. Approved PRs use a **merge queue** ("Merge when ready"), which re-runs `CI` against the current `main` via a `merge_group` event before fast-forwarding. This replaces manual branch updates. Dependabot PRs require standard admin review; there is no auto-merge.

### Merge behavior

Only squash-merges are permitted. The PR title becomes the commit subject (with `(#NN)` appended) and the body becomes the commit message. Use the PR template (Summary / Test plan / Related Issues). Include `Closes #NN` in the body or link the issue in the **Development** sidebar to auto-close it on merge. Auto-merge is enabled repo-wide via "Enable auto-merge (squash)".

### AI reviewers

Marketplace apps provide advisory reviews:

- **CodeRabbit**: Reviews on open/push; re-trigger with `@coderabbitai review`.
- **Copilot**: Appears when a maintainer with Copilot Pro is a reviewer.

These are non-gating; the `required_reviewers` rule and thread resolution remain the actual merge gates.

### Reviewer assignment and the Task Board

- **Assignment**: The ruleset requests `@Wave-RF/wavehouse-admins`, and GitHub's team code-review assignment load-balances members. `dismiss_stale_reviews_on_push` clears approvals on new commits.
- **Merge gate**: Requires an `APPROVED` review from the admin team, thread resolution, linear history, and squash-only merges.
- **Task Board** (Projects v2, project #7): Managed via native Projects v2 automation; no workflow is used for state transitions. Priority is set in the board's `Priority` field during triage.

### Invoking bots manually

- **CodeRabbit**: Comment `@coderabbitai review` to re-trigger or `@coderabbitai <question>` for inquiries.
- **Copilot**: Use the "re-request-review" button on the PR page.

### Review-response expectations

All comments must receive a substantive reply; `required_review_thread_resolution: true` blocks merges until resolved. Agents follow the "Review Response" pattern in `AGENTS.md`. When pushing back against bots, end replies with their mention (e.g., `@coderabbitai`) to ensure a response loop.

### Issue triage

`.github/workflows/triage.yml` uses GitHub Models (`gpt-4o-mini`) to apply:

- `area/*` labels based on the body (pulled from existing repo label descriptions).
- `security` and `breaking-change` labels if flagged.
- Priority in project #7 via `PROJECT_BOARD_TOKEN`.

### Auto-labeling PRs

The `PR housekeeping` workflow (`.github/workflows/housekeeping.yml`) runs `actions/labeler` with `.github/labeler.yml` to apply `area/*`, `dependencies`, `github_actions`, `go`, and `documentation` labels based on changed files.

### When adding a new `internal/<pkg>/` package

Per `AGENTS.md`:

1. Create an `area/<pkg>` repo label with a description (used as a classifier hint).
2. Add the path $\to$ label mapping to `.github/labeler.yml`.

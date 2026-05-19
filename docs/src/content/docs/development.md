---
title: "Development"
description: "Building, testing, linting, project structure, and contribution workflow."
sidebar:
  order: 9
---

Everything you need to build, test, lint, and contribute to WaveHouse — from first-clone to hot-reload dev server to full end-to-end SDK tests. If you're only trying the product, start with the [Getting Started](getting-started.md) guide instead.

## Prerequisites

You need these on your `PATH` before any `make` recipe will work end-to-end:

| Tool | Required version | Why | Install |
| ---- | ---------------- | --- | ------- |
| **Go** | 1.26+ (matches `go.mod`) | Compiles `cmd/wavehouse`; also runs the pinned `tool` deps (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `deadcode`, `gsa`, `goda`) via `go tool` | [go.dev/dl](https://go.dev/dl/) |
| **GNU Make** | **4.0+** | The Makefile uses `--output-sync=target` (Make 4 only) and bash-pinned recipes. macOS ships with BSD Make 3.81, which **will not work** | macOS: `brew install make` then use `gmake` or put `$(brew --prefix make)/libexec/gnubin` on your PATH. Linux: usually already installed |
| **bash** | 4+ recommended | Recipes are pinned to `bash`; the helper scripts under `scripts/` use `set -euo pipefail` and bash arrays | macOS default is bash 3.2 (works for current recipes, but `brew install bash` is safer); Linux distros ship 4+ |
| **Docker** *(or Podman)* | Engine 20.10+ with the Compose **v2** plugin (`docker compose`, no hyphen) | Compose stacks under `deployments/compose/` and `tests/e2e/compose.yaml`; integration tests boot a ClickHouse testcontainer | [Docker Desktop](https://docs.docker.com/get-docker/), [colima](https://github.com/abiosoft/colima), or [Podman](https://podman.io) with `podman-compose` / the `podman compose` plugin. The testcontainers Go library also honors `DOCKER_HOST` for rootless Podman setups |
| **Node.js** | 20+ | Runtime for pnpm and the Vitest suites | [nodejs.org](https://nodejs.org/) or `nvm`/`fnm`/`volta` |
| **pnpm** | 10.33+ (pinned via `packageManager` in `clients/ts/package.json`, `tests/e2e/sdk/package.json`, and `docs/package.json`) | Package manager for the TypeScript SDK, E2E test harness, and docs site; `make build-sdk`, `make test-sdk`, `make test-e2e`, `make build-docs`, `make dev-docs`, `make preview-docs` all shell out to `pnpm` | `corepack enable && corepack prepare pnpm@10.33.0 --activate` (recommended), or `npm i -g pnpm` |
| **git** + **curl** | any recent | `git` for source + version metadata in builds; `curl` is used by the Makefile to fetch the pinned `golangci-lint` binary into `.bin/` | usually preinstalled |

### Auto-installed by `make tools`

Run `make tools` once after cloning to populate everything that doesn't have to be on your PATH:

- **`golangci-lint` v2.11.4** → installed to `.bin/<os>_<arch>/` (version-pinned in the Makefile; bumping the version triggers a reinstall). Not in `go.mod` because its dependency tree conflicts with the main module.
- **`air` v1.65.1** → installed to `.bin/<os>_<arch>/` via `go install`; used by `make dev` for hot-reload. Same exclusion principle as `golangci-lint` — air's transitive deps (Hugo, Sass libs) would bloat `go.sum`.
- **Go `tool` deps** (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `go-test-coverage`, `deadcode`, `gsa`, `goda`) — pinned in `go.mod` via native `tool` directives (Go 1.24+), invoked with `go tool <name>`. `make tools` runs `go mod download` so they're cached; they compile lazily on first invocation.
- **pnpm deps** for `clients/ts/`, `tests/e2e/sdk/`, and `docs/` (via `pnpm install --frozen-lockfile`).

### Verify your setup

```bash
go version          # go1.26+
make --version      # GNU Make 4.x
docker compose version
node --version      # v20+
pnpm --version      # 10.33+
```

If any of those are wrong/missing, the Makefile recipes will fail with confusing errors (e.g. `--output-sync` is unrecognized on Make 3.81; `pnpm: command not found` on `make test-sdk`).

### Optional but recommended

| Tool | Why | Install |
| ---- | --- | ------- |
| **[Claude Code](https://claude.com/claude-code)** | The repo ships team-wide configuration in `.claude/` — slash commands, subagents, hooks, status line. See [Claude Code & AI agents](claude-code.md) for setup. | `brew install --cask claude-code` (macOS) or follow [official install](https://code.claude.com/docs/en/quickstart) |
| **[worktrunk](https://worktrunk.dev)** | Wraps `git worktree` for parallel-agent workflows. Project hooks live in `.config/wt.toml` (auto-runs `make tools` on new worktrees, `make verify` on pre-merge). | `brew install worktrunk && wt config shell install` |

## Quick Start

This is the fastest way to get a fully functional local environment:

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

WaveHouse is now running at `http://localhost:8080` in standalone mode with:

- **Embedded NATS** (JetStream) — no external MQ needed
- **L1 cache only** (Ristretto) — no external cache needed
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

### How `make dev` works

`make dev` is a one-stop convenience target for backend and frontend
development. The recipe is essentially:

```make
dev: deps-up $(AIR)
    air -c .air.toml
```

`deps-up` runs `docker compose ... up -d --wait clickhouse`, which blocks until the ClickHouse container's `/ping` healthcheck flips to healthy. `$(AIR)` lazily installs air to `.bin/<os>_<arch>/` if missing. Then air takes over: it watches `cmd/` and `internal/` plus `config.yaml`, rebuilds `tmp/wavehouse` on change, and restarts the binary.

`air` is pinned to a specific version and installed via `go install` rather than a `go.mod` tool directive — its transitive deps (Hugo, godartsass, Sass libs) would bloat `go.sum` for everyone. Same exclusion principle as `golangci-lint`.

**While `make dev` is running you get:**

- WaveHouse on `http://localhost:8080` with `cors_allowed_origins: ["*"]`, so a browser-based SDK playground or example app on any localhost port can hit the API directly.
- Auth disabled by default — every request goes through. Override with env vars (see below).
- ClickHouse on `http://localhost:8123` (HTTP) and `localhost:9000` (native protocol), Compose project name `wavehouse-dev` so containers/volumes are namespaced.
- Hot reload: editing any `.go` file under `cmd/` or `internal/` (or `config.yaml`) triggers a debounced rebuild + restart. Air's stdout/stderr stream live so you see compile errors and server logs in the same terminal.

### Dev convenience targets

These are the small targets behind `make dev` — useful directly when you want
to run WaveHouse outside of air (e.g. `make build && ./bin/wavehouse`), or
when you need to poke at ClickHouse:

| Target | What it does |
| ------ | ------------ |
| `make deps-up` | Start ClickHouse and block until healthy. Idempotent. |
| `make deps-down` | Stop ClickHouse. Data volume is preserved. |
| `make deps-logs` | `docker compose logs -f clickhouse` (Ctrl+C detaches; container keeps running). |
| `make deps-shell` | Drop into a `clickhouse-client` REPL on the running container. |
| `make deps-wipe` | Stop ClickHouse **and destroy its data volume**. Use when you want a clean schema. |
| `make clean-all` | Nuclear option — every `make` artifact + dev/E2E containers + volumes + `data/`. |

**Stopping `make dev`**: `Ctrl+C` stops air, which propagates SIGINT to WaveHouse for a graceful shutdown (NATS JetStream flush, etc.). ClickHouse stays up — re-running `make dev` is fast because the volume is preserved. Use `make deps-down` or `make deps-wipe` to stop ClickHouse explicitly.

### Using the SDK playground against `make dev`

The `clients/ts/playground/` scripts (`public.ts`, `auth.ts`, `admin.ts`) target a WaveHouse with auth enabled in dev mode and the secret `sdk-dev-secret`. To match those defaults under `make dev`:

```bash
WH_AUTH_ENABLED=true \
WH_AUTH_DEV_MODE=true \
WH_AUTH_JWT_SECRET=sdk-dev-secret \
make dev
```

Then in another terminal:

```bash
cd clients/ts
pnpm install
npx tsx playground/setup.ts          # seed sample tables + data
npx tsx playground/public.ts         # SDK demo against the live server
```

Frontend devs running their own dev server (Vite, Next.js, etc.) can `import { createClient } from '@wavehouse/sdk'` and point `baseURL: 'http://localhost:8080'`; CORS is permissive so cross-origin browser requests just work.

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

## Running Modes at a Glance

| What you want | Command |
| ------------- | ------- |
| Hot-reload standalone dev server | `make dev` |
| Standalone binary (default config) | `make build && ./bin/wavehouse` |
| Standalone via Docker Compose | `docker compose -f deployments/compose/standalone.yaml up -d` |
| Infrastructure deps only (ClickHouse) | `docker compose -f deployments/compose/dependencies.yaml up -d clickhouse` |

## Testing

### How It Works

All test commands use [gotestsum](https://github.com/gotestyourself/gotestsum) for pytest-style colored output with pass/fail icons, durations, and a summary. Tool versions are pinned in `go.mod` via `tool` directives — the Makefile uses `go run` so no global installation is needed.

All tests run with Go's **race detector** (`-race`) enabled by default. WaveHouse is highly concurrent (NATS consumers, singleflight caching, SSE/WS hubs) — the race detector catches data races that would panic in production.

### Quick Reference

```bash
make test                              # Unit tests (compact output) — alias for `test-unit`
V=1 make test                          # Unit tests (verbose output)
make test ARGS="-run TestValidate"     # Run specific test(s)
V=1 make test ARGS="-run TestValidate" # Specific test, verbose
make test-integration                  # Go integration tests (requires Docker)
V=1 make test-integration              # Integration tests, verbose
make test-sdk                          # SDK vitest unit tests
make test-e2e                          # E2E SDK suite against bin/wavehouse-cov
make test-all                          # All four suites sequentially + merged coverage
make ci                                # Full CI: parallel verify+builds+test+test-sdk, then test-integration+test-e2e+cov
make cov                               # Merge available covdata + gate against total threshold
```

Each test target writes `covdata` to `tmp/coverage/<suite>/data/`, renders a textfmt + HTML report, and gates against the per-suite threshold in `.testcoverage.yml`. `make cov` merges whichever suites have run and gates against the total.

**Verbose output**: Use `V=1` to switch from compact `testdox` format to full verbose output. This is a standard Makefile convention (`make test -v` can't work because `-v` is a `make` flag).

**Extra flags**: All test targets accept `ARGS="..."` for additional `go test` flags (e.g., `-run`, `-count`, `-timeout`).

**Note on timing**: gotestsum's `DONE ... in X.XXXs` reports pure test execution time. The total wall time includes Go compiling all packages — the first run compiles everything (~15s), subsequent runs use the build cache (~1s).

### Test Structure

| Category | Location | Docker? | Command |
|----------|----------|---------|---------|
| Unit tests | `internal/*/_test.go` | No | `make test` |
| SDK unit tests | `clients/ts/src/**/*.test.ts` | No | `make test-sdk` |
| Integration tests (Go) | `tests/integration/*_test.go` | Yes | `make test-integration` |
| E2E tests (SDK) | `tests/e2e/sdk/*.test.ts` | Yes | `make test-e2e` |

- **Unit tests** live beside the code they test (e.g., `internal/discovery/discovery_test.go`). They use mocks or embedded NATS (in-process, no Docker needed).
- **Integration tests** use the `//go:build integration` build tag. The `setupTestEnv` helper starts a ClickHouse testcontainer, embedded NATS, Bento ingest worker, and a full API router via `httptest.Server`. Subtests run sequentially because Bento's global registrations are one-time-per-process. DLQ tests use `assert.Eventually` with a 30-second timeout for the 5-second Bento batch window.

Shared test utilities live in `internal/testutil/` (e.g., `testutil.NopLogger()` for silencing embedded NATS output).

### Adding New Tests

- **Unit test for `internal/foo/`** → create `internal/foo/foo_test.go` (same package).
- **Integration test needing Docker** → add a subtest under `tests/integration/` (e.g. a new file with `//go:build integration`).
- **E2E test via SDK** → add a `tests/e2e/sdk/*.test.ts` file. These tests exercise the full pipeline (ingest → ClickHouse → query) through the TypeScript SDK. Run with `make test-e2e`.
- **Test helpers** → add to `internal/testutil/` (Go) or `tests/e2e/sdk/helpers.ts` (E2E).

### E2E Tests via SDK

The primary E2E integration test suite lives in `tests/e2e/sdk/`. It uses the TypeScript SDK as the test harness — every ingest→query test simultaneously validates the full Go backend pipeline and confirms SDK compatibility.

**Architecture**:
- `tests/e2e/compose.yaml` — Single Docker Compose file with **profiles**: ClickHouse always starts; WaveHouse starts only with `--profile app`, so you can also point the suite at a hot-reload `make dev` instance instead.
- `tests/e2e/sdk/setup.ts` — Smart `globalSetup` that probes ports before starting Docker services, so tests work seamlessly whether you started services manually or let the setup do it.
- `tests/e2e/sdk/helpers.ts` — JWT factories, typed client constructors, async wait helpers, direct ClickHouse query helper.

**Running E2E tests**:

```bash
make test-e2e                    # Build the cover binary, install deps, run all E2E tests
KEEP_RUNNING=true make test-e2e  # Don't tear down services after tests
```

`make test-e2e` builds `bin/wavehouse-cov` (coverage-instrumented) and runs the orchestrator under `scripts/orchestrator/` to wire ClickHouse + the cover binary into the suite. covdata flushes on SIGINT into `tmp/coverage/e2e/data/`.

**If you already have `make dev` running**, the setup detects the healthy WaveHouse on `:8080` and skips starting it via Docker — only ClickHouse is started if needed.

**Test files**: `ingest.test.ts`, `query.test.ts`, `auth.test.ts`, `admin.test.ts`, `streaming.test.ts`.

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
│   └── wavehouse/          # Standalone all-in-one binary
├── internal/               # Private application packages
│   ├── api/                # HTTP handlers, router, middleware
│   ├── cache/              # L1 (Ristretto) + L2 caching
│   ├── config/             # YAML + env var configuration
│   ├── dedupe/             # Optional deduplication (Pebble)
│   ├── discovery/          # ClickHouse schema introspection + validation
│   ├── ingest/             # Batch buffering + DLQ + Active Sweeper
│   ├── mq/                 # NATS message queue abstraction
│   ├── pipes/              # Named query pipes (NATS KV + .sql bootstrap)
│   ├── policy/             # Access control policies (evaluation + NATS KV store)
│   ├── query/              # Structured query AST + SQL builder
│   └── testutil/           # Shared test helpers and mocks
├── tests/                  # Integration & E2E tests
│   ├── compose.yaml        # Shared Docker Compose (ClickHouse + optional WaveHouse)
│   ├── fixtures/           # Idempotent ClickHouse DDL scripts
│   └── sdk/                # E2E tests via TypeScript SDK (Vitest)
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
- **Interface-first design**: Core behaviors (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`) are defined as interfaces so implementations can be swapped behind a stable contract.
- **Package boundaries**: The `internal/` directory ensures packages are private to this module.
- **Error handling**: Return errors to callers. Use `slog` for structured logging.
- **Schema-driven**: ClickHouse is the schema source of truth. WaveHouse discovers and validates against real table schemas.

## Makefile Targets

Run `make help` to see all targets. Key ones:

| Target | Description |
| ------ | ----------- |
| `make help` | Show all targets with descriptions (always the source of truth) |
| `make tools` | Bootstrap: install pinned tools (`golangci-lint`, `air`), Go modules, pnpm deps |
| **Dev** | |
| `make dev` | Hot-reload dev server: ClickHouse via Compose + WaveHouse under air on `:8080` |
| `make deps-up` | Start ClickHouse alone (idempotent; blocks until healthy) |
| `make deps-down` | Stop ClickHouse (preserves data volume) |
| `make deps-logs` | Tail ClickHouse logs |
| `make deps-shell` | `clickhouse-client` REPL on the running container |
| `make deps-wipe` | Stop ClickHouse AND destroy its data volume (DESTRUCTIVE) |
| **Static checks** | |
| `make fmt` | Check formatting (run `make fix` to apply) |
| `make tidy` | Verify `go.mod`/`go.sum` are tidy (run `make fix` to apply) |
| `make lint` | Run `golangci-lint` |
| `make vulncheck` | Run `govulncheck` (V=1 for full call stacks) |
| `make verify` | All four above (parallel-safe: `make -j verify`) |
| `make fix` | Auto-apply: `tidy` + `gofumpt` + `goimports` + `lint --fix` |
| **Build** | |
| `make build` | Compile `wavehouse` → `bin/wavehouse` (debug symbols kept) |
| `make build-release` | Stripped release-style build → `bin/wavehouse-release` |
| `make build-cover` | Coverage-instrumented build → `bin/wavehouse-cov` (used by E2E) |
| `make build-sdk` | Build TypeScript SDK → `clients/ts/dist/` |
| **Test** | |
| `make test` | Alias for `test-unit` |
| `make test-unit` | Go unit tests + render coverage + gate suite threshold |
| `make test-integration` | Go integration tests (requires Docker) + coverage gate |
| `make test-sdk` | SDK vitest unit tests + coverage gate |
| `make test-e2e` | E2E SDK suite against `bin/wavehouse-cov` + coverage gate |
| `make test-all` | All four suites sequentially + merged coverage gate |
| `make cov` | Merge available `covdata` + gate against total threshold |
| `make ci` | Full pipeline: parallel `verify` + builds + unit/SDK tests, then integration + E2E + cov |
| **Analysis** (informational, not in CI) | |
| `make size` | Binary size analysis → `tmp/analysis/` (text + SVG + interactive HTML) |
| `make audit-cgo` | Audit dependency tree for C files (builds use `CGO_ENABLED=0`) |
| `make deadcode` | Find unreachable functions |
| `make dep-cut` | Top cuttable deps by transitive weight (`LIMIT=N` to override) |
| `make binary-analysis` | Combined: `size` + `audit-cgo` + `deadcode` |
| **Cleanup** (tiered — compose explicitly for partial resets) | |
| `make clean` | Build outputs only (`bin/`, `dist/`, `clients/ts/dist/`) |
| `make clean-test` | Test outputs only (`tmp/` — coverage data, logs, NATS state) |
| `make clean-tools` | Installed tools and pnpm deps (`.bin/`, `node_modules/`) |
| `make clean-all` | Full reset: above + `data/` + Docker volumes |

All test targets accept `ARGS="..."` for pass-through `go test` flags. Build targets accept `TAGS="..."` for Go build tags. `V=1` switches to verbose `gotestsum` output.

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

For a combined security scan, run `make verify` — it runs `vulncheck` alongside `lint`, and `gosec` is one of the linters enabled in `.golangci.yml`. This is also what CI runs on every push and pull request.

### Dependabot

Dependabot is configured in `.github/dependabot.yml` to open weekly grouped PRs for four ecosystems:

- **Go modules** (root) — outdated or vulnerable Go dependencies, commit prefix `deps:`
- **GitHub Actions** (root) — outdated action versions tracked against the SHA pins in `ci.yml` / `release.yml`, commit prefix `ci:`
- **npm — TypeScript SDK** (`clients/ts/`), commit prefix `deps(sdk):`
- **npm — E2E tests** (`tests/e2e/sdk/`), commit prefix `deps(tests):`

PRs are grouped by ecosystem to reduce noise.

**Auto-merge for Dependabot.** `.github/workflows/dependabot-automerge.yml` auto-approves and enables auto-merge on Dependabot PRs classified as `version-update:semver-patch` or `version-update:semver-minor`. Once CI passes, they merge hands-off. Major-version bumps get a comment flagging them for human review and stay open. Dependabot PRs bypass the `Admin approval` required check entirely (see `admin-approval.yml`), so **all** patch/minor bumps — including workflow-touching ones — merge without human intervention; the trust model for that is CI passing + `dependabot/fetch-metadata` classification.

## CI & review automation

This repo has three tiers of AI automation sitting alongside the normal CI checks. Full detail lives in `AGENTS.md`; this section covers the contributor-facing behavior.

### PR title and Conventional Commits

PR titles must match Conventional Commits format (enforced by `.github/workflows/pr-title.yml` as the required `Validate` status check):

```
<type>(optional-scope)(optional-!): <lowercase subject, no trailing period>
```

Allowed types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `deps`, `build`, `perf`, `revert`, `style`.

The `!` before `:` marks a breaking change per Conventional Commits 1.0.0 (e.g., `feat!: remove deprecated endpoint`, `refactor(api)!: rename handlers`).

If the title doesn't match, a sticky comment posts on the PR explaining the format; it auto-removes once the title is fixed.

### Required status checks

The `main branch protection` ruleset requires the following checks to pass before any PR can merge:

- `Check` — module tidiness, format verification, vulnerability scan
- `Build` — compile all binaries
- `Validate` — PR title is Conventional Commits

Plus 1 approving review, and the `Admin approval` check (enforced by `.github/workflows/admin-approval.yml`) requires at least one `APPROVED` review specifically from an admin (Eric or Taite). Linear history, no deletion, no force-push, squash-merge only.

Dependabot PRs bypass `Admin approval` (`dependabot-automerge.yml` handles patch/minor bumps hands-off once CI is green; majors get a comment and stay open for human review).

> **Note — temporarily relaxed**: `Lint`, `Test`, and `Integration Tests` are *not* currently required while pre-existing failures on `main` are being fixed (tracked in #57). They'll rejoin required once main is green.

### Merge behavior

Squash-only merges. The **PR title** becomes the commit subject (with `(#NN)` appended automatically), the **PR body** becomes the commit message. Keep PR bodies tight — they land in `git log` on `main`. The PR template gives the right shape (Summary / Test plan / Related Issues).

Include `Closes #NN` in the PR body to auto-close the related issue on merge. Alternatively, link the issue in the sidebar's **Development** section — that triggers auto-close even without the keyword.

Auto-merge is enabled repo-wide: click "Enable auto-merge (squash)" on a PR and it merges once checks + approvals land.

### AI reviewers

Three bots review PRs:

- **Claude** (`.github/workflows/claude-review.yml`) — auto-reviews PRs from OWNER / MEMBER / COLLABORATOR / CONTRIBUTOR authors on open, push, ready-for-review. Uses Anthropic's canonical PR-review template with WaveHouse-specific focus on Go concurrency, ClickHouse SQL injection, and AGENTS.md documentation-sync rules. Skips drafts and Dependabot PRs. Sticky comment mode — updates one comment across pushes instead of spamming.
- **Gemini Code Assist** — Marketplace App, reads `.gemini/styleguide.md` and `.gemini/config.yaml`. Configured with `comment_severity_threshold: LOW` to surface more findings.
- **Copilot** — tied to individual reviewer subscriptions; shows up on PRs where a maintainer with Copilot Pro is listed as a reviewer.

All three are **advisory** — the `Admin approval` status check (admin review mandated via workflow) + the ruleset's approval / thread-resolution / linear-history rules are the actual merge-gate.

### Task Board is the single signal

`.github/workflows/project-orchestrator.yml` drives the Task Board (project #7) as the real "who needs to look at this next" channel. GitHub's review-request notifications are treated as noise; what matters is the position of your assigned card on the board.

- **Coder flow**: open PR (draft or not) → address bot feedback → once all required checks pass and all review threads are resolved, the orchestrator adds the PR to the board, sets its Status to `Ready`, and assigns the non-author admin. You're done for now.
- **Reviewer flow**: PR card shows up on your board in `Ready`. You move it to `In progress` when you start reviewing (this is the one manual step). You review. Either (a) approve → `admin-approval.yml` passes, auto-merge takes over, card auto-flips to `Done`; or (b) click "Request changes" → orchestrator moves PR card to `In review`, linked issue card to `Ready` (now the coder's ball).
- **Coder addressing feedback**: push fixes, resolve threads, then click "re-request review" on your reviewer in GitHub's sidebar (this is the trigger the orchestrator listens for). Orchestrator moves PR card back to `Ready`, issue card back to `In review`. Reviewer sees the card returned to their column.

Dependabot PRs bypass `Admin approval` (`dependabot-automerge.yml` handles patch/minor bumps hands-off once CI is green; majors get a comment and stay open for human review). Dependabot PRs do not appear on the Task Board.

### Invoking bots manually

- **Claude**: comment `@claude <instruction>` on an issue, PR, or review. Gated to OWNER / MEMBER / COLLABORATOR / CONTRIBUTOR. Applying the `agent` label to an issue also triggers Claude. See `.github/workflows/claude-agent.yml`.
- **Gemini**: comment `@gemini-code-assist <question>` or use slash commands `/gemini review`, `/gemini summary`, `/gemini help`. Works in both top-level and inline review comments.
- **Copilot**: the re-request-review button on the PR page sends a fresh request.

### Review-response expectations

Every review comment (human or AI) must get a substantive reply before merge — not "fixed" alone. The ruleset's `required_review_thread_resolution: true` means unresolved conversations literally block merge. Agents working on PRs follow the pattern documented in `AGENTS.md` §"Review Response (MANDATORY)": accept / push back / defer, reply with detail, resolve when settled.

When pushing back on a bot's suggestion, end the reply with `@claude` or `@gemini-code-assist` to invite a counter-reply so the dialog actually loops.

### Issue triage

`.github/workflows/triage.yml` classifies new and edited issues via GitHub Models (`gpt-4o-mini`) and applies:

- `area/*` labels based on the issue body (areas pulled dynamically from the `area/*` repo labels — adding a new `area/foo` label with a description is all you need; no workflow edit)
- `security` if the model flags a security concern
- `breaking-change` if the model flags a public-API break
- Priority on the **Task Board** project #7 via the board's `Priority` field (requires `PROJECT_BOARD_TOKEN` secret — labels apply with or without it)

### Auto-labeling PRs

`.github/workflows/label.yml` uses `actions/labeler` with `.github/labeler.yml` to apply `area/*`, `dependencies`, `github_actions`, `go`, and `documentation` labels to PRs based on the files they change. Sync-mode: labels follow the current changed-file set.

### When adding a new `internal/<pkg>/` package

Follow the checklist in `AGENTS.md` §"Common Tasks / Adding a new internal package" — the automation-relevant steps are:

1. Create a matching `area/<pkg>` repo label with a meaningful description (triage reads the description as the classifier's per-area hint).
2. Add the path → label mapping to `.github/labeler.yml` so PRs touching the new package get auto-labeled.

Triage picks up the new label automatically; no workflow edit needed.

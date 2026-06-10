---
title: "Development"
description: "Building, testing, linting, project structure, and contribution workflow."
sidebar:
  order: 11
---

Everything you need to build, test, lint, and contribute to WaveHouse — from first-clone to hot-reload dev server to full end-to-end SDK tests. If you're only trying the product, start with the [Getting Started](/getting-started) guide instead.

## Prerequisites

You need these on your `PATH` before any `make` recipe will work end-to-end:

| Tool | Required version | Why | Install |
| ---- | ---------------- | --- | ------- |
| **Go** | 1.26+ (matches `go.mod`) | Compiles `cmd/wavehouse`; also runs the pinned `tool` deps (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `deadcode`, `gsa`, `goda`) via `go tool` | [go.dev/dl](https://go.dev/dl/) |
| **GNU Make** | **4.0+** | The Makefile uses `--output-sync=target` (Make 4 only) and bash-pinned recipes. macOS ships with BSD Make 3.81, which **will not work** | macOS: `brew install make` then use `gmake` or put `$(brew --prefix make)/libexec/gnubin` on your PATH. Linux: usually already installed |
| **bash** | 4+ recommended | Recipes are pinned to `bash`; the helper scripts under `scripts/` use `set -euo pipefail` and bash arrays | macOS default is bash 3.2 (works for current recipes, but `brew install bash` is safer); Linux distros ship 4+ |
| **Docker** *(or Podman)* | Engine 20.10+ with the Compose **v2** plugin (`docker compose`, no hyphen) | Compose stacks under `deployments/compose/`; the E2E and integration suites boot ClickHouse via testcontainers (no compose file) | [Docker Desktop](https://docs.docker.com/get-docker/), [colima](https://github.com/abiosoft/colima), or [Podman](https://podman.io) with `podman-compose` / the `podman compose` plugin. The testcontainers Go library also honors `DOCKER_HOST` for rootless Podman setups |
| **Node.js** | 22 LTS — pinned via `.nvmrc` at the repo root | Runtime for pnpm and the Vitest suites. Pinned to match CI (`setup-node` uses 22) and to avoid Node-major surprises; older Vitest versions in this repo were known to crash on Node 26 with a V8 heap-allocation abort | [nodejs.org](https://nodejs.org/) or `nvm use` / `fnm use` / `volta` (all read `.nvmrc`) |
| **pnpm** | 11.1+ (pinned via `packageManager` in the root `package.json`) | Package manager for the TypeScript SDK, E2E test harness, and docs site (managed as a single pnpm workspace from the repo root); `make build-ts`, `make test-ts`, `make test-e2e`, `make build-docs`, `make dev-docs`, `make preview-docs` all shell out to `pnpm` | `corepack enable && corepack prepare pnpm@11.1.3 --activate` (recommended), or `npm i -g pnpm` |
| **git** + **curl** | any recent | `git` for source + version metadata in builds; `curl` is used by the Makefile to fetch the pinned `golangci-lint` binary into `.bin/` | usually preinstalled |

### Auto-installed by `make tools`

Run `make tools` once after cloning to populate everything that doesn't have to be on your PATH:

- **`golangci-lint` v2.11.4** → installed to `.bin/<os>_<arch>/` (version-pinned in the Makefile; bumping the version triggers a reinstall). Not in `go.mod` because its dependency tree conflicts with the main module.
- **`air` v1.65.1** → installed to `.bin/<os>_<arch>/` via `go install`; used by `make dev` for hot-reload. Same exclusion principle as `golangci-lint` — air's transitive deps (Hugo, Sass libs) would bloat `go.sum`.
- **Go `tool` deps** (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `go-test-coverage`, `deadcode`, `gsa`, `goda`) — pinned in `go.mod` via native `tool` directives (Go 1.24+), invoked with `go tool <name>`. `make tools` runs `go mod download` so they're cached; they compile lazily on first invocation.
- **pnpm deps** for `clients/ts/`, `tests/e2e/sdk/`, and `docs/` (via `pnpm install --frozen-lockfile`). `make tools` runs only the pnpm install; the Playwright Chromium binary (~130 MB) is fetched on-demand by `make build-docs` / `make dev-docs` via the internal `install-playwright-docs` target, so Go-only contributors don't pay the download cost. When you do hit `build-docs` / `dev-docs`, Chromium is required by `rehype-mermaid` (SVG diagram rendering at build time; nothing else in the docs *build* uses a browser — the manual `docs/scripts/screenshot.mjs` QA helper drives the same Chromium). `starlight-links-validator` runs under `build-docs` / CI only — the `dev-docs` watch loop skips it so a mid-edit dangling link doesn't fail every rebuild (CI still enforces link validity before merge; run `DOCS_WATCH_STRICT=1 make dev-docs` to keep the validator on locally). The `--with-deps` flag (which apt-installs Chromium's system libraries: `libnspr4`, `libnss3`, etc.) is only added when `$CI` is set, so contributor laptops don't get an unexpected `sudo` prompt. On Linux dev machines without those libs already present, run `pnpm exec playwright install-deps chromium` once manually. The docs site is a pnpm workspace package (`wavehouse-docs`); the root Makefile drives it directly via `pnpm --filter` (no sub-Makefile) — the `*-docs` targets show up in `make help`. It is also a real `@wavehouse/sdk` consumer (the landing page's live demo imports the workspace package), so `check-docs` / `build-docs` / `dev-docs` build the SDK first via `build-ts`; if you drive Astro directly through pnpm (e.g. `pnpm --filter wavehouse-docs run start`), run `make build-ts` once first so the dep resolves.

### Verify your setup

```bash
go version          # go1.26+
make --version      # GNU Make 4.x
docker compose version
node --version      # v22.x (matches .nvmrc and CI)
pnpm --version      # 11.1+
```

If any of those are wrong/missing, the Makefile recipes will fail with confusing errors (e.g. `--output-sync` is unrecognized on Make 3.81; `pnpm: command not found` on `make test-ts`).

### Optional but recommended

| Tool | Why | Install |
| ---- | --- | ------- |
| **[Claude Code](https://claude.com/claude-code)** | The repo ships team-wide configuration in `.claude/` — slash commands, subagents, hooks, status line. See [Claude Code & AI agents](/claude-code) for setup. | `brew install --cask claude-code` (macOS) or follow [official install](https://code.claude.com/docs/en/quickstart) |
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
- **Fail-closed** by default — `config.yaml` seeds no policy, so every request is denied until you seed one (see [Test the API](#test-the-api))
- **Dedup disabled** by default — no Pebble needed
- **Schema discovery** — automatically finds your ClickHouse tables

### Test the API

`make dev` is **fail-closed** — `config.yaml` seeds no policy, so every request is denied. Point it at the shipped dev policy (the `public` trial role: read/write `clicks`/`events`, no token) and (re)start it:

```bash
WH_POLICY_FILE_PATH=deployments/compose/dev-policy.yaml make dev
```

Then the tokenless data-plane calls work (create a `clicks` table first — see the [Getting Started](/getting-started) walkthrough):

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

The admin surface — `/v1/schema`, `/v1/admin/query` (raw SQL), `/v1/dlq/stats` — needs the **admin** role, which the `public` trial role doesn't have. Mint an admin JWT (see [Validating tokens](#validating-tokens) below) and pass it:

```bash
curl -s http://localhost:8080/v1/schema -H "Authorization: Bearer $TOKEN" | jq
curl -s -X POST http://localhost:8080/v1/admin/query -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
curl -s http://localhost:8080/v1/dlq/stats -H "Authorization: Bearer $TOKEN"
```

### How `make dev` works

`make dev` is a one-stop convenience target for backend and frontend
development. The recipe is essentially:

```make
dev: deps-up $(AIR)
    air -c .air.toml
```

`deps-up` runs `docker compose ... up -d --wait clickhouse`, which blocks until the ClickHouse container's `/ping` healthcheck flips to healthy. `$(AIR)` lazily installs air to `.bin/<os>_<arch>/` if missing. Then air takes over: it watches `cmd/` and `internal/` (the `.go` and `.yaml` files within them), rebuilds `tmp/wavehouse` on change, and restarts the binary. Config is **not** hot-reloaded: `make dev` runs the binary with `WH_CONFIG=.config.local.yaml` — a gitignored personal copy seeded **once** from `config.yaml` on first run (it won't re-copy if it already exists). So to change dev config, edit `.config.local.yaml` (not `config.yaml`) and restart `make dev`; air watches neither root file.

`air` is pinned to a specific version and installed via `go install` rather than a `go.mod` tool directive — its transitive deps (Hugo, godartsass, Sass libs) would bloat `go.sum` for everyone. Same exclusion principle as `golangci-lint`.

**While `make dev` is running you get:**

- WaveHouse on `http://localhost:8080` with `cors_allowed_origins: ["*"]`, so a browser-based app on any localhost port can hit the API directly.
- A placeholder JWT secret (`change-me-in-production`) ships in `config.yaml`, but **no policy** is seeded — so the stack is fail-closed until you seed one (see [Test the API](#test-the-api)). Override the secret via `WH_AUTH_JWT_SECRET`.
- ClickHouse on `http://localhost:8123` (HTTP) and `localhost:9000` (native protocol), Compose project name `wavehouse-dev` so containers/volumes are namespaced.
- Hot reload: editing any `.go` file under `cmd/` or `internal/` triggers a debounced rebuild + restart. Config isn't hot-reloaded — `make dev` loads `.config.local.yaml` (a gitignored copy seeded once from `config.yaml`), so edit `.config.local.yaml` and restart to apply config changes. Air's stdout/stderr stream live so you see compile errors and server logs in the same terminal.

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

### Running with observability

WaveHouse natively exports standard OpenTelemetry (OTLP) data to `127.0.0.1:4317`. Rather than coupling a heavy observability database stack to the dev server, we provide three lightweight, single-container dashboard options.

You run these in a separate terminal tab alongside `make dev` or your test suites (`make test-e2e`).

They block the terminal and stream logs; simply press `Ctrl+C` to instantly tear them down and clean up the container.

| Target | What it does | UI URL |
| ------ | ------------ | ------ |
| `make obs-aspire` | Boots the Aspire dashboard. Extremely fast, in-memory only, and requires no login. Ideal for quick trace and log debugging. | `http://localhost:18888` |
| `make obs-grafana` | Boots Grafana LGTM (Loki, Grafana, Tempo, Prometheus). Pre-configured to bypass login. Best for advanced UI charting and trace-to-log correlation. | `http://localhost:3000` |
| `make obs-front` | Boots OTel-Front for a basic, alternative trace viewer. | `http://localhost:8000` |

**Typical Workflow:**

1. Open Tab 1: run `make obs-aspire` (UI opens automatically)
2. Open Tab 2: run `make dev` (or `make test-e2e`)
3. View traces, metrics, and logs flowing into the UI instantly. No accounts or auth tokens required.

### Using the SDK against `make dev`

There's no bundled playground — point the published `@wavehouse/sdk` client at your local server (`baseURL: "http://localhost:8080"`), with the dev policy seeded so requests are authorized:

```bash
WH_POLICY_FILE_PATH=deployments/compose/dev-policy.yaml make dev
```

See the [SDK guide](/sdk) for the client API and examples.

Frontend devs running their own dev server (Vite, Next.js, etc.) can `import { createClient } from '@wavehouse/sdk'` and point `baseURL: 'http://localhost:8080'`; CORS is permissive so cross-origin browser requests just work.

### Validating tokens

There is no auth on/off switch — the JWT middleware always runs, but authorization is the policy's job (a `nil`/unseeded policy denies everyone, admins included). To exercise token auth in dev, seed a policy *and* set a known secret — the dev policy's `admin_role` defaults to `admin`, so a JWT with `role: admin` unlocks the admin surface:

```bash
WH_POLICY_FILE_PATH=deployments/compose/dev-policy.yaml WH_AUTH_JWT_SECRET=my-secret make dev
```

Then mint a token (role == the policy `admin_role`) and call an admin endpoint:

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

Then include the dedup field in your ingest body:

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

All tests run with Go's **race detector** (`-race`) enabled by default. WaveHouse is highly concurrent (NATS consumers, singleflight caching, SSE hubs) — the race detector catches data races that would panic in production.

### Quick Reference

```bash
make test                              # Unit tests (compact output) — alias for `test-unit`
V=1 make test                          # Unit tests (verbose output)
make test ARGS="-run TestValidate"     # Run specific test(s)
V=1 make test ARGS="-run TestValidate" # Specific test, verbose
make test-integration                  # Go integration tests (requires Docker)
V=1 make test-integration              # Integration tests, verbose
make test-ts                           # SDK vitest unit tests + coverage + gate against suites.ts-unit
                                       # (`make cov` auto-merges ts-unit + ts-e2e — no separate command)
make test-e2e                          # E2E SDK suite against bin/wavehouse-cov
make test-all                          # All four suites sequentially + merged coverage
make ci                                # Full CI: parallel verify + builds (Go + SDK + docs) + test + test-ts, then test-integration + test-e2e + cov
make cov                               # Merge available covdata + gate against total threshold
```

Each test target writes `covdata` to `tmp/coverage/<suite>/data/`, renders a textfmt + HTML report, and gates against the per-suite threshold in `.testcoverage.yml`. `make cov` merges whichever suites have run and gates against the total.

**Verbose output**: Use `V=1` to switch from compact `testdox` format to full verbose output. This is a standard Makefile convention (`make test -v` can't work because `-v` is a `make` flag).

**Extra flags**: All test targets accept `ARGS="..."` for additional `go test` flags (e.g., `-run`, `-count`, `-timeout`).

**Note on timing**: gotestsum's `DONE ... in X.XXXs` reports pure test execution time. The total wall time includes Go compiling all packages — the first run compiles everything (~15s), subsequent runs use the build cache (~1s).

### Test Structure

| Category | Location | Docker? | Command |
| -------- | -------- | ------- | ------- |
| Unit tests | `internal/*/_test.go` | No | `make test` |
| SDK unit tests | `clients/ts/src/**/*.test.ts` | No | `make test-ts` (always includes coverage + gate) |
| Integration tests (Go) | `tests/integration/*_test.go` | Yes | `make test-integration` |
| E2E tests (SDK) | `tests/e2e/sdk/*.test.ts` | Yes | `make test-e2e` |

- **Unit tests** live beside the code they test (e.g., `internal/discovery/discovery_test.go`). They use mocks or embedded NATS (in-process, no Docker needed).
- **Integration tests** use the `//go:build integration` build tag. The `setupTestEnv` helper starts a ClickHouse testcontainer, embedded NATS, ingest worker, and a full API router via `httptest.Server`. DLQ tests use `assert.Eventually` with a 30-second timeout for the 5-second ingest worker batch window.

Shared test utilities live in `internal/testutil/` (e.g., `testutil.NopLogger()` for silencing embedded NATS output).

### Adding New Tests

- **Unit test for `internal/foo/`** → create `internal/foo/foo_test.go` (same package).
- **Integration test needing Docker** → add a subtest under `tests/integration/` (e.g. a new file with `//go:build integration`).
- **E2E test via SDK** → add a `tests/e2e/sdk/*.test.ts` file. These tests exercise the full pipeline (ingest → ClickHouse → query) through the TypeScript SDK. Run with `make test-e2e`.
- **Test helpers** → add to `internal/testutil/` (Go) or `tests/e2e/sdk/helpers.ts` (E2E).

### E2E Tests via SDK

The primary E2E integration test suite lives in `tests/e2e/sdk/`. It uses the TypeScript SDK as the test harness — every ingest→query test simultaneously validates the full Go backend pipeline and confirms SDK compatibility.

**Architecture**:

- `scripts/orchestrator` — the E2E entrypoint behind `make test-e2e`: it starts a clean ClickHouse **testcontainer** per run, launches the `wavehouse-cov` binary on a random free port, runs the SDK suite against it, then SIGINTs the binary to flush coverage. No Compose file is involved. With `E2E_SHARDED=1` (what CI uses), `scripts/e2e-shards.sh` runs several orchestrators concurrently — each with its own ClickHouse + server — so the suite's wall-clock is the slowest shard instead of the file sum; the shard map (and why files can't parallelize within one server — shared global policy state, [#214](https://github.com/Wave-RF/WaveHouse/issues/214)) lives in that script.
- `tests/e2e/sdk/setup.ts` — Smart `globalSetup` that probes ports before starting Docker services, so tests work seamlessly whether you started services manually or let the setup do it.
- `tests/e2e/sdk/helpers.ts` — JWT factories, typed client constructors, async wait helpers, direct ClickHouse query helper.

**Running E2E tests**:

```bash
make test-e2e                    # Build the cover binary, install deps, run all E2E tests
KEEP_RUNNING=true make test-e2e  # Don't tear down services after tests
```

`make test-e2e` builds `bin/wavehouse-cov` (coverage-instrumented) and runs the orchestrator under `scripts/orchestrator/` to wire ClickHouse + the cover binary into the suite. covdata flushes on SIGINT into `tmp/coverage/e2e/data/`.

**If you already have `make dev` running**, the setup detects the healthy WaveHouse on `:8080` and skips starting it via Docker — only ClickHouse is started if needed.

**Test files** (`tests/e2e/sdk/*.test.ts`): `admin`, `auth`, `batching`, `cache`, `dlq`, `ingest`, `ndjson`, `query`, `streaming`, `stress`.

## Linting

```bash
make lint
```

`golangci-lint` is installed separately (not in `go.mod` — its massive dependency tree causes conflicts). If not found, `make lint` prints install instructions.

Install options:

- **macOS**: `brew install golangci-lint`
- **Binary**: See [golangci-lint.run/welcome/install/](https://golangci-lint.run/welcome/install/)
- **Go install**: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`

The configuration is in `.golangci.yml` (v2 format with `default: none` for explicit control) — that file is the authoritative list of enabled linters. Highlights:

- **errcheck** — Unchecked error returns
- **govet** — Suspicious constructs
- **staticcheck** — Static analysis
- **unused** — Unused code
- **gosec** — Security issues
- **gocritic** — Opinionated style checks
- **revive** — Extensible linter (replaces golint)
- **ineffassign** — Ineffective assignments
- **misspell** — Spelling errors in comments/strings
- **bodyclose** — Unclosed HTTP response bodies
- **noctx** — HTTP requests without context
- **errorlint** — Proper error wrapping checks (`%w`, `errors.Is/As`)
- **tparallel** — Missing `t.Parallel()` in test subtests

Formatting (**gofumpt** — strict superset of gofmt — and **goimports** import grouping) is enforced through the v2 `formatters:` section rather than as linters.

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
│   └── testutil/           # Shared test helpers and mocks
├── tests/                  # Integration & E2E tests
│   ├── integration/        # Go integration tests (//go:build integration)
│   └── e2e/                # E2E suite (orchestrator + ClickHouse testcontainer)
│       ├── fixtures/       # ClickHouse DDL + config/policy fixtures
│       └── sdk/            # E2E specs driven through the TypeScript SDK (Vitest)
├── deployments/
│   ├── compose/            # Docker Compose files (standalone.yaml, dependencies.yaml)
│   ├── Dockerfile          # Runtime image
│   └── Dockerfile.goreleaser  # Release image (built by GoReleaser)
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
| **Observability** | |
| `make obs-aspire` | Prebuilt 0-config o11y UI to show WaveHouse metrics, logs, and traces locally |
| `make obs-grafana` | Grafana alternative to aspire, more advanced and complicated |
| `make obs-front` | Custom graphs like grafana, but is simpler and easier to configure like aspire |
| **Static checks** | |
| `make fmt` | Check formatting across Go (`gofumpt`) + TS (Biome). Run `make fix` to apply. |
| `make tidy` | Verify `go.mod`/`go.sum` are tidy (run `make fix` to apply) |
| `make lint` | Run linters across Go (`golangci-lint`) + TS (Biome) |
| `make vulncheck` | Run `govulncheck` (V=1 for full call stacks) |
| `make verify` | Repo-wide static checks: Go (tidy + fmt + vulncheck + lint) + TS (Biome + `tsc` typecheck) (parallel-safe: `make -j verify`) |
| `make fix` | Auto-fixes across Go (`tidy` + `gofumpt` + `goimports` + `lint --fix`) and TS (Biome `--write`) |
| **Build** | |
| `make build` | Compile `wavehouse` → `bin/wavehouse` (debug symbols kept) |
| `make build-release` | Stripped release-style build → `bin/wavehouse-release` |
| `make build-cover` | Coverage-instrumented build → `bin/wavehouse-cov` (used by E2E) |
| `make build-ts` | Build TypeScript SDK → `clients/ts/dist/` |
| **Test** | |
| `make test` | Alias for `test-unit` |
| `make test-unit` | Go unit tests + render coverage + gate suite threshold |
| `make test-integration` | Go integration tests (requires Docker) + coverage gate |
| `make test-ts` | SDK vitest unit tests + v8 coverage + gate against `suites.ts-unit` (matches Go's "always coverage" pattern) |
| `make cov` | Merge Go + TS coverage and gate against thresholds. Auto-runs after `make test-all` and `make ci`; standalone `make cov` is "show me the merged numbers without re-running." Each side skips silently if its data is missing, but `make cov` fails if *both* are empty (you ran it before any test target). |
| `make test-e2e` | E2E SDK suite against `bin/wavehouse-cov` + coverage gate |
| `make test-all` | All four suites sequentially + merged coverage gate |
| `make ci` | Full pipeline: parallel `verify` + builds + unit/SDK tests, then integration + E2E + cov |
| **Analysis** (informational, not in CI) | |
| `make size` | Binary size analysis → `tmp/analysis/` (text + SVG + interactive HTML) |
| `make audit-cgo` | Audit dependency tree for C files (builds use `CGO_ENABLED=0`) |
| `make deadcode` | Find unreachable functions |
| `make dep-cut` | Top cuttable deps by transitive weight (`LIMIT=N` to override) |
| `make binary-analysis` | Combined: `size` + `audit-cgo` + `deadcode` |
| **Cleanup** (tiered — compose explicitly for partial resets) | |
| `make clean` | Build outputs only (`bin/`, `dist/`, `clients/ts/dist/`, `docs/dist/`, `docs/.dev-dist/`) |
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

Dependabot is configured in `.github/dependabot.yml` to open weekly grouped PRs for five ecosystems:

- **Go modules** (root) — outdated or vulnerable Go dependencies, commit prefix `deps:`
- **GitHub Actions** (root) — outdated action versions tracked against the SHA pins in `ci.yml` / `release.yml`, commit prefix `ci:`
- **npm — docs site** (`docs/`), commit prefix `docs:`
- **npm — TypeScript SDK** (`clients/ts/`), commit prefix `deps(sdk):`
- **npm — E2E tests** (`tests/e2e/sdk/`), commit prefix `deps(tests):`

PRs are grouped by ecosystem to reduce noise.

**No auto-merge.** Dependabot PRs go through the same merge gate as any other PR — an approval from the `@Wave-RF/wavehouse-admins` team (the ruleset's `required_reviewers` rule) plus the required checks. (The former `dependabot-automerge.yml`, which auto-approved and merged patch/minor bumps hands-off, was removed — every bump now gets a human admin review.)

## Releasing the SDK

The TypeScript SDK (`@wavehouse/sdk`, in `clients/ts/`) publishes to npm via `.github/workflows/publish-npm.yml` using OIDC trusted publishing — no `NPM_TOKEN`. It is independent of the server's Go/Docker release (`release.yml`): the `v*` (server) and `sdk-v*` (SDK) tag globs are disjoint, so the two never collide. There are two channels:

- **Dev snapshots.** Every push to `main` publishes `0.0.0-dev.<hash>` under the `dev` dist-tag — but only when the built `dist/` actually changed (the version is a hash of the build output, so an unchanged build resolves to an already-published version and is skipped). Install the bleeding edge with `npm install @wavehouse/sdk@dev`.
- **Tagged releases.** Pushing a `sdk-vX.Y.Z` tag publishes that version and creates a GitHub Release. A stable version goes to the `latest` dist-tag; a prerelease (`sdk-v0.2.0-rc.1`) is published under `alpha`/`beta`/`rc`/`next` — derived from the suffix — and marked as a GitHub pre-release. The tag **must** match `clients/ts/package.json`'s `version`, or the job fails fast.

To cut a release:

```bash
# 1. Bump "version" in clients/ts/package.json, commit, and merge to main.
# 2. Tag the release commit and push the tag:
git tag sdk-v0.1.0
git push origin sdk-v0.1.0
```

> **The first tagged release promotes `latest`.** npm sets a package's `latest` dist-tag on its *first* publish even under `--tag dev`, so until the first `sdk-v*` release a bare `npm install @wavehouse/sdk` (and the bare CDN URLs) resolve to a `0.0.0-dev.*` snapshot. The first tagged stable release moves `latest` to a real version and fixes this for every consumer.

## CI & review automation

This repo has three tiers of AI automation sitting alongside the normal CI checks. Full detail lives in `AGENTS.md`; this section covers the contributor-facing behavior.

### PR title and Conventional Commits

PR titles must match Conventional Commits format and stay ≤ 72 characters — the title becomes the squash-merge commit subject. Both rules are enforced by the `PR title` job under the required `CI` check (`.github/workflows/ci.yml`); validate locally with `scripts/lint-pr-title.sh "<title>"`:

```text
<type>(optional-scope)(optional-!): <lowercase subject, no trailing period>
```

Allowed types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `deps`, `build`, `perf`, `revert`, `style`.

The `!` before `:` marks a breaking change per Conventional Commits 1.0.0 (e.g., `feat!: remove deprecated endpoint`, `refactor(api)!: rename handlers`). Titles are also capped at **72 characters** — they become squash-merge commit subjects (Dependabot PRs are exempt from the cap).

If the title doesn't match, a sticky comment posts on the PR explaining the format (from the `PR housekeeping` workflow, which mirrors the same script); it auto-removes once the title is fixed. Fixing the title needs no new push — the edit triggers housekeeping, which re-runs the failed `PR title` job (the job re-reads the title from the API, not the stale event payload).

### Required status checks

The `main branch protection` ruleset requires one status check to pass before any PR can merge:

- `CI` — the aggregator job of `.github/workflows/ci.yml`. The workflow is a job DAG over the same Makefile targets local `make ci` runs: `lint` (`make verify`), `unit` (`make test-unit test-ts`), `integration` (`make test-integration`), `e2e` (`make -j test-e2e E2E_SHARDED=1` — classifies the change set itself so it starts with no `needs`, builds its own SDK dist + cover binary on a warm cache, runs the suite as concurrent shards, then runs `make cov` over the merged coverage fragments + threshold gates), `docs-build` (`make build-docs` when docs-affecting files changed, uploading the docs dist artifact that `docs-preview` polls for), `PR title` (Conventional Commits), and the docs preview/deploy jobs. The aggregator fails if any job failed or was canceled, treats skipped jobs as passing — docs-only PRs skip the Go test suites by design, and fork PRs run everything except the (secret-bearing) docs deploys — and cross-checks the `changes` job's classification against `e2e`'s so classifier drift can't silently skip the suite.

The `PR housekeeping` workflow still runs on every PR (labels + the title explainer comment) but is no longer a required check.

The ruleset also requires an approval from the `@Wave-RF/wavehouse-admins` team (the `required_reviewers` rule — this is what mandates an admin sign-off, replacing the old `Admin approval` status-check workflow), plus 1 approving review, approval of the most recent push by someone other than its author, resolution of all review threads, linear history, no branch deletion, no force-push, and squash-merge only. Repository admins may bypass these requirements when merging their own PR (e.g. a trivial `.github` change) but still cannot push directly to `main`.

Dependabot PRs go through the same admin review as any other PR — there is no auto-merge (see the Dependabot section above).

### Merge behavior

Squash-only merges. The **PR title** becomes the commit subject (with `(#NN)` appended automatically), the **PR body** becomes the commit message. Keep PR bodies tight — they land in `git log` on `main`. The PR template gives the right shape (Summary / Test plan / Related Issues).

Include `Closes #NN` in the PR body to auto-close the related issue on merge. Alternatively, link the issue in the sidebar's **Development** section — that triggers auto-close even without the keyword.

Auto-merge is enabled repo-wide: click "Enable auto-merge (squash)" on a PR and it merges once checks + approvals land.

### AI reviewers

Advisory PR review comes from marketplace apps configured at the org/repo level:

- **CodeRabbit** — automated PR review; auto-reviews on open + push, re-trigger with `@coderabbitai review`.
- **Copilot** — tied to individual reviewer subscriptions; shows up on PRs where a maintainer with Copilot Pro is listed as a reviewer.

Both are **advisory** — the ruleset's `required_reviewers` rule (an `@Wave-RF/wavehouse-admins` approval) plus its thread-resolution / linear-history / required-check rules are the actual merge-gate.

### Reviewer assignment and the Task Board

Reviewer assignment is GitHub-native, and so is the board:

- **Reviewer assignment**: the `main branch protection` ruleset's `required_reviewers` rule requests the `@Wave-RF/wavehouse-admins` team on every PR, and the team's **code-review assignment** (configured on the team) auto-assigns and load-balances a specific member. No workflow is involved; `dismiss_stale_reviews_on_push` clears approvals on new commits, and GitHub re-requests per its own rules.
- **Merge gate**: the ruleset's `required_reviewers` rule requires an `APPROVED` review from the `@Wave-RF/wavehouse-admins` team; it also adds review-thread resolution, linear history, and squash-only. Auto-merge (squash) takes over once checks and approvals land.
- **Task Board** (Projects v2, project #7): card placement and status are handled by GitHub-native Projects v2 automation configured in the project UI — there is no workflow-driven board state machine. Priority lives on the board's `Priority` field (set during issue triage, below).

Dependabot PRs go through the same admin review as any other PR — there is no auto-merge (see the Dependabot section above).

### Invoking bots manually

- **CodeRabbit**: comment `@coderabbitai review` to re-trigger a review, or `@coderabbitai <question>` to ask it something. Works in both top-level and inline review comments.
- **Copilot**: the re-request-review button on the PR page sends a fresh request.

### Review-response expectations

Every review comment (human or AI) must get a substantive reply before merge — not "fixed" alone. The ruleset's `required_review_thread_resolution: true` means unresolved conversations literally block merge. Agents working on PRs follow the pattern documented in `AGENTS.md` §"Review Response": accept / push back / defer, reply with detail, resolve when settled.

When pushing back on a bot's suggestion, end the reply with the bot's mention (e.g. `@coderabbitai`) to invite a counter-reply so the dialog actually loops.

### Issue triage

`.github/workflows/triage.yml` classifies new and edited issues via GitHub Models (`gpt-4o-mini`) and applies:

- `area/*` labels based on the issue body (areas pulled dynamically from the `area/*` repo labels — adding a new `area/foo` label with a description is all you need; no workflow edit)
- `security` if the model flags a security concern
- `breaking-change` if the model flags a public-API break
- Priority on the **Task Board** project #7 via the board's `Priority` field (requires `PROJECT_BOARD_TOKEN` secret — labels apply with or without it)

### Auto-labeling PRs

The `PR housekeeping` workflow (`.github/workflows/housekeeping.yml`) runs `actions/labeler` with `.github/labeler.yml` to apply `area/*`, `dependencies`, `github_actions`, `go`, and `documentation` labels to PRs based on the files they change. Sync-mode: labels follow the current changed-file set.

### When adding a new `internal/<pkg>/` package

Follow the checklist in `AGENTS.md` §"Common Tasks / Adding a new internal package" — the automation-relevant steps are:

1. Create a matching `area/<pkg>` repo label with a meaningful description (triage reads the description as the classifier's per-area hint).
2. Add the path → label mapping to `.github/labeler.yml` so PRs touching the new package get auto-labeled.

Triage picks up the new label automatically; no workflow edit needed.

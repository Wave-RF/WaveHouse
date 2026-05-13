# AGENTS.md — AI Agent Instructions for WaveHouse

This file provides context for AI coding agents (Copilot, Cursor, Cody, Aider, etc.) working on this codebase.

## Project Overview

WaveHouse is a **schema-aware real-time API gateway for ClickHouse**, written in Go. It handles ingestion with schema validation, optional deduplication, caching, real-time streaming, query proxying, and a Dead Letter Queue. It sits entirely in front of ClickHouse as the exclusive data entry/exit point.

## Architecture (Quick Reference)

One binary:

- **`cmd/wavehouse/`** — Standalone mode (all-in-one with embedded NATS, optional Pebble dedup)

Twelve internal packages under `internal/`:

- **`api/`** — Chi HTTP router, JWT/JWKS middleware, ingest/query/structured-query/SSE/WS/schema/DLQ/policy/pipes handlers, Hub
- **`cache/`** — `Cache` interface → `LocalCache` (Ristretto) + `SharedCache` (TBD) + `TieredCache` (singleflight)
- **`config/`** — YAML + env var config loading (cleanenv)
- **`dedupe/`** — `Deduplicator` interface → `Embedded` (Pebble) — optional, controlled by `dedupe.enabled`
- **`discovery/`** — `SchemaRegistry` that introspects ClickHouse `system.columns` + `Validate()` for ingest payloads
- **`ingest/`** — Bento-based ingest pipeline (`bento.go`: JetStream input → per-table batch INSERT with DLQ output, plus delete handling) + `Sweeper` (Active Sweeper for NATS message lifecycle) + `EventMessage`/`BufferConsumerName` types (`types.go`)
- **`mq/`** — `Publisher`/`Subscriber` interfaces → `EmbeddedNATS` + `RemoteNATS`
- **`observability/`** — OpenTelemetry pipeline: `InitProvider` wires trace/metric/log providers via OTLP gRPC (each signal independently gated). A top-level `Prometheus` config block drives an optional `/metrics` scrape endpoint that runs independently of OTLP push — standalone (Alloy/Mimir scrape, no collector), alongside OTLP, or off. `NewLogger` produces a slog handler that fans out to stdout AND OTLP (stdout always 100%, OTLP sample-rate-aware). `TraceHandler` injects trace_id/span_id from active spans. `tracer.go` provides W3C trace context propagation over NATS headers.
- **`pipes/`** — Named query pipes: `NamedQuery` type + NATS KV store (`WAVEHOUSE_PIPES`) + `.sql` file bootstrap
- **`policy/`** — Hasura-style access control: `Policy`/`TablePolicy`/`RolePermissions` types, `Evaluate()` engine with JWT claim templating, NATS KV store (`WAVEHOUSE_POLICY`)
- **`query/`** — Structured query AST types + SQL builder with schema validation, permission injection, timestamp bucketing

## Key Design Decisions

1. **Interface-first**: Core behaviors are defined as Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`). Standalone and (future) clustered modes use different implementations.
2. **Bring Your Own Schema (BYOS)**: Users create tables in ClickHouse directly. WaveHouse discovers schemas by querying `system.columns` and validates ingest payloads against real column definitions. No auto-migration, no fixed table schema.
3. **Schema-driven ingest**: `POST /v1/ingest/{table}` accepts a flat JSON body. The table name comes from the URL. The body is validated against the discovered schema (unknown fields rejected, types checked, nullable constraints enforced). No envelope — just data.
4. **Async ingestion**: Ingest returns 200 immediately after optional dedup + MQ publish. ClickHouse writes happen asynchronously via the Bento ingest pipeline (`StartIngestWorker`). If NATS stream is full, returns 503 + Retry-After.
5. **Per-table batching**: The Bento ingest pipeline groups events by table name and performs dynamic INSERTs using the schema's column order. Each table's batch is independent.
6. **Dead Letter Queue**: Failed batch inserts are published to a separate NATS stream (`WAVEHOUSE_DLQ`) with subjects `dlq.<table>`. This prevents silent data loss. Controlled by `dlq.enabled`.
7. **Optional auth with JWKS**: JWT authentication is opt-in via `auth.enabled`. Supports HMAC shared secret and/or JWKS endpoint (`auth.jwks_url`). Roles are extracted from a configurable claim path (`auth.role_claim`). Dev mode (`auth.dev_mode`) bypasses validation for development.
8. **Optional dedup**: Deduplication is opt-in via `dedupe.enabled`. When enabled, the `dedupe.id_field` config specifies which JSON field to use as the dedup key.
9. **Singleflight**: TieredCache uses `golang.org/x/sync/singleflight` to prevent cache stampede.
10. **Active Sweeper**: NATS messages are retained for SSE/WS gap-fill. The Sweeper purges messages that are both ACKed (written to ClickHouse) and older than the gap window. Gap-fill uses NATS `DeliverByStartTime` — no in-process ring buffer.
11. **Hasura-style access control**: Per-table, per-role column-level and row-level permissions with JWT claim templating (`{{ jwt.path }}`). Policies stored in NATS KV with file-based bootstrap and cluster-wide sync via KV Watch.
12. **Structured queries**: Type-safe query AST endpoint (`POST /v1/tables/{table}/query`) validated against schema, with permission enforcement, timestamp bucketing for cache optimization, and `DefaultMaxRows` (10,000) limit cap.
13. **Named query pipes**: Pre-defined SQL templates (inspired by Tinybird) with parameter binding, role restrictions, and caching. Stored in NATS KV with `.sql` file directory bootstrap.
14. **TypeScript SDK**: `@wavehouse/sdk` — zero-dependency client with typed query builder, real-time SSE, live queries with smart aggregation classification (incrementable/decomposable/poll), and codegen CLI.
15. **Observability invariants**: Stdout is *always* 100% — the slog logger fans out to stdout AND OTLP, and stdout sampling would silently hide records that scraping pipelines (Promtail/Alloy/Vector → Loki) are paying to store. Sampling knobs apply only to OTLP push. WARN+ERROR records always export at 100% regardless of `logs.sample_rate` — silently dropping errors during incidents would be a worse failure mode than the cost of forwarding them all (this is a *non-configurable* floor; do not expose it). gRPC OTel exporters dial lazily, so an unreachable collector never blocks startup; transient failures surface via the OTel SDK's error handler. The OTel Prometheus exporter (when enabled) uses a *private* `prometheus.Registry` to avoid leaking process/Go collectors that `prometheus.DefaultRegisterer` auto-registers into our `/metrics` output. When changing the logger, the sampler, or the provider wiring, preserve these invariants.
16. **Bearer-token-only CORS posture**: WaveHouse is a Bearer-token API — `Authorization: Bearer <jwt>` on every authenticated request, no cookies, no session middleware. The CORS middleware (`internal/api/router.go` `corsMiddleware`) deliberately **never** emits `Access-Control-Allow-Credentials`, because (a) we don't need it (Bearer tokens are explicit request headers, not browser-managed credentials) and (b) the historical pairing of `Allow-Credentials: true` with `Allow-Origin: *` is a CORS spec violation that browsers reject. The `cors_allowed_origins` allowlist controls *which origins can read responses*, not cookie scope. CSRF protection is structural: cross-site requests can't smuggle a Bearer token because the browser won't auto-attach `Authorization` headers cross-origin. Do not reintroduce cookie-based auth or `Access-Control-Allow-Credentials` without a separate design discussion — the current posture is the answer to GitHub issues #29 and #30.

## Code Conventions

- **Go 1.26**, strict formatting (`gofumpt`, enforced by CI)
- **Structured logging** with `log/slog` (JSON handler)
- **Chi v5** for HTTP routing
- **Error handling**: Return errors, don't panic. Wrap with `fmt.Errorf("context: %w", err)`.
- **No global state**: Dependencies are passed explicitly (constructor injection).
- **Package naming**: Lowercase, single word (or abbreviated). `internal/` enforces module privacy.

## Build & Test Commands

`make help` is the source of truth — run it to see every target with its
one-line description. Common targets, grouped:

```bash
# Setup
make tools             # Install pinned tools, Go modules, and pnpm deps
make help              # Show all targets with descriptions

# Static checks (parallel-safe: `make -j verify`)
make fmt               # Check formatting (run `make fix` to apply)
make tidy              # Verify go.mod/go.sum are tidy (run `make fix` to apply)
make lint              # golangci-lint run ./...
make vulncheck         # Run govulncheck (V=1 for full call stacks)
make verify            # tidy + fmt + vulncheck + lint
make fix               # Auto-apply: tidy + gofumpt + goimports + lint --fix

# Build
make build             # Compile wavehouse → bin/wavehouse (debug symbols kept)
make build-release     # Stripped release-style build → bin/wavehouse-release
make build-cover       # Coverage-instrumented build → bin/wavehouse-cov (used by E2E)
make build-sdk         # Build TypeScript SDK → clients/ts/dist/

# Test (each suite renders coverage and gates against .testcoverage.yml)
make test              # Alias for test-unit
make test-unit         # Go unit tests + coverage gate
make test-integration  # Go integration tests (requires Docker) + coverage gate
make test-sdk          # SDK vitest unit tests + coverage gate
make test-e2e          # E2E SDK suite against bin/wavehouse-cov + coverage gate
make test-all          # All four suites sequentially + merged coverage gate
make cov               # Merge whichever covdata exists + gate against total threshold

# CI
make ci                # Phase 1 (parallel): verify + builds + test-unit + test-sdk
                       # Phase 2 (sequential): test-integration + test-e2e + cov

# Analysis (informational, not in CI)
make size              # Binary size analysis → text + SVG + interactive HTML
make audit-cgo         # Audit deps for C code (builds use CGO_ENABLED=0)
make deadcode          # Find unreachable functions
make dep-cut           # Top cuttable deps by transitive weight (LIMIT=N)
make binary-analysis   # size + audit-cgo + deadcode

# Dev loop (Docker required for everything in this group)
make dev               # ClickHouse + WaveHouse with air hot-reload on :8080.
                       # CORS=*, auth off by default. Override with env vars; e.g.
                       #   WH_AUTH_ENABLED=true WH_AUTH_DEV_MODE=true make dev
                       # for the SDK playground (clients/ts/playground/).
make deps-up           # Start ClickHouse alone (idempotent; blocks until healthy)
make deps-down         # Stop ClickHouse (preserves data volume)
make deps-logs         # Tail ClickHouse logs
make deps-shell        # clickhouse-client REPL on the running container
make deps-wipe         # Stop AND destroy ClickHouse data volume (DESTRUCTIVE)

# Cleanup (tiered — compose explicitly for partial resets)
make clean             # Build outputs only (bin/, dist/, clients/ts/dist/)
make clean-test        # Test outputs only (tmp/ — coverage, logs, NATS state)
make clean-tools       # Installed tools and pnpm deps (.bin/, node_modules/)
make clean-all         # Full reset: above + data/ + docker volumes
```

Verbose test output: `V=1 make test`. Extra flags: `make test ARGS="-run TestFoo"`.
Build tags: `make build TAGS="foo bar"`.

Tooling notes:

- Most dev tools (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`,
  `go-test-coverage`, `deadcode`, `gsa`, `goda`) are pinned in `go.mod` via
  native `tool` directives and invoked with `go tool <name>` — no manual
  install needed.
- `golangci-lint` is pinned in the Makefile (currently v2.11.4) and
  auto-installed to `.bin/<os>_<arch>/` on first `make lint` (or via
  `make tools`). Not in `go.mod` — its dependency tree conflicts with the
  main module.
- `pnpm` (>= 10.33) and `Node.js` (>= 20) must be on your PATH; the SDK and
  E2E test harnesses both shell out to `pnpm`. `make tools` runs
  `pnpm install --frozen-lockfile` in `clients/ts/` and `tests/e2e/sdk/`.
- `GNU Make 4+` is required (uses `--output-sync=target`); macOS ships BSD
  Make 3.81 which will not parse the Makefile. See `docs/development.md` §
  Prerequisites for the full setup checklist.

## Testing Conventions

- **Table-driven tests**: Use `tests := []struct{ name string; ... }` with `t.Run(tt.name, ...)` for test cases.
- **Shared mocks in `internal/testutil/`**: Use `MockPublisher`, `MockCache`, `MockDeduplicator`, `MockSubscriber` instead of creating ad-hoc mocks. See `testutil/mocks.go`.
- **JWT helpers**: Use `testutil.MakeJWT(t, claims)` and `testutil.MakeExpiredJWT(t, claims)` for auth tests. See `testutil/jwt.go`.
- **Schema helpers**: Use `testutil.NewTestSchemaRegistry(tables)` or `discovery.NewSchemaRegistryFromMap(tables)` for schema-aware tests.
- **Policy helpers**: Use `policy.NewMemoryStore(p)` for in-memory policy testing without NATS.
- **Pipes helpers**: Use `pipes.NewMemoryStore(queries...)` for in-memory pipes testing without NATS.
- **Response assertions**: Use `testutil.AssertJSONResponse(t, rec, status, expected)` and `testutil.AssertJSONContains(t, rec, status, substring)`.
- **Coverage target**: 80% project-wide (CI enforces `threshold.total` in `.testcoverage.yml` against the merged unit + integration + e2e profile). Per-suite minima also enforced: unit 70%, integration 12%, e2e 50%, sdk 50%. Aim for 80%+ on new code.
- **Every new function should have corresponding test cases.** Run `make lint` and `make test` before considering work complete.
- **E2E tests via SDK**: The TypeScript SDK is the primary E2E test harness. Tests in `tests/e2e/sdk/` exercise the full pipeline (ingest → ClickHouse → query) and simultaneously validate backend behavior and SDK correctness. Use `make test-e2e` to run. Add new E2E scenarios as `tests/e2e/sdk/*.test.ts` files using helpers from `tests/e2e/sdk/helpers.ts`.

## Local-First Validation (MANDATORY)

**Validate locally before pushing. Do not use CI as your first feedback loop.** The repo runs on a shared 4-runner self-hosted VM with finite throughput and bills AI-reviewer (Claude, Gemini, Copilot) credits on every push. A speculative "let's see what CI says" commit costs real minutes and real dollars and is visible to the entire team as churn. Every push should represent a change you have locally verified to pass the same gates CI will run.

### Before every push

Run the CI-equivalent locally:

```bash
make ci   # Full parity with CI: parallel verify + builds + unit/SDK tests, then integration + E2E + cov
```

`make ci` runs every gate that CI runs (verify = tidy + fmt + lint + vulncheck; build + build-cover + build-sdk; test-unit + test-sdk; test-integration + test-e2e; final merged coverage gate). If it passes, your commit has crossed the same gates CI will run. If it fails, fix it before pushing — don't rely on CI to surface issues that took seconds to catch locally.

For workflow-only changes where `make ci` isn't relevant, manually read through your YAML diff line-by-line before pushing. If you already have `actionlint` installed locally, also run `actionlint .github/workflows/*.yml`; CI's own billing makes "push and see" for workflow-file iteration especially wasteful.

### If local passes but CI fails

**Treat this as an environment mismatch, not a test bug, until proven otherwise.** Tests that pass on a dev machine in milliseconds but time out on the self-hosted VM point to runner-side problems (I/O pressure, zombie processes, disk contention, shared-VM fsync storms) — not to flaky test code. Investigate the runner before changing tests or production code. Masking environment issues with longer timeouts or retries tends to compound: today's 5s bump becomes tomorrow's 30s bump becomes next week's unbounded wait, and the underlying runner problem keeps slowly degrading.

Order of operations before patching tests for "CI flakiness":

1. **Reproduce the reported failure locally first.** `go test -race -run TestFoo ./...` on your machine. If it fails locally, you have a real test bug; fix it with deterministic primitives (use `c.Wait()` not `time.Sleep`, use `require.Eventually` not `time.Sleep` then assert, use channel sync not goroutine scheduling assumptions).
2. **If it passes locally, try to reproduce under load.** Run 4 concurrent copies of `make test-unit` to simulate the VM's shared-runner contention. If that still passes, the problem is the VM — not the test.
3. **Only then touch the runner.** SSH in, check `iostat -x 2`, `pgrep -af nats-server`, `df -h`, `du -sh /opt/github/action-runner-*/_work`. Environment fixes (cleanup crons, tmpfs for test temp dirs, slower runner count, faster disk) stay scoped to the runner and don't pollute the codebase.

### When delegating to another agent

If you hand work to a subagent or another Claude session, tell them explicitly: *"Run locally first. Do not push to CI until `make ci` passes on your checkout."* Agents default to "commit and let CI run" because it looks like progress; in this repo that default is expensive. Override it at delegation time.

## Review Response (MANDATORY)

**Every review comment on a PR gets a substantive reply, and every conversation gets resolved before merge. This applies equally to human reviewers and AI reviewers (Copilot, Gemini Code Assist, claude-review, future bots). The `main branch protection` ruleset enforces `required_review_thread_resolution: true`, so unresolved threads literally block merge.**

### What to do on every review comment

1. **Read it, decide**: accept (it's right), push back (it's wrong or out-of-scope), or defer (it's right but deserves its own PR).
2. **Reply substantively** — not "fixed" alone. Say *what* was changed and in which commit SHA, or *why* you're pushing back. For cross-reviewer disagreements (one bot contradicts another, or a bot contradicts a human), argue with code references or spec citations — don't just assert.
3. **Always @mention the bot you're replying to** (except Copilot). Without the mention the bot never sees your reply and the dialog silently terminates. Put the mention *below* the signature trailer, on its own line:
   - **Claude**: end with `@claude` (or `/review`) to re-invoke `claude-review.yml` for a fresh review on the current head SHA (gated to OWNER/MEMBER/COLLABORATOR/CONTRIBUTOR)
   - **Gemini**: end with `@gemini-code-assist` to re-invoke Gemini, or open with `/gemini review` / `/gemini <question>`
   - **Copilot**: no mention works — add a parenthetical noting the re-request-review button instead
   
   Mention on **every** bot reply, not just pushback cases. If you accept and fix, the bot may still want to verify; if you push back, the bot may have a counter-argument. Silent termination defeats the dialog design.
4. **Fix in the PR when the suggestion is clearly right and in scope.** If it's right but out-of-scope, reply with a tracking link (issue or planned follow-up PR) before resolving.
5. **Resolve the thread** once the reply fully addresses the concern AND no counter-reply is pending. Don't resolve threads a human reviewer is still engaging with; wait. Bot threads can be resolved after a substantive reply since bots only re-engage when mentioned — if the reply doesn't mention the bot, it's accepted as terminal.
6. **Re-request review** from humans after substantive code changes. Bot reviewers re-run on `synchronize` (Claude, Gemini) or via an explicit re-request button (Copilot).

### What not to do

- No empty acknowledgements (`LGTM`, `fixed`, `good catch`). Always include detail so the reply makes sense standalone.
- Don't argue in circles. If a reviewer comes back with the same point after your reply, escalate to a human maintainer rather than looping.
- Don't resolve a thread that has an open child comment from the reviewer you haven't addressed.

### Review tooling reference

| Reviewer | How it runs | Re-runs on new commits | Blocks merge |
| -------- | ----------- | ---------------------- | ------------ |
| Claude (`.github/workflows/claude-review.yml`) | Our workflow, `pull_request: [opened, synchronize, reopened, ready_for_review]` trigger — fires on every PR push directly (not chained off CI). Manual re-trigger via `@claude` or `/review` in a PR comment, or `gh workflow run "Claude PR review" -f pr_number=<N>` | Yes, auto — **posts inline review comments** at specific lines (resolution required by the ruleset) plus a sticky verdict-summary top-level comment that edits in place across pushes | Yes for inline comments — the ruleset's `required_review_thread_resolution: true` blocks merge until each `claude[bot]` thread is resolved. The workflow's check itself is advisory |
| Gemini Code Assist | Marketplace App at repo level | Yes on synchronize, **but silently skips `.github/workflows/**`** (built-in exclusion, can't be overridden) — so Gemini reviews rarely see infra PRs | No (advisory) |
| Copilot | GitHub-native when reviewer has Copilot Pro enabled | Yes if enabled in Copilot settings | No (advisory) |
| Human admins (Eric / Taite) | Review requested from the non-author admin by `.github/workflows/housekeeping.yml` on `pull_request_target: opened` and `ready_for_review` (not on every synchronize — `dismiss_stale_reviews_on_push` would otherwise re-spam the reviewer after each push). Reviewer selection rule: PR author == Eric → Taite; author == Taite → Eric; any other author → admin chosen by `PR_NUM % len(ADMINS)` for deterministic load spreading. The composite action also sets the GitHub `assignees` field to the same user (the assignee + review-request pair encode "this is your PR" + "GitHub should notify you"). Board placement on project #7 is handled by GitHub's native Projects v2 workflows (`Auto-add to project`, `Item added`, `Pull request merged`) configured in the project UI. | Not on synchronize. Manual re-request via the GitHub UI's "Re-request review" if `dismiss_stale_reviews_on_push` clears the request after a CHANGES_REQUESTED. | Yes — `.github/workflows/admin-approval.yml` is a required status check that fails unless Eric or Taite has an `APPROVED` review. Dependabot patch/minor PRs bypass it (handled by `dependabot-automerge.yml`); major bumps fall through to the same admin-review evaluation as human PRs. |

> **Known limitation**: Gemini Code Assist silently ignores all files under `.github/workflows/**` — a hardcoded Google default that `.gemini/config.yaml`'s `ignore_patterns` can't remove. For workflow-heavy PRs, Claude review is the primary AI reviewer. Gemini still covers `CHANGELOG.md`, docs, source code, and configuration outside `.github/`.

## Documentation & Consistency Sync (MANDATORY)

**This is a hard requirement. Every code change MUST include corresponding updates to all affected files below. Do NOT wait for the user to ask — verify and update these automatically as part of every task. A code change without its documentation counterpart is incomplete.**

### What to check on EVERY change

1. **API docs** (`docs/api.md`) — If you add, modify, or remove an endpoint, request/response field, error code, or query parameter, update the API reference. Ensure JSON field names, HTTP status codes, and curl examples match the actual handler code.
2. **Configuration docs** (`docs/configuration.md`) — If you add or change a field in `internal/config/config.go`, update the config reference table, the example YAML block, and the mode-specific settings section.
3. **Architecture docs** (`docs/architecture.md`) — If you add/rename a package, change a data flow, or modify component wiring, update the architecture overview, package descriptions, and data flow diagrams.
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
5. **Add a matching `area/<pkg>` repo label** (e.g. `area/foo` for `internal/foo/`) so the issue triage workflow can route issues to it.
6. **Update the area enumeration** in `.github/workflows/triage.yml` (the `system-prompt:` block lists every legal area the LLM is allowed to return). Without this, the triager can't categorize issues about the new package.

### Writing tests

1. Create `*_test.go` files in the same package as the code under test.
2. Use table-driven tests with `t.Run(tt.name, ...)` for multiple scenarios.
3. Use shared mocks from `internal/testutil/` (MockPublisher, MockCache, MockDeduplicator, MockSubscriber).
4. Use `testutil.MakeJWT(t, claims)` for auth tests, `discovery.NewSchemaRegistryFromMap(...)` for schema-aware tests, `policy.NewMemoryStore(p)` for policy tests, `pipes.NewMemoryStore(queries...)` for pipes tests.
5. Use `testutil.AssertJSONResponse` and `testutil.AssertJSONContains` for HTTP handler assertions.
6. Run `make test` — it gates the unit-test coverage threshold from `.testcoverage.yml`, so a passing run already confirms coverage.
7. Aim for 80%+ coverage on new code. The project-wide CI-enforced minimum is 80% (merged unit + integration + e2e via `.testcoverage.yml`'s `threshold.total`); per-suite minima are unit 70%, integration 12%, e2e 50%, sdk 50%.

## File Structure

```text
cmd/                    → Binary entry points (thin — just wiring)
internal/api/           → HTTP layer (handlers, router, middleware, Hub, schema/DLQ/policy/pipes endpoints)
internal/cache/         → Caching (interface + L1/L2/tiered implementations)
internal/config/        → Configuration structs + loader
internal/dedupe/        → Optional deduplication (interface + embedded/distributed)
internal/discovery/     → ClickHouse schema introspection + ingest validation
internal/ingest/        → Batch buffer with DLQ + Active Sweeper (NATS message lifecycle)
internal/mq/            → MQ abstraction (interface + embedded/remote NATS)
internal/observability/ → OpenTelemetry pipeline (traces/metrics/logs providers, Prometheus exporter, slog fan-out, NATS trace propagation)
internal/pipes/         → Named query pipes (NATS KV store + SQL file bootstrap)
internal/policy/        → Access control policies (types, evaluation, NATS KV store)
internal/query/         → Structured query AST + SQL builder
internal/testutil/      → Shared test helpers (NopLogger, etc.)
tests/                  → Integration & E2E tests
tests/integration/      → Go integration tests (//go:build integration; ClickHouse testcontainer)
tests/fixtures/         → Idempotent ClickHouse DDL scripts for test tables
tests/e2e/              → E2E test stack
tests/e2e/compose.yaml  → Docker Compose with profiles (ClickHouse always; WaveHouse via --profile app)
tests/e2e/sdk/          → E2E integration tests via TypeScript SDK (Vitest)
deployments/compose/    → Docker Compose files
deployments/docker/     → Dockerfiles
docs/                   → Project documentation
.vscode/                → Workspace settings (gopls build flags, recommended extensions)
```

## Security Considerations

- JWT secret must be cryptographically strong when auth is enabled in production
- All `/v1/*` routes are behind optional JWT auth middleware
- Input JSON is validated against ClickHouse schemas before processing
- ClickHouse queries are passed through directly — use appropriate access controls on ClickHouse itself
- **Dependency vulnerability scanning**: `govulncheck ./...` runs in CI on every push/PR. Dependabot (`.github/dependabot.yml`) opens weekly grouped PRs for outdated Go modules and GitHub Actions.
- **GitHub Actions supply chain**: Third-party actions are pinned to full commit SHAs with version comments (see `.github/workflows/ci.yml`, `release.yml`). New workflows must follow the same pattern — never `@main` or floating tags on third-party actions. Prefer inline bash or official `actions/*` / `github/*` actions when feasible (e.g. `pr-title.yml` is an inline check rather than a third-party action).

## Repository Automation (two tiers)

1. **Tier 1 — Issue triage** (`.github/workflows/triage.yml`): GitHub Models (`gpt-4o-mini` via `actions/ai-inference`) classifies new/edited issues and applies `area/*` + `security` + `breaking-change` labels. Optionally writes the `Priority` custom field on the Task Board (Project #7) when a `PROJECT_BOARD_TOKEN` secret with project scope is configured.
2. **Tier 2 — Code review** (two reviewers, both advisory; the `Admin approval` required status check + the ruleset are the actual merge-gate):
   - **Gemini Code Assist App** configured via `.gemini/styleguide.md` — Marketplace App attached at the repo/org level, no workflow file.
   - **Claude PR review** (`.github/workflows/claude-review.yml`) — `anthropics/claude-code-action` runs on every PR open / push, gated on the HEAD commit's author or committer having at least read permission on the repo (catches "admin pushed a fixup onto an external author's PR"). Dependabot is filtered at the workflow level. Claude posts findings as **inline review comments** (tagged `[MUST]` / `[SHOULD]` / `[MAY]` per the prompt template) plus a short sticky verdict summary; inline threads count against the ruleset's `required_review_thread_resolution`, so they block merge until resolved — same mechanism Gemini uses. Manual re-trigger via `@claude` or `/review` in a PR comment from a trusted actor, or via `gh workflow run "Claude PR review" -f pr_number=<N>`. The workflow is review-only — Claude can comment but cannot push commits. Requires the `CLAUDE_CODE_OAUTH_TOKEN` secret (generated via `claude setup-token`).

### Dependabot automation

`.github/workflows/dependabot-automerge.yml` auto-approves and enables auto-merge on Dependabot PRs for patch and minor version bumps. Major bumps get a comment flagging them for human review, both admins are assigned as reviewers, and the PR stays open until an admin actually approves. CI still has to pass for auto-merge to actually squash the PR.

**Dependabot patch/minor bumps bypass the `Admin approval` required status check** (see `admin-approval.yml`) — the auto-approval from the workflow + CI passing is the trust model. **Major bumps are NOT bypassed**: they fall through to the same admin-review evaluation as human PRs, so an admin's `APPROVED` review is required to clear the status. This closed a hole where a bot's APPROVED review (e.g. from CodeRabbit) could satisfy the merge gates on a major bump without admin involvement.

The ruleset's `required_approving_review_count` is set to `0` intentionally — the `Admin approval` status check is the single admin-review gate. Without that, *any* APPROVED review (including from non-admin bots) would satisfy the count rule independently of admin involvement. Title-length cap (72 chars) and other PR housekeeping rules still apply, except Dependabot is exempted from the length cap (its grouped-update titles routinely exceed 72 chars and the title format isn't configurable).

## Governance Files

- **No `CODEOWNERS` file**: Removed 2026-04-21 in favor of workflow-driven reviewer assignment and approval enforcement. `CODEOWNERS`'s one-ping-on-open behavior conflicted with the "don't notify reviewers until bots are clean" design goal. Admin approval is now enforced by `.github/workflows/admin-approval.yml` (required status check that fails unless Eric or Taite has an `APPROVED` review; Dependabot patch/minor PRs bypass, majors fall through to the same admin-review evaluation as human PRs). Reviewer assignment is handled by `.github/workflows/housekeeping.yml` — fires on `pull_request_target: opened` / `ready_for_review` and requests review from the non-author admin via the `assign-and-request-review` composite. Task Board placement and Status moves are handled by the project's *native* Projects v2 workflows (`Auto-add to project`, `Item added`, `Pull request merged`) configured in the project UI.

### Task Board state machine

The Task Board (project #7) is a queue of "what needs attention next." Each PR / Issue card has two axes:

- **Assignee** — _set once, per card_. The PR card is assigned to the **reviewer** (the non-author admin, picked by `housekeeping.yml` per the parity rule below). Issue cards are assigned to the **implementer**. Assignees don't rotate across state transitions — they represent "this card is your card." Use card assignment to find what's in your queue; use the card Status to know what state the work is in.
- **Status** — moved by GitHub's native Projects v2 workflows (configured in the project UI):
  - `Auto-add to project` adds new PRs/issues to the board automatically.
  - `Item added to project` sets the initial Status when something lands on the board.
  - `Pull request merged` flips the PR card to `Done` on merge (and via `Auto-close issue` the linked-Closes issue closes + flips to `Done` as well).

**Reviewer-assignment rule** (the only PR-side automation we keep in a workflow file, in `housekeeping.yml`): when the PR is opened or flipped from draft → ready,
- if the PR author is Eric → reviewer is Taite;
- if the PR author is Taite → reviewer is Eric;
- if the PR author is anyone else → reviewer is `ADMINS[PR_NUM % len(ADMINS)]` (deterministic load-spread).

The composite sets both `assignees` and a GitHub review-request on the same user. Drafts and Dependabot PRs are skipped (Dependabot's major-bump PRs assign both admins via `dependabot-automerge.yml` instead).

**Things we used to automate but don't anymore** (handled manually now, the simplification PR that landed on 2026-05-12 had the explicit trade-off discussion):

- Auto-flip draft → ready on bot-clean: dropped. Drafts are informative signal; the author marks the PR ready when they want review.
- Auto-move PR card to `In review` on a `CHANGES_REQUESTED` review: dropped. One-click manual move on the board.
- Auto-mirror Issue card Status when the linked PR moves (PR `Ready` ↔ Issue `In review`): dropped. The PR list shows what needs review; the linked-issue mirror was a clever feature with low practical value for a 4-person team.
- Re-firing review-request on author "re-request review" when `dismiss_stale_reviews_on_push` has cleared it: dropped. Author uses the GitHub UI's Re-request Review button.

**Dependabot PR handling** via `dependabot-automerge.yml`:
- Patch/minor bumps: auto-approved + auto-merged hands-off. Native "Auto-add to project" still adds them to the board.
- Major-version bumps: held for human review, both admins assigned, review requested from both (Dependabot is the author, so the parity rule doesn't apply — either admin can pick it up). The sticky comment explains the hold and edits in place across re-syncs via the marker-comment upsert pattern.
- **`CLAUDE.md`** and **`.gemini/styleguide.md`**: Thin pointer files. `AGENTS.md` (this file) is the single source of truth. Keep those pointers short; never duplicate content.
- **`CONTRIBUTING.md`**: Conventional Commits type list must stay in sync with the regex in `.github/workflows/housekeeping.yml` (formerly `pr-title.yml`). The title linter validates squash-merge commit messages.

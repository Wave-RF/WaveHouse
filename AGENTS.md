# AGENTS.md — AI Agent Instructions for WaveHouse

This file provides context for AI coding agents (Copilot, Cursor, Cody, Aider, etc.) working on this codebase.

## Project Overview

WaveHouse is a **schema-aware real-time API gateway for ClickHouse**, written in Go. It handles ingestion with schema validation, optional deduplication, caching, real-time streaming, query proxying, and a Dead Letter Queue. It sits entirely in front of ClickHouse as the exclusive data entry/exit point.

## Architecture (Quick Reference)

One binary:

- **`cmd/wavehouse/`** — Standalone mode (all-in-one with embedded NATS, optional Pebble dedup)

Eleven internal packages under `internal/` (plus `internal/testutil/` for shared test helpers):

- **`api/`** — Chi HTTP router, JWT/JWKS middleware, ingest/query/structured-query/SSE/WS/schema/DLQ/policy/pipes handlers, Hub
- **`cache/`** — `Cache` interface → `LocalCache` (Ristretto) + `SharedCache` (TBD) + `TieredCache` (singleflight)
- **`config/`** — YAML + env var config loading (cleanenv)
- **`dedupe/`** — `Deduplicator` interface → `Embedded` (Pebble) — optional, controlled by `dedupe.enabled`
- **`discovery/`** — `SchemaRegistry` that introspects ClickHouse `system.columns` + `Validate()` for ingest payloads
- **`ingest/`** — Bento-based ingest pipeline (`bento.go`: JetStream input → per-table batch INSERT with DLQ output, plus inline delete handling — failed deletes route to `dlq.<table>` and `DoubleAck` rather than `Nak`, to break the infinite-redelivery loop that a deterministic delete error would otherwise produce) + `Sweeper` (Active Sweeper for NATS message lifecycle) + `EventMessage`/`BufferConsumerName` types (`types.go`)
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

`make help` is the source of truth — run it to see every target with its one-line description. Common targets, grouped:

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

- Most dev tools (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `go-test-coverage`, `deadcode`, `gsa`, `goda`) are pinned in `go.mod` via native `tool` directives and invoked with `go tool <name>` — no manual install needed.
- `golangci-lint` is pinned in the Makefile (currently v2.11.4) and auto-installed to `.bin/<os>_<arch>/` on first `make lint` (or via `make tools`). Not in `go.mod` — its dependency tree conflicts with the main module.
- `pnpm` (>= 10.33) and `Node.js` (>= 20) must be on your PATH; the SDK and E2E test harnesses both shell out to `pnpm`. `make tools` runs `pnpm install --frozen-lockfile` in `clients/ts/` and `tests/e2e/sdk/`.
- `GNU Make 4+` is required (uses `--output-sync=target`); macOS ships BSD Make 3.81 which will not parse the Makefile. See `docs/src/content/docs/development.md` § Prerequisites for the full setup checklist.

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

## Local-First Validation

**Validate locally before pushing. Don't use CI as your first feedback loop.** Every push consumes shared CI capacity and AI-reviewer credits, and produces visible churn for the rest of the team.

### Before every push

```bash
make ci   # Full parity with CI: parallel verify + builds + unit/SDK tests, then integration + E2E + cov
```

If `make ci` passes locally, your commit has crossed the same gates CI will run. For workflow-only changes, read the YAML diff carefully and run `actionlint` if you have it installed.

### If local passes but CI fails

Treat as environment mismatch first, test bug second. Reproduce the failure locally (`go test -race -run TestFoo ./...`); if it passes, try concurrent copies to simulate runner contention; only then look at the runner itself. Masking environment issues with longer timeouts compounds — today's 5s bump becomes tomorrow's 30s bump.

When delegating to a subagent: tell them explicitly *"run locally first."* Agents default to "commit and let CI run" because it looks like progress.

## Review Response

Every review comment gets a substantive reply, and every thread gets resolved before merge. The `main branch protection` ruleset enforces `required_review_thread_resolution: true`, so unresolved threads block merge. Applies to human reviewers and AI reviewers alike (Copilot, Gemini Code Assist, claude-review).

### What to do

1. **Decide**: accept, push back, or defer (right but out-of-scope).
2. **Reply substantively** with the fix's commit SHA or your reasoning. No bare "fixed" / "LGTM" / "good catch".
3. **@mention the bot you're replying to** (except Copilot), on its own line below your signature trailer:
   - Claude: `@claude` or `/review` re-invokes the workflow
   - Gemini: `@gemini-code-assist` or `/gemini <question>`
   - Copilot: no mention works — note the re-request-review button

   Without the mention, the bot never sees the reply and the dialog silently terminates.
4. **Fix in this PR** if the suggestion is right and in scope. Out-of-scope but valid: link a tracking issue before resolving.
5. **Resolve the thread** once the reply addresses the concern and no counter-reply is pending. Bot threads are safe to resolve after a substantive reply (bots only re-engage on mention); human threads — wait for them.
6. **Re-request review** from humans after substantive changes. Bot reviewers re-run on `synchronize` (Claude, Gemini) or via a re-request button (Copilot).

### What not to do

- Don't argue in circles. If the reviewer repeats the same point, escalate to a maintainer rather than looping.
- Don't resolve a thread that has an open child comment.

### Review tooling reference

| Reviewer | How it runs | Re-runs on new commits | Blocks merge |
| -------- | ----------- | ---------------------- | ------------ |
| Claude (`.github/workflows/claude-review.yml`) | Our workflow, fires on every PR push (open/sync/reopen/ready). Manual re-trigger via `@claude` / `/review` in a PR comment, or `gh workflow run "Claude PR review" -f pr_number=<N>` | Yes — posts inline review comments plus a sticky verdict-summary comment that edits in place | Yes for inline comments — `required_review_thread_resolution: true` blocks merge until each `claude[bot]` thread is resolved. The workflow's check itself is advisory |
| Gemini Code Assist | Marketplace App at repo level | Yes on synchronize. **Silently skips `.github/workflows/**`** (built-in exclusion, can't be overridden) — Gemini rarely sees infra PRs | No (advisory) |
| Copilot | GitHub-native, requires a reviewer with Copilot Pro | Yes if enabled | No (advisory) |
| Human admins | Review requested from a non-author admin by `housekeeping.yml` on PR open / ready-for-review (not on every push). Selection picks the other admin if the author is one, otherwise round-robins. The composite also sets `assignees`. | Not on synchronize. Manual re-request via the GitHub UI's "Re-request review" if `dismiss_stale_reviews_on_push` clears the request. | Yes — `admin-approval.yml` is a required status check that fails unless an admin has approved. Dependabot patch/minor bypasses (auto-merge handles those); major bumps fall through to admin review. |

> **Known limitation**: Gemini Code Assist silently ignores all files under `.github/workflows/**` — a hardcoded Google default that `.gemini/config.yaml`'s `ignore_patterns` can't remove. For workflow-heavy PRs, Claude review is the primary AI reviewer. Gemini still covers `CHANGELOG.md`, docs, source code, and configuration outside `.github/`.

## Documentation Sync

Every code change should update the corresponding docs in the same PR. A code change without its doc update is incomplete.

| Change | Files to update |
| ------ | --------------- |
| Add/modify API endpoint | `docs/src/content/docs/api.md`, `README.md` (if user-facing) |
| Add/modify config option | `docs/src/content/docs/configuration.md`, `config.yaml`, `deployments/compose/*` env blocks, `docs/src/content/docs/deployment.md` |
| Change architecture / add a package | `docs/src/content/docs/architecture.md`, `AGENTS.md` |
| Change ingest / event format | `docs/src/content/docs/api.md`, `docs/src/content/docs/deployment.md` (CH schema) |
| Change deployment / Docker | `docs/src/content/docs/deployment.md`, compose files |
| Change build / test process | `docs/src/content/docs/development.md`, `Makefile` |
| Any notable change | `CHANGELOG.md` under `[Unreleased]` |

Source-of-truth pairs that must agree:

- Config struct tags in `internal/config/config.go` ↔ `docs/src/content/docs/configuration.md`, `config.yaml`, compose env blocks
- `EventMessage` JSON tags ↔ `docs/src/content/docs/api.md` event format, SSE/WS examples, ClickHouse INSERT columns
- Route registrations in `router.go` ↔ `docs/src/content/docs/api.md` endpoint list
- Handler error responses ↔ `docs/src/content/docs/api.md` error tables

Before finishing a task, grep for the identifiers you touched (field names, env var names, endpoint paths) across docs to catch staleness.

## Common Tasks

### Adding a new API endpoint

1. Create or modify a handler in `internal/api/` (follow existing patterns like `ingest.go`).
2. Register the route in `internal/api/router.go`.
3. If it needs new dependencies, add to the `Dependencies` struct in `router.go`.
4. Wire dependencies in `cmd/wavehouse/main.go`.
5. Add tests.
6. Document in `docs/src/content/docs/api.md`.

### Adding a new config option

1. Add the field to the appropriate struct in `internal/config/config.go` with `yaml`, `env`, and `env-default` tags.
2. Use the new config value in `cmd/wavehouse/main.go` or the relevant internal package.
3. Document in `docs/src/content/docs/configuration.md`.

### Adding a new internal package

1. Create the package under `internal/`.
2. Define an interface if there will be multiple implementations.
3. Wire it into `cmd/wavehouse/main.go`.
4. Document in `docs/src/content/docs/architecture.md`.
5. **Add a matching `area/<pkg>` repo label** (e.g. `area/foo` for `internal/foo/`) so the issue triage workflow can route issues to it. `triage.yml` discovers `area/*` labels at runtime via `gh label list`, so the new label is picked up automatically — no workflow edit needed.

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
tests/e2e/              → E2E test stack
tests/e2e/fixtures/     → Idempotent ClickHouse DDL scripts for test tables
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

## Repository Automation

- **Issue triage** (`triage.yml`): GitHub Models classifies new/edited issues and applies `area/*` + `security` + `breaking-change` labels.
- **Code review** (advisory; the `Admin approval` required status check + the ruleset are the actual merge gate):
  - **Gemini Code Assist App** configured via `.gemini/styleguide.md`.
  - **Claude PR review** (`claude-review.yml`) runs on every PR open or push, gated on the HEAD commit's author or committer having ≥read permission. Dependabot is filtered at workflow level. Findings post as inline review comments (blocked by `required_review_thread_resolution`) plus a sticky verdict summary. Manual re-trigger via `@claude` / `/review` from a trusted commenter or via `workflow_dispatch`. Review-only — Claude can comment but not push. Requires the `CLAUDE_CODE_OAUTH_TOKEN` secret (`claude setup-token`).
- **Dependabot auto-merge** (`dependabot-automerge.yml`): patch/minor bumps auto-approve + auto-merge; major bumps hold for human review. CI still gates the actual merge. Patch/minor bypass `Admin approval` (the workflow + CI passing is the trust model); major bumps fall through to admin review like any human PR — this closed a hole where a bot's APPROVED review (e.g. CodeRabbit) could merge a major bump without admin involvement (see #130).

## Governance Files

- **No `CODEOWNERS`**: replaced by workflow-driven reviewer assignment + approval enforcement.
  - `admin-approval.yml` — required status check that fails unless an admin has an `APPROVED` review. Dependabot patch/minor bypasses; major bumps go through admin review.
  - `housekeeping.yml` — requests review from a non-author admin on PR open / ready-for-review via the `assign-and-request-review` composite. Task Board placement is handled by native Projects v2 workflows configured in the project UI.
- **`CLAUDE.md`** and **`.gemini/styleguide.md`**: thin pointer files to AGENTS.md. Keep those pointers short; never duplicate content.
- **`CONTRIBUTING.md`**: the Conventional Commits type list must stay in sync with the regex in `housekeeping.yml`. The title linter validates squash-merge commit messages.

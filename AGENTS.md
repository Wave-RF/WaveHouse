# AGENTS.md — AI Agent Instructions for WaveHouse

This file provides context for AI coding agents (Copilot, Cursor, Cody, Aider, etc.) working on this codebase.

## Project Overview

WaveHouse is a **schema-aware real-time API gateway for ClickHouse**, written in Go. It handles ingestion with schema validation, optional deduplication, caching, real-time streaming, query proxying, and a Dead Letter Queue. It sits entirely in front of ClickHouse as the exclusive data entry/exit point.

## Architecture (Quick Reference)

Three binaries:

- **`cmd/wavehouse/`** — Standalone mode (all-in-one with embedded NATS, optional Pebble dedup)
- **`cmd/wavehouse-api/`** — Clustered API server (stateless, horizontally scalable)
- **`cmd/wavehouse-worker/`** — Clustered background worker (batch consumer + sweeper)

Ten internal packages under `internal/`:

- **`api/`** — Chi HTTP router, JWT/JWKS middleware, ingest/query/structured-query/SSE/WS/schema/DLQ/policy/pipes handlers, Hub
- **`cache/`** — `Cache` interface → `LocalCache` (Ristretto) + `SharedCache` (Redis) + `TieredCache` (singleflight)
- **`config/`** — YAML + env var config loading (cleanenv)
- **`dedupe/`** — `Deduplicator` interface → `Embedded` (Pebble) + `Distributed` (ScyllaDB) — optional, controlled by `dedupe.enabled`
- **`discovery/`** — `SchemaRegistry` that introspects ClickHouse `system.columns` + `Validate()` for ingest payloads
- **`ingest/`** — Bento-based ingest pipeline (`bento.go`: JetStream input → per-table batch INSERT with DLQ output, plus delete handling) + `Sweeper` (Active Sweeper for NATS message lifecycle) + `EventMessage`/`BufferConsumerName` types (`types.go`)
- **`mq/`** — `Publisher`/`Subscriber` interfaces → `EmbeddedNATS` + `RemoteNATS`
- **`pipes/`** — Named query pipes: `NamedQuery` type + NATS KV store (`WAVEHOUSE_PIPES`) + `.sql` file bootstrap
- **`policy/`** — Hasura-style access control: `Policy`/`TablePolicy`/`RolePermissions` types, `Evaluate()` engine with JWT claim templating, NATS KV store (`WAVEHOUSE_POLICY`)
- **`query/`** — Structured query AST types + SQL builder with schema validation, permission injection, timestamp bucketing

## Key Design Decisions

1. **Interface-first**: Core behaviors are defined as Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`). Standalone and clustered modes use different implementations.
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

## Code Conventions

- **Go 1.25**, strict formatting (`gofumpt`, enforced by CI)
- **Structured logging** with `log/slog` (JSON handler)
- **Chi v5** for HTTP routing
- **Error handling**: Return errors, don't panic. Wrap with `fmt.Errorf("context: %w", err)`.
- **No global state**: Dependencies are passed explicitly (constructor injection).
- **Package naming**: Lowercase, single word (or abbreviated). `internal/` enforces module privacy.

## Build & Test Commands

```bash
make help              # Show all targets with descriptions
make setup             # Download Go modules and cache tools
make tools             # Install external dev tools (golangci-lint, air, goreleaser)
make check-tools       # Verify all required tools are installed
make build             # Compile all 3 binaries to bin/
make build-debug       # Compile with debug symbols (for delve/profiling)
make fmt               # Format code (gofumpt + goimports)
make fmt-check         # Verify formatting (non-zero exit if unformatted)
make lint              # golangci-lint run ./...
make lint-fix          # golangci-lint run --fix ./...
make fix               # Auto-format + auto-fix linters
make test              # Unit tests with race detector
make test-integration  # Integration tests (needs Docker)
make test-all          # Unit + integration tests
make ci                # Full CI check: tidy + fmt + lint + vulncheck + build + tests
make coverage          # Unit test coverage → tmp/coverage/
make coverage-enforce  # Fail if coverage is below 60% threshold (interim; #67 tracks restoring 70%)
make mod-tidy-check    # Verify go.mod/go.sum are tidy
make vulncheck         # Run govulncheck vulnerability scanner
make security          # Combined scan: vulncheck + gosec via linter
make deadcode          # Find unreachable functions
make audit-cgo         # Audit deps for C code (informational — builds use CGO_ENABLED=0)
make size-report       # Show binary sizes
make size-tree         # Top packages by size in the binary (text table)
make size-treemap      # Full binary analysis → text + SVG + interactive HTML
make dep-graph         # Dependency graph → tmp/analysis/graph.svg (requires graphviz)
make dep-why MOD=...   # Show why a module is included
make dep-cut           # Top cuttable deps by transitive impact (LIMIT=N)
make binary-analysis   # Combined: sizes + dead code + CGO audit
make smoke-test        # Manual Bento insert+delete (needs running WaveHouse)
make test-sdk          # TypeScript SDK unit tests
make test-e2e          # E2E integration tests via SDK (starts Docker services)
make test-e2e-dev      # E2E tests in watch mode
make test-everything   # All four test layers: unit + SDK + integration + E2E
make dev               # Hot-reload dev server (air) — starts ClickHouse + applies fixtures
make docker            # Build Docker image
make clean             # Remove bin/, tmp/, data/, dist/
```

Verbose test output: `V=1 make test`. Extra flags: `make test ARGS="-run TestFoo"`.

Dev tools (`gotestsum`, `gofumpt`, `goimports`) are pinned in `go.mod` via native `tool` directives and invoked with `go run` — no manual installation needed. `golangci-lint` is installed separately (binary install recommended).

## Testing Conventions

- **Table-driven tests**: Use `tests := []struct{ name string; ... }` with `t.Run(tt.name, ...)` for test cases.
- **Shared mocks in `internal/testutil/`**: Use `MockPublisher`, `MockCache`, `MockDeduplicator`, `MockSubscriber` instead of creating ad-hoc mocks. See `testutil/mocks.go`.
- **JWT helpers**: Use `testutil.MakeJWT(t, claims)` and `testutil.MakeExpiredJWT(t, claims)` for auth tests. See `testutil/jwt.go`.
- **Schema helpers**: Use `testutil.NewTestSchemaRegistry(tables)` or `discovery.NewSchemaRegistryFromMap(tables)` for schema-aware tests.
- **Policy helpers**: Use `policy.NewMemoryStore(p)` for in-memory policy testing without NATS.
- **Pipes helpers**: Use `pipes.NewMemoryStore(queries...)` for in-memory pipes testing without NATS.
- **Response assertions**: Use `testutil.AssertJSONResponse(t, rec, status, expected)` and `testutil.AssertJSONContains(t, rec, status, substring)`.
- **Coverage target**: 60% interim minimum (CI enforced via `.testcoverage.yml`; #67 tracks restoring the 70% target). Aim for 80%+ on new code.
- **Every new function should have corresponding test cases.** Run `make lint` and `make test` before considering work complete.
- **E2E tests via SDK**: The TypeScript SDK is the primary E2E test harness. Tests in `tests/sdk/` exercise the full pipeline (ingest → ClickHouse → query) and simultaneously validate backend behavior and SDK correctness. Use `make test-e2e` to run, or `make test-e2e-dev` for watch mode. Add new E2E scenarios as `tests/sdk/*.test.ts` files using helpers from `tests/sdk/helpers.ts`.

## Review Response (MANDATORY)

**Every review comment on a PR gets a substantive reply, and every conversation gets resolved before merge. This applies equally to human reviewers and AI reviewers (Copilot, Gemini Code Assist, claude-review, future bots). The `main branch protection` ruleset enforces `required_review_thread_resolution: true`, so unresolved threads literally block merge.**

### What to do on every review comment

1. **Read it, decide**: accept (it's right), push back (it's wrong or out-of-scope), or defer (it's right but deserves its own PR).
2. **Reply substantively** — not "fixed" alone. Say *what* was changed and in which commit SHA, or *why* you're pushing back. For cross-reviewer disagreements (one bot contradicts another, or a bot contradicts a human), argue with code references or spec citations — don't just assert.
3. **Always @mention the bot you're replying to** (except Copilot). Without the mention the bot never sees your reply and the dialog silently terminates. Put the mention *below* the signature trailer, on its own line:
   - **Claude**: end with `@claude` to re-invoke `claude-agent.yml` (gated to OWNER/MEMBER/COLLABORATOR/CONTRIBUTOR)
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
| Claude (`.github/workflows/claude-review.yml`) | Our workflow, `pull_request: synchronize` trigger | Yes, auto — **updates the same sticky comment** rather than posting new ones | No (advisory) unless added to required checks |
| Gemini Code Assist | Marketplace App at repo level | Yes on synchronize, **but silently skips `.github/workflows/**`** (built-in exclusion, can't be overridden) — so Gemini reviews rarely see infra PRs | No (advisory) |
| Copilot | GitHub-native when reviewer has Copilot Pro enabled | Yes if enabled in Copilot settings | No (advisory) |
| Human admins (Eric / Taite) | Auto-assigned to the **PR** (as the `assignees` field, not a review request) by `.github/workflows/project-orchestrator.yml` **only once** the PR is bot-clean: required checks green AND all review threads resolved. Draft PRs get flipped to ready at the same moment, PR card goes on Task Board (project #7) with Status=Ready. Assignment logic: PR author == Eric → assign Taite; author == Taite → assign Eric; other authors → assign both. The Task Board card + assignee are the single signal — GitHub's native review-request channel is intentionally NOT used so notifications don't fire mid-iteration. | Workflow re-checks on every push (`pull_request_target`), review (`pull_request_review`), thread resolution (`pull_request_review_thread`), and check completion (`check_suite: completed`). Idempotent across re-fires. | Yes — `.github/workflows/admin-approval.yml` is a required status check that fails unless Eric or Taite has an `APPROVED` review (Dependabot PRs bypass). |

> **Known limitation**: Gemini Code Assist silently ignores all files under `.github/workflows/**` — a hardcoded Google default that `.gemini/config.yaml`'s `ignore_patterns` can't remove. For workflow-heavy PRs, Claude review is the primary AI reviewer. Gemini still covers `CHANGELOG.md`, docs, source code, and configuration outside `.github/`.

## Documentation & Consistency Sync (MANDATORY)

**This is a hard requirement. Every code change MUST include corresponding updates to all affected files below. Do NOT wait for the user to ask — verify and update these automatically as part of every task. A code change without its documentation counterpart is incomplete.**

### What to check on EVERY change

1. **API docs** (`docs/api.md`) — If you add, modify, or remove an endpoint, request/response field, error code, or query parameter, update the API reference. Ensure JSON field names, HTTP status codes, and curl examples match the actual handler code.
2. **Configuration docs** (`docs/configuration.md`) — If you add or change a field in `internal/config/config.go`, update the config reference table, the example YAML block, and the mode-specific settings section.
3. **Architecture docs** (`docs/architecture.md`) — If you add/rename a package, change a data flow, or modify component wiring, update the architecture overview, package descriptions, data flow diagrams, and the standalone-vs-clustered comparison table.
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
6. Run `make test` to verify. Run `make coverage` to check coverage.
7. Aim for 80%+ coverage on new code. 60% is the interim CI-enforced minimum (#67 tracks restoring the 70% target).

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
internal/pipes/         → Named query pipes (NATS KV store + SQL file bootstrap)
internal/policy/        → Access control policies (types, evaluation, NATS KV store)
internal/query/         → Structured query AST + SQL builder
internal/testutil/      → Shared test helpers (NopLogger, etc.)
tests/                  → Integration & E2E tests
tests/compose.yaml      → Shared Docker Compose (ClickHouse + optional WaveHouse via profiles)
tests/fixtures/         → Idempotent ClickHouse DDL scripts for test tables
tests/sdk/              → E2E integration tests via TypeScript SDK (Vitest)
tests/cmd/bento_pub/    → Manual smoke-test tool (insert+delete via NATS)
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

## Repository Automation (three tiers)

1. **Tier 1 — Issue triage** (`.github/workflows/triage.yml`): GitHub Models (`gpt-4o-mini` via `actions/ai-inference`) classifies new/edited issues and applies `area/*` + `security` + `breaking-change` labels. Optionally writes the `Priority` custom field on the Task Board (Project #7) when a `PROJECT_BOARD_TOKEN` secret with project scope is configured.
2. **Tier 2 — Code review** (two reviewers, both advisory; the `Admin approval` required status check + the ruleset are the actual merge-gate):
   - **Gemini Code Assist App** configured via `.gemini/styleguide.md` — Marketplace App attached at the repo/org level, no workflow file.
   - **Claude PR review** (`.github/workflows/claude-review.yml`) — `anthropics/claude-code-action` runs automatically on PR open/push/ready-for-review, but only when the PR author is already OWNER/MEMBER/COLLABORATOR to bound token cost. Dependabot PRs are skipped. Fork PRs from first-time contributors aren't auto-reviewed here; a maintainer can invoke Claude on them via `@claude` (Tier 3).
3. **Tier 3 — Agentic execution** (`.github/workflows/claude-agent.yml`): same action in a different mode. Runs when an OWNER, MEMBER, or COLLABORATOR mentions `@claude` in an issue, PR, review, or comment, or applies the `agent` label to an issue. Can make code changes and open PRs. Requires the `CLAUDE_CODE_OAUTH_TOKEN` secret (generated via `claude setup-token`).

### Dependabot automation

`.github/workflows/dependabot-automerge.yml` auto-approves and enables auto-merge on Dependabot PRs for patch and minor version bumps. Major bumps get a comment flagging them for human review and stay open until a maintainer acts. CI still has to pass for auto-merge to actually squash the PR. Dependabot PRs **bypass the `Admin approval` required status check** (see `admin-approval.yml`) — the auto-approval from the workflow + CI passing is the trust model for patch/minor bumps.

## Governance Files

- **No `CODEOWNERS` file**: Removed 2026-04-21 in favor of workflow-driven reviewer assignment and approval enforcement. `CODEOWNERS`'s one-ping-on-open behavior conflicted with the "don't notify reviewers until bots are clean" design goal. Admin approval is now enforced by `.github/workflows/admin-approval.yml` (required status check that fails unless Eric or Taite has an `APPROVED` review, Dependabot PRs bypass). Reviewer assignment + Task Board orchestration is handled by `.github/workflows/project-orchestrator.yml` (adds PR to board, assigns the non-author admin, transitions card state on review events).

### Task Board state machine

The Task Board (project #7) card position + assignee is the single source of truth for "who needs to look at this next." `project-orchestrator.yml` automates most of the flow:

- **PR bot-clean** (required checks green + threads resolved): PR added to board, Status=`Ready`, assignee=non-author admin. Draft PRs flipped to ready-for-review at the same moment.
- **Review submitted with `CHANGES_REQUESTED`**: PR card → `In review` (reviewer now waiting on coder), linked issue card → `Ready` (coder attention needed).
- **Author re-requests review** (explicit "I've addressed feedback" signal via GitHub's re-request-review button): PR card → `Ready` (reviewer attention), linked issue card → `In review` (coder waiting).
- **Review approved**: no workflow action; `admin-approval.yml` flips its status check to success, auto-merge takes over, GitHub's native project workflows transition PR+issue cards to `Done` after merge.

The **one manual step** intentionally NOT automated: when the reviewer starts reviewing, they move the PR card `Ready` → `In progress` themselves. GitHub doesn't emit a "review started" event we could hook, and making this automatic would misrepresent state.

Dependabot PRs skip the orchestrator entirely — they go through `dependabot-automerge.yml` and don't appear on the board.
- **`CLAUDE.md`** and **`.gemini/styleguide.md`**: Thin pointer files. `AGENTS.md` (this file) is the single source of truth. Keep those pointers short; never duplicate content.
- **`CONTRIBUTING.md`**: Conventional Commits type list must stay in sync with the regex in `.github/workflows/pr-title.yml`. The PR-title linter validates squash-merge commit messages.

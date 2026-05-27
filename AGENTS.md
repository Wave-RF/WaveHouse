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
- **`ingest/`** — Ingest worker pipeline (`worker.go`: JetStream input → per-table batch INSERT with DLQ output). The pipeline is **insert-only**. The wire format `EventMessage` (`types.go`) carries `{table_name, scope, received_timestamp, data}` and nothing else; the worker validates the table name and the payload's presence, then bulk-INSERTs. In the embedded-NATS deployment (the default), the server runs with `DontListen: true` (`internal/mq/embedded.go`), so the only Publishers reachable on the `ingest.>` subjects are in-process Go code — today, only the HTTP `/v1/ingest?table={table}` handler. Non-insert mutations (`DELETE`/`UPDATE`/`TRUNCATE`/…) must go through `POST /v1/admin/query` under the admin role (the same `RequireAdmin` gate as the rest of `/v1/admin/*`), so non-admin callers never reach the proxy. A request with no token (or an invalid one) resolves to the `default_role`, which in a production config is not the admin role (setting them equal is a loudly-warned dev-only setting), so it can't reach this endpoint. Plus `Sweeper` (Active Sweeper for NATS message lifecycle) + `EventMessage`/`BufferConsumerName` types (`types.go`)
- **`mq/`** — `Publisher`/`Subscriber` interfaces → `EmbeddedNATS` + `RemoteNATS`
- **`observability/`** — OpenTelemetry pipeline: `InitProvider` wires trace/metric/log providers via OTLP gRPC (each signal independently gated). A top-level `Prometheus` config block drives an optional `/metrics` scrape endpoint that runs independently of OTLP push — standalone (Alloy/Mimir scrape, no collector), alongside OTLP, or off. `NewLogger` produces a slog handler that fans out to stdout AND OTLP (stdout always 100%, OTLP sample-rate-aware). `TraceHandler` injects trace_id/span_id from active spans. `tracer.go` provides W3C trace context propagation over NATS headers.
- **`pipes/`** — Named query pipes: `NamedQuery` type + NATS KV store (`WAVEHOUSE_PIPES`) + `.sql` file bootstrap
- **`policy/`** — Hasura-style access control: `Policy`/`TablePolicy`/`RolePermissions` types, `Evaluate()` engine with JWT claim templating, NATS KV store (`WAVEHOUSE_POLICY`)
- **`query/`** — Structured query AST types + SQL builder with schema validation, permission injection, timestamp bucketing

## Key Design Decisions

1. **Interface-first**: Core behaviors are defined as Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`). Standalone and (future) clustered modes use different implementations.
2. **Bring Your Own Schema (BYOS)**: Users create tables in ClickHouse directly. WaveHouse discovers schemas by querying `system.columns` and validates ingest payloads against real column definitions. No auto-migration, no fixed table schema.
3. **Schema-driven ingest**: `POST /v1/ingest?table={table}` accepts a flat JSON body. The table name comes from the `table` query parameter. The body is validated against the discovered schema (unknown fields rejected, types checked, nullable constraints enforced). No envelope — just data.
4. **Async ingestion**: Ingest returns 200 immediately after optional dedup + MQ publish. ClickHouse writes happen asynchronously via the ingest worker pipeline (`StartIngestWorker`). If NATS stream is full, returns 503 + Retry-After.
5. **Per-table batching**: The ingest worker pipeline groups events by table name and performs dynamic INSERTs using the schema's column order. Each table's batch is independent.
6. **Dead Letter Queue**: Failed batch inserts are published to a separate NATS stream (`WAVEHOUSE_DLQ`) with subjects `dlq.<table>`. This prevents silent data loss. Controlled by `dlq.enabled`.
7. **Auth (JWT, no on/off switch)**: The JWT middleware (`internal/auth`) **always** runs — there is no `auth.enabled` or `auth.dev_mode` flag. It verifies tokens with **either** a JWKS endpoint (`auth.jwks_url`) **or** an HMAC shared secret, not both — when `jwks_url` is set it is the sole verifier and `jwt_secret` is ignored (an unreachable JWKS endpoint fails startup loudly); roles come from a configurable claim path (`auth.role_claim`). Accepted signing algorithms are pinned to the active verifier (HMAC → `HS*`; JWKS → asymmetric `RS*/ES*/PS*/EdDSA`) via `jwt.WithValidMethods`, and the `alg` header is checked before any key is used, so `alg: none` and cross-family alg-confusion tokens are rejected. **Authentication is decoupled from authorization:** a request with **no** token, or an **invalid/expired/malformed** one, falls back to an empty role (which `policy.ResolveRole` maps to the policy `default_role`) — the bad-token reason is stashed in the request context so a gate that ultimately denies can **fail loud** (`401` "invalid/expired token") instead of a bare `403`. Elevated access requires a valid token whose role is granted (or equals the policy `admin_role`). **Public (no-token) access is policy-driven:** set a usable `default_role` to open it, remove it to close it — a policy PUT/delete flips it live, no restart. `/v1/admin/*` **and** the schema/DLQ routes are admin-only via `RequireAdmin`, which reads `policy.AdminRole` from the store live (so `admin_role` changes apply without a restart); a pipe with no `allowed_roles` denies everyone but the admin role. There is **no** fail-closed cache hack: a deleted policy becomes `nil`, and `Evaluate(nil)`/`IsAdmin(nil)` deny everyone — including the admin role — on their own (fail fully closed), so "delete the policy" structurally locks everyone out (bootstrap a fresh deployment from the policy file, not an implicit admin grant).
8. **Optional dedup**: Deduplication is opt-in via `dedupe.enabled`. When enabled, the `dedupe.id_field` config specifies which JSON field to use as the dedup key.
9. **Singleflight**: TieredCache uses `golang.org/x/sync/singleflight` to prevent cache stampede.
10. **Active Sweeper**: NATS messages are retained for SSE/WS gap-fill. The Sweeper purges messages that are both ACKed (written to ClickHouse) and older than the gap window. Gap-fill uses NATS `DeliverByStartTime` — no in-process ring buffer.
11. **Hasura-style access control**: Per-table, per-role column-level and row-level permissions with JWT claim templating (`{{ jwt.path }}`). Policies stored in NATS KV with file-based bootstrap and cluster-wide sync via KV Watch. The **single** admin check is `policy.IsAdmin` (role == `admin_role`, configurable per policy, `"admin"` by default, **exact case-sensitive** match — no `ToLower`/`TrimSpace` normalization); it is the one source of truth shared by `Evaluate`, `ResolveRole`, `Validate`, the `/v1/admin` gate, and pipe authorization (`policy.RoleAllowed`). An empty/absent role is fail-closed: it never matches a role key — matching is exact (no `"*"` any-role wildcard) and only the admin role bypasses the allowlist — `Validate` rejects empty role keys at write time, and a `nil` policy denies everyone, including the admin role (a total lockout — bootstrap from the policy file, not an implicit admin grant). Preserve this when touching `internal/policy` (the policy twin of the pipe invariant in #13, see #159). The optional `default_role` is the one sanctioned exception: `ResolveRole` maps an empty role to it *before* evaluation (so a roleless request gets that role's perms). Setting `default_role` equal to the `admin_role` is permitted and makes every roleless request admin — a local/dev-only convenience that is **not** refused by `ResolveRole` or `Validate`, but the store logs a loud warning on every node that adopts such a policy (`policy.DefaultRoleGrantsAdmin`, the single source of truth for the condition); never use it in production. Roles do **not** inherit the default's permissions.
12. **Structured queries**: Type-safe query AST endpoint (`POST /v1/query?table={table}`) validated against schema, with permission enforcement, timestamp bucketing for cache optimization, and `DefaultMaxRows` (10,000) limit cap.
13. **Named query pipes**: Pre-defined SQL templates (inspired by Tinybird) with parameter binding, role restrictions, and caching. Stored in NATS KV with `.sql` file directory bootstrap. Per-pipe `allowed_roles` is the *only* authorization gate on the execute path — `GET/POST /v1/pipes/{name}` sit outside the `/v1/admin/*` `RequireAdmin` block — and it **fails closed** via `policy.RoleAllowed`: authorization is exact allowlist membership (there is no `"*"` any-role wildcard), the admin role (`policy.AdminRole`) always passes, and any request whose resolved role is empty or absent (no token, or a JWT missing `auth.role_claim`, with no usable `default_role`) matches nothing; empty-string allowlist entries are ignored so a stray `""` cannot authorize an empty role, and a pipe with no `allowed_roles` authorizes nobody but the admin role. When changing pipe execution, preserve this — and exercise it via the shared `testutil.RunRoleMatrix` / `StandardRoleMatrix` matrix (see #159).
14. **TypeScript SDK**: `@wavehouse/sdk` — zero-dependency client with typed query builder, real-time SSE, live queries with smart aggregation classification (incrementable/decomposable/poll), and codegen CLI.
15. **Observability invariants**: Stdout is *always* 100% — the slog logger fans out to stdout AND OTLP, and stdout sampling would silently hide records that scraping pipelines (Promtail/Alloy/Vector → Loki) are paying to store. Sampling knobs apply only to OTLP push. WARN+ERROR records always export at 100% regardless of `logs.sample_rate` — silently dropping errors during incidents would be a worse failure mode than the cost of forwarding them all (this is a *non-configurable* floor; do not expose it). gRPC OTel exporters dial lazily, so an unreachable collector never blocks startup; transient failures surface via the OTel SDK's error handler. The OTel Prometheus exporter (when enabled) uses a *private* `prometheus.Registry` to avoid leaking process/Go collectors that `prometheus.DefaultRegisterer` auto-registers into our `/metrics` output. When changing the logger, the sampler, or the provider wiring, preserve these invariants.
16. **Bearer-token-only CORS posture**: WaveHouse is a Bearer-token API — `Authorization: Bearer <jwt>` on every authenticated request, no cookies, no session middleware. The CORS middleware (`internal/api/router.go` `corsMiddleware`) deliberately **never** emits `Access-Control-Allow-Credentials`, because (a) we don't need it (Bearer tokens are explicit request headers, not browser-managed credentials) and (b) the historical pairing of `Allow-Credentials: true` with `Allow-Origin: *` is a CORS spec violation that browsers reject. The `cors_allowed_origins` allowlist controls *which origins can read responses*, not cookie scope. CSRF protection is structural: cross-site requests can't smuggle a Bearer token because the browser won't auto-attach `Authorization` headers cross-origin. Do not reintroduce cookie-based auth or `Access-Control-Allow-Credentials` without a separate design discussion — the current posture is the answer to GitHub issues #29 and #30.
17. **Non-fatal boot**: Schema-discovery failure on boot (ClickHouse unreachable, missing database, transient network blip) is non-fatal — `cmd/wavehouse` records an `api.BootState` diagnostic, binds `:8080`, and retries via `SchemaRegistry.RetryRefresh` (exp backoff 2s → 60s) in the background. `/health` and `/ready` return 503 with the latest diagnostic until a Refresh succeeds, after which `/health` stays 200 for the rest of the process. Keeps supervisor restart loops bounded and gives operators a queryable failure surface (`curl /health`) instead of a restart-log grep.

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
make tools             # Install pinned tools, Go modules, pnpm deps, and git hooks
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
                       # CORS=*. No auth flag: with no WH_AUTH_JWT_SECRET set,
                       # every request resolves to the policy default_role. Set
                       # WH_AUTH_JWT_SECRET=<secret> and mint a JWT (role ==
                       # admin_role) to exercise admin/elevated endpoints.
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
- `pnpm` (>= 11.1) and `Node.js` (22 LTS — pinned via `.nvmrc` at the repo root, matches CI) must be on your PATH; the SDK and E2E test harnesses both shell out to `pnpm`. `make tools` runs `pnpm install --frozen-lockfile` in `clients/ts/` and `tests/e2e/sdk/`.
- `GNU Make 4+` is required (uses `--output-sync=target`); macOS ships BSD Make 3.81 which will not parse the Makefile. See `docs/src/content/docs/development.md` § Prerequisites for the full setup checklist.
- **Worktrunk** (the team's `wt` CLI) reads `.config/wt.toml` — the project-level, committed config that applies to all teammates. On `wt switch --create <branch>`, `post-start` first runs `wt step copy-ignored` (copies gitignored caches/builds/`node_modules`/`.bin/` from the main worktree, skipping `tmp/` and `data/` runtime dirs) then `make tools` (idempotent — verifies + installs `core.hooksPath`). A fresh worktree is ready in seconds instead of waiting for `pnpm install` × 3 + golangci-lint download. Personal overrides live in `~/.config/worktrunk/config.toml`.

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

### Enforced via git hooks

`make tools` installs team-wide git hooks via `git config core.hooksPath .githooks`. They apply to humans and Claude Code alike:

- **`.githooks/pre-commit`** runs `make verify` (~30s) on every commit; blocks on failure. Also emits informational nudges for likely doc-sync / SDK-sync misses (see §Documentation Sync, §SDK Sync).
- **`.githooks/pre-push`** checks for a `tmp/ci-passed-tree-<TREE-sha>` marker — written by `make ci` on success. Blocks the push if `make ci` hasn't been run for the tree being pushed. The marker is keyed by the **tree SHA** of the working dir at the time `make ci` ran (via `git stash create`), not the commit SHA — so the standard `make ci → git add → git commit → git push` flow doesn't spuriously re-block when the post-commit tree matches what CI just validated. Editing the tree (or staging a different subset of changes than CI saw) requires a re-run.

Bypass with `git commit --no-verify` / `git push --no-verify` only when explicitly intentional (WIP / draft pushes where you accept the consequences). Don't disable the hooks globally; that defeats the gate.

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
6. **Re-request review** from humans after substantive changes. Bot reviewers re-run on `synchronize` (Gemini), via PR-comment mention (Claude), or via a re-request button (Copilot).

### What not to do

- Don't argue in circles. If the reviewer repeats the same point, escalate to a maintainer rather than looping.
- Don't resolve a thread that has an open child comment.

### Review tooling reference

| Reviewer | How it runs | Re-runs on new commits | Blocks merge |
| -------- | ----------- | ---------------------- | ------------ |
| Claude (`.github/workflows/claude-review.yml`) | Manual-only. Comment `@claude` or `/review` on the PR (trusted reviewers), or run `gh workflow run "Claude PR review" -f pr_number=<N>`. Findings post as inline review comments plus a sticky verdict-summary comment that edits in place | No — re-trigger by mention after pushing new commits | Yes for inline comments — `required_review_thread_resolution: true` blocks merge until each `claude[bot]` thread is resolved. The workflow's check itself is advisory |
| Gemini Code Assist | Marketplace App at repo level | Yes on synchronize. **Silently skips `.github/workflows/**`** (built-in exclusion, can't be overridden) — Gemini rarely sees infra PRs | No (advisory) |
| Copilot | GitHub-native, requires a reviewer with Copilot Pro | Yes if enabled | No (advisory) |
| Human admins | Review requested from a non-author admin by `housekeeping.yml` on PR open / ready-for-review (not on every push). Selection picks the other admin if the author is one, otherwise round-robins. The composite also sets `assignees`. | Not on synchronize. Manual re-request via the GitHub UI's "Re-request review" if `dismiss_stale_reviews_on_push` clears the request. | Yes — `admin-approval.yml` is a required status check that fails unless an admin has approved. Dependabot patch/minor bypasses (auto-merge handles those); major bumps fall through to admin review. |

> **Known limitation**: Gemini Code Assist silently ignores all files under `.github/workflows/**` — a hardcoded Google default that `.gemini/config.yaml`'s `ignore_patterns` can't remove. For workflow-heavy PRs, Claude review is the primary AI reviewer. Gemini still covers `CHANGELOG.md`, docs, source code, and configuration outside `.github/`.

## Branch Maintenance

### Syncing a PR branch with main

When the GitHub UI shows "This branch is out-of-date with the base branch" — or a feature branch needs to absorb upstream `main` commits — **merge, don't rebase**:

```bash
git fetch origin main
git merge origin/main --no-edit
git push
```

Force-pushes (`--force`, `--force-with-lease`) are blocked by `.claude/settings.json`'s `deny` rules and would lose inline review-thread anchors. Rebase changes commit SHAs and requires force-push, so it's wrong for the same reason. Long-lived WaveHouse branches have historically lost `pull_request` event firing (symptom: only `pull_request_target` checks appear) — the recovery is `git merge origin/main`, not close+reopen, not empty commits, not toggling draft/ready.

The `pre-push` hook will block until `make ci` re-runs after the merge (the merge commit's tree differs from any prior tree CI validated). That's the point — the merge commit itself needs CI to have passed locally.

If merge introduces conflicts: surface them to a human reviewer. Don't auto-resolve — collisions in `internal/api/router.go`, `internal/config/config.go`, or `internal/ingest/types.go` can look mechanically resolvable but break runtime behavior.

See also: `.claude/skills/pr-sync-with-main/SKILL.md` for the same workflow in Claude-Code-skill form.

## Agent PR Discipline

These rules apply to AI agents (Claude Code etc.) working on WaveHouse PRs. Humans keep the standard git/gh affordances; agents have additional gating, enforced by `.claude/hooks/agent-bash-gate.sh` (PreToolUse Bash) plus deny rules in `.claude/settings.json`.

### Drafts only

Agents must create PRs with `gh pr create --draft`. Only humans transition draft → ready-for-review (`gh pr ready` is blocked for agents). Only humans approve or request changes (`gh pr review --approve` / `--request-changes` are blocked).

### Human reviewer assignment is humans-only

Adding/removing human reviewers (`gh pr edit --add-reviewer <login>`, `gh pr edit --add-assignee <login>`, or `POST /repos/.../pulls/<N>/requested_reviewers`) is blocked for agents. The `housekeeping.yml` workflow auto-assigns the non-author admin on PR open / ready-for-review; humans handle anything else.

### Bot reviewer re-triggers go through PR comments

Agents CAN re-request bot reviewers by mentioning them in PR comments (`gh pr comment` is allowed). This bypasses the reviewer-assignment API entirely:

| Bot | Re-trigger via PR comment |
| --- | -------------------------- |
| Claude review | `@claude` or `/review` |
| Gemini Code Assist | `@gemini-code-assist` or `/gemini review` |
| CodeRabbit | `@coderabbitai review` |
| Copilot Pull Request Reviewer | No comment-mention; humans use the GitHub UI's re-request button |

### Pre-push self-review is mandatory on PR branches

Before pushing to any branch with an open PR, agents must invoke the `pre-push-reviewer` subagent in fresh context. The subagent reviews:

- The full PR diff against `main` (merge-base)
- The latest commit specifically
- All open PR comments and reviews (top-level + inline)
- CI status / failing checks
- Linked issues' acceptance criteria

The subagent's verdict is one of `ship_it`, `iterate`, or `block`. **`ship_it` requires zero findings at any severity** (`[MUST]`, `[SHOULD]`, `[MAY]` sections all empty). Anything in the findings list — including `[MAY]` — forces `iterate`. The rule is: if there's anything left to do, the PR isn't shippable. "Ship it, just do this one thing first" is iteration, not shipping.

When the subagent's response ends with the parseable line `VERDICT: ship_it`, `.claude/hooks/review-marker.sh` writes `tmp/review-passed-<HEAD-sha>` and the next `git push` succeeds. On `VERDICT: iterate` or `VERDICT: block`, no marker — the orchestrator agent **loops**: address every finding, commit, re-invoke `pre-push-reviewer` in fresh context, repeat until `ship_it`. Never push with open findings.

The orchestrator agent cannot override the subagent's system prompt (it's the fixed file content of `.claude/agents/pre-push-reviewer.md`), and the subagent runs in a clean conversation context, so it doesn't share the orchestrator's bias toward its own work.

### No bypass for agents

- `git push --no-verify` and `git commit --no-verify` are blocked at the `.claude/hooks/agent-bash-gate.sh` PreToolUse layer for agents. Humans retain `--no-verify` for explicit intentional bypass (see §"Local-First Validation").
- The obvious tool-level writes to `tmp/ci-passed-*` or `tmp/review-passed-*` are denied at the `.claude/settings.json` permission layer (`Bash(touch tmp/ci-passed:*)`, `Write(tmp/ci-passed-*)`, `Edit(tmp/ci-passed-*)`, and the review-passed equivalents). The `tmp/ci-passed-*` glob covers both legacy commit-SHA markers and the current `tmp/ci-passed-tree-<TREE>` form.
- **Markers are written exclusively by `make ci` (ci-passed, tree-keyed) and the `pre-push-reviewer` SubagentStop hook (review-passed, commit-keyed). You do not write a marker file by any other means — period.** Bash can write a file by a dozen paths and the deny list does not enumerate all of them; this is an honest-agent rule, not an adversarial gate. If you ever feel tempted to write a marker, stop: the marker is wrong-shaped if you're the one writing it. Run `make ci`, invoke the subagent, get the verdict — that's the path.

### Reviewing someone else's PR locally

For "review PR <N>" workflows, use `.claude/skills/pr-review-locally/SKILL.md`. Procedure:

```bash
wt switch pr:<N>                # worktrunk + gh CLI; or `gh pr checkout <N>` fallback
```

Then invoke `pre-push-reviewer`. Findings stay local — agents must not post comments on the PR manually. To make the bot comment on the PR remotely, fire the CI workflow:

```bash
gh workflow run "Claude PR review" -f pr_number=<N>
```

That's the canonical path (also reachable via `@claude` / `/review` in a PR comment).

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

## SDK Sync

The TypeScript SDK (`@wavehouse/sdk` in `clients/ts/`) is the canonical client and ships from this repo. When backend changes alter the public API surface, the SDK needs corresponding updates. The `pre-commit` git hook flags likely misses informationally; consult this table when deciding what to update.

| Backend change | SDK considerations |
| -------------- | ------------------ |
| New user-facing API endpoint | Add a typed client method (in `clients/ts/src/client.ts` or the relevant subsystem file: `query-builder.ts`, `pipes.ts`, `policy.ts`, `stream/`, etc.); update `docs/src/content/docs/sdk.md` |
| Change to JWT auth / role extraction | Update auth handling in `clients/ts/src/http.ts` and types in `clients/ts/src/client.ts` |
| Change to `EventMessage` / ingest event format | Update payload types in `clients/ts/src/` (some are codegen-regenerated — re-run the SDK codegen CLI) |
| New / changed structured query AST | Update `clients/ts/src/query-builder.ts` types + builder methods |
| Change to live-query aggregation classification | Update live-query helpers in `clients/ts/src/stream/` |
| Named pipes API change | Update `clients/ts/src/pipes.ts` |
| Policy / access-control change | Update `clients/ts/src/policy.ts` |
| ClickHouse schema-driven type changes | Re-run the SDK codegen CLI; commit regenerated types |

Internal-only backend changes (middleware refactors, observability internals, dedup implementation, sweeper logic, NATS plumbing) generally don't need SDK updates. The `pre-commit` hook can't tell internal-only from public-surface from staged paths alone, so it'll nudge on anything in `internal/api/`. Ignore the nudge for internal-only changes; act on it for anything user-visible.

**The decision test**: would a `@wavehouse/sdk` user's *code* need to change to take advantage of (or be compatible with) this change? If yes, SDK update needed. If no (purely internal optimization), no.

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

- JWT secret (or JWKS endpoint) must be cryptographically strong in production — the JWT middleware always runs (no enable flag), so token validation is the sole gate on elevated access
- All `/v1/*` routes run the JWT auth middleware (always on); a request with no/invalid token falls back to the policy `default_role`
- Input JSON is validated against ClickHouse schemas before processing
- ClickHouse queries are passed through directly — use appropriate access controls on ClickHouse itself
- **Dependency vulnerability scanning**: `govulncheck ./...` runs in CI on every push/PR. Dependabot (`.github/dependabot.yml`) opens weekly grouped PRs for outdated Go modules and GitHub Actions.
- **GitHub Actions supply chain**: Third-party actions are pinned to full commit SHAs with version comments (see `.github/workflows/ci.yml`, `release.yml`). New workflows must follow the same pattern — never `@main` or floating tags on third-party actions. Prefer inline bash or official `actions/*` / `github/*` actions when feasible (e.g. `pr-title.yml` is an inline check rather than a third-party action).

## Repository Automation

- **Issue triage** (`triage.yml`): GitHub Models classifies new/edited issues and applies `area/*` + `security` + `breaking-change` labels.
- **Code review** (advisory; the `Admin approval` required status check + the ruleset are the actual merge gate):
  - **Gemini Code Assist App** configured via `.gemini/styleguide.md`.
  - **Claude PR review** (`claude-review.yml`) runs only on manual trigger: `@claude` or `/review` from a trusted commenter on a PR, or `workflow_dispatch`. Gated on the HEAD commit's author or committer having ≥read permission so a comment on a fork PR can't run untrusted code with write tokens. Findings post as inline review comments (blocked by `required_review_thread_resolution`) plus a sticky verdict summary. Review-only — Claude can comment but not push. Requires the `CLAUDE_CODE_OAUTH_TOKEN` secret (`claude setup-token`).
- **Dependabot auto-merge** (`dependabot-automerge.yml`): patch/minor bumps auto-approve + auto-merge; major bumps hold for human review. CI still gates the actual merge. Patch/minor bypass `Admin approval` (the workflow + CI passing is the trust model); major bumps fall through to admin review like any human PR — this closed a hole where a bot's APPROVED review (e.g. CodeRabbit) could merge a major bump without admin involvement (see #130).

## Governance Files

- **No `CODEOWNERS`**: replaced by workflow-driven reviewer assignment + approval enforcement.
  - `admin-approval.yml` — required status check that fails unless an admin has an `APPROVED` review. Dependabot patch/minor bypasses; major bumps go through admin review.
  - `housekeeping.yml` — requests review from a non-author admin on PR open / ready-for-review via the `assign-and-request-review` composite. Task Board placement is handled by native Projects v2 workflows configured in the project UI.
- **`CLAUDE.md`** and **`.gemini/styleguide.md`**: thin pointer files to AGENTS.md. Keep those pointers short; never duplicate content.
- **`CONTRIBUTING.md`**: the Conventional Commits type list must stay in sync with the regex in `housekeeping.yml`. The title linter validates squash-merge commit messages.

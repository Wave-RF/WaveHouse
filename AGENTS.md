# AGENTS.md — AI Agent Instructions for WaveHouse

This file provides context for AI coding agents (Copilot, Cursor, Cody, Aider, etc.) working on this codebase.

## Operating Rules

The non-negotiables, ordered by how often agents miss them. Each links to its detail section — read that before acting. These override convenience: if a rule blocks you, satisfy it; don't work around it.

1. **Validate locally before every push** — run `make ci` the documented way ([§Running `make ci`](#running-make-ci-for-agents)). Don't use CI as your first feedback loop.
2. **A PR-branch push needs every pre-push reviewer satisfied** — run **`/prepush`**, which discovers the reviewers from `scripts/pre-push-reviewers.sh`, runs the ones the change needs in parallel (fresh context), skips any with nothing to do *on the record*, and loops until each it ran returns `ship_it`. Every reviewer needs a marker for HEAD — earned by a `ship_it` or a logged skip; the set is the single source of truth and grows over time (code, docs, security, …), so never hardcode it ([§Pre-push self-review](#pre-push-self-review-is-mandatory-on-pr-branches)).
3. **Every code change updates its docs + `CHANGELOG.md` in the same PR** — a code change without its doc update is incomplete ([§Documentation Sync](#documentation-sync)).
4. **Address and resolve every review finding** — substantive reply, fix it or track it in an issue, @-mention the bot, then resolve; never silently drop one ([§Review Response](#review-response)).
5. **Drafts only; valid title** — `gh pr create --draft` (never `gh pr ready`/approve); the PR **title** must pass the Conventional-Commits gate (≤ 72 chars) — check it with `scripts/lint-pr-title.sh "<title>"` before creating ([§Agent PR Discipline](#agent-pr-discipline)).
6. **Never force-push or rebase a PR branch** — to absorb upstream main, `git merge origin/main` ([§Branch Maintenance](#branch-maintenance)).
7. **Never hand-write markers or `--no-verify`** — if you're tempted, the gate is wrong-shaped for your situation; fix that instead ([§Don't bypass the gates](#dont-bypass-the-gates)).

Everything below is **reference** — architecture and design invariants, command/convention detail, and the full PR-workflow rules — consulted when relevant, not memorized. Skim [§Key Design Decisions](#key-design-decisions) before changing a core package.

## Project Overview

WaveHouse is a **schema-aware real-time API gateway for ClickHouse**, written in Go. It handles ingestion with schema validation, optional deduplication, caching, real-time streaming, query proxying, and a Dead Letter Queue. It sits entirely in front of ClickHouse as the exclusive data entry/exit point.

## Architecture (Quick Reference)

One binary:

- **`cmd/wavehouse/`** — Standalone mode (all-in-one with embedded NATS, optional Pebble dedup)

Eleven internal packages under `internal/` (plus `internal/testutil/` for shared test helpers):

- **`api/`** — Chi HTTP router, JWT/JWKS middleware, ingest/query/structured-query/SSE/schema/DLQ/policy/pipes handlers, Hub
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

The invariant index — what must stay true. Full narrative and rationale live in [`docs/src/content/docs/architecture.md`](docs/src/content/docs/architecture.md) and the cited code; the numbers are **stable** (cross-referenced from code comments and architecture.md), so preserve the named invariant when you touch its package. Items tagged **(security)** are fail-closed gates — change them only with a security review.

1. **Interface-first** — core behaviors are Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`); standalone vs. future-clustered swap implementations.
2. **Bring Your Own Schema** — users create ClickHouse tables; WaveHouse discovers them via `system.columns` and never auto-migrates.
3. **Schema-driven ingest** — `POST /v1/ingest?table={table}` takes flat JSON, validated against the discovered schema (unknown fields rejected, types/nullability enforced). No envelope.
4. **Async ingestion** — ingest returns 200 after optional dedup + MQ publish; ClickHouse writes happen later via `StartIngestWorker`. NATS full → 503 + Retry-After.
5. **Per-table batching** — the worker groups events by table and bulk-INSERTs in schema column order; each table's batch is independent.
6. **Dead Letter Queue** — failed batch inserts publish to `WAVEHOUSE_DLQ` (`dlq.<table>`), gated by `dlq.enabled`. No silent data loss.
7. **Auth: always on, fail-loud, decoupled from authz (security)** — the JWT middleware always runs (no `auth.enabled`/`dev_mode` flag); it verifies with HMAC **or** JWKS (not both), with accepted `alg` pinned to the active verifier and checked before any key is used (rejects `alg:none` and cross-family confusion). No/invalid/expired token → empty role → policy `default_role`, with the bad-token reason stashed so a denying gate returns a loud `401`, not a bare `403`. Elevated access needs a valid granted role. Detail: architecture.md § `api/` + `internal/auth`; see also #11, §Security Considerations.
8. **Optional dedup** — opt-in via `dedupe.enabled`; `dedupe.id_field` selects the JSON key.
9. **Singleflight** — `TieredCache` coalesces concurrent misses (`x/sync/singleflight`) to prevent cache stampede.
10. **Active Sweeper** — purges NATS messages that are both ACKed (written to CH) and older than the gap window; SSE gap-fill uses `DeliverByStartTime`, no in-process ring buffer.
11. **Hasura-style access control: fail-closed (security)** — `policy.IsAdmin` (role == `admin_role`, **exact case-sensitive**, default `"admin"`) is the single admin check, shared by `Evaluate`/`ResolveRole`/`Validate`/the `/v1/admin` gate/`RoleAllowed`. Empty/absent role matches nothing (no `"*"` wildcard); `Validate` rejects empty role keys; a `nil` policy (deleted) denies **everyone incl. admin** — a total lockout, so bootstrap from the policy file, never an implicit admin grant. `default_role` is the one sanctioned roleless exception (`ResolveRole` maps empty → it pre-eval); `default_role == admin_role` is permitted but dev-only and loudly warned (`policy.DefaultRoleGrantsAdmin`). Preserve when touching `internal/policy` (policy twin of #13; see #159). Detail: architecture.md § `policy/`.
12. **Structured queries** — `POST /v1/query?table={table}`: typed AST validated against schema, permission-enforced, timestamp-bucketed for cache, `DefaultMaxRows` (10,000) cap.
13. **Named query pipes: fail-closed (security)** — pre-defined SQL templates (Tinybird-style) with param binding + caching; `GET/POST /v1/pipes/{name}` sit outside `RequireAdmin`, so per-pipe `allowed_roles` is the *only* execute-path gate, via `policy.RoleAllowed`: exact allowlist membership (no `"*"`), admin always passes, empty/absent role and empty-string entries authorize nobody, and no `allowed_roles` → admin-only. Preserve and exercise via `testutil.RunRoleMatrix` / `StandardRoleMatrix` (see #159). Detail: architecture.md § `pipes/`.
14. **TypeScript SDK** — `@wavehouse/sdk`: zero-dep client, typed query builder, real-time SSE, live queries (incrementable/decomposable/poll aggregation), codegen CLI. The canonical client (see §SDK Sync).
15. **Observability invariants** — stdout always 100% (sampling is OTLP-push-only); WARN+ERROR always export at 100% (a non-configurable floor — don't expose it); gRPC OTel exporters dial lazily so an unreachable collector never blocks startup; the OTel Prometheus exporter uses a **private** `prometheus.Registry`. Preserve when touching the logger/sampler/provider. Detail: architecture.md § `observability/`.
16. **Bearer-token-only CORS posture (security)** — Bearer JWT on every request, no cookies/sessions; `corsMiddleware` deliberately **never** emits `Access-Control-Allow-Credentials` (not needed, and `*` + credentials is a spec violation browsers reject). `cors_allowed_origins` controls who can *read* responses, not cookie scope; CSRF protection is structural. Don't reintroduce cookie auth or `Allow-Credentials` without a design discussion — answers GitHub #29/#30. Code: `internal/api/router.go`.
17. **Non-fatal boot** — schema-discovery failure on boot is non-fatal: `cmd/wavehouse` records an `api.BootState`, binds `:8080`, serves 503 on `/livez`/`/readyz` with the diagnostic, and retries via `SchemaRegistry.RetryRefresh` (backoff 2s → 60s). Bounds supervisor restart loops.
18. **Health endpoints** — liveness `/livez`, readiness `/readyz` (k8s convention); `/healthz` is a permanent alias of `/livez`; `/health` + `/ready` are deprecated (removal v0.2.0, CHANGELOG #144). `/v1/health` is the SDK's content-free public ping (no ClickHouse check), a `/v1` route so it survives reverse-proxy probe-path filtering. Point k8s at `/livez`/`/readyz`, SDK/online-checks at `/v1/health`, never the deprecated aliases.

## Code Conventions

- **Go 1.26**, strict formatting (`gofumpt`, enforced by CI)
- **Structured logging** with `log/slog` (JSON handler)
- **Chi v5** for HTTP routing
- **Error handling**: Return errors, don't panic. Wrap with `fmt.Errorf("context: %w", err)`.
- **No global state**: Dependencies are passed explicitly (constructor injection).
- **Package naming**: Lowercase, single word (or abbreviated). `internal/` enforces module privacy.

## Craftsmanship

Cross-cutting habits that keep the codebase reviewable — they apply to every change, in every language.

- **Comment the *why*, not the *what*.** Add a comment only when the reason isn't obvious from the code; a line that matches the surrounding pattern needs none. Keep comments to 1–2 lines and match the file's existing density. Re-read each comment you add and cut any that merely restates the code — don't write three lines of comment for one line of code. The "what" lives in the code; the "why" usually belongs in the commit message / PR / `CHANGELOG.md`.
- **DRY — one source of truth.** Before adding logic, look for an existing helper, type, or constant to reuse; before duplicating a rule, factor it into one place every caller reads. This is the repo's standing pattern: `scripts/lint-pr-title.sh` (the PR-title rule for the local gate *and* the required CI check), `scripts/docs-prose.sh` (the docs-review scope), and `scripts/pre-push-reviewers.sh` (the pre-push reviewer set) are each *the* canonical source. Duplicated logic drifts out of sync.
- **Leave it neater than you found it — within reason.** Fix the small, safe things you touch in passing: a stale comment, an obvious typo, a misnamed local, dead code on your path. Keep such cleanups in the same spirit and size as your change so the diff stays reviewable. If a cleanup is large, risky, or you can't confidently judge it, don't fold it in — open a tracking issue instead (the same rule §Review Response applies to reviewer findings).

## Build & Test Commands

**`make help` is the source of truth — run it for the full annotated list.** The targets agents reach for:

```bash
make verify            # Static checks: Go (tidy+fmt+vulncheck+lint) + TS (Biome+tsc)
make fix               # Auto-fix everything fixable (gofumpt, goimports, lint --fix, Biome)
make test              # Go unit tests + coverage gate (alias for test-unit)
make test-integration  # Go integration tests + gate (Docker; testcontainers)
make test-e2e          # E2E SDK suite vs the cover binary + gate (Docker; testcontainers)
make test-ts           # SDK vitest unit tests + coverage + gate
make ci                # Full pre-push pipeline — run it the documented way (§Local-First Validation)
make build             # Compile → bin/wavehouse
make dev               # ClickHouse + hot-reload server on :8080 (Docker)
make deps-up           # Start ClickHouse alone — for `make dev`; NOT needed by `make ci`
make build-docs        # Production docs build → docs/dist/
```

Verbose: `V=1 make test`. Extra args: `make test ARGS="-run TestFoo"`. Build tags: `make build TAGS="foo"`.

Tooling notes (the non-obvious bits `make help` won't tell you):

- Dev tools (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `go-test-coverage`, `deadcode`, `gsa`, `goda`) are pinned in `go.mod` via `tool` directives — `go tool <name>`, no manual install.
- `golangci-lint` is pinned in the Makefile (v2.11.4), auto-installed to `.bin/` on first `make lint` — kept out of `go.mod` (its deps conflict with the main module).
- `pnpm` (≥ 11.1) + `Node 22 LTS` (`.nvmrc`, matches CI) must be on PATH; `make tools` runs one root `pnpm install --frozen-lockfile` across the three workspaces (SDK `clients/ts/`, E2E `tests/e2e/sdk/`, docs `docs/`).
- **GNU Make 4+** required (uses `--output-sync=target`); macOS BSD Make 3.81 won't parse it. Full setup: `docs/src/content/docs/development.md` § Prerequisites.
- **Lint split**: Biome owns JS/TS/JSON, markdownlint owns Markdown *style*, misspell owns spelling (all under `make lint`/`make fix`); accuracy/clarity/doc-sync is the `docs-reviewer` gate (§Docs review).
- **Worktrunk** (`wt`, `.config/wt.toml`): `wt switch --create` seeds `.bin/` + `node_modules/` from main, then runs `make tools`.

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
- **Per-suite table isolation**: Each e2e test file owns its own ClickHouse tables — `clicks_<suite>` / `events_<suite>` / `users_<suite>`, generated from `tests/e2e/sdk/tables.ts` and created by `setup.ts`. A new test file must (1) add its suite name to `SUITES` in `tables.ts` and (2) get its names via `const T = suiteTables("<suite>")`, then reference `T.clicks` etc. — never a bare `clicks`. This makes cross-file *data* contamination structurally impossible. Files still run **sequentially** (`vitest.config.ts` `maxWorkers: 1`): running them in parallel is blocked by shared *global policy* state (several files read-modify-write the single policy document; `streaming.test.ts` flips the global `default_role`), so policy-mutating tests snapshot the full policy and restore it. Dropping `maxWorkers: 1` is a deferred follow-up tracked in #214 (per-table policy storage; see `docs/src/content/docs/ingest-pipeline.md` § Deferred).

## Local-First Validation

**Validate locally before pushing. Don't use CI as your first feedback loop.** Every push consumes shared CI capacity and AI-reviewer credits, and produces visible churn for the rest of the team.

### Before every push

```bash
make ci   # Full parity with CI: parallel verify + builds + unit/SDK tests, then integration + E2E + cov
```

If `make ci` passes locally, your commit has crossed the same gates CI will run. For workflow-only changes, read the YAML diff carefully and run `actionlint` if you have it installed.

### Running `make ci` (for agents)

`make ci` is **self-contained**: the integration suite (`tests/integration/`) and the E2E orchestrator (`scripts/orchestrator/`) each boot ClickHouse via **testcontainers on random host ports**. The only prerequisite is a running **Docker daemon** — do **not** `make deps-up` or start ClickHouse first (`deps-up` is for `make dev` only).

Run it via the **background Bash tool** (`run_in_background: true`) and wait for the completion notification; the harness re-invokes you on exit, so polling the log with `tail` only burns context:

```bash
NO_COLOR=1 make ci > tmp/ci.log 2>&1
```

- **Background, never foreground** — a full run can exceed the 10-minute foreground Bash cap and be killed mid-pipeline (which then looks like a failure).
- **No `| tee` / `| tail`** — a pipe masks `make ci`'s real exit code in the completion notification, and `tail -f` never returns. Redirect to the file and nothing else.
- **`NO_COLOR=1`** keeps ANSI escapes out of the log so a failure greps cleanly (the Makefile emits raw color codes otherwise).
- Read `tmp/ci.log` only when the exit code is non-zero.

On success `make ci` writes the tree-keyed `tmp/ci-passed-tree-<TREE>` marker (see §Enforced via git hooks for the tree-keying and the commit-after-green rule; `tmp/` is gitignored, so the marker never enters the tree). That's one of the markers a PR-branch push requires — the rest are written by the mandatory review subagents, one per reviewer in `scripts/pre-push-reviewers.sh` (see §Agent PR Discipline → Pre-push self-review). End to end: `make ci` green → commit → run every pre-push reviewer in parallel (fresh context) via `/prepush` → loop until each reaches `ship_it` → push. Re-run `make ci` only if a finding makes you edit a tracked file.

### Enforced via git hooks

`make tools` installs team-wide git hooks via `git config core.hooksPath .githooks`. They apply to humans and Claude Code alike:

- **`.githooks/pre-commit`** runs `make verify` (~30s) on every commit; blocks on failure. Skipped if `make ci` or `make verify` already ran for the current tree state (cached marker). See §Documentation Sync / §SDK Sync for what to update by hand.
- **`.githooks/pre-push`** checks for `tmp/ci-passed-tree-<TREE-sha>` written by `make ci`. Tree-keyed (not commit-keyed) so `make ci → commit → push` works without a re-run when the tree is unchanged. Editing the tree (or staging a different subset than CI saw) requires a re-run. `make ci` skips the marker write entirely when `$CI` is set (CI runners don't push).

Bypass with `git commit --no-verify` / `git push --no-verify` only when explicitly intentional (WIP / draft pushes where you accept the consequences). Don't disable the hooks globally; that defeats the gate.

### If local passes but CI fails

Treat as environment mismatch first, test bug second. Reproduce the failure locally (`go test -race -run TestFoo ./...`); if it passes, try concurrent copies to simulate runner contention; only then look at the runner itself. Masking environment issues with longer timeouts compounds — today's 5s bump becomes tomorrow's 30s bump.

When delegating to a subagent: tell them explicitly *"run locally first."* Agents default to "commit and let CI run" because it looks like progress.

## Review Response

Every review comment gets a substantive reply and is addressed — fixed, or tracked in an issue — and every thread gets resolved before merge. The `main branch protection` ruleset enforces `required_review_thread_resolution: true`, so unresolved threads block merge. Applies to human reviewers and AI reviewers alike (CodeRabbit, Copilot).

### What to do

1. **Decide**: accept, push back, or defer (right but out-of-scope).
2. **Reply substantively** with the fix's commit SHA or your reasoning. No bare "fixed" / "LGTM" / "good catch".
3. **@mention the bot you're replying to** (except Copilot), on its own line below your signature trailer:
   - CodeRabbit: `@coderabbitai` re-engages on the thread
   - Copilot: no mention works — note the re-request-review button

   Without the mention, the bot never sees the reply and the dialog silently terminates.
4. **Address it — never silently drop it.** If the suggestion is right and in scope, fix it in this PR. If it's a small, safe improvement that's valid but tangential to your PR, fix it anyway — cooperatively leaving the tree neater helps the whole team (§Craftsmanship). If it's valid but too large or risky to do here, or you can't confidently judge its validity, open a tracking issue and link it in your reply before resolving — *unless* an existing issue already tracks it, in which case link that one. This applies to findings from every reviewer, human or bot, including ones outside the lane of whichever reviewer raised them.
5. **Resolve the thread** once the reply addresses the concern and no counter-reply is pending. Bot threads are safe to resolve after a substantive reply (bots only re-engage on mention); human threads — wait for them.
6. **Re-request review** from humans after substantive changes. Bot reviewers re-run via their own triggers — CodeRabbit on `@coderabbitai review`, Copilot via the re-request button.

### What not to do

- Don't argue in circles. If the reviewer repeats the same point, escalate to a maintainer rather than looping.
- Don't resolve a thread that has an open child comment.

### Review tooling reference

| Reviewer | How it runs | Re-runs on new commits | Blocks merge |
| -------- | ----------- | ---------------------- | ------------ |
| CodeRabbit | Marketplace App at repo level. Auto-reviews on open + push; re-trigger by commenting `@coderabbitai review` | Yes, on push | Inline findings post as review threads — `required_review_thread_resolution: true` blocks merge until each is resolved. Its own check is advisory |
| Copilot | GitHub-native, requires a reviewer with Copilot Pro | Yes if enabled | No (advisory) |
| Human admins | Review requested from a non-author admin by `housekeeping.yml` on PR open / ready-for-review (not on every push). Selection picks the other admin if the author is one, otherwise round-robins. The composite also sets `assignees`. | Not on synchronize. Manual re-request via the GitHub UI's "Re-request review" if `dismiss_stale_reviews_on_push` clears the request. | Yes — `admin-approval.yml` is a required status check that fails unless an admin has approved. Dependabot patch/minor bypasses (auto-merge handles those); major bumps fall through to admin review. |

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

Agents follow the same universal git hooks as humans (pre-commit + pre-push in `.githooks/`). On top of that, four PR-workflow rules have no human analog and are checked by `.claude/hooks/agent-bash-gate.sh`. The gate is a *guard rail against accidents*, not adversarial enforcement — an agent that wants to bypass can edit the gate itself. The rules below are policy; follow them.

### Drafts only

Agents must create PRs with `gh pr create --draft`. Only humans transition draft → ready-for-review (`gh pr ready` is blocked for agents). Only humans approve or request changes (`gh pr review --approve` / `--request-changes` are blocked).

**PR title format.** The title becomes the squash-merge subject on `main` and is gated by the required `PR housekeeping` check — a bad title blocks merge, so don't discover it from CI. It must be Conventional Commits — `<type>(optional-scope)(optional-!): <subject>` — **≤ 72 chars**, subject **lowercase-first** with **no trailing period**. Types: `feat fix docs refactor test chore ci deps build perf revert style`. Validate before creating: `scripts/lint-pr-title.sh "<title>"` (exit 0 = valid; it prints the reason on failure). `.claude/hooks/agent-bash-gate.sh` runs the same check on `gh pr create` / `gh pr edit --title`, so a malformed title is caught locally before the PR exists. The rule has a single source of truth — `scripts/lint-pr-title.sh` — used by **both** this local gate and the required `PR housekeeping` check (`.github/workflows/housekeeping.yml` calls the same script), so local and CI never drift.

### Human reviewer assignment is humans-only

Adding/removing human reviewers (`gh pr edit --add-reviewer <login>`, `gh pr edit --add-assignee <login>`, or `POST /repos/.../pulls/<N>/requested_reviewers`) is blocked for agents. The `housekeeping.yml` workflow auto-assigns the non-author admin on PR open / ready-for-review; humans handle anything else.

### Bot reviewer re-triggers go through PR comments

Agents CAN re-request bot reviewers by mentioning them in PR comments (`gh pr comment` is allowed). This bypasses the reviewer-assignment API entirely:

| Bot | Re-trigger via PR comment |
| --- | -------------------------- |
| CodeRabbit | `@coderabbitai review` |
| Copilot Pull Request Reviewer | No comment-mention; humans use the GitHub UI's re-request button |

### Pre-push self-review is mandatory on PR branches

Before pushing a non-main branch, **every** review subagent listed in `scripts/pre-push-reviewers.sh` must end with a marker for HEAD — earned either by **running** it in fresh context (the default) or by **deliberately skipping** it when it's genuinely out of lane for this diff (a *logged* skip; see below). That list is the single source of truth (don't assume how many there are — read it). **The one-command form is `/prepush`**, which reads the list, judges which reviewers the change actually needs, runs those in parallel, skips the rest on the record, and loops the ones it ran to `ship_it`; prefer it over invoking reviewers by hand. (The push gate fires on any non-main branch with commits ahead of `main`, **not** only when a PR is already open — the first push, before `gh pr create`, is exactly when the diff most needs review.)

Today the list holds two reviewers; it's designed to grow (security is the obvious next one):

- **`pre-push-reviewer`** (code) reviews: the full PR diff against `main` (merge-base); the latest commit specifically; all open PR comments and reviews (top-level + inline); CI status / failing checks; linked issues' acceptance criteria.
- **`docs-reviewer`** (docs) reviews: docs prose for accuracy-vs-code, runnable examples, clarity, and completeness, **plus code↔docs sync** — code that changed but whose docs didn't (per §Documentation Sync). It runs on **every** push, even code-only ones: docs may not change but *should*, and catching that is the point. (See §Docs review for scope.)

Each subagent's verdict is one of `ship_it`, `iterate`, or `block`. **`ship_it` requires zero findings at any severity** (`[MUST]`, `[SHOULD]`, `[MAY]` sections all empty). Anything in the findings list — including `[MAY]` — forces `iterate`. The rule is: if there's anything left to do, the PR isn't shippable. "Ship it, just do this one thing first" is iteration, not shipping. **Every** listed reviewer must reach `ship_it`, and you address findings from all of them (§Review Response) — not just the ones in whichever reviewer's lane you expected.

When a reviewer you ran ends with the parseable line `VERDICT: ship_it`, `.claude/hooks/review-marker.sh` writes its marker — `tmp/<name>-passed-<HEAD-sha>`, derived from the reviewer's name (so `pre-push-reviewer` → `tmp/pre-push-reviewer-passed-…`, `docs-reviewer` → `tmp/docs-reviewer-passed-…`). A reviewer you **skip** instead earns the same marker via `scripts/skip-pre-push-review.sh <name> "<reason>"`, which also appends the reason to `tmp/review-skips-<HEAD>.log` (the push gate echoes these skips for the record). `git push` succeeds only when a marker exists for HEAD from **every** listed reviewer — run *or* skipped. On `VERDICT: iterate` or `VERDICT: block`, that reviewer writes no marker — the orchestrator agent **loops**: address every finding, commit, re-invoke the reviewer(s) in fresh context, repeat until all say `ship_it`. Never push with open findings.

The orchestrator agent cannot override any subagent's system prompt (the fixed file content of `.claude/agents/<name>.md`), and each runs in a clean conversation context, so they don't share the orchestrator's bias toward its own work.

**Skipping is your judgment, on the record.** Skip a reviewer only when you're confident it has nothing to do with *this* diff — the code reviewer on a docs-only typo, the docs reviewer on a test-only change. Bias to running; when unsure, run it (or `/prepush all` to force the full set). `skip-pre-push-review.sh` prints a ⚠️ when a skip looks wrong (e.g. skipping docs review while docs files changed) — heed it. This is a deliberate trust trade-off: a careless skip is exactly the failure the fresh-context reviewers exist to catch, so don't skip a change that deserves a look just to save minutes. The detailed run/skip rules of thumb live in `/prepush`.

### Adding a pre-push reviewer

The reviewer set is meant to grow. To add one — with **no** edits to the hooks, which read the list at push time:

1. **Write the subagent** at `.claude/agents/<name>.md` (frontmatter `name`/`description`/`tools`/`model`; body is its system prompt). End its output with the parseable `VERDICT: ship_it|iterate|block` line under the same strict rubric as the others (zero findings ⇒ `ship_it`). Model it on `pre-push-reviewer.md` / `docs-reviewer.md`.
2. **Add `<name>`** to `scripts/pre-push-reviewers.sh` — *after* step 1, because a name with no agent file blocks every push until the agent exists.

That's all: the marker is `tmp/<name>-passed-<HEAD>` automatically, the push gate requires it, `review-marker.sh` writes it on `ship_it`, `/prepush` launches it alongside the rest, and the missing-reviewer nudge covers it. Also add a row to the subagent table in `docs/src/content/docs/claude-code.md`.

### Don't bypass the gates

- `--no-verify` on `git commit` / `git push` exists for human WIP / draft pushes. Agents should not use it.
- Markers are written by tooling, never by hand: `tmp/ci-passed-tree-*` by `make ci`; `tmp/<reviewer>-passed-*` (one per reviewer in `scripts/pre-push-reviewers.sh`) by the `review-marker.sh` SubagentStop hook on `ship_it`, **or** by `scripts/skip-pre-push-review.sh` for a deliberately-skipped reviewer (which logs the reason to `tmp/review-skips-<HEAD>.log`). Don't `touch` / `Write` / `Edit` a marker by hand — to skip a reviewer, use the skip command so the skip is recorded; if you're tempted to hand-write a review marker any other way, the marker is wrong-shaped for your situation. Run `make ci`, run or skip each reviewer, get the verdicts.

These are policy, not mechanically enforced. Bash can write a file a dozen ways; an agent can edit `.claude/hooks/agent-bash-gate.sh` itself. Trust beats whack-a-mole regex.

### Reviewing someone else's PR locally

For "review PR <N>" workflows, use `.claude/skills/pr-review-locally/SKILL.md`. Procedure:

```bash
wt switch pr:<N>                # worktrunk + gh CLI; or `gh pr checkout <N>` fallback
```

Then run the reviewers relevant to the PR's diff (the same set from `scripts/pre-push-reviewers.sh`, judged per the diff — `pr-review-locally` launches them in parallel in fresh context). Findings stay local — agents must not post comments on the PR manually; surface them to the user, who decides what to act on. (No markers or skips here — that's an audit of someone else's branch, not your push.)

### Docs review

Documentation *prose* — accuracy against the code, runnable examples, clarity, completeness — **and code↔docs sync** (code that changed but whose docs didn't) are reviewed by the **`docs-reviewer`** subagent, not the code-focused `pre-push-reviewer`. The canonical rubric is `.github/prompts/docs-review.md`. It complements the deterministic prose tools — misspell, markdownlint, starlight-links-validator — reviewing only what they can't, and it never edits docs or posts PR comments.

**Scope** is the canonical docs-prose set from `scripts/docs-prose.sh` — a *denylist*: every tracked `.md`/`.mdx` EXCEPT `.claude/**`, `.github/**`, `CHANGELOG.md`, `AGENTS.md`, `CLAUDE.md`, `*.draft.md`/`*.old.md`, `PERF-CLAIMS-REVIEW.md`. So it covers the Starlight site under `docs/src/content/` **and** the governance docs (`README.md`, the SDK readme `clients/ts/README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `SUPPORT.md`) — new docs are picked up automatically. `CODE_OF_CONDUCT.md`/`SUPPORT.md` are deep-reviewed only on change or material suspicion.

**It is a hard pre-push gate**, run in parallel with the other pre-push reviewers (see §Pre-push self-review). Invoked with the **default (branch) scope** it emits a `VERDICT:` line; on `ship_it` the `review-marker.sh` SubagentStop hook writes `tmp/docs-reviewer-passed-<HEAD-sha>`, which the push gate requires — unconditionally, on every PR-branch push (even code-only ones). Run it via **`/docs-review`**; with **no arg** that's the gating review (branch scope), while an explicit **path/glob** or **`all`** is **advisory** (no `VERDICT:`, no marker) for ad-hoc audits. The whole dev team runs Claude Code and this command is tracked in-repo, so everyone runs it themselves; there is intentionally **no PR/cloud path** for docs review.

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
- `EventMessage` JSON tags ↔ `docs/src/content/docs/api.md` event format, SSE examples, ClickHouse INSERT columns
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

Internal-only backend changes (middleware refactors, observability internals, dedup implementation, sweeper logic, NATS plumbing) generally don't need SDK updates. Use judgement — table above is the source of truth; nothing automated nudges you.

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
tests/e2e/              → E2E test stack (scripts/orchestrator boots a ClickHouse testcontainer + the wavehouse-cov binary)
tests/e2e/fixtures/     → Idempotent ClickHouse DDL scripts for test tables
tests/e2e/sdk/          → E2E integration tests via TypeScript SDK (Vitest)
deployments/compose/    → Docker Compose files (standalone.yaml, dependencies.yaml)
deployments/Dockerfile  → Runtime image (+ Dockerfile.goreleaser for release builds)
docs/                   → Project documentation
.vscode/                → Workspace settings (gopls build flags, recommended extensions)
```

## Security Considerations

- JWT secret (or JWKS endpoint) must be cryptographically strong in production — the JWT middleware always runs (no enable flag), so token validation is the sole gate on elevated access
- All `/v1/*` routes run the JWT auth middleware (always on); a request with no/invalid token falls back to the policy `default_role`
- Input JSON is validated against ClickHouse schemas before processing
- ClickHouse queries are passed through directly — use appropriate access controls on ClickHouse itself
- **Dependency vulnerability scanning**: `govulncheck ./...` runs in CI on every push/PR. Dependabot (`.github/dependabot.yml`) opens weekly grouped PRs for outdated Go modules and GitHub Actions.
- **GitHub Actions supply chain**: Third-party actions are pinned to full commit SHAs with version comments (see `.github/workflows/ci.yml`, `release.yml`). New workflows must follow the same pattern — never `@main` or floating tags on third-party actions. Prefer inline bash or official `actions/*` / `github/*` actions when feasible (e.g. the PR-title check in `housekeeping.yml` is inline bash calling `scripts/lint-pr-title.sh`, not a third-party action).

## Repository Automation

- **Issue triage** (`triage.yml`): GitHub Models classifies new/edited issues and applies `area/*` + `security` + `breaking-change` labels.
- **Code review** (advisory; the `Admin approval` required status check + the ruleset are the actual merge gate): handled by external marketplace apps (CodeRabbit, Copilot) configured at the org/repo level, not by in-repo workflows. Inline findings post as review threads that `required_review_thread_resolution: true` blocks merge on until resolved.
- **Dependabot auto-merge** (`dependabot-automerge.yml`): patch/minor bumps auto-approve + auto-merge; major bumps hold for human review. CI still gates the actual merge. Patch/minor bypass `Admin approval` (the workflow + CI passing is the trust model); major bumps fall through to admin review like any human PR — this closed a hole where a bot's APPROVED review (e.g. CodeRabbit) could merge a major bump without admin involvement (see #130).
- **Docs site deploy** (`wavehouse.dev`): a tail step of the CI job (`.github/workflows/ci.yml`), **not** Cloudflare's Workers Builds. Workers Builds can't build this site — `rehype-mermaid` renders diagrams to themed SVG at build time via headless Chromium, and the Workers Builds image has no browser (and no root to apt-install one). `make ci` already builds `docs/dist/` on a runner with a cached Chromium, so the deploy reuses that artifact and runs only once the whole pipeline is green: push to `main` runs `wrangler deploy` (production → `wavehouse.dev`); PR branches run `wrangler versions upload`, publishing a per-version preview at `<version-prefix>-wavehouse-docs.wave-rf.workers.dev` posted as a sticky PR comment. Deploys are skipped when no docs-affecting files changed and on fork PRs. **Requires `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` repo secrets**, and Cloudflare Workers Builds must stay **disconnected** from the `wavehouse-docs` Worker (else it double-deploys and fails the browser-dependent build on every push). Wrangler config (custom domain, observability, source maps, preview URLs) lives in `docs/wrangler.jsonc`. The worker (`docs/worker/index.ts`, delegating to `cloudflare-md-router`) deploys alongside the static assets so `Accept: text/markdown` content negotiation works in production.

## Governance Files

- **No `CODEOWNERS`**: replaced by workflow-driven reviewer assignment + approval enforcement.
  - `admin-approval.yml` — required status check that fails unless an admin has an `APPROVED` review. Dependabot patch/minor bypasses; major bumps go through admin review.
  - `housekeeping.yml` — requests review from a non-author admin on PR open / ready-for-review via the `assign-and-request-review` composite. Task Board placement is handled by native Projects v2 workflows configured in the project UI.
- **`CLAUDE.md`**: a thin pointer file to AGENTS.md. Keep the pointer short; never duplicate content.
- **`CONTRIBUTING.md`**: the Conventional Commits type list must stay in sync with the regex in `housekeeping.yml`. The title linter validates squash-merge commit messages.
- **`SUPPORT.md`** (alpha-stage public triage policy): the externally-promised cadence is **best-effort, 1–2 business days for an initial response** on bugs / features / usage questions; **security reports are prioritized** with the 48-hour acknowledge / 5-business-day initial-assessment targets in `SECURITY.md`. Usage questions ("how do I…") are routed to [GitHub Discussions → Q&A](https://github.com/Wave-RF/WaveHouse/discussions/categories/q-a) — do not file them as bug-report Issues; bug-reporters who use the wrong template get redirected. There is no Discord/Slack. Don't quietly let threads slip — if one sits longer than a week, that's a miss. **Out-of-scope items publicly stated in `SUPPORT.md` are only "Older releases" and "Non-ClickHouse backends"**. When tweaking the policy, update `SUPPORT.md` first and keep this paragraph in sync. The docs footer (`docs/src/components/Footer.astro`) and sidebar (`docs/src/config/sidebar.ts`) cross-link Discussions, `SUPPORT.md`, and `SECURITY.md` so they're one click from anywhere on `wavehouse.dev`; `README.md`, `CONTRIBUTING.md`, and both issue templates (`.github/ISSUE_TEMPLATE/bug_report.md`, `feature_request.md`) also link out — change those together if the policy moves.

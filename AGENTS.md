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

Fourteen internal packages under `internal/` (plus `internal/testutil/` for shared test helpers):

- **`api/`** — Chi HTTP router, JWT/JWKS middleware (from `auth/`), ingest/query/structured-query/SSE/schema/DLQ/policy/pipes handlers
- **`auth/`** — JWT auth middleware: HMAC **or** JWKS verification with `alg` pinned to the active verifier, role extraction from a configurable claim path; always runs, never rejects (bad token → empty role + stashed reason)
- **`cache/`** — `Cache` interface → `LocalCache` (Ristretto) + `SharedCache` (TBD) + `TieredCache` (singleflight)
- **`chsql/`** — dependency-free ClickHouse SQL helpers shared by `query`/`policy` (avoids an import cycle): `QuoteIdent` (backtick-quote every identifier) + `BindUnsafe` (reject names with a literal `?`)
- **`config/`** — YAML + env var config loading (cleanenv)
- **`dedupe/`** — `Deduplicator` interface → `Embedded` (Pebble) — optional, controlled by `dedupe.enabled`
- **`discovery/`** — `SchemaRegistry` that introspects ClickHouse `system.columns` + `Validate()` for ingest payloads + `CanonicalizeTimestamps()` rewriting top-level `DateTime`/`DateTime64` column values to the canonical RFC 3339 UTC wire form pre-publish (Key Design Decision #19)
- **`ingest/`** — Ingest worker pipeline (`worker.go`: JetStream input → per-table batch INSERT with DLQ output). The pipeline is **insert-only**. The wire format `EventMessage` (`types.go`) carries `{table_name, scope, received_timestamp, data}` and nothing else; the worker accepts whatever table name the envelope carries (table existence was already checked by the HTTP ingest handler, which `404`s an unknown table before publish; the worker doesn't re-validate), then bulk-INSERTs. In the embedded-NATS deployment (the default), the server runs with `DontListen: true` (`internal/mq/embedded.go`), so the only Publishers reachable on the `ingest.>` subjects are in-process Go code — today, only the HTTP `/v1/ingest?table={table}` handler. Non-insert mutations (`DELETE`/`UPDATE`/`TRUNCATE`/…) must go through `POST /v1/ops/query` under the admin role (the same `RequireAdmin` gate as the rest of `/v1/ops/*`), so non-admin callers never reach the proxy. A request with no token (or an invalid one) resolves to the `default_role`, which in a production config is not the admin role (setting them equal is a loudly-warned dev-only setting), so it can't reach this endpoint. Plus `Sweeper` (Active Sweeper for NATS message lifecycle) + `EventMessage`/`BufferConsumerName` types (`types.go`)
- **`mq/`** — `Publisher`/`Subscriber` interfaces → `EmbeddedNATS` + `RemoteNATS`
- **`observability/`** — OpenTelemetry pipeline: `InitProvider` wires trace/metric/log providers via OTLP gRPC (each signal independently gated). A top-level `Prometheus` config block drives an optional `/metrics` scrape endpoint that runs independently of OTLP push — standalone (Alloy/Mimir scrape, no collector), alongside OTLP, or off. `NewLogger` produces a slog handler that fans out to stdout AND OTLP (stdout always 100%, OTLP sample-rate-aware). `TraceHandler` injects trace_id/span_id from active spans. `tracer.go` provides W3C trace context propagation over NATS headers.
- **`pipes/`** — Named query pipes: `NamedQuery` type + NATS KV store (`WAVEHOUSE_PIPES`) + `.sql` file bootstrap
- **`policy/`** — Hasura-style access control: `Policy`/`TablePolicy`/`RolePermissions` types, `Evaluate()` engine with JWT claim templating, NATS KV store (`WAVEHOUSE_POLICY`)
- **`query/`** — Structured query AST types + SQL builder with schema validation, structural policy predicate/limit emission, timestamp bucketing
- **`stream/`** — SSE fan-out: the event `Hub` (registers subscribers by `(topic, role)`; `Broadcast` projects + serializes each event once per role, the #294 delivery hot path — a role carrying a row-level `filter` keeps the shared projection but delivers per subscriber, each subscriber's claims evaluated against the row, #319), `Subscriber` (per-connection outbound `Frame` queue, `Send`/`Frames`; claims fixed at construction, immutable), the `Bucket` fan-out set (`subscriberSet`, one per `(topic, role)`), the `Heartbeater` keepalive wheel, and `Metrics` (the `wavehouse_sse_*` stream instruments)

## Key Design Decisions

The invariant index — what must stay true. Full narrative and rationale live in [`docs/src/content/docs/architecture.md`](docs/src/content/docs/architecture.md) and the cited code; the numbers are **stable** (cross-referenced from code comments and architecture.md), so preserve the named invariant when you touch its package. Items tagged **(security)** are fail-closed gates — change them only with a security review.

1. **Interface-first** — core behaviors are Go interfaces (`Cache`, `Deduplicator`, `Publisher`, `Subscriber`); standalone vs. future-clustered swap implementations.
2. **Bring Your Own Schema** — users create ClickHouse tables; WaveHouse discovers them via `system.columns` and never auto-migrates.
3. **Schema-driven ingest** — `POST /v1/ingest?table={table}` takes flat JSON, validated against the discovered schema (unknown fields rejected, types/nullability enforced). No envelope.
4. **Async ingestion** — ingest returns 200 after optional dedup + MQ publish; ClickHouse writes happen later via `StartIngestWorker`. NATS full → 503 + Retry-After.
5. **Per-table batching** — the worker groups events by table and bulk-INSERTs in schema column order; each table's batch is independent.
6. **Dead Letter Queue** — failed batch inserts publish to `WAVEHOUSE_DLQ` (`dlq.<table>`), gated by `dlq.enabled`. No silent data loss.
7. **Auth: always on, fail-loud, decoupled from authz (security)** — the JWT middleware always runs (no `auth.enabled`/`dev_mode` flag); it verifies with HMAC **or** JWKS (not both), with accepted `alg` pinned to the active verifier and checked before any key is used (rejects `alg:none` and cross-family confusion). No/invalid/expired token → empty role → policy `default_role`, with the bad-token reason stashed so a denying gate returns a loud `401`, not a bare `403`. Elevated access needs a valid granted role. **Sanctioned exception:** a configured non-JWT operator key (`auth.operator_key`; presented via `Authorization: Operator <key>` or the `X-Operator-Key` alias) deliberately couples authN+authZ — a constant-time match authorizes a full-access platform operator (stamps the admin role plus an operator bit) independent of the verifier (see #11). Detail: architecture.md § `api/` + `internal/auth`; see also #11, §Security Considerations.
8. **Optional dedup** — opt-in via `dedupe.enabled`; `dedupe.id_field` selects the JSON key.
9. **Singleflight** — `TieredCache` coalesces concurrent misses (`x/sync/singleflight`) to prevent cache stampede.
10. **Active Sweeper** — purges NATS messages that are both ACKed (written to CH) and older than the gap window; SSE gap-fill uses `DeliverByStartTime`, no in-process ring buffer.
11. **Hasura-style access control: fail-closed (security)** — `policy.IsAdmin` (role == `admin_role`, **exact case-sensitive**, default `"admin"`) is the single admin check, shared by `Evaluate`/`ResolveRole`/`Validate`/the `/v1/ops` gate/`RoleAllowed`. Empty/absent role matches nothing (no `"*"` wildcard); `Validate` rejects empty role keys; a `nil` policy (deleted) denies **everyone incl. admin** via a role — a total lockout for token-based callers, so bootstrap from the policy file, never an implicit admin grant (**exception:** the operator key's `auth.IsOperator` bit passes the `/v1/ops` gate even under a `nil` policy — a deliberate break-glass restore over HTTP, see #7). `default_role` is the one sanctioned roleless exception (`ResolveRole` maps empty → it pre-eval); `default_role == admin_role` is permitted but dev-only and loudly warned (`policy.DefaultRoleGrantsAdmin`). Preserve when touching `internal/policy` (policy twin of #13; see #159). Detail: architecture.md § `policy/`.
12. **Structured queries: column authz fail-closed (security)** — `POST /v1/query?table={table}`: typed AST validated against schema, permission-enforced, timestamp-bucketed for cache, `DefaultMaxRows` (10,000) cap. Every column reference — projection, aggregation args, `filters`, `group_by`, `order_by`, `time_range` — is authorized inside `query.Build` (the single chokepoint that enumerates them all), so no clause can skip the role's `allow_columns`/`deny_columns` check (#223). A `select_all` read by a *column-restricted* role expands to its allowed columns via `policy.AllowedProjection`, never a bare `SELECT *`; *unrestricted*/admin roles keep `SELECT *` (`policy.RestrictsColumns` decides). Omitting `columns` selects nothing (`ErrEmptyProjection` → `200 []`); `["*"]` is the literal column `*` (schema-gated, not a wildcard); a table-granted role with no readable columns fails closed (`ErrNoReadableColumns` → `403`). Structured and live-stream (`stream.filterColumns`) reads share the one per-column decision `policy.IsColumnAllowed`, so column visibility can't drift. Row visibility has the same one-source guarantee (#319): `Evaluate` resolves a role's row-`filter` once (`resolvePredicates`), and both surfaces consume that single resolution — the query path renders it to SQL (`predicatesToSQL`), the stream evaluates it in memory per subscriber (`ResolvedPermissions.RowVisible`, whose type-aware comparison fails closed on anything it can't prove about the ingested payload — `policy.ColumnSpec`, with `DateTime`/`DateTime64` operands compared as instants through the ingest grammar (`discovery.Column.TimeParser`) and claim constants rendered canonically and digit-exact by the one shared rule `policy.CanonicalScalar` (#457 — which also refuses a float64 at/past 2^53 rather than match a neighboring ID, and whose ok=false — an absent claim, a structured value, no canonical form — makes the predicate match no rows on BOTH surfaces: `1 = 0` in SQL, every row withheld in memory); numeric comparison runs in the column's STORAGE domain (`policy.NumericSpec`, classified by `discovery.NumericStorageOf` — Float width rounding, Decimal scale truncation, integer exactness, both operands narrowed as ClickHouse narrows stored value and bound constant, out-of-range operands refused rather than modeled; the `tests/integration` differential oracle holds in-range verdicts equal to a live ClickHouse's and the never-admit-where-SQL-hides direction for the refused out-of-range ones); an event whose insert later fails into the DLQ is the one residual payload-vs-stored asymmetry, documented in the access-control enforcement caution) — so row visibility can't drift either. Preserve when touching `internal/query` or the structured-query handler. Detail: architecture.md § `query/`.
13. **Named query pipes: fail-closed (security)** — pre-defined SQL templates (Tinybird-style) with param binding + caching; `GET/POST /v1/pipes/{name}` sit outside `RequireAdmin`, so per-pipe `allowed_roles` is the *only* execute-path gate, via `policy.RoleAllowed`: exact allowlist membership (no `"*"`), admin always passes, empty/absent role and empty-string entries authorize nobody, and no `allowed_roles` → admin-only. Preserve and exercise via `testutil.RunRoleMatrix` / `StandardRoleMatrix` (see #159). Detail: architecture.md § `pipes/`.
14. **Client SDKs** — TypeScript (`@wavehouse/sdk` in `clients/ts/`) and Go (`github.com/Wave-RF/WaveHouse/clients/go`, package `wavehouse`, in `clients/go/`) are both canonical, officially supported clients with full API-tree parity. Each ships a typed query builder, real-time SSE over header-authenticated HTTP, live queries (incrementable/decomposable/poll aggregation), and a codegen CLI. TypeScript has exactly one runtime dependency — `eventsource-parser` (SSE framing, itself dependency-free); adding a second needs the same scrutiny the first got. Go has zero third-party runtime dependencies (stdlib-only, hand-rolled SSE framing). See §SDK Sync.
15. **Observability invariants** — stdout always 100% (sampling is OTLP-push-only); WARN+ERROR always export at 100% (a non-configurable floor — don't expose it); gRPC OTel exporters dial lazily so an unreachable collector never blocks startup; the OTel Prometheus exporter uses a **private** `prometheus.Registry`. The OTLP endpoint/TLS/custom-CA/mTLS/headers are delegated to the OpenTelemetry SDK's standard `OTEL_EXPORTER_OTLP_*` env vars — `InitProvider` passes **no** endpoint/header options. Known gap, intentionally not patched in WaveHouse app code: the pinned gRPC logs exporter (`otlploggrpc` v0.19/v0.20) ignores the env TLS-cert vars, so a custom/private CA and mutual TLS apply to traces/metrics but **not** the logs signal (public-CA/system-roots TLS and plaintext still work for logs) — upstream bug open-telemetry/opentelemetry-go#6661. A malformed `OTEL_EXPORTER_OTLP_HEADERS` is logged and skipped by the SDK (fail-soft), not fatal. Preserve when touching the logger/sampler/provider. Detail: architecture.md § `observability/`.
16. **Bearer-token-only CORS posture (security)** — Bearer JWT on every request, no cookies/sessions; `corsMiddleware` deliberately **never** emits `Access-Control-Allow-Credentials` (not needed, and `*` + credentials is a spec violation browsers reject). `cors_allowed_origins` controls who can *read* responses, not cookie scope; CSRF protection is structural. Don't reintroduce cookie auth or `Allow-Credentials` without a design discussion — answers GitHub #29/#30. Code: `internal/api/router.go`.
17. **Non-fatal boot** — schema-discovery failure on boot is non-fatal: `cmd/wavehouse` records an `api.BootState`, binds `:8080`, serves 503 on `/livez`/`/readyz` with the diagnostic, and retries via `SchemaRegistry.RetryRefresh` (backoff 2s → 60s). Bounds supervisor restart loops.
18. **Health endpoints** — liveness `/livez`, readiness `/readyz` (k8s convention); `/healthz` is a permanent alias of `/livez`; `/health` + `/ready` are deprecated (removal v0.2.0, CHANGELOG #144). `/v1/health` is the SDK's content-free public ping (no ClickHouse check), a `/v1` route so it survives reverse-proxy probe-path filtering. Point k8s at `/livez`/`/readyz`, SDK/online-checks at `/v1/health`, never the deprecated aliases.
19. **Canonical timestamp wire form (fail-open at ingest)** — the HTTP ingest handler rewrites every top-level `DateTime`/`DateTime64` column value it can parse to RFC 3339 UTC (`discovery.CanonicalizeTimestamps`; per-column precision + zone precomputed at schema refresh) after validation + policy checks and **before** the NATS publish, so the one payload every consumer shares — SSE subscribers, the ClickHouse insert, the DLQ — carries the same spelling `/v1/query` renders: live and query reads can't drift on the instant (#372). Zone-less inputs are read in the column's declared zone, else the discovered server default — ClickHouse's own rule, so the spelling changes but never the instant. Deliberately **fail-open**: an unparseable value or unresolvable zone (no tzdata embedded — never a failed refresh, never a silent UTC reinterpretation, which would move instants) publishes verbatim; ingest must not reject a record over its timestamp spelling — fail-closed enforcement belongs to the stream row-filter (#381). Don't re-spell timestamps downstream. Preserve when touching `internal/discovery`, the ingest handler, or the SSE fan-out. Detail: architecture.md § `discovery/` + §Ingest Path; the exact spelling spec (truncation, zero-trimming, `Z`-only) lives in api.md §Timestamp canonicalization — keep it in sync with `canonicalTimestamp`.

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
make dev-docs          # Prod-faithful docs dev loop: rebuild-on-save + wrangler dev on :4321 (next free port if busy)
make build-docs        # Production build → docs/dist/
make preview-docs      # Wrangler preview of the production build (auto-builds if dist/ missing)
make branding-docs     # Regenerate logo/favicon/OG assets from docs/src/assets/branding/mark.svg
```

Verbose: `V=1 make test`. Extra args: `make test ARGS="-run TestFoo"`. Build tags: `make build TAGS="foo"`.

Tooling notes (the non-obvious bits `make help` won't tell you):

- Dev tools (`gotestsum`, `gofumpt`, `goimports`, `govulncheck`, `go-test-coverage`, `gocover-cobertura`, `deadcode`, `gsa`, `goda`) are pinned in `go.mod` via `tool` directives — `go tool <name>`, no manual install.
- `golangci-lint` is pinned in the Makefile (v2.11.4), auto-installed to `.bin/` on first `make lint` — kept out of `go.mod` (its deps conflict with the main module).
- `pnpm` (≥ 11.21) + `Node 22 LTS` (`.nvmrc`, matches CI) must be on PATH; `make tools` runs one root `pnpm install --frozen-lockfile` across the three workspaces (SDK `clients/ts/`, E2E `tests/e2e/sdk/`, docs `docs/`).
- **GNU Make 4+** required (uses `--output-sync=target`); macOS BSD Make 3.81 won't parse it. Full setup: `docs/src/content/docs/development.md` § Prerequisites.
- **Lint split**: Biome owns JS/TS/JSON, markdownlint owns Markdown *and MDX* style — including two repo-local rules, WH001 (no hard-wrapped prose) and WH002 (MDX fence beside a JSX tag) in `scripts/markdownlint-rules/` — misspell owns spelling (all under `make lint`/`make fix`); accuracy/clarity/doc-sync is the `docs-reviewer` gate (§Docs review). See §Markdown authoring rules.
- **Worktrunk** (`wt`, `.config/wt.toml`): `wt switch --create` seeds `.bin/` + `node_modules/` from main, then runs `make tools`.

## Testing Conventions

- **Table-driven tests**: Use `tests := []struct{ name string; ... }` with `t.Run(tt.name, ...)` for test cases.
- **Shared mocks in `internal/testutil/`**: Use `MockPublisher`, `MockCache`, `MockDeduplicator`, `MockSubscriber` instead of creating ad-hoc mocks. See `testutil/mocks.go`.
- **JWT helpers**: Use `testutil.MakeJWT(t, claims)` and `testutil.MakeExpiredJWT(t, claims)` for auth tests. See `testutil/jwt.go`.
- **Schema helpers**: Use `testutil.NewTestSchemaRegistry(t, tables)` for schema-aware tests — it builds the registry through the real discovery path (`Refresh` against a mock ClickHouse connection), so timestamp specs are precomputed like production.
- **Policy helpers**: Use `policy.NewMemoryStore(p)` for in-memory policy testing without NATS.
- **Pipes helpers**: Use `pipes.NewMemoryStore(queries...)` for in-memory pipes testing without NATS.
- **Response assertions**: Use `testutil.AssertJSONResponse(t, rec, status, expected)` and `testutil.AssertJSONContains(t, rec, status, substring)`.
- **Coverage target**: 80% project-wide (CI enforces `threshold.total` in `.testcoverage.yml` against the merged unit + integration + e2e profile). Per-suite minima also enforced: unit 80%, integration 20%, e2e 60%, go-sdk 75%, ts-unit 40%, ts-e2e 40%, ts-total 50%. The Go SDK (`clients/go`) is a nested module — invisible to the root module's `-coverpkg=./...`, so it is gated on its own `suites.go-sdk` floor and never merged into the project-wide total. Aim for 80%+ on new code. Coverage is published as a README badge (Go merged-total) and as PR comments via GitHub Code Quality — see `.github/workflows/README.md` "Coverage publishing"; the gate is unchanged.
- **Every new function should have corresponding test cases.** Run `make lint` and `make test` before considering work complete.
- **E2E tests via SDK**: The TypeScript SDK is the primary E2E test harness. Tests in `tests/e2e/sdk/` exercise the full pipeline (ingest → ClickHouse → query) and simultaneously validate backend behavior and SDK correctness. Use `make test-e2e` to run. Add new E2E scenarios as `tests/e2e/sdk/*.test.ts` files using helpers from `tests/e2e/sdk/helpers.ts`.
- **Per-suite table isolation**: Each e2e test file owns its own ClickHouse tables — `clicks_<suite>` / `events_<suite>` / `users_<suite>`, generated from `tests/e2e/sdk/tables.ts` and created by `setup.ts`. A new test file must (1) add its suite name to `SUITES` in `tables.ts` and (2) get its names via `const T = suiteTables("<suite>")`, then reference `T.clicks` etc. — never a bare `clicks`. This makes cross-file *data* contamination structurally impossible. Files still run **sequentially** (`vitest.config.ts` `maxWorkers: 1`): running them in parallel is blocked by shared *global policy* state (several files read-modify-write the single policy document; `streaming.test.ts` flips the global `default_role`), so policy-mutating tests snapshot the full policy and restore it. Dropping `maxWorkers: 1` is a deferred follow-up tracked in #214 (per-table policy storage; see `docs/src/content/docs/ingest-pipeline.md` § Deferred).

## Local-First Validation

**Validate locally before pushing. Don't use CI as your first feedback loop.** Every push consumes shared CI capacity and AI-reviewer credits, and produces visible churn for the rest of the team.

### Before every push

```bash
make ci   # Full parity with CI: parallel verify + builds + unit/SDK tests, then integration + E2E + cov
```

If `make ci` passes locally, your commit has crossed the same gates CI will run — the CI workflow (`.github/workflows/ci.yml`) is a job DAG over the *same Makefile targets* (`verify`, `build-docs`, `test-unit`/`test-ts`, `test-integration`, `test-e2e`, `cov`), just spread across parallel runners. For workflow-only changes, read the YAML diff carefully and run `actionlint` if you have it installed.

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
- **`.githooks/pre-push`** scales the bar to what the push changes, classified by the same allowlist CI uses (`scripts/classify-paths.sh`): a code change requires the full `make ci` marker (`tmp/ci-passed-tree-<TREE-sha>`), while a docs/prose-only push requires only the `make verify` marker (`tmp/verify-passed-tree-<TREE-sha>`) — CI skips the Go/SDK suites for those too, so there's no reason to run them locally (and pre-commit's `make verify` usually already wrote it). Fail-closed: an unclassifiable push (no resolvable base, classifier error) falls back to requiring `make ci`. Both markers are tree-keyed (not commit-keyed) so `make {ci,verify} → commit → push` works without a re-run when the tree is unchanged; editing the tree (or staging a different subset than CI saw) requires a re-run. The marker write is skipped entirely when `$CI` is set (CI runners don't push).

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
| Human admins | The `required_reviewers` ruleset rule requests the `@Wave-RF/wavehouse-admins` team on every PR; the team's **code-review assignment** (configured on the team) auto-assigns + load-balances a specific member. No workflow involved. | GitHub re-requests per its own rules; `dismiss_stale_reviews_on_push` clears approvals on new commits. | Yes — the `required_reviewers` rule requires an approval from the `@Wave-RF/wavehouse-admins` team before merge. Dependabot PRs need the same admin approval (no auto-merge). |

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

**PR title format.** The title becomes the squash-merge subject on `main` and is gated by the required `CI` check's `PR title` job — a bad title blocks merge, so don't discover it from CI. It must be Conventional Commits — `<type>(optional-scope)(optional-!): <subject>` — **≤ 72 chars**, subject **lowercase-first** with **no trailing period**. Types: `feat fix docs refactor test chore ci deps build perf revert style`. Validate before creating: `scripts/lint-pr-title.sh "<title>"` (exit 0 = valid; it prints the reason on failure). `.claude/hooks/agent-bash-gate.sh` runs the same check on `gh pr create` / `gh pr edit --title`, so a malformed title is caught locally before the PR exists. The rule has a single source of truth — `scripts/lint-pr-title.sh` — called by this local gate, by the `PR title` job in `ci.yml` (from a trusted `main` checkout), and by `housekeeping.yml`'s advisory sticky-comment mirror, so local and CI never drift. Fixing a title after the fact needs no new push: editing it triggers housekeeping, which re-runs the failed `PR title` job (the job re-reads the title from the API).

### Human reviewer assignment is humans-only

Adding/removing human reviewers (`gh pr edit --add-reviewer <login>`, `gh pr edit --add-assignee <login>`, or `POST /repos/.../pulls/<N>/requested_reviewers`) is blocked for agents. Reviewer assignment is handled natively by GitHub — the `required_reviewers` rule requests the `@Wave-RF/wavehouse-admins` team and the team's code-review assignment picks the member; humans handle anything else.

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

That's all: the marker is `tmp/<name>-passed-<HEAD>` automatically, the push gate requires it, `review-marker.sh` writes it on `ship_it`, and `/prepush` launches it alongside the rest. Also add a row to the subagent table in `docs/src/content/docs/claude-code.md`.

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

**Scope** is the canonical docs-prose set from `scripts/docs-prose.sh` — a *denylist*: every tracked `.md`/`.mdx` EXCEPT `.claude/**`, `.github/**`, `CHANGELOG.md`, `AGENTS.md`, `CLAUDE.md`, `*.draft.md`/`*.old.md`, `PERF-CLAIMS-REVIEW.md`, `docs/posthog-setup-report.md`. So it covers the Starlight site under `docs/src/content/` **and** the governance docs (`README.md`, the SDK readmes `clients/ts/README.md` / `clients/go/README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `SUPPORT.md`) — new docs are picked up automatically. `CODE_OF_CONDUCT.md`/`SUPPORT.md` are deep-reviewed only on change or material suspicion.

**It is a hard pre-push gate**, run in parallel with the other pre-push reviewers (see §Pre-push self-review). Invoked with the **default (branch) scope** it emits a `VERDICT:` line; on `ship_it` the `review-marker.sh` SubagentStop hook writes `tmp/docs-reviewer-passed-<HEAD-sha>`, which the push gate requires — unconditionally, on every PR-branch push (even code-only ones). Run it via **`/docs-review`**; with **no arg** that's the gating review (branch scope), while an explicit **path/glob** or **`all`** is **advisory** (no `VERDICT:`, no marker) for ad-hoc audits. The whole dev team runs Claude Code and this command is tracked in-repo, so everyone runs it themselves; there is intentionally **no PR/cloud path** for docs review.

## Documentation Sync

Every code change should update the corresponding docs in the same PR. A code change without its doc update is incomplete.

| Change | Files to update |
| ------ | --------------- |
| Add/modify API endpoint | `docs/src/content/docs/api.md`, `README.md` (if user-facing) |
| Add/modify config option | `docs/src/content/docs/configuration.mdx`, `config.yaml`, `deployments/compose/*` env blocks, `docs/src/content/docs/deployment.md` |
| Change architecture / add a package | `docs/src/content/docs/architecture.md`, `AGENTS.md` |
| Change ingest / event format | `docs/src/content/docs/api.md`, `docs/src/content/docs/deployment.md` (CH schema) |
| Change deployment / Docker | `docs/src/content/docs/deployment.md`, compose files |
| Change build / test process | `docs/src/content/docs/development.md`, `Makefile` |
| Any notable change | `CHANGELOG.md` under `[Unreleased]` |

Source-of-truth pairs that must agree:

- Config struct tags in `internal/config/config.go` ↔ `docs/src/content/docs/configuration.mdx`, `config.yaml`, compose env blocks, `docs/src/content/docs/deployment.md`
- `EventMessage` JSON tags ↔ `docs/src/content/docs/api.md` event format, SSE examples, ClickHouse INSERT columns
- Route registrations in `router.go` ↔ `docs/src/content/docs/api.md` endpoint list
- Handler error responses ↔ `docs/src/content/docs/api.md` error tables

Before finishing a task, grep for the identifiers you touched (field names, env var names, endpoint paths) across docs to catch staleness.

### Markdown authoring rules

- **Never hard-wrap prose. One paragraph is one line.** No wrapping at 72/80 columns, no "semantic linefeeds" splitting a paragraph at sentence boundaries. Wrapped prose makes every later edit rewrap the whole block, so a one-word change lands as a five-line diff. Enforced by WH001 (`scripts/markdownlint-rules/no-hard-wrapped-prose.mjs`), which autofixes. Tables (with or without leading pipes), code, headings, setext underlines, blockquotes, JSX, `$$` display math, multi-line MDX `import`/`export`, and `:::` aside delimiters are left alone; a list item is joined as a unit, marker line included; an aside's *body* is joined but its delimiters are not.
- **In MDX, leave a blank line between a JSX tag and a code fence.** MDX itself renders the glued form correctly — verified by compiling both shapes with the same `@mdx-js/mdx` Astro uses. The blank line is what keeps *markdownlint* agreeing with it: markdownlint parses CommonMark, where `<TabItem …>` opens an HTML block that runs to the next blank line, so a glued fence is not a code block to any generic rule and `markdownlint --fix` will reformat the code inside it:

  ````mdx
  <TabItem label="YAML">

  ```yaml
  data_dir: ./data
  ```

  </TabItem>
  ````

  Enforced by WH002. **`.mdx` is never auto-fixed by the generic markdownlint rules** — `make fix` scopes that pass to `**/*.md`, because where markdownlint's CommonMark parse and MDX disagree a generic autofix rewrites the inside of a code block. MDX gets exactly one *structural* fixer, `scripts/fix-mdx-fences.mjs`, which only ever inserts a blank line beside a JSX tag (misspell still corrects spelling there — its curated list needs no parse). So `make lint` reports MDX problems but `make fix` will not silently repair them — including WH001 wrapping, which you must unwrap by hand in `.mdx`. You can't reach MDX with a bare `markdownlint-cli2 --fix` either — the config globs `.md` only, and the `.mdx` glob lives on `lint:md` — so that hazard is closed by construction rather than by this instruction.
- **Editors see WH001 in `.md` only.** The markdownlint extension reads `.markdownlint-cli2.jsonc`, `customRules` included, so no `.vscode` setting is needed (`markdownlint.customRules` is deprecated in favor of that file). But it activates on the `markdown` language ID, and `.mdx` is not associated with it — so WH002 never squiggles in the editor, and WH001 squiggles only in `.md`. Don't "fix" that with a `files.associations` entry: it would enable `source.fixAll.markdownlint` on `.mdx`, running exactly the generic fixers that must never see MDX. `make fix` and the agent hook are the MDX path.
- **These fix themselves as you write.** `.claude/hooks/markdown-on-save.sh` (PostToolUse, sibling of `gofumpt-on-save.sh`) runs the MDX fence pass on `.mdx`, markdownlint `--fix` on `.md`, and misspell on both, so an agent's output is corrected in the same pass rather than costing a lint failure and a manual cleanup. It only sees `Edit`/`Write`/`MultiEdit` — a file written through a Bash heredoc bypasses it, so run `make fix` after doing that.
- **WH001 is off under `.github/` and `.claude/`** (CI docs and agent prompts) via their own `.markdownlint.json`. It applies everywhere else, `AGENTS.md` and `CHANGELOG.md` included — so this is a narrower exclusion than `scripts/docs-prose.sh`, which also skips those two.

### Authoring docs-site pages

Three invariants the docs site enforces in code, each of which fails quietly rather than loudly if you hand-write around it:

- **Opt a page into the Cloud CTA with `cloudCta` frontmatter**, not by importing the component. `cloudCta: true` takes the default copy; `cloudCta: { title?, body? }` overrides it, which is the point — the CTA lands hardest when it names the work *that* page just described. Schema in `docs/src/content.config.ts`; the footer renders it. (The homepage is the exception: it passes `<CloudCta variant="band">` inline, because the wide band variant is splash-only and `template: splash` pages don't render the footer's copy.)
- **Never hand-write `®` or `™` in prose.** `rehype-trademarks` appends the symbol to each mark's first mention automatically, and `markFirstMentions` (`docs/src/config/trademarks.ts`) matches the bare name with no check for a symbol already there — so "ClickHouse®" renders as "ClickHouse®®". Add the mark to the registry in `trademarks.ts` and let the plugin place it; the footer notice is generated from the same registry.
- **Never hand-write `utm_*` params or `rel` on a link to `wavehouse.cloud` or `wave-rf.com`.** Use `cloudLink()` / `relFor()` from `docs/src/config/outbound.ts`. First-party links deliberately carry `rel="noopener"` *without* `noreferrer`, because `noreferrer` suppresses the `Referer` header PostHog turns into `$referring_domain` — writing the `rel` by hand is the easy way to silently destroy the attribution the whole feature exists for. Third-party links keep both.

### Authoring Mermaid diagrams

Diagrams render inside the Starlight content column (~46–58rem wide) as build-time SVG via `astro-themed-mermaid`, themed by `docs/src/config/mermaid-theme.mjs`. **Author them vertically so they fit the page at a legible size** — the single most common diagram mistake here is a wide left-to-right flowchart that gets scaled down to fit the column until its labels are unreadable.

- **Default to top-down**: `flowchart TB`/`TD`, and `direction TB` inside subgraphs — not `LR`/`RL`. A tall diagram keeps full-size nodes and just costs page height (cheap); a wide one shrinks to illegibility. Reserve `LR` for genuinely short chains (≤3–4 nodes) that read naturally as a single line.
- **Never sit two large diagrams side-by-side.** Wrap comparisons in `<div class="diagram-pair">…</div>`, which stacks them vertically so each gets the full column width. (Two detailed diagrams in a row each shrink to ~half width and stop being readable.)
- **Keep node labels short.** Labels are measured at build time to size the box; use `<br/>` for a second line rather than one long line. Lean on the `:::` semantic node classes (`wh`, `win`, `pain`, `fail`, `infra`, `neutral`, `store`, `client`) and the `--wh-mermaid-*` vars rather than inline colors.
- Diagrams are **click-to-zoom** on the site (`docs/src/components/MermaidZoom.astro`), so fine detail is always recoverable — but that's a fallback, not a license to ship an illegible inline diagram.

## SDK Sync

The TypeScript SDK (`@wavehouse/sdk` in `clients/ts/`) and Go SDK (`github.com/Wave-RF/WaveHouse/clients/go`, in `clients/go/`) are both canonical, officially supported clients. Both ship from this repo with full API-tree parity. When backend changes alter the public API surface, both SDKs need corresponding updates. The `pre-commit` git hook flags likely misses informationally; consult this table when deciding what to update.

| Backend change | SDK considerations |
| -------------- | ------------------ |
| New user-facing API endpoint | Add a typed client method in **both** SDKs (TS: `clients/ts/src/` — `client.ts`, `query-builder.ts`, `pipes.ts`, `policy.ts`, `stream/`; Go: `clients/go/` — corresponding file). Update doc pages under `docs/src/content/docs/sdk/` for both `ts/` and `go/`. Add a wire case to `clients/go/testdata/wire_cases.json` with dispatch in both conformance runners. |
| Change to JWT auth / role extraction | TS: `clients/ts/src/http.ts` + `client.ts`. Go: `clients/go/http.go` + `wavehouse.go`. |
| Change to `EventMessage` / ingest event format | Update payload types in both SDKs (some are codegen-regenerated — re-run both codegen CLIs). |
| New / changed structured query AST | TS: `clients/ts/src/query-builder.ts`. Go: `clients/go/query_builder.go` + `types.go`. |
| Change to live-query aggregation classification | TS: `clients/ts/src/stream/`. Go: `clients/go/live_query.go`. |
| Named pipes API change | TS: `clients/ts/src/pipes.ts`. Go: `clients/go/pipes.go`. |
| Policy / access-control change | TS: `clients/ts/src/policy.ts`. Go: `clients/go/policy.go`. |
| ClickHouse schema-driven type changes | Re-run both SDK codegen CLIs; commit regenerated types. |

Internal-only backend changes (middleware refactors, observability internals, dedup implementation, sweeper logic, NATS plumbing) generally don't need SDK updates. Use judgement — table above is the source of truth; nothing automated nudges you.

**The decision test**: would a user's *code* need to change to take advantage of (or be compatible with) this change? If yes, both SDKs need updates. If no (purely internal optimization), no.

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
3. Document in `docs/src/content/docs/configuration.mdx`.

### Adding a new internal package

1. Create the package under `internal/`.
2. Define an interface if there will be multiple implementations.
3. Wire it into `cmd/wavehouse/main.go`.
4. Document in `docs/src/content/docs/architecture.md`.
5. **Add a matching `area/<pkg>` repo label** (e.g. `area/foo` for `internal/foo/`) so issues can be routed to it during triage, and add the path → label mapping to `.github/labeler.yml` so PRs touching the package get auto-labeled. Issue triage is manual (see §Repository Automation); only the PR-side labeling is automated.

### Writing tests

1. Create `*_test.go` files in the same package as the code under test.
2. Use table-driven tests with `t.Run(tt.name, ...)` for multiple scenarios.
3. Use shared mocks from `internal/testutil/` (MockPublisher, MockCache, MockDeduplicator, MockSubscriber).
4. Use `testutil.MakeJWT(t, claims)` for auth tests, `testutil.NewTestSchemaRegistry(t, ...)` for schema-aware tests, `policy.NewMemoryStore(p)` for policy tests, `pipes.NewMemoryStore(queries...)` for pipes tests.
5. Use `testutil.AssertJSONResponse` and `testutil.AssertJSONContains` for HTTP handler assertions.
6. Run `make test` — it gates the unit-test coverage threshold from `.testcoverage.yml`, so a passing run already confirms coverage.
7. Aim for 80%+ coverage on new code. The project-wide CI-enforced minimum is 80% (merged unit + integration + e2e via `.testcoverage.yml`'s `threshold.total`); per-suite minima are unit 80%, integration 20%, e2e 60%, go-sdk 75%, ts-unit 40%, ts-e2e 40%, ts-total 50%.

## File Structure

```text
cmd/                    → Binary entry points (thin — just wiring)
clients/ts/             → TypeScript SDK (@wavehouse/sdk)
clients/go/             → Go SDK (github.com/Wave-RF/WaveHouse/clients/go) — nested module; file layout mirrors clients/ts/, plus cmd/wavehouse-codegen/ and testdata/wire_cases.json
internal/api/           → HTTP layer (handlers, router, middleware, schema/DLQ/policy/pipes endpoints)
internal/auth/          → JWT/JWKS authentication middleware (HMAC or JWKS, role extraction from claims)
internal/cache/         → Caching (interface + L1/L2/tiered implementations)
internal/chsql/         → Shared ClickHouse SQL helpers (identifier quoting + bind-safety)
internal/config/        → Configuration structs + loader
internal/dedupe/        → Optional deduplication (interface + embedded/distributed)
internal/discovery/     → ClickHouse schema introspection + ingest validation
internal/ingest/        → Batch buffer with DLQ + Active Sweeper (NATS message lifecycle)
internal/mq/            → MQ abstraction (interface + embedded/remote NATS)
internal/observability/ → OpenTelemetry pipeline (traces/metrics/logs providers, Prometheus exporter, slog fan-out, NATS trace propagation)
internal/pipes/         → Named query pipes (NATS KV store + SQL file bootstrap)
internal/policy/        → Access control policies (types, evaluation, NATS KV store)
internal/query/         → Structured query AST + SQL builder
internal/stream/        → SSE fan-out (event Hub: project once per role, Subscriber outbound queue, Bucket fan-out, keepalive Heartbeater wheel)
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
- **Column-level access control is a hard cap on every read path.** A role's `allow_columns`/`deny_columns` is enforced against *every* column a structured query references (projection, aggregations, `filters`, `group_by`, `order_by`, `time_range`) inside `query.Build`, and a `select_all` request expands to the role's allowed columns rather than `SELECT *` (an omitted projection selects nothing; `["*"]` is a literal column, not a wildcard — see Key Design Decision #12). The structured-query and live-stream paths share one decision function (`policy.IsColumnAllowed`). Don't move column checks out of the builder or special-case `SELECT *` — that reintroduces the #223 fail-open.
- ClickHouse queries are passed through directly — use appropriate access controls on ClickHouse itself
- **Dependency vulnerability scanning**: `govulncheck ./...` runs in CI on every push/PR. Dependabot (`.github/dependabot.yml`) opens weekly grouped PRs for outdated Go modules and GitHub Actions.
- **GitHub Actions supply chain**: Third-party actions are pinned to full commit SHAs with version comments (see `.github/workflows/ci.yml`, `release.yml`). New workflows must follow the same pattern — never `@main` or floating tags on third-party actions. Prefer inline bash or official `actions/*` / `github/*` actions when feasible (e.g. the PR-title check in `ci.yml` / `housekeeping.yml` is inline bash calling `scripts/lint-pr-title.sh`, and the change-detection job in `ci.yml` is inline `gh api`, not a third-party paths-filter action).

## Repository Automation

- **CI** (`ci.yml`): a job DAG over the same Makefile targets as local `make ci` (~3m15s push → green). **The architecture doc is [`.github/workflows/README.md`](.github/workflows/README.md)** — DAG diagram, design invariants, cache policy, and the add-a-job recipe; read it before editing `ci.yml`. The load-bearing facts: the `changes` job (`scripts/ci/classify-changes.sh`, fail-closed) gates the test/docs jobs; the long-pole `e2e` job builds its own SDK dist + cover binary and runs the suite exactly like local `make test-e2e`; each suite uploads a `coverage-<suite>` fragment and a dedicated `coverage` job merges them + applies every threshold gate via `make cov` (like local `make ci`'s final step — kept separate so the gate is decoupled from the e2e suite; it's `needs: changes` only and *polls* for the fragments via `scripts/ci/wait-artifact.sh` rather than `needs`-ing the suites, so its ~50s setup overlaps them and the merge fires ~10s after the last suite instead of serializing setup onto the critical path); an **aggregator job named `CI`** is the ruleset's sole required status check (fails on failed/cancelled needs, treats skipped as passing) — the Cloudflare `docs-preview` deploy is deliberately NOT a need (non-gating, like `timing`: only `docs-build` gates, so a slow/failed preview never delays or reds the required check); caches are owned end-to-end by `.github/actions/setup-env` (nested `actions/cache`, automatic post-job saves — never add save steps to `ci.yml`). Plain `make build` binaries are not linked in CI — compile breakage is caught by lint/tests/the cover-binary link, release builds by `goreleaser-validate.yml` / `publish-dev.yml`. Fork PRs run the full secretless pipeline; merge-queue `merge_group` runs re-test the full suite against current main (the queue replaces the old require-up-to-date rule — never remove the `merge_group:` trigger, or queued PRs hang). CI logic lives in `scripts/ci/*.sh`, gated by `make lint-sh` (shellcheck) and `make lint-gha` (actionlint) inside `make verify` — not in inline YAML, except in the trusted-main deploy jobs where inline is the trust boundary.
- **Issue triage**: manual. `triage.yml` classified new/edited issues with GitHub Models until GitHub retired the service on 2026-07-30 — the endpoint now returns `410` unconditionally, so the workflow failed on every issue event and was removed, along with `.github/board-config.env` ([#431](https://github.com/Wave-RF/WaveHouse/issues/431)). The `PROJECT_BOARD_TOKEN` repo secret now has no consumers and **should be deleted, with its PAT revoked** — a branch cannot remove a repo secret, so this is a manual step. `area/*` / `security` / `breaking-change` labels and the board's `Priority` field are set by hand; maintainers on Claude Code can run the `/pm-triage` skill. PR labeling is untouched — `actions/labeler` in `housekeeping.yml` is path-based.
- **Code review** (advisory; the `main branch protection` ruleset is the actual merge gate — its `required_reviewers` rule requires an approval from the `@Wave-RF/wavehouse-admins` team, alongside the required `CI` status check): handled by external marketplace apps (CodeRabbit, Copilot) configured at the org/repo level, not by in-repo workflows. Inline findings post as review threads that `required_review_thread_resolution: true` blocks merge on until resolved.
- **Dependabot** (`.github/dependabot.yml`): opens weekly grouped PRs for Go modules, GitHub Actions, and the npm workspaces. They go through the same gate as any PR — an `@Wave-RF/wavehouse-admins` approval (`required_reviewers`) + the required checks — with **no auto-merge** (the former `dependabot-automerge.yml` was removed; auto-approve-and-merge is intentionally off, so every bump gets a human admin review).
- **Docs site deploy** (`wavehouse.dev`): the `docs-preview` / `docs-deploy` jobs of the CI workflow (`.github/workflows/ci.yml`), **not** Cloudflare's Workers Builds. Workers Builds can't build this site — `rehype-mermaid` renders diagrams to themed SVG at build time via headless Chromium, and the Workers Builds image has no browser (and no root to apt-install one). CI's `docs-build` job builds `docs/dist/` on a runner with a cached Chromium and uploads it as an artifact; the deploy jobs consume that artifact from a checkout of **trusted `main`** — wrangler, the worker source, and `docs/wrangler.jsonc` never resolve from a PR tree, so PR-authored code can't reach the Cloudflare token (#305); a PR's `docs/worker/` or wrangler-config changes take effect on merge, not in its preview. Push to `main` runs `wrangler deploy` once the whole pipeline is green (production → `wavehouse.dev`); same-repo PR branches run `wrangler versions upload` right after the build (an unrelated test flake no longer blocks the preview), publishing a per-version preview at `<version-prefix>-wavehouse-docs.wave-rf.workers.dev` posted as a sticky PR comment. Deploys are skipped when no docs-affecting files changed and on fork PRs. **Requires `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` repo secrets** (referenced only in the two deploy jobs), and Cloudflare Workers Builds must stay **disconnected** from the `wavehouse-docs` Worker (else it double-deploys and fails the browser-dependent build on every push). Wrangler config (custom domain, observability, source maps, preview URLs) lives in `docs/wrangler.jsonc`. The worker (`docs/worker/index.ts`, delegating to `cloudflare-md-router`) deploys alongside the static assets so `Accept: text/markdown` content negotiation works in production.

## Governance Files

- **No `CODEOWNERS`**: admin approval is enforced natively by the `main branch protection` ruleset's `required_reviewers` rule — an approval from the `@Wave-RF/wavehouse-admins` team — rather than a CODEOWNERS file or a custom status-check workflow.
  - Reviewer *assignment* is native too: the `required_reviewers` rule requests the `@Wave-RF/wavehouse-admins` team and the team's code-review assignment auto-assigns + load-balances the member (no workflow). `housekeeping.yml` (non-required, `pull_request_target` so it can write on fork PRs) applies path-labels, mirrors the PR-title check as a sticky explainer comment, and re-runs CI's failed `PR title` job when a title edit fixes it — the *blocking* title gate is the `PR title` job under `ci.yml`'s required `CI` aggregator. Task Board placement is handled by native Projects v2 workflows configured in the project UI.
- **`CLAUDE.md`**: a thin pointer file to AGENTS.md. Keep the pointer short; never duplicate content.
- **`CONTRIBUTING.md`**: the Conventional Commits type list must stay in sync with the regex in `scripts/lint-pr-title.sh` (the single source of truth used by the local gate, the required `CI` check's `PR title` job, and housekeeping's sticky-comment mirror). The title linter validates squash-merge commit messages.
- **`SUPPORT.md`** (alpha-stage public triage policy): the externally-promised cadence is **best-effort, 1–2 business days for an initial response** on bugs / features / usage questions; **security reports are prioritized** with the 48-hour acknowledge / 5-business-day initial-assessment targets in `SECURITY.md`. Usage questions ("how do I…") are routed to [GitHub Discussions → Q&A](https://github.com/Wave-RF/WaveHouse/discussions/categories/q-a) — do not file them as bug-report Issues; bug-reporters who use the wrong template get redirected. There is no Discord/Slack. Don't quietly let threads slip — if one sits longer than a week, that's a miss. **Out-of-scope items publicly stated in `SUPPORT.md` are only "Older releases" and "Non-ClickHouse backends"**. When tweaking the policy, update `SUPPORT.md` first and keep this paragraph in sync. The docs footer (`docs/src/components/Footer.astro`) and sidebar (`docs/src/config/sidebar.ts`) cross-link Discussions, `SUPPORT.md`, and `SECURITY.md` so they're one click from anywhere on `wavehouse.dev`; `README.md`, `CONTRIBUTING.md`, and both issue templates (`.github/ISSUE_TEMPLATE/bug_report.md`, `feature_request.md`) also link out — change those together if the policy moves.

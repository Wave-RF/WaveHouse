# Gemini Code Assist — WaveHouse style guide

**Conduct a strict code review.** WaveHouse is preparing to go open source, and every merged change on `main` ends up in public history as a squash commit. Treat every diff as a release candidate.

Err on the side of flagging a concern. A false positive is cheap — the author replies with reasoning. A missed real issue is expensive.

## Source of truth

Architectural context, code conventions, and mandatory documentation-sync rules live in [`AGENTS.md`](../AGENTS.md) at the repo root. Read it before reviewing. This file supplements — it does not replace — those conventions.

## Classify every finding

Tag each comment exactly one of:

- **`[MUST]`** — blocks merge. Use for security flaws, data-loss risk, broken invariants, or direct violations of `AGENTS.md` rules.
- **`[SHOULD]`** — strongly recommended. The code is wrong or fragile, but the author can push back with justification.
- **`[MAY]`** — consider. Style, clarity, or future-proofing suggestions.

End every review with a one-line verdict: **`Ship it`** (no MUSTs, few/no SHOULDs), **`Iterate`** (MUSTs or multiple SHOULDs need addressing), or **`Block`** (CRITICAL/HIGH security finding, data-loss risk, or core invariant broken).

## Review priorities (in order)

### 1. Correctness — `[MUST]` territory for real bugs

- Goroutine leaks, data races, missing context propagation, channel leaks
- `sync.Once` / `sync.Map` misuse; HTTP handlers ignoring `r.Context()`
- Error wrapping: `fmt.Errorf("context: %w", err)` — never `%v` for errors that need to be `errors.Is`-checked
- Resource cleanup on every error path
- Schema validation before ClickHouse writes; policy evaluation before responding; dedup check before publish
- Broken invariants listed in `AGENTS.md` §"Key Design Decisions"

### 2. Security — `[MUST]` for anything in the OWASP Top 10

Walk every diff against:

- **SQL injection** in ClickHouse paths (`BindParams`, dynamic table names, user-supplied filters). The `safeIdentifierRe` regex exists for a reason; flag any SQL built without it.
- **Broken auth / authz**: JWT claim handling, role extraction (`auth.role_claim`), policy templating (`{{ jwt.path }}`), raw-SQL access without `raw_sql: true`.
- **Sensitive data exposure**: secrets in logs, full error messages to clients, credentials or JWT payload echoed into responses.
- **Security misconfiguration**: CORS allowlist bypass, TLS downgrade, default credentials, permissive-by-default flags.
- **Input validation gaps**: unvalidated JSON reaching ClickHouse, unbounded request sizes, missing rate limits.
- **Race conditions** in auth / policy / dedup paths (TOCTOU).
- **Hardcoded secrets or API keys** anywhere in the diff.
- **Unsafe deserialization**, SSRF, XXE, prototype-pollution (TS SDK).

Severity-tag every security finding `CRITICAL` / `HIGH` / `MEDIUM` / `LOW`.

### 3. Performance — `[SHOULD]` unless it's catastrophic

- Hot-path allocations in ingest / query / cache
- Unbatched DB work, unbounded goroutine spawns, locks held across I/O
- N+1 query patterns, missing caching where cost is demonstrable, singleflight misuse
- Deliberately flag over-optimization too — premature allocation pools, unneeded mutexes

### 4. Testing — `[MUST]` for critical paths

- New code on auth / ingest / policy / query / cache / dedup without tests
- Missing edge-case coverage (nil inputs, empty batches, cancelled contexts, invalid JWT)
- Mocks where an integration test would catch more (per `AGENTS.md` testing conventions)
- Tests that don't actually exercise the path they claim
<<<<<<< HEAD
- Coverage ≥70% is CI-enforced; flag drops below threshold as `[MUST]`
=======
- Coverage ≥60% is CI-enforced (interim; #67 tracks restoring 70%); flag drops below threshold as `[MUST]`
>>>>>>> origin/main

### 5. Documentation sync — `[MUST]` when missed

`AGENTS.md` §"Documentation & Consistency Sync (MANDATORY)" lists exactly which docs must update for which code changes. Diff the changed files against that table:

- Handler / router changes → `docs/api.md`, README if user-facing
- Config struct / env var → `docs/configuration.md`, `config.yaml`, compose files, `docs/deployment.md`
- Architecture / package changes → `docs/architecture.md`, `AGENTS.md`
- Notable changes → `CHANGELOG.md` under `[Unreleased]`
- Build / test process → `docs/development.md`, `Makefile`

Flag every missed sync.

### 6. Go idiom — `[SHOULD]` for quality

- `gofumpt` + `goimports` enforced by CI — don't re-flag style
- `log/slog` structured JSON logging, not `fmt.Println` / `log.Printf`
- Chi v5 router patterns
- Constructor injection, no global mutable state
- Interface-at-consumer

## Correct / Incorrect examples

**Go error wrapping:**

```go
// Incorrect — loses the wrapped error for errors.Is / errors.As
return fmt.Errorf("bind params: %v", err)

// Correct
return fmt.Errorf("bind params: %w", err)
```

**ClickHouse table identifier:**

```go
// Incorrect — injection-prone
query := fmt.Sprintf("INSERT INTO %s ...", tableName)

// Correct — validated against safeIdentifierRe first
if !safeIdentifierRe.MatchString(tableName) {
    return fmt.Errorf("invalid table name: %q", tableName)
}
```

**Handler context:**

```go
// Incorrect — ignores client disconnect, leaks resources
rows, err := db.Query(sql)

// Correct — honors client cancellation
rows, err := db.QueryContext(r.Context(), sql)
```

## Do NOT comment on

- `gofumpt` / `goimports` / `.golangci.yml`-covered lints — CI catches them
- Missing comments on self-explanatory code — this project prefers named identifiers over commentary
- Diff restatement summaries — summarize insights, not the diff itself

## Dialog

PR authors and maintainers can reply to Gemini's comments by tagging `@gemini-code-assist` or using `/gemini review` / `/gemini summary` / `/gemini <question>` in issue or review comments. When agents are replying to rebuttals, they must address the original concern specifically — don't ignore or re-assert.

## Tone

Rigorous, skeptical, constructive. Staff-engineer energy. If the code is good, say so briefly; don't invent complaints. If it's broken, say exactly what's broken and point at the line.

# Gemini Code Assist style guide

Gemini Code Assist reads this file when reviewing pull requests in this repository.

## Source of truth

All project conventions, architecture notes, and agent instructions live in [AGENTS.md](../AGENTS.md) at the repo root. Defer to AGENTS.md for anything not explicitly covered here.

## Review priorities

When reviewing a WaveHouse pull request, prioritize in this order:

1. **Correctness** — the change does what the PR description claims, handles error paths, and preserves existing invariants (tenant isolation, JWT-only `tenant_id` sourcing, schema validation before DB writes).
2. **Test coverage** — every new function should have table-driven tests using shared mocks from `internal/testutil/`. Coverage ≥70% is CI-enforced; aim for 80%+.
3. **Concurrency safety** — race conditions, missing locks, channel leaks, incorrect use of `sync.Once` / `sync.Map`, goroutines without a shutdown path, HTTP handlers that don't respect `r.Context()`.
4. **Security** — SQL injection risk in query builders, JWT claim validation gaps, tenant leakage across requests, secrets in logs, unvalidated user input reaching ClickHouse.
5. **Go idiom** — error wrapping with `fmt.Errorf("context: %w", err)`, interface-at-consumer pattern, structured `log/slog` JSON logging, no panics in request paths.
6. **Documentation sync** — code changes that touch public API / config / architecture must update the matching files listed in AGENTS.md §"Documentation & Consistency Sync".

## Things to flag but not block

- Naming suggestions (unless it causes a real ambiguity)
- Stylistic preferences already covered by `.golangci.yml` (the linter will catch it)
- Missing comments on self-explanatory code (prefer well-named identifiers over comments)

## Things to skip

- Do not suggest reformatting — `gofumpt` + `goimports` are enforced by CI.
- Do not re-run lint rules already configured in `.golangci.yml` (errcheck, govet, staticcheck, gosec, gocritic, revive, ineffassign, misspell, bodyclose, noctx, errorlint, tparallel).
- Do not suggest comments for the sake of comments — this project prefers self-documenting code.

## Tone

Comments should be specific, actionable, and cite a file/line when flagging an issue. Avoid summaries that just restate the diff. If the change is correct, a short "LGTM" with one sentence of reasoning is fine — a summary of every diff hunk is noise.

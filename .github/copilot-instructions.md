# GitHub Copilot Instructions

For detailed project context, architecture, code conventions, and common tasks, see [AGENTS.md](../AGENTS.md) in the project root.

## Quick Reference

- **Language**: Go 1.25, strict formatting (`gofumpt`)
- **Router**: Chi v5 (`github.com/go-chi/chi/v5`)
- **Logging**: `log/slog` with JSON handler
- **Testing**: `testing` + `testify` for assertions
- **Linting**: golangci-lint v2 (see `.golangci.yml`)

## Key Rules

1. **Tenant ID comes only from JWT** — never read `tenant_id` from request body, query params, or headers.
2. **Interface-first** — define interfaces where consumed, implement separately for standalone/clustered.
3. **Return errors, don't panic** — wrap with `fmt.Errorf("context: %w", err)`.
4. **MANDATORY: Keep ALL docs, configs, and examples in sync** — every code change MUST include updates to all affected documentation, config files, Docker Compose files, examples, and CHANGELOG.md. Do NOT wait for the user to ask. See the full "Documentation & Consistency Sync" section in [AGENTS.md](../AGENTS.md) for the complete checklist and cross-referencing rules. A code change without its documentation counterpart is considered incomplete.
5. **Verify before finishing** — before completing any task, search docs for identifiers you touched (field names, env vars, endpoints, struct names) and fix any stale references.
6. **Every new function must have tests** — use table-driven tests, shared mocks from `internal/testutil/`, and aim for 80%+ coverage. Run `make lint` and `make test` before considering work complete.
7. **Use testutil helpers** — use `MockPublisher`, `MockCache`, `MockDeduplicator`, `MockSubscriber` from `testutil/mocks.go` instead of creating ad-hoc mocks. Use `testutil.MakeJWT()` for auth tests. Use `policy.NewMemoryStore(p)` for policy tests. Use `pipes.NewMemoryStore(queries...)` for pipes tests.
8. **Validate locally before pushing** — run `make ci` (or at minimum `make coverage`) in your sandbox and confirm it passes before opening or updating a PR. The repo runs on a shared self-hosted runner pool with finite throughput and bills AI-reviewer credits per push; a speculative commit to "see what CI says" costs real minutes and dollars. If a test passes locally but flakes on CI, investigate the runner environment before patching the test — see the "Local-First Validation" section in [AGENTS.md](../AGENTS.md) for full guidance.

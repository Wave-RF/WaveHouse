# GitHub Copilot Instructions

For detailed project context, architecture, code conventions, and common tasks, see [AGENTS.md](../AGENTS.md) in the project root.

## Quick Reference

- **Language**: Go 1.25, standard formatting (`gofmt`)
- **Router**: Chi v5 (`github.com/go-chi/chi/v5`)
- **Logging**: `log/slog` with JSON handler
- **Testing**: `testing` + `testify` for assertions
- **Linting**: golangci-lint v2 (see `.golangci.yml`)

## Key Rules

1. **Tenant ID comes only from JWT** — never read `tenant_id` from request body, query params, or headers.
2. **Interface-first** — define interfaces where consumed, implement separately for standalone/clustered.
3. **Return errors, don't panic** — wrap with `fmt.Errorf("context: %w", err)`.
4. **Keep docs updated** — when modifying code, update the corresponding file in `docs/`.
5. **Add changelog entries** — notable changes go in `CHANGELOG.md` under `[Unreleased]`.

# Contributing to WaveHouse

Thank you for your interest in contributing to WaveHouse! This guide will help you get started.

## Getting Started

1. [Fork the repository](https://github.com/Wave-RF/WaveHouse/fork) and clone your fork.
2. Follow the [Development Guide](docs/development.md) to set up your local environment.
3. Create a feature branch: `git checkout -b feat/my-feature`

## How to Contribute

### Reporting Bugs

Open a [bug report issue](https://github.com/Wave-RF/WaveHouse/issues/new?template=bug_report.md) with:

- WaveHouse version
- Steps to reproduce
- Expected vs. actual behavior
- Relevant logs or error messages

### Requesting Features

Open a [feature request issue](https://github.com/Wave-RF/WaveHouse/issues/new?template=feature_request.md) describing:

- The problem you're trying to solve
- Your proposed solution
- Any alternatives you've considered

### Submitting Pull Requests

1. Ensure your changes pass all checks:

   ```bash
   make lint       # Linter must pass
   make test       # Unit tests must pass
   make build      # All binaries must compile
   ```

2. Write tests for new functionality. Unit tests go alongside the code in `internal/`. Integration tests go in `tests/` with the `//go:build integration` tag.

3. Update documentation if your change affects:
   - API endpoints → update `docs/api.md`
   - Configuration options → update `docs/configuration.md`
   - Deployment → update `docs/deployment.md`
   - Architecture → update `docs/architecture.md`

4. Follow the commit message format (see below).

5. Open a pull request against `main`.

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(<scope>): <description>

[optional body]
```

Types:

- `feat` — New feature
- `fix` — Bug fix
- `docs` — Documentation only
- `refactor` — Code change that neither fixes a bug nor adds a feature
- `test` — Adding or updating tests
- `chore` — Miscellaneous tasks that don't fit elsewhere
- `ci` — CI/CD and GitHub Actions changes
- `deps` — Dependency version bumps (typically opened by Dependabot)
- `build` — Build system changes (Makefile, Dockerfile, goreleaser)
- `perf` — Performance improvements
- `revert` — Reverting a previous commit
- `style` — Formatting changes with no functional effect

The PR title is enforced via the `Lint PR title` workflow; it becomes the squash-merge commit message on `main`, so keep it parseable.

Examples:

```text
feat(api): add rate limiting middleware
fix(dedupe): handle external DB timeout gracefully
docs(api): add WebSocket authentication example
test(cache): add tiered cache stampede test
```

## Code Style

- **Formatting**: Code must be formatted with `gofumpt` (a strict superset of `gofmt`). `make fmt` runs it; CI enforces it via `make fmt-check`.
- **Linting**: All lint checks in `.golangci.yml` must pass (see `make lint`).
- **Naming**: Follow [Go naming conventions](https://go.dev/doc/effective_go#names).
- **Interfaces**: Define interfaces where they are consumed, not where they are implemented.
- **Errors**: Return errors rather than panicking. Use `fmt.Errorf("context: %w", err)` for wrapping.

## Code Review

All submissions require review before merging. Reviewers will look for:

- Correctness and test coverage
- Adherence to existing architecture patterns
- Documentation updates where applicable
- No security regressions (tenant isolation, input validation)

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).

# Contributing to WaveHouse

Thank you for your interest in contributing to WaveHouse! This guide will help you get started.

> Looking for help instead of contributing code? See [SUPPORT.md](SUPPORT.md) for where to ask questions, report bugs, and what the alpha-stage response cadence is. Found a security issue? Use the private channel in [SECURITY.md](SECURITY.md), not a public issue.

## Getting Started

1. [Fork the repository](https://github.com/Wave-RF/WaveHouse/fork) and clone your fork.
2. Follow the [Development Guide](docs/src/content/docs/development.md) to set up your local environment.
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
   make ci         # full local pipeline: verify + builds + all test suites (Docker required)
   ```

   The pre-push hook (installed by `make tools`) blocks a push until the tree has been validated locally: a code change needs `make ci`, a docs/prose-only change needs only `make verify` (the same split CI makes). `make lint` / `make test` / `make build` are fast inner-loop subsets.

2. Write tests for new functionality. Unit tests go alongside the code in `internal/`; SDK tests live in `clients/ts/src/` and `clients/go/`. Integration tests go in `tests/` with the `//go:build integration` tag.

3. Update documentation if your change affects:
   - API endpoints → update `docs/src/content/docs/api.md`
   - Configuration options → update `docs/src/content/docs/configuration.mdx`
   - Deployment → update `docs/src/content/docs/deployment.md`
   - Architecture → update `docs/src/content/docs/architecture.md`
   - Client SDK surface → update **both** SDKs (`clients/ts/src/`, `clients/go/`), the shared topic pages under `docs/src/content/docs/sdk/` (one `<Tabs syncKey="lang">` block per topic) plus the per-language setup pages `sdk/setup/typescript.mdx` / `sdk/setup/go.md`, and the shared wire fixture `clients/go/testdata/wire_cases.json`; see AGENTS.md §SDK Sync

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

The PR title is enforced by the required `CI` check's `PR title` job (`.github/workflows/ci.yml`): Conventional Commits format, ≤ 72 characters, lowercase-first subject with no trailing period — it becomes the squash-merge commit message on `main`, so keep it parseable. Check a title locally with `scripts/lint-pr-title.sh "<title>"` before opening the PR.

Examples:

```text
feat(api): add rate limiting middleware
fix(dedupe): handle external DB timeout gracefully
docs(api): add SSE authentication example
test(cache): add tiered cache stampede test
```

## Code Style

- **Formatting**: Code must be formatted with `gofumpt` (a strict superset of `gofmt`). `make fmt` checks both modules — the root one and the nested `clients/go` — as do `make lint` and `make tidy`. `make fix` applies the corresponding fixes to both.
- **Linting**: All lint checks in `.golangci.yml` must pass (see `make lint`).
- **Naming**: Follow [Go naming conventions](https://go.dev/doc/effective_go#names).
- **Interfaces**: Define interfaces where they are consumed, not where they are implemented.
- **Errors**: Return errors rather than panicking. Use `fmt.Errorf("context: %w", err)` for wrapping.
- **Docs prose**: never hard-wrap Markdown — one paragraph is one line. `make lint` enforces it (rule `WH001`) everywhere, and `make fix` applies it in `.md`. In `.mdx` the generic fixers are deliberately never run, so unwrap by hand there. See [Development → Markdown and MDX](https://wavehouse.dev/development#markdown-and-mdx).

## Code Review

All submissions require review before merging. Reviewers will look for:

- Correctness and test coverage
- Adherence to existing architecture patterns
- Documentation updates where applicable
- No security regressions (role-based access control, input validation)

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).

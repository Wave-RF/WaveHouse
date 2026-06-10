#!/usr/bin/env bash
# Pure path classifier — the single source of truth for which file changes
# count as `code` (Go/SDK test work) vs `docs` (docs-site build inputs).
#
# Reads a newline-delimited file list on stdin, writes two key=value lines:
#
#   code=true|false   false only when EVERY path is prose/repo-meta
#                     (docs/, *.md, license/attribution, labels, issue +
#                     PR templates, editor + agent config) — then the
#                     Go/SDK suites can skip.
#   docs=true|false   true when any path feeds the docs-site build: docs/,
#                     clients/ts/ (the landing page bundles @wavehouse/sdk),
#                     the workspace lockfile, or the CI build plumbing
#                     itself (ci.yml / setup-env).
#
# Dependency-free and side-effect-free on purpose, so every caller can
# trust it with just a file list:
#   - CI    — scripts/ci/classify-changes.sh pipes the GitHub API file
#             list through here (CI checkouts are shallow, so the list
#             can't come from `git diff`).
#   - hooks — the local git hooks pipe `git diff --name-only`.
# The event-level fail-closed policy (pushes / dispatches / merge-group
# runs always run everything) is a CI decision and lives in the wrapper,
# NOT here. Empty stdin ⇒ code=false docs=false; callers decide what an
# empty change set means (CI's wrapper fails closed to true).

set -euo pipefail

files="$(cat)"

# Empty guard: without it the `grep -qvE` below sees a lone blank line,
# which matches no allowlist entry and would wrongly read as code=true.
if [ -z "$files" ]; then
  echo "code=false"
  echo "docs=false"
  exit 0
fi

# code=true unless EVERY changed path matches the prose/meta allowlist.
if printf '%s\n' "$files" | grep -qvE '^(docs/|.*\.md$|LICENSE|NOTICE|\.gitignore$|\.gitattributes$|\.github/labeler\.yml$|\.github/ISSUE_TEMPLATE/|\.github/pull_request_template|\.claude/|\.vscode/)'; then
  echo "code=true"
else
  echo "code=false"
fi

# docs=true when ANY changed path is a docs-site build input.
if printf '%s\n' "$files" | grep -qE '^(docs/|clients/ts/|pnpm-lock\.yaml|pnpm-workspace\.yaml|\.github/workflows/ci\.yml|\.github/actions/setup-env/)'; then
  echo "docs=true"
else
  echo "docs=false"
fi

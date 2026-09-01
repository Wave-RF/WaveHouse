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
# `grep` here is case-sensitive: GitHub meta paths use their real casing
# (`.github/ISSUE_TEMPLATE/`, `.github/PULL_REQUEST_TEMPLATE.md`).
# matches reports whether grep found anything, distinguishing "no match" (exit 1)
# from "grep failed" (exit 2+: can't fork/exec, read error, bad pattern). An `if
# grep -q ...` collapses those two into the same branch, so a transient failure
# under load answers the question wrongly and confidently — and `set -e` does not
# help, because it is suppressed for a command used as an `if` condition. CI's
# `changes` job gates the docs pipeline on this answer, where a wrong `docs=false`
# skips the docs build and still reports success. Fail loudly instead.
matches() {
  local rc=0
  # Here-string, not a pipe: `grep -q` exits at the first match, and once $files
  # exceeds the pipe buffer (~64 KB here, a few thousand paths) the upstream
  # printf dies of SIGPIPE, which `set -o pipefail` reports as 141 — landing in
  # the error arm below and aborting on a perfectly ordinary large change set.
  # That is the same failure class this helper exists to close, through a
  # different door.
  grep -qE "$@" <<<"$files" || rc=$?
  case $rc in
    0) return 0 ;;
    1) return 1 ;;
    *) echo "classify-paths: grep failed (exit $rc)" >&2; exit 2 ;;
  esac
}

if matches -v '^(docs/|.*\.md$|LICENSE|NOTICE|\.gitignore$|\.gitattributes$|\.github/labeler\.yml$|\.github/ISSUE_TEMPLATE/|\.github/PULL_REQUEST_TEMPLATE|\.claude/|\.vscode/)'; then
  echo "code=true"
else
  echo "code=false"
fi

# docs=true when ANY changed path is a docs-site build input.
if matches '^(docs/|clients/ts/|pnpm-lock\.yaml|pnpm-workspace\.yaml|\.github/workflows/ci\.yml|\.github/actions/setup-env/)'; then
  echo "docs=true"
else
  echo "docs=false"
fi

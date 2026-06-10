#!/usr/bin/env bash
# Behavioral test for scripts/classify-paths.sh — the shared change
# classifier behind CI's `changes` job and (potentially) the local git
# hooks. Pins the canonical change shapes so the allowlists can't silently
# regress; run by `make verify` (target: test-classify-paths), so it gates
# in CI exactly like a unit test. Dependency-free — no network, no gh.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1 # repo root (scripts/..)

classify=scripts/classify-paths.sh
fails=0

# check <name> <want_code> <want_docs> <file>...
check() {
  local name="$1" want_code="$2" want_docs="$3"; shift 3
  local got code docs
  got="$(printf '%s\n' "$@" | "$classify")"
  code="$(printf '%s\n' "$got" | sed -n 's/^code=//p')"
  docs="$(printf '%s\n' "$got" | sed -n 's/^docs=//p')"
  if [ "$code" = "$want_code" ] && [ "$docs" = "$want_docs" ]; then
    printf '  ok   %-18s code=%s docs=%s\n' "$name" "$code" "$docs"
  else
    printf '  FAIL %-18s want code=%s docs=%s, got code=%s docs=%s\n' \
      "$name" "$want_code" "$want_docs" "$code" "$docs" >&2
    fails=$((fails + 1))
  fi
}

#     name              code   docs   paths...
check docs-only         false  true   docs/src/content/docs/intro.md
check prose-only        false  false  README.md CHANGELOG.md
check go-only           true   false  internal/api/handler.go
check sdk               true   true   clients/ts/src/client.ts
check workflow-ci       true   true   .github/workflows/ci.yml
check workflow-other    true   false  .github/workflows/housekeeping.yml
check setup-env         true   true   .github/actions/setup-env/action.yml
check worker-under-docs false  true   docs/worker/index.ts
check agent-config      false  false  .claude/hooks/agent-bash-gate.sh
check editor-config     false  false  .vscode/settings.json
# GitHub meta paths, real casing. labeler.yml + ISSUE_TEMPLATE/config.yml
# are NOT .md, so they exercise their own allowlist entries (not `.md$`).
check labeler           false  false  .github/labeler.yml
check issue-template    false  false  .github/ISSUE_TEMPLATE/config.yml
check pr-template       false  false  .github/PULL_REQUEST_TEMPLATE.md
check dep-bump-pnpm     true   true   pnpm-lock.yaml
check dep-bump-go       true   false  go.mod go.sum
check mixed-docs-go     true   true   docs/x.md internal/a.go
# Empty change set (no paths) — the empty guard, distinct from "a blank line".
check empty             false  false

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "classify-paths: all cases passed"

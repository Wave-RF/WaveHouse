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

# check <name> <want_code> <want_docs> <want_go_deps> <file>...
check() {
  local name="$1" want_code="$2" want_docs="$3" want_go_deps="$4"; shift 4
  local got code docs go_deps
  got="$(printf '%s\n' "$@" | "$classify")"
  code="$(printf '%s\n' "$got" | sed -n 's/^code=//p')"
  docs="$(printf '%s\n' "$got" | sed -n 's/^docs=//p')"
  go_deps="$(printf '%s\n' "$got" | sed -n 's/^go-deps=//p')"
  if [ "$code" = "$want_code" ] && [ "$docs" = "$want_docs" ] && [ "$go_deps" = "$want_go_deps" ]; then
    printf '  ok   %-18s code=%s docs=%s go-deps=%s\n' "$name" "$code" "$docs" "$go_deps"
  else
    printf '  FAIL %-18s want code=%s docs=%s go-deps=%s, got code=%s docs=%s go-deps=%s\n' \
      "$name" "$want_code" "$want_docs" "$want_go_deps" "$code" "$docs" "$go_deps" >&2
    fails=$((fails + 1))
  fi
}

#     name              code   docs   go-deps paths...
check docs-only         false  true   false  docs/src/content/docs/intro.md
check prose-only        false  false  false  README.md CHANGELOG.md
check go-only           true   false  false  internal/api/handler.go
check sdk               true   true   false  clients/ts/src/client.ts
check workflow-ci       true   true   false  .github/workflows/ci.yml
check workflow-other    true   false  false  .github/workflows/housekeeping.yml
check setup-env         true   true   false  .github/actions/setup-env/action.yml
check worker-under-docs false  true   false  docs/worker/index.ts
check agent-config      false  false  false  .claude/hooks/agent-bash-gate.sh
check editor-config     false  false  false  .vscode/settings.json
# GitHub meta paths, real casing. labeler.yml + ISSUE_TEMPLATE/config.yml
# are NOT .md, so they exercise their own allowlist entries (not `.md$`).
check labeler           false  false  false  .github/labeler.yml
check issue-template    false  false  false  .github/ISSUE_TEMPLATE/config.yml
check pr-template       false  false  false  .github/PULL_REQUEST_TEMPLATE.md
check dep-bump-pnpm     true   true   false  pnpm-lock.yaml
check dep-bump-go       true   false  true   go.mod go.sum
# Any module's go.mod/go.sum (the cache keys hash `**/go.mod`, `**/go.sum`),
# and only the exact file names.
check go-sum-only       true   false  true   go.sum
check nested-module     true   false  true   clients/go/go.mod
check go-mod-lookalike  true   false  false  internal/x/go.mod.tmpl internal/x/notgo.sum
check mixed-docs-go     true   true   false  docs/x.md internal/a.go
check mixed-docs-deps   true   true   true   docs/x.md go.sum
# Empty change set (no paths) — the empty guard, distinct from "a blank line".
check empty             false  false  false

# A failing grep must abort, not answer. Without this the script reads exit 2
# ("grep couldn't run") as exit 1 ("no match") and prints a confident wrong
# answer — which is how a transient failure under parallel load silently
# skipped the docs build. Stub grep onto PATH so it always exits 2.
stub="$(mktemp -d)"
printf '#!/bin/sh\nexit 2\n' > "$stub/grep"
chmod +x "$stub/grep"
out="$(printf 'docs/x.md\n' | PATH="$stub:$PATH" "$classify" 2>&1)"
rc=$?
rm -rf "$stub"
if [ "$rc" -eq 2 ] && printf '%s' "$out" | grep -q 'grep failed'; then
  printf '  ok   %-18s aborts instead of answering\n' "grep-failure"
else
  printf '  FAIL %-18s want exit 2 + diagnostic, got exit %s: %s\n' "grep-failure" "$rc" "$out" >&2
  fails=$((fails + 1))
fi

# A large change set must classify normally. `grep -q` exits at the first match,
# so feeding it through a pipe made the upstream printf die of SIGPIPE once the
# list outgrew the pipe buffer — which `pipefail` reported as a grep failure and
# the script turned into an abort. In CI that abort is silent: the wrapper reads
# the classifier through process substitution, so every code job would skip and
# the aggregator would report green having run nothing.
big="$(mktemp)"
i=0
while [ "$i" -lt 5000 ]; do printf 'docs/file%05d.md\n' "$i" >> "$big"; i=$((i + 1)); done
big_out="$(env "$classify" < "$big")"
big_rc=$?
rm -f "$big"
# Matched with [[ ]] on the captured string, so no pipeline or grep exec sits
# in the assertion, and each condition is reported on its own.
big_why=""
[ "$big_rc" -eq 0 ] || big_why="$big_why exit=$big_rc;"
[[ $'\n'"$big_out"$'\n' == *$'\ndocs=true\n'* ]] || big_why="$big_why docs=true line missing;"
[[ $'\n'"$big_out"$'\n' == *$'\ncode=false\n'* ]] || big_why="$big_why code=false line missing;"
[[ $'\n'"$big_out"$'\n' == *$'\ngo-deps=false\n'* ]] || big_why="$big_why go-deps=false line missing;"
if [ -z "$big_why" ]; then
  printf '  ok   %-18s classifies without SIGPIPE\n' "large-input"
else
  printf '  FAIL %-18s want exit 0 + code=false/docs=true/go-deps=false, failed:%s\n' "large-input" "$big_why" >&2
  printf '       output (%d bytes), as %%q: %q\n' "${#big_out}" "$big_out" >&2
  fails=$((fails + 1))
fi

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "classify-paths: all cases passed"

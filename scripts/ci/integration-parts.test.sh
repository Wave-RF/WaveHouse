#!/usr/bin/env bash
# Behavioral test for scripts/ci/integration-parts.sh, which reads the
# integration suite's parts out of the Makefile for CI's matrix and the
# coverage job's wait list. A part it missed would run locally and never in
# CI, so every shape it cannot read exactly must fail rather than drop a part.
# Run by `make verify` (target: test-integration-parts). Dependency-free.

set -uo pipefail
script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/integration-parts.sh"
fails=0

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
cd "$dir" || exit 1

# reads <name> <want matrix> <Makefile lines...>
reads() {
  local name="$1" want="$2" got
  shift 2
  printf '%s\n' "$@" >Makefile
  if got="$("$script" 2>/dev/null | sed -n "s/^integration-parts=//p")" && [ "$got" = "$want" ]; then
    printf '  ok   %-28s %s\n' "$name" "$got"
  else
    printf '  FAIL %-28s want %s, got %s\n' "$name" "$want" "${got:-<nothing>}" >&2
    fails=$((fails + 1))
  fi
}

# refuses <name> <Makefile lines...>: must exit non-zero.
refuses() {
  local name="$1"
  shift
  printf '%s\n' "$@" >Makefile
  if "$script" >/dev/null 2>&1; then
    printf '  FAIL %-28s should have been refused\n' "$name" >&2
    fails=$((fails + 1))
  else
    printf '  ok   %-28s refused\n' "$name"
  fi
}

# check <want: ok|fail> <part>...: --check against make's list.
check() {
  local want="$1" rc
  shift
  "$script" --check "$@" >/dev/null 2>&1
  rc=$?
  if { [ "$want" = ok ] && [ "$rc" -eq 0 ]; } || { [ "$want" = fail ] && [ "$rc" -ne 0 ]; }; then
    printf '  ok   check %-22s %s\n' "'$*'" "$want"
  else
    printf '  FAIL check %-22s want %s, exit %s\n' "'$*'" "$want" "$rc" >&2
    fails=$((fails + 1))
  fi
}

reads plain '["app","backends"]' 'INTEGRATION_PARTS := app backends' 'X := 1'
reads trailing-comment '["app"]' 'INTEGRATION_PARTS := app # the wired app'
reads extra-spaces '["a","b"]' 'INTEGRATION_PARTS   :=   a    b  '
refuses none 'X := 1'
refuses empty 'INTEGRATION_PARTS :='
refuses appended 'INTEGRATION_PARTS := app' 'INTEGRATION_PARTS += extra'
refuses reassigned 'INTEGRATION_PARTS := app' 'INTEGRATION_PARTS := other'
refuses recursive 'INTEGRATION_PARTS = app'
refuses conditional 'INTEGRATION_PARTS ?= app'
refuses posix-simple 'INTEGRATION_PARTS ::= app'
refuses override 'override INTEGRATION_PARTS := app'
refuses indented '  INTEGRATION_PARTS := app'
refuses bad-name 'INTEGRATION_PARTS := App'

printf '%s\n' 'INTEGRATION_PARTS := app backends' >Makefile
check ok app backends
check fail app
check fail app backends extra
check fail backends app

if [ "$fails" -gt 0 ]; then
  printf '%d case(s) failed\n' "$fails" >&2
  exit 1
fi

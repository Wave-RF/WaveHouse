#!/usr/bin/env bash
# Behavioral test for scripts/ci/integration-parts.sh, which reads the
# integration suite's parts out of the Makefile for CI's matrix and the
# coverage job's wait list. A part it missed would run locally and never in
# CI, so every shape it cannot read exactly must fail rather than drop a part.
# Run by `make verify` (target: test-integration-parts). Dependency-free.

set -uo pipefail
script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/integration-parts.sh"
fails=0

root="$(cd "$(dirname "$script")/../.." && pwd)"
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

# Targets as the Makefile defines them, beside the .PHONY line that names them
# (not a target) and the fixtures' own test-integration-parts (not a part).
tgt() { printf 'test-integration-%s: x\n' "$@"; }
reads plain '["app","backends"]' 'INTEGRATION_PARTS := app backends' "$(tgt app backends)" 'X := 1'
reads phony-and-fixtures '["app"]' 'INTEGRATION_PARTS := app' '.PHONY: test-integration-other' "$(tgt app parts)"
reads trailing-comment '["app"]' 'INTEGRATION_PARTS := app # the wired app' "$(tgt app)"
reads extra-spaces '["a","b"]' 'INTEGRATION_PARTS   :=   a    b  ' "$(tgt a b)"
refuses target-not-listed 'INTEGRATION_PARTS := app' "$(tgt app backends)"
refuses listed-without-target 'INTEGRATION_PARTS := app backends' "$(tgt app)"
refuses no-targets 'INTEGRATION_PARTS := app'
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

printf '%s\n' 'INTEGRATION_PARTS := app backends' "$(tgt app backends)" >Makefile
check ok app backends
check fail app
check fail app backends extra
check fail backends app

# The repository's own Makefile: its list and targets agree, and the packages
# check-integration-parts hands tagged-tests.sh follow the parts listed.
if (cd "$root" && "$script" >/dev/null 2>&1); then
  printf '  ok   %-28s agrees\n' "real Makefile"
else
  printf '  FAIL %-28s lists and targets differ\n' "real Makefile" >&2
  fails=$((fails + 1))
fi
all="$(make -C "$root" -n check-integration-parts 2>&1 | grep 'tagged-tests.sh check')"
one="$(make -C "$root" -n check-integration-parts INTEGRATION_PARTS=app 2>&1 | grep 'tagged-tests.sh check')"
if [[ "$all" == *./internal/mq* && "$one" != *./internal/mq* && "$one" == *./tests/integration* ]]; then
  printf '  ok   %-28s packages follow the parts\n' "real Makefile"
else
  printf '  FAIL %-28s packages do not follow the parts: %s | %s\n' "real Makefile" "$all" "$one" >&2
  fails=$((fails + 1))
fi

if [ "$fails" -gt 0 ]; then
  printf '%d case(s) failed\n' "$fails" >&2
  exit 1
fi

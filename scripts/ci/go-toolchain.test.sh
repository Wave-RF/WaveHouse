#!/usr/bin/env bash
# Behavioral test for scripts/ci/go-toolchain.sh, the single derivation of the
# Go toolchain the Makefile and CI pin to. Run by `make verify` (target:
# test-go-toolchain). Dependency-free.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1

script=scripts/ci/go-toolchain.sh
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
fails=0

# ok <name> <expected> <go.mod body>
ok() {
  local name="$1" want="$2" got
  printf '%b' "$3" >"$dir/go.mod"
  if got="$("$script" "$dir/go.mod" 2>/dev/null)" && [ "$got" = "$want" ]; then
    printf '  ok   %-28s -> %s\n' "$name" "$got"
  else
    printf '  FAIL %-28s want %s, got %s\n' "$name" "$want" "${got:-<none>}" >&2
    fails=$((fails + 1))
  fi
}

# rejects <name> <go.mod body>
rejects() {
  printf '%b' "$2" >"$dir/go.mod"
  if "$script" "$dir/go.mod" >/dev/null 2>&1; then
    printf '  FAIL %-28s should have been rejected\n' "$1" >&2
    fails=$((fails + 1))
  else
    printf '  ok   %-28s rejected\n' "$1"
  fi
}

ok "go line only" go1.26.6 'module m\n\ngo 1.26.6\n'
ok "toolchain line wins" go1.26.7 'module m\n\ngo 1.26.6\n\ntoolchain go1.26.7\n'
ok "trailing comment" go1.26.6 'module m\n\ngo 1.26.6 // pinned\n'
ok "indented directives" go1.26.7 'module m\n\n\tgo 1.26.6\n  toolchain go1.26.7\n'
ok "indented go line" go1.26.6 'module m\n\n  go 1.26.6\n'
rejects "toolchain suffix" 'module m\n\ngo 1.26.6\n\ntoolchain go1.26.6-custom\n'
rejects "go line suffix" 'module m\n\ngo 1.26.6-custom\n'
rejects "language version only" 'module m\n\ngo 1.27\n'
rejects "no go line" 'module m\n'

# The real go.mod must resolve, or every make target is broken.
if "$script" >/dev/null 2>&1; then
  printf '  ok   %-28s resolves\n' "repo go.mod"
else
  printf '  FAIL repo go.mod does not resolve\n' >&2
  fails=$((fails + 1))
fi

if [ "$fails" -ne 0 ]; then
  printf '%d case(s) failed\n' "$fails" >&2
  exit 1
fi

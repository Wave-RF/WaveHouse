#!/usr/bin/env bash
# Behavioral test for scripts/ci/tagged-tests.sh, which picks the tests
# `make test-integration` runs by build tag: a tagged test it misses is never
# run, and nothing reports it (#692). Each case builds a throwaway module and
# checks the selection against `go test -list`, the Go tool's own answer, so a
# name the selector's pattern would miss fails here. Run by `make verify`
# (target: test-tagged-tests).

set -uo pipefail
script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tagged-tests.sh"
fails=0

mod="$(mktemp -d)"
trap 'rm -rf "$mod"' EXIT
cd "$mod" || exit 1
export GOWORK=off GOFLAGS=-mod=mod TAGS=

# write <file> <build constraint or ""> <package> <test funcs...>
write() {
  local file="$1" constraint="$2" pkg="$3" f
  shift 3
  mkdir -p "$(dirname "$file")"
  {
    [ -n "$constraint" ] && printf '//go:build %s\n\n' "$constraint"
    printf 'package %s\n\nimport "testing"\n' "$pkg"
    for f in "$@"; do
      case "$f" in
      Fuzz*) printf '\nfunc %s(f *testing.F) { f.Add(1); f.Fuzz(func(*testing.T, int) {}) }\n' "$f" ;;
      *) printf '\nfunc %s(t *testing.T) {}\n' "$f" ;;
      esac
    done
  } >"$file"
}

printf 'module example.com/tt\n\ngo 1.21\n' >go.mod
# mixed: unit tests plus tagged ones, internal and external, one with a new
# prefix and odd-but-valid names; a TestMain and a helper are not tests.
write mixed/unit_test.go "" mixed TestUnit TestNATSUnit
write mixed/a_test.go integration mixed TestNATS_One TestBrandNewPrefix Test_underscore Test
write mixed/x_test.go integration mixed_test TestExternal FuzzTagged
write mixed/main_test.go integration mixed
printf '\nfunc TestMain(m *testing.M) { m.Run() }\n\nfunc helper(t *testing.T) {}\n' >>mixed/main_test.go
write mixed/extra_test.go "integration && extra" mixed TestNeedsExtra
write pure/p_test.go integration pure TestPure
write unit/u_test.go "" unit TestOnlyUnit
write helpers/h_test.go integration helpers
printf '\nfunc helper(t *testing.T) {}\n' >>helpers/h_test.go
# Examples (tagged and not), and a test whose signature wraps.
cat >mixed/ex_test.go <<'GO'
//go:build integration

package mixed_test

import (
	"fmt"
	"testing"
)

func Example_tagged() {
	fmt.Println("tagged")
	// Output: tagged
}

func TestWrapped(
	t *testing.T,
) {
}
GO
cat >mixed/unit_ex_test.go <<'GO'
package mixed_test

import "fmt"

func Example_untagged() {
	fmt.Println("untagged")
	// Output: untagged
}
GO

# lists <tags> <run> <pkg>: every entry `go test -list` prints, sorted, but
# its summary line (none where the tags leave the package nothing to build).
lists() { go test -tags "$1" -list "$2" "./$3" 2>/dev/null | grep -vE '^(ok|FAIL|\?)[[:space:]]' | sort; }

# selects <pkg> [tags]: the selection runs exactly the tests the tag adds.
selects() {
  local pkg="$1" extra="${2:-}" re want got
  if ! re="$(TAGS="$extra" "$script" run integration "./$pkg" 2>&1)"; then
    printf '  FAIL run %-14s exited non-zero: %s\n' "$pkg" "$re" >&2
    fails=$((fails + 1))
    return
  fi
  want="$(comm -23 <(lists "${extra:+$extra,}integration" '.*' "$pkg") <(lists "$extra" '.*' "$pkg"))"
  got="$(lists "${extra:+$extra,}integration" "$re" "$pkg")"
  if [ -n "$want" ] && [ "$got" = "$want" ]; then
    printf '  ok   run %-14s %s\n' "$pkg${extra:+ +$extra}" "$(paste -sd' ' - <<<"$got")"
  else
    printf '  FAIL run %-14s want [%s], got [%s] from %s\n' "$pkg" "$(paste -sd' ' - <<<"$want")" "$(paste -sd' ' - <<<"$got")" "$re" >&2
    fails=$((fails + 1))
  fi
}

# refuses <args...>: must exit non-zero and print no selection.
refuses() {
  local out
  if out="$("$script" "$@" 2>/dev/null)" || [ -n "$out" ]; then
    printf '  FAIL %-36s should have been refused (printed %s)\n' "$*" "${out:-nothing}" >&2
    fails=$((fails + 1))
  else
    printf '  ok   %-36s refused\n' "$*"
  fi
}

# check <want: ok|fail> <patterns...>
check() {
  local want="$1" out rc
  shift
  out="$("$script" check integration "$@" 2>&1)"
  rc=$?
  if { [ "$want" = ok ] && [ "$rc" -eq 0 ]; } || { [ "$want" = fail ] && [ "$rc" -ne 0 ] && grep -q 'example.com/tt/pure' <<<"$out"; }; then
    printf '  ok   check %-30s %s\n' "$*" "$want"
  else
    printf '  FAIL check %-30s want %s, got exit %s: %s\n' "$*" "$want" "$rc" "$out" >&2
    fails=$((fails + 1))
  fi
}

selects mixed
selects mixed extra
selects pure
refuses run integration ./unit
refuses run integration ./helpers
refuses run integration
check ok ./mixed ./pure ./helpers
check ok ./...
check fail ./mixed ./helpers

if [ "$fails" -gt 0 ]; then
  printf '%d case(s) failed\n' "$fails" >&2
  exit 1
fi

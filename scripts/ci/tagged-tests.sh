#!/usr/bin/env bash
# Select tests by build tag rather than by name, for `make test-integration`.
#
#   scripts/ci/tagged-tests.sh run <tag> <package>
#       Print a `go test -run` expression matching exactly the top-level tests
#       of <package> that only build with <tag>: the ones in its test files
#       that `go list` includes with the tag and leaves out without it. A
#       package whose untagged tests belong to the unit suite runs its tagged
#       ones this way, and a new test is selected whatever its name (#692).
#
#   scripts/ci/tagged-tests.sh check <tag> <package pattern>...
#       Fail, naming them, if any package in the module has test files that
#       only build with <tag> and none of the patterns covers it: a new
#       package of tagged tests must join one of the suite's parts.
#
# Both exit non-zero rather than print an empty selection, which `-run` would
# read as "every test". $TAGS (the Makefile's extra build tags) applies to
# both sides of the comparison. Run from the module root.

set -euo pipefail
export LC_ALL=C # comm needs sort's collation

usage() {
  echo "usage: $0 run <tag> <package> | check <tag> <package pattern>..." >&2
  exit 2
}
[ $# -ge 3 ] || usage
mode="$1" tag="$2"
shift 2
base="$(tr -s ' ' ',' <<<"${TAGS:-}" | sed 's/^,//; s/,$//')"
with="${base:+$base,}$tag"

# test_files <tags> <go list flags and packages...>: "<import path>
# <dir>/<file>" per test file.
test_files() {
  local tags="$1"
  shift
  # shellcheck disable=SC2016 # a Go template, not a shell expansion
  go list -tags "$tags" -f '{{$p := .ImportPath}}{{$d := .Dir}}{{range .TestGoFiles}}{{$p}} {{$d}}/{{.}}
{{end}}{{range .XTestGoFiles}}{{$p}} {{$d}}/{{.}}
{{end}}' "$@" | sort -u
}

# tag_only <packages...>: the test files present only with the tag. -e on the
# untagged side: a package of tagged files alone has nothing to build there.
# Captured first: a failed `go list` must not read as "no tagged tests".
tag_only() {
  local tagged untagged
  tagged="$(test_files "$with" "$@")" || return 1
  untagged="$(test_files "$base" -e "$@")" || return 1
  comm -23 <(printf '%s\n' "$tagged") <(printf '%s\n' "$untagged")
}

case "$mode" in
run)
  [ $# -eq 1 ] || usage
  files="$(tag_only "$1" | cut -d' ' -f2-)"
  if [ -z "$files" ]; then
    echo "tagged-tests: $1 has no test files that only build with '$tag'" >&2
    exit 1
  fi
  # Every top-level func named like a test, fuzz target or example: gofumpt
  # keeps `func Name(` at the start of its line however the signature wraps,
  # and a helper caught by the name only adds a pattern that matches nothing.
  # shellcheck disable=SC2086 # one path per word; Go file paths hold no spaces
  names="$({ grep -hoE '^func (Test|Fuzz|Example)[A-Za-z0-9_]*\(' $files || true; } |
    sed -E 's/^func ([A-Za-z0-9_]+)\(.*/\1/' | sort -u | paste -sd'|' -)"
  if [ -z "$names" ]; then
    echo "tagged-tests: $1's '$tag' test files declare no tests" >&2
    exit 1
  fi
  echo "^($names)\$"
  ;;
check)
  covered="$(go list -tags "$with" "$@" | sort -u)"
  missing="$(tag_only ./... | cut -d' ' -f1 | sort -u | comm -23 - <(echo "$covered"))"
  if [ -n "$missing" ]; then
    echo "tagged-tests: packages with '$tag' tests that no part of the suite runs:" >&2
    while IFS= read -r pkg; do echo "  $pkg" >&2; done <<<"$missing"
    echo "Add each to a part of the suite in the Makefile." >&2
    exit 1
  fi
  ;;
*) usage ;;
esac

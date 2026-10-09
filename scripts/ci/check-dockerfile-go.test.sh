#!/usr/bin/env bash
# Behavioral test for scripts/ci/check-dockerfile-go.sh, which requires the
# Dockerfile's golang image tag to equal the Go version go.mod pins. Run by
# `make verify` (target: check-dockerfile-go). Dependency-free.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1

script=scripts/ci/check-dockerfile-go.sh
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
fails=0

# run <go.mod body> <Dockerfile body>: exit status of the check
run() {
  printf '%b' "$1" >"$dir/go.mod"
  printf '%b' "$2" >"$dir/Dockerfile"
  "$script" "$dir/Dockerfile" "$dir/go.mod" >/dev/null 2>&1
}

# passes <name> <go.mod body> <Dockerfile body>
passes() {
  if run "$2" "$3"; then
    printf '  ok   %-28s passes\n' "$1"
  else
    printf '  FAIL %-28s should have passed\n' "$1" >&2
    fails=$((fails + 1))
  fi
}

# rejects <name> <go.mod body> <Dockerfile body>
rejects() {
  if run "$2" "$3"; then
    printf '  FAIL %-28s should have been rejected\n' "$1" >&2
    fails=$((fails + 1))
  else
    printf '  ok   %-28s rejected\n' "$1"
  fi
}

mod='module m\n\ngo 1.26.9\n'
digest='sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'

passes "match" "$mod" 'FROM golang:1.26.9-alpine AS builder\nFROM scratch\n'
passes "match, no variant" "$mod" 'FROM golang:1.26.9\n'
passes "digest" "$mod" "FROM golang:1.26.9-alpine@$digest AS builder\n"
passes "platform flag" "$mod" "FROM --platform=\$BUILDPLATFORM golang:1.26.9-alpine AS builder\n"
passes "lowercase from" "$mod" 'from golang:1.26.9-alpine as builder\n'
passes "toolchain line wins" 'module m\n\ngo 1.26.6\n\ntoolchain go1.26.9\n' 'FROM golang:1.26.9-alpine\n'
rejects "mismatch" "$mod" 'FROM golang:1.26.8-alpine AS builder\n'
rejects "minor-only tag" "$mod" 'FROM golang:1.26-alpine AS builder\n'
rejects "latest tag" "$mod" 'FROM golang:alpine\n'
rejects "mismatch with digest" "$mod" "FROM golang:1.26.8-alpine@$digest\n"
rejects "mismatch with platform" "$mod" "FROM --platform=\$BUILDPLATFORM golang:1.26.8-alpine\n"
rejects "one of two stages stale" "$mod" 'FROM golang:1.26.9-alpine AS a\nFROM golang:1.26.8-alpine AS b\n'
rejects "toolchain line overrides go" 'module m\n\ngo 1.26.9\n\ntoolchain go1.26.10\n' 'FROM golang:1.26.9-alpine\n'
rejects "no golang FROM" "$mod" 'FROM alpine:3.20\nRUN true\n'
rejects "golang only in a comment" "$mod" '# FROM golang:1.26.9-alpine\nFROM alpine:3.20\n'
rejects "unpinnable go.mod" 'module m\n\ngo 1.27\n' 'FROM golang:1.27-alpine\n'

# The real Dockerfile must match the real go.mod, or the gate is already red.
if "$script" >/dev/null 2>&1; then
  printf '  ok   %-28s matches\n' "repo Dockerfile"
else
  printf '  FAIL repo Dockerfile does not match go.mod\n' >&2
  fails=$((fails + 1))
fi

if [ "$fails" -ne 0 ]; then
  printf '%d case(s) failed\n' "$fails" >&2
  exit 1
fi

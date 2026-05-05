#!/usr/bin/env bash
# Run one Go test suite (unit / integration / e2e) with coverage instrumentation,
# then render + gate via scripts/coverage.sh.
#
# Per-suite knobs (build tags, package globs, gotestsum format) live in the
# case block below. Adding a new suite is one block here + one Makefile target
# that calls `scripts/test-suite.sh <new-suite>`.
#
# Convention: each suite writes covdata to tmp/coverage/<suite>/data/, which
# scripts/coverage.sh then renders alongside the textfmt + HTML.

set -euo pipefail

# shellcheck source=scripts/_colors.sh
. "$(dirname "$0")/_colors.sh"

suite=${1:?suite name required (unit|integration|e2e)}
shift || true
extra_args=("$@")  # passthrough for `make test ARGS="..."`

ROOT=tmp/coverage
DATA_DIR="$ROOT/$suite/data"

# Threshold defaults match the Makefile. Override via env (the Makefile recipe
# does this when COV_THRESHOLD_<SUITE> is set).
case "$suite" in
unit)         threshold=${COV_THRESHOLD_UNIT:-70} ;;
integration)  threshold=${COV_THRESHOLD_INT:-30} ;;
e2e)          threshold=${COV_THRESHOLD_E2E:-10} ;;
*)
	printf '%sUnknown suite: %s%s\n' "$RED" "$suite" "$RESET" >&2
	exit 2
	;;
esac

# Per-suite go-test invocation. The /testutil package is filtered out at
# `go list` time — it has no tests of its own, only serves as helpers for
# other packages' tests.
case "$suite" in
unit)
	printf "%s==> Running Unit Tests...%s\n" "$CYAN" "$RESET"
	rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR"
	pkgs=$(go list ./internal/... | grep -v /testutil)
	# shellcheck disable=SC2086  # $pkgs is intentionally word-split into args
	GOCOVERDIR="$PWD/$DATA_DIR" go tool gotestsum \
		--format "${GOTESTSUM_FMT:-pkgname-and-test-fails}" -- \
		-tags="${TAGS:-}" -cover -race $pkgs "${extra_args[@]}" \
		-args -test.gocoverdir="$PWD/$DATA_DIR"
	;;

integration)
	printf "%s==> Running Integration Tests...%s\n" "$CYAN" "$RESET"
	rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR"
	pkgs=$(go list ./tests/integration/... | grep -v /testutil)
	# shellcheck disable=SC2086  # $pkgs is intentionally word-split into args
	GOCOVERDIR="$PWD/$DATA_DIR" go tool gotestsum \
		--format "${GOTESTSUM_FMT:-pkgname-and-test-fails}" -- \
		-tags="integration ${TAGS:-}" -timeout 120s -coverpkg=./... -race -count=1 \
		$pkgs "${extra_args[@]}" \
		-args -test.gocoverdir="$PWD/$DATA_DIR"
	;;

e2e)
	# TODO: refactor into a Go test-containers harness in tests/. For now this
	# matches the legacy Makefile recipe: build the coverage-instrumented
	# binary, run the SDK suite against it, SIGINT to flush coverage.
	printf "%s==> Running E2E Tests...%s\n" "$CYAN" "$RESET"
	rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR" tmp
	make build-cover >/dev/null

	printf "%s==> Starting instrumented binary...%s\n" "$YELLOW" "$RESET"
	GOCOVERDIR="$PWD/$DATA_DIR" bin/wavehouse-cov &
	pid=$!
	echo "$pid" >tmp/wavehouse.pid
	sleep 2

	printf "%s==> Running SDK E2E suite...%s\n" "$YELLOW" "$RESET"
	if ! (cd tests/e2e/sdk && npm install --silent && npx vitest run); then
		kill -SIGINT "$pid" 2>/dev/null || true
		rm -f tmp/wavehouse.pid
		exit 1
	fi

	printf "%s==> Stopping binary (flushes coverage)...%s\n" "$YELLOW" "$RESET"
	kill -SIGINT "$pid" 2>/dev/null || true
	wait "$pid" 2>/dev/null || true
	rm -f tmp/wavehouse.pid
	;;
esac

# Render + gate.
"$(dirname "$0")/coverage.sh" render "$suite" "$threshold"

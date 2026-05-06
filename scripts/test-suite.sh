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
integration)  threshold=${COV_THRESHOLD_INTEGRATION:-12} ;;
e2e)          threshold=${COV_THRESHOLD_E2E:-50} ;;
*)
	printf '%sUnknown suite: %s%s\n' "$RED" "$suite" "$RESET" >&2
	exit 2
	;;
esac

# Per-suite go-test invocation. Coverage exclusions (e.g. internal/testutil/)
# live in .testcoverage.yml — the single source for "what doesn't count."
#
# Coverage scope intentionally differs by suite:
#   unit:         -cover (per-package — each test covers its own package).
#                 No -coverpkg because unit tests should map 1:1 to the
#                 package under test.
#   integration:  -cover -coverpkg=./... (cross-package — integration tests
#                 drive end-to-end paths and should attribute coverage to
#                 every package those paths touch, not just the test pkg).
#   e2e:          coverage comes from running the cover-instrumented binary;
#                 the process flushes covdata on SIGINT, capturing whatever
#                 the binary actually executed.
#
# `make cov` merges all three into tmp/coverage/total/ and gates against
# COV_THRESHOLD_TOTAL.
case "$suite" in
unit)
	printf "%s==> Running Unit Tests...%s\n" "$CYAN" "$RESET"
	rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR"
	GOCOVERDIR="$PWD/$DATA_DIR" go tool gotestsum \
		--format "${GOTESTSUM_FMT:-pkgname-and-test-fails}" -- \
		-tags="${TAGS:-}" -cover -race ./internal/... "${extra_args[@]}" \
		-args -test.gocoverdir="$PWD/$DATA_DIR"
	;;

integration)
	printf "%s==> Running Integration Tests...%s\n" "$CYAN" "$RESET"
	rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR"
	GOCOVERDIR="$PWD/$DATA_DIR" go tool gotestsum \
		--format "${GOTESTSUM_FMT:-pkgname-and-test-fails}" -- \
		-tags="integration ${TAGS:-}" -timeout 120s -coverpkg=./... -race -count=1 \
		./tests/integration/... "${extra_args[@]}" \
		-args -test.gocoverdir="$PWD/$DATA_DIR"
	;;

e2e)
	printf "%s==> Running E2E Tests...%s\n" "$CYAN" "$RESET"
	rm -rf "$DATA_DIR" && mkdir -p "$DATA_DIR" tmp

	if [ ! -x "$PWD/bin/wavehouse-cov" ]; then
		printf '%sERROR: bin/wavehouse-cov missing — run `make build-cover` first.%s\n' \
			"$RED" "$RESET" >&2
		exit 1
	fi

	# The orchestrator now picks a random free port for the cover binary
	# (WH_SERVER_PORT) so a `make dev` instance on :8080 no longer
	# competes — no pre-flight conflict check needed.
	go run ./tests/e2e/orchestrator
	;;
esac

# Render + gate.
"$(dirname "$0")/coverage.sh" render "$suite" "$threshold"

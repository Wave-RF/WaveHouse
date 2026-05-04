#!/usr/bin/env bash
# Coverage helpers extracted from the Makefile.
#
# Subcommands:
#   unit    Render unit-test coverage (called by `make coverage`).
#   report  Merge whichever of unit/integration/e2e covdata exists, print
#           per-suite + combined percentages, gate against .testcoverage.yml
#           (called by `make coverage-report`).
#
# Reads color + path env vars from the Makefile recipe so this script doesn't
# have to re-implement NO_COLOR detection or path constants. Defaults are set
# below so the script can also be run directly during development.

set -euo pipefail

: "${CYAN:=}"
: "${GREEN:=}"
: "${YELLOW:=}"
: "${RED:=}"
: "${RESET:=}"

# Returns 0 if $1 is a directory containing covdata (covmeta.* files).
has_covdata() {
	local d=$1
	[ -d "$d" ] && compgen -G "$d/covmeta.*" >/dev/null
}

# Print the weighted total percentage from a covdata directory, or "n/a".
suite_pct() {
	local d=$1
	if ! has_covdata "$d"; then
		printf "n/a"
		return
	fi
	local tmp
	tmp=$(mktemp)
	if go tool covdata textfmt -i="$d" -o "$tmp" 2>/dev/null && [ -s "$tmp" ]; then
		go tool cover -func="$tmp" 2>/dev/null | tail -n 1 | awk '{print $NF}'
	else
		printf "n/a"
	fi
	rm -f "$tmp"
}

case "${1:-}" in
unit)
	: "${COV_UNIT:?COV_UNIT must be set}"
	: "${COV_OUT:?COV_OUT must be set}"
	mkdir -p "$COV_OUT"
	if ! has_covdata "$COV_UNIT"; then
		printf '%s==> No unit covdata in %s. Did "make test" run?%s\n' \
			"$RED" "$COV_UNIT" "$RESET" >&2
		exit 1
	fi
	go tool covdata textfmt -i="$COV_UNIT" -o "$COV_OUT/coverage.txt"
	go tool cover -html="$COV_OUT/coverage.txt" -o "$COV_OUT/coverage.html"
	printf "%s==> Unit coverage:%s\n" "$CYAN" "$RESET"
	total=$(go tool cover -func="$COV_OUT/coverage.txt" | tail -n 1 | awk '{print $NF}')
	printf "  Total: %s%s%s\n" "$YELLOW" "$total" "$RESET"
	printf "  HTML: %s\n" "$COV_OUT/coverage.html"
	;;
report)
	: "${COV_UNIT:?COV_UNIT must be set}"
	: "${COV_INT:?COV_INT must be set}"
	: "${COV_E2E:?COV_E2E must be set}"
	: "${COV_OUT:?COV_OUT must be set}"
	: "${GOCOVER:?GOCOVER must be set}"
	mkdir -p "$COV_OUT"
	printf "%s==> Merging coverage data...%s\n" "$CYAN" "$RESET"

	dirs=()
	for d in "$COV_UNIT" "$COV_INT" "$COV_E2E"; do
		has_covdata "$d" && dirs+=("$d")
	done
	if [ ${#dirs[@]} -eq 0 ]; then
		printf "%s==> No coverage data found. Run test / test-integration / test-e2e first.%s\n" \
			"$RED" "$RESET" >&2
		exit 1
	fi

	joined=$(IFS=,; echo "${dirs[*]}")
	printf "    Sources: %s\n" "$joined"
	go tool covdata textfmt -i="$joined" -o "$COV_OUT/coverage.txt"
	go tool cover -html="$COV_OUT/coverage.txt" -o "$COV_OUT/coverage.html"

	printf "\n%s==> Per-suite coverage (informational):%s\n" "$CYAN" "$RESET"
	for d in "$COV_UNIT" "$COV_INT" "$COV_E2E"; do
		pct=$(suite_pct "$d")
		printf "  %s%-12s%s %s\n" "$CYAN" "$(basename "$d"):" "$RESET" "$pct"
	done

	printf "\n%s==> Combined coverage (gated):%s\n" "$CYAN" "$RESET"
	# shellcheck disable=SC2086  # GOCOVER is intentionally unquoted to word-split
	$GOCOVER --config=.testcoverage.yml
	printf "%s==> HTML report: %s%s\n" "$GREEN" "$COV_OUT/coverage.html" "$RESET"
	;;
*)
	printf "Usage: %s {unit|report}\n" "$0" >&2
	exit 2
	;;
esac

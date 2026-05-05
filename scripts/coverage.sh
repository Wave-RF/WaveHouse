#!/usr/bin/env bash
# Coverage rendering + threshold gating.
#
# Usage:
#   coverage.sh render <suite> <threshold>   render that suite's covdata to
#                                            text + HTML, gate against the
#                                            given threshold percentage.
#   coverage.sh merge <threshold>            merge whichever suites have
#                                            covdata, render to total/, gate.
#
# Convention (no env vars to pass): each suite lives at tmp/coverage/<suite>/
#   data/         binary covdata (covmeta.* / covcounters.*)
#   coverage.txt  rendered textfmt
#   coverage.html rendered HTML

set -euo pipefail

# shellcheck source=scripts/_colors.sh
. "$(dirname "$0")/_colors.sh"

ROOT=tmp/coverage
SUITES=(unit integration e2e)

# Returns 0 if $1 is a covdata directory containing covmeta.* files.
has_covdata() {
	local d=$1
	[ -d "$d" ] && compgen -G "$d/covmeta.*" >/dev/null
}

# Render covdata in $1/data → $1/coverage.{txt,html}, then echo the total %.
render() {
	local dir=$1
	go tool covdata textfmt -i="$dir/data" -o "$dir/coverage.txt"
	go tool cover -html="$dir/coverage.txt" -o "$dir/coverage.html"
	go tool cover -func="$dir/coverage.txt" | tail -n 1 | awk '{print $NF}'
}

# Threshold gate via go-test-coverage with CLI override. Single config sources
# exclusions (testutil, tests/); per-suite numbers come from the CLI.
gate() {
	local profile=$1 threshold=$2
	go tool go-test-coverage \
		--config=.testcoverage.yml \
		--profile="$profile" \
		--threshold-total="$threshold"
}

case "${1:-}" in
render)
	suite=${2:?suite name required (unit|integration|e2e)}
	threshold=${3:?threshold percentage required}
	dir="$ROOT/$suite"

	if ! has_covdata "$dir/data"; then
		printf '%s==> No covdata in %s/data. Did the test target run?%s\n' \
			"$RED" "$dir" "$RESET" >&2
		exit 1
	fi

	total=$(render "$dir")
	echo
	printf "%s==> %s coverage: %s%s%s  (threshold: %s%%)\n" \
		"$CYAN" "$suite" "$YELLOW" "$total" "$RESET" "$threshold"
	printf "  HTML: %s/coverage.html\n" "$dir"
	echo

	if gate "$dir/coverage.txt" "$threshold"; then
		printf "%s==> %s gate passed (≥ %s%%)%s\n" "$GREEN" "$suite" "$threshold" "$RESET"
	else
		printf "%s==> %s gate FAILED (below %s%%)%s\n" "$RED" "$suite" "$threshold" "$RESET" >&2
		exit 1
	fi
	;;

merge)
	threshold=${2:?total threshold percentage required}
	mkdir -p "$ROOT/total"

	printf "%s==> Merging coverage data...%s\n" "$CYAN" "$RESET"

	dirs=()
	for suite in "${SUITES[@]}"; do
		path="$ROOT/$suite/data"
		if has_covdata "$path"; then
			dirs+=("$path")
			printf "  %s✔%s %-13s %s\n" "$GREEN" "$RESET" "$suite" "$path"
		else
			suggested="test-$suite"
			[ "$suite" = "unit" ] && suggested="test"
			printf "  %s✗%s %-13s (no covdata; run \`make %s\` to include)\n" \
				"$YELLOW" "$RESET" "$suite" "$suggested"
		fi
	done

	if [ ${#dirs[@]} -eq 0 ]; then
		printf "%s==> No coverage data anywhere. Run make test (or test-all) first.%s\n" \
			"$RED" "$RESET" >&2
		exit 1
	fi

	# Merge.
	joined=$(IFS=,; echo "${dirs[*]}")
	go tool covdata textfmt -i="$joined" -o "$ROOT/total/coverage.txt"
	go tool cover -html="$ROOT/total/coverage.txt" -o "$ROOT/total/coverage.html"

	# Per-suite breakdown for context.
	echo
	printf "%s==> Per-suite breakdown:%s\n" "$CYAN" "$RESET"
	for suite in "${SUITES[@]}"; do
		dir="$ROOT/$suite"
		if has_covdata "$dir/data"; then
			if [ -f "$dir/coverage.txt" ]; then
				pct=$(go tool cover -func="$dir/coverage.txt" | tail -n 1 | awk '{print $NF}')
			else
				tmp=$(mktemp)
				go tool covdata textfmt -i="$dir/data" -o "$tmp" 2>/dev/null
				pct=$(go tool cover -func="$tmp" | tail -n 1 | awk '{print $NF}')
				rm -f "$tmp"
			fi
		else
			pct="n/a"
		fi
		printf "  %s%-13s%s %s\n" "$CYAN" "$suite:" "$RESET" "$pct"
	done

	echo
	printf "%s==> Combined coverage gate (threshold %s%%):%s\n" "$CYAN" "$threshold" "$RESET"
	if gate "$ROOT/total/coverage.txt" "$threshold"; then
		printf "%s==> Total gate passed%s\n" "$GREEN" "$RESET"
		printf "  HTML: %s/total/coverage.html\n" "$ROOT"
	else
		printf "%s==> Total gate FAILED%s\n" "$RED" "$RESET" >&2
		exit 1
	fi
	;;

*)
	printf "Usage: %s {render <suite> <threshold> | merge <threshold>}\n" "$0" >&2
	exit 2
	;;
esac

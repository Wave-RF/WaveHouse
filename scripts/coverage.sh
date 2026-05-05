#!/usr/bin/env bash
# Coverage rendering + threshold gating.
#
# Subcommands:
#   render-suite <name>  Render covdata for one suite (unit / integration / e2e)
#                        to its own output dir, render HTML, gate against
#                        the suite's threshold.
#   merge-total          Aggregate whichever covdata exists across all suites,
#                        render to tmp/coverage/total/, gate against the
#                        total threshold. (Used by `make cov`.)
#
# Reads color env vars + path/threshold env vars from the Makefile recipe.
# Threshold gating uses `go tool go-test-coverage --config=.testcoverage.yml`
# with a CLI override for `--threshold-total` so per-suite numbers can vary
# while exclusions (testutil, tests/) live in one config.

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

# Render covdata in $1 → text + HTML in $2. Echoes the total percentage.
render_one() {
	local covdir=$1 outdir=$2
	mkdir -p "$outdir"
	go tool covdata textfmt -i="$covdir" -o "$outdir/coverage.txt"
	go tool cover -html="$outdir/coverage.txt" -o "$outdir/coverage.html"
	go tool cover -func="$outdir/coverage.txt" | tail -n 1 | awk '{print $NF}'
}

# Run the threshold gate on a profile, with an explicit threshold override.
# Returns the gate's exit code (0 = pass, non-zero = below threshold).
gate() {
	local profile=$1 threshold=$2
	go tool go-test-coverage \
		--config=.testcoverage.yml \
		--profile="$profile" \
		--threshold-total="$threshold"
}

case "${1:-}" in
render-suite)
	suite=${2:?suite name required (unit|integration|e2e)}
	covdir_var="COV_${suite^^}"
	outdir_var="COV_OUT_${suite^^}"
	threshold_var="COV_THRESHOLD_${suite^^}"
	# Map shell-quoted "INTEGRATION" → COV_INT etc. for compactness in Make.
	case "$suite" in
		integration) covdir_var="COV_INT"; outdir_var="COV_OUT_INT"; threshold_var="COV_THRESHOLD_INT" ;;
	esac
	covdir=${!covdir_var:?}
	outdir=${!outdir_var:?}
	threshold=${!threshold_var:?}

	if ! has_covdata "$covdir"; then
		printf '%s==> No covdata in %s. Did the test target run?%s\n' "$RED" "$covdir" "$RESET" >&2
		exit 1
	fi

	total=$(render_one "$covdir" "$outdir")
	echo
	printf "%s==> %s coverage: %s%s%s  (threshold: %s%%)\n" \
		"$CYAN" "$suite" "$YELLOW" "$total" "$RESET" "$threshold"
	printf "  HTML: %s/coverage.html\n" "$outdir"
	echo

	# Gate. Output is informative on its own; we want it visible.
	if gate "$outdir/coverage.txt" "$threshold"; then
		printf "%s==> %s gate passed (≥ %s%%)%s\n" "$GREEN" "$suite" "$threshold" "$RESET"
	else
		printf "%s==> %s gate FAILED (below %s%%)%s\n" "$RED" "$suite" "$threshold" "$RESET" >&2
		exit 1
	fi
	;;

merge-total)
	: "${COV_UNIT:?}" "${COV_INT:?}" "${COV_E2E:?}" "${COV_OUT_TOTAL:?}" "${COV_THRESHOLD_TOTAL:?}"

	mkdir -p "$COV_OUT_TOTAL"
	printf "%s==> Merging coverage data...%s\n" "$CYAN" "$RESET"

	dirs=()
	missing=()
	for pair in "unit:$COV_UNIT" "integration:$COV_INT" "e2e:$COV_E2E"; do
		name=${pair%%:*}
		path=${pair#*:}
		if has_covdata "$path"; then
			dirs+=("$path")
			printf "  %s✔%s %-13s %s\n" "$GREEN" "$RESET" "$name" "$path"
		else
			missing+=("$name")
			printf "  %s✗%s %-13s (no covdata; run make %s if you want it included)\n" "$YELLOW" "$RESET" "$name" \
				"$([ "$name" = "integration" ] && echo test-integration || echo "test-$name")"
		fi
	done

	if [ ${#dirs[@]} -eq 0 ]; then
		printf "%s==> No coverage data anywhere. Run make test (or test-all) first.%s\n" "$RED" "$RESET" >&2
		exit 1
	fi

	# Merge.
	joined=$(IFS=,; echo "${dirs[*]}")
	go tool covdata textfmt -i="$joined" -o "$COV_OUT_TOTAL/coverage.txt"
	go tool cover -html="$COV_OUT_TOTAL/coverage.txt" -o "$COV_OUT_TOTAL/coverage.html"

	# Per-suite breakdown for context (uses each suite's already-rendered
	# coverage.txt if it exists, else computes from covdir).
	echo
	printf "%s==> Per-suite breakdown:%s\n" "$CYAN" "$RESET"
	for pair in "unit:$COV_UNIT:$COV_OUT_UNIT" "integration:$COV_INT:$COV_OUT_INT" "e2e:$COV_E2E:$COV_OUT_E2E"; do
		name=${pair%%:*}
		rest=${pair#*:}
		covdir=${rest%%:*}
		outdir=${rest#*:}
		if has_covdata "$covdir"; then
			if [ -f "$outdir/coverage.txt" ]; then
				pct=$(go tool cover -func="$outdir/coverage.txt" | tail -n 1 | awk '{print $NF}')
			else
				# Render on the fly if test target hasn't run since covdata appeared.
				tmp=$(mktemp)
				go tool covdata textfmt -i="$covdir" -o "$tmp" 2>/dev/null
				pct=$(go tool cover -func="$tmp" | tail -n 1 | awk '{print $NF}')
				rm -f "$tmp"
			fi
		else
			pct="n/a"
		fi
		printf "  %s%-13s%s %s\n" "$CYAN" "$name:" "$RESET" "$pct"
	done

	echo
	printf "%s==> Combined coverage gate (threshold %s%%):%s\n" "$CYAN" "$COV_THRESHOLD_TOTAL" "$RESET"
	if gate "$COV_OUT_TOTAL/coverage.txt" "$COV_THRESHOLD_TOTAL"; then
		printf "%s==> Total gate passed%s\n" "$GREEN" "$RESET"
		printf "  HTML: %s/coverage.html\n" "$COV_OUT_TOTAL"
	else
		printf "%s==> Total gate FAILED%s\n" "$RED" "$RESET" >&2
		exit 1
	fi
	;;

*)
	printf "Usage: %s {render-suite <unit|integration|e2e> | merge-total}\n" "$0" >&2
	exit 2
	;;
esac

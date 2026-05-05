#!/usr/bin/env bash
# Top cuttable dependencies by transitive impact, via goda.
#
# Surfaces packages with few dependents (low InDegree) that pull in heavy
# transitive weight — the best candidates to remove or replace.
#
# Override the default top-N with `LIMIT=50 scripts/dep-cut.sh`.

set -euo pipefail

: "${CYAN:=}"
: "${YELLOW:=}"
: "${RESET:=}"

limit=${LIMIT:-30}

printf "%s==> Dependency cut analysis (top %d, InDegree ≤ 3):%s\n" "$CYAN" "$limit" "$RESET"
echo "  Packages with few dependents that pull in the most transitive weight."
echo

# awk does the limiting itself — piping to `head` would cause SIGPIPE under
# pipefail when head closes the pipe before goda finishes streaming.
go tool goda cut ./...:all 2>/dev/null |
	awk -v limit="$limit" '
		NR==1 { printf "  %-58s %4s %5s %10s\n", "Package", "Deps", "Pkgs", "Size"; next }
		$2+0 <= 3 && shown < limit {
			name=$1; gsub(/github\.com\//, "", name)
			printf "  %-58s %4s %5s %10s\n", name, $2, $3, $4
			shown++
		}'
echo
printf "  %sFull output: go tool goda cut ./...:all%s\n" "$CYAN" "$RESET"

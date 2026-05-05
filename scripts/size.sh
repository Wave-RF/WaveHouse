#!/usr/bin/env bash
# Binary size analysis via go-size-analyzer (gsa).
#
# Builds both debug (bin/wavehouse) and release (bin/wavehouse-release) if
# they aren't already present, runs gsa on each, and reports a side-by-side
# comparison plus an interactive HTML treemap. Auto-opens the HTML in the
# default browser unless CI=1.

set -euo pipefail

# shellcheck source=scripts/_colors.sh
. "$(dirname "$0")/_colors.sh"

export GOEXPERIMENT=jsonv2  # gsa imports encoding/json/v2

DEBUG_BIN=bin/wavehouse
RELEASE_BIN=bin/wavehouse-release
OUT_DIR=tmp/analysis

if [ ! -f "$DEBUG_BIN" ]; then
	printf '%s==> %s not found. Run "make build" first.%s\n' "$RED" "$DEBUG_BIN" "$RESET" >&2
	exit 1
fi

if [ ! -f "$RELEASE_BIN" ]; then
	printf '%s==> %s not found. Building it for size comparison...%s\n' "$YELLOW" "$RELEASE_BIN" "$RESET"
	make build-release >/dev/null
fi

mkdir -p "$OUT_DIR"

# Portable size in bytes — `wc -c` is in POSIX, `stat`'s flags aren't.
size_bytes() { wc -c <"$1" | tr -d ' '; }

debug_total=$(size_bytes "$DEBUG_BIN")
release_total=$(size_bytes "$RELEASE_BIN")

human() {
	awk -v b="$1" 'BEGIN {
		if (b > 1024*1024*1024) printf "%.1fG", b/(1024*1024*1024)
		else if (b > 1024*1024)  printf "%.1fM", b/(1024*1024)
		else if (b > 1024)       printf "%.1fK", b/1024
		else                     printf "%dB", b
	}'
}

# Heads-up on a single common point of confusion in the gsa output.
# Section-name reference lives in docs/, not reprinted every run.
printf '%s==> Reading the gsa output:%s\n' "$CYAN" "$RESET"
printf '  %s"CGO" rows are mostly Go reflection metadata, not C code%s — this build is CGO_ENABLED=0.\n' \
	"$YELLOW" "$RESET"
printf '  Focus on large NAMED packages (vendor / std); treat CGO/Unknown rows as noise.\n\n'

# ── Side-by-side debug / release comparison.
debug_overhead=$((debug_total - release_total))
overhead_pct=$(awk -v a="$debug_overhead" -v b="$debug_total" 'BEGIN { printf "%.0f%%", 100*a/b }')
printf '%s==> Binary sizes:%s\n' "$CYAN" "$RESET"
printf '  %sDebug   %s (%s)  %s%s%s  — DWARF kept; what '"'"'make build'"'"' produces\n' \
	"$CYAN" "$RESET" "$DEBUG_BIN" "$YELLOW" "$(human "$debug_total")" "$RESET"
printf '  %sRelease %s (%s)  %s%s%s  — stripped; what '"'"'make build-release'"'"' produces\n' \
	"$CYAN" "$RESET" "$RELEASE_BIN" "$YELLOW" "$(human "$release_total")" "$RESET"
printf '  %sOverhead%s                          %s%s%s  — DWARF + symbol tables removed by -s -w (%s of debug)\n' \
	"$CYAN" "$RESET" "$YELLOW" "$(human "$debug_overhead")" "$RESET" "$overhead_pct"

# ── Per-package breakdown of the debug binary (more accurate attribution
#    because DWARF is intact). We render artifacts only once, against debug.
printf '\n%s==> Per-package breakdown (debug binary, gsa):%s\n\n' "$CYAN" "$RESET"
go tool gsa --format text --hide-sections "$DEBUG_BIN" 2>/dev/null
echo

go tool gsa --format svg --output "$OUT_DIR/size-map.svg" --hide-sections "$DEBUG_BIN" 2>/dev/null
printf "  ${CYAN}SVG  → %s/size-map.svg${RESET}\n" "$OUT_DIR"

go tool gsa --format html --output "$OUT_DIR/size-map.html" --hide-sections "$DEBUG_BIN" 2>/dev/null
printf "  ${CYAN}HTML → %s/size-map.html (interactive treemap)${RESET}\n" "$OUT_DIR"

# Auto-open unless under CI.
if [ -z "${CI:-}" ]; then
	if command -v open >/dev/null 2>&1; then
		open "$OUT_DIR/size-map.html"
	elif command -v xdg-open >/dev/null 2>&1; then
		xdg-open "$OUT_DIR/size-map.html"
	fi
fi

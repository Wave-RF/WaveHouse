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

# Compute total bytes for both binaries via the Go script. binary-overhead.go
# emits `total_bytes=N` and `dwarf_bytes=N`; we only use total_bytes here.
total_bytes=0
eval "$(go run scripts/binary-overhead.go "$DEBUG_BIN")"
debug_total=$total_bytes

eval "$(go run scripts/binary-overhead.go "$RELEASE_BIN")"
release_total=$total_bytes

human() {
	awk -v b="$1" 'BEGIN {
		if (b > 1024*1024*1024) printf "%.1fG", b/(1024*1024*1024)
		else if (b > 1024*1024)  printf "%.1fM", b/(1024*1024)
		else if (b > 1024)       printf "%.1fK", b/1024
		else                     printf "%dB", b
	}'
}

# ── Reading-the-output guide (printed at the start so it's visible at runtime,
#    not buried in source-code comments). The gsa table that follows uses some
#    labels that look alarming or cryptic; this preamble explains them.
cat <<EOF
${CYAN}==> Reading the gsa output:${RESET}

  ${YELLOW}"CGO" label is misleading on this binary.${RESET} The build is
  CGO_ENABLED=0 (verify: ${CYAN}go version -m bin/wavehouse | grep CGO${RESET}).
  gsa heuristically labels symbols whose names start with ${CYAN}_${RESET} as
  CGO — that's the C ABI convention on darwin — but Go emits ${CYAN}_type:*${RESET}
  symbols for reflection type descriptors that also start with underscore.
  The bulk of the bytes under "CGO" here are Go type metadata, not C code.

  ${YELLOW}Common unattributed sections you'll see:${RESET}
    ${CYAN}__text${RESET}        executable code
    ${CYAN}__gopclntab${RESET}   Go PC-line table — stack traces, panic recovery, profiling
    ${CYAN}__rodata${RESET}      string constants, type info, error messages
    ${CYAN}__zdebug_*${RESET}    DWARF debug info (kept in debug build, stripped via -s -w)
    ${CYAN}__typelink${RESET}    pointer fixups for Go's runtime type system

  ${YELLOW}Signal vs noise:${RESET} focus on the large NAMED packages
  (vendor / std). Treat "CGO" and "Unknown" rows as noise unless you're
  specifically investigating reflection or generic-instantiation bloat.

EOF

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

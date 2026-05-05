#!/usr/bin/env bash
# Binary size analysis via go-size-analyzer (gsa).
#
# Renders text + SVG + interactive HTML treemap. Auto-opens the HTML in the
# default browser unless CI=1.
#
# Reading the gsa output:
#   * "CGO" label is misleading on darwin builds. gsa classifies symbols with
#     leading underscore as CGO (because that's the C ABI convention on
#     macOS), but Go's runtime emits `_type:*` symbols for reflection type
#     descriptors that also start with underscore. The bulk of the bytes
#     under "CGO" for this binary are Go type metadata, not C code. Confirm
#     no actual C is linked: `go version -m bin/wavehouse | grep CGO_ENABLED`.
#   * Large unattributed sections you'll see:
#       __text        executable code
#       __gopclntab   PC-line table (stack traces, panic recovery, profiling)
#       __rodata      string constants and read-only data
#       __zdebug_*    DWARF debug info (kept in `make build`, stripped in
#                     `make build-release` via -s -w)
#       __typelink    type relocations for Go's runtime type system
#   * Signal: focus on the large NAMED packages (vendor/std). Treat
#     "CGO" and "Unknown" rows as noise unless you're specifically
#     investigating reflection bloat.

set -euo pipefail

: "${CYAN:=}"
: "${GREEN:=}"
: "${YELLOW:=}"
: "${RED:=}"
: "${RESET:=}"

# go-size-analyzer imports encoding/json/v2, still gated behind a build
# experiment in current Go releases.
export GOEXPERIMENT=jsonv2

BIN=bin/wavehouse
OUT_DIR=tmp/analysis

if [ ! -f "$BIN" ]; then
	printf '%s==> %s not found. Run "make build" first.%s\n' "$RED" "$BIN" "$RESET" >&2
	exit 1
fi

mkdir -p "$OUT_DIR"

# Compute total + DWARF bytes via a small Go script that reads the binary
# directly (debug/macho or debug/elf from stdlib). Avoids a python3 / jq
# dependency, and is more robust than parsing gsa's JSON schema.
total_bytes=0  # set by the eval below
dwarf_bytes=0
eval "$(go run scripts/binary-overhead.go "$BIN")"
stripped_bytes=$((total_bytes - dwarf_bytes))

# Human-readable byte formatting.
human() {
	awk -v b="$1" 'BEGIN {
		if (b > 1024*1024*1024) printf "%.1fG", b/(1024*1024*1024)
		else if (b > 1024*1024)  printf "%.1fM", b/(1024*1024)
		else if (b > 1024)       printf "%.1fK", b/1024
		else                     printf "%dB", b
	}'
}

printf "%s==> Binary size: %s%s%s\n" "$CYAN" "$YELLOW" "$(human "$total_bytes")" "$RESET"
printf "  Debug build:    %s  (current — DWARF kept for gsa per-package attribution)\n" "$(human "$total_bytes")"
if [ "$dwarf_bytes" -gt 0 ]; then
	printf "  Release-equiv:  ~%s  (DWARF subtracted; actual 'make build-release' may be a few MB smaller via -s symbol stripping)\n" "$(human "$stripped_bytes")"
	printf "  Debug overhead: %s  (DWARF; goes away with -w)\n" "$(human "$dwarf_bytes")"
fi
echo
printf "%s==> Per-package breakdown (gsa):%s\n" "$CYAN" "$RESET"
echo

go tool gsa --format text --hide-sections "$BIN" 2>/dev/null
echo

go tool gsa --format svg --output "$OUT_DIR/size-map.svg" --hide-sections "$BIN" 2>/dev/null
printf "  %sSVG  → %s/size-map.svg%s\n" "$CYAN" "$OUT_DIR" "$RESET"

go tool gsa --format html --output "$OUT_DIR/size-map.html" --hide-sections "$BIN" 2>/dev/null
printf "  %sHTML → %s/size-map.html (interactive treemap)%s\n" "$CYAN" "$OUT_DIR" "$RESET"

# Auto-open the HTML in the default browser unless under CI.
if [ -z "${CI:-}" ]; then
	if command -v open >/dev/null 2>&1; then
		open "$OUT_DIR/size-map.html"
	elif command -v xdg-open >/dev/null 2>&1; then
		xdg-open "$OUT_DIR/size-map.html"
	fi
fi

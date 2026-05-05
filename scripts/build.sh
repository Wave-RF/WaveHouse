#!/usr/bin/env bash
# Build a single Go binary in one of three variants.
#
# Usage: scripts/build.sh <name> <variant>
#   <name>    binary name (matches a directory under cmd/, e.g. `wavehouse`)
#   <variant> debug | release | cover
#
# Variant differences:
#   debug    bin/<name>           DWARF + symbol tables kept (LDFLAGS empty);
#                                 what `make build` produces.
#   release  bin/<name>-release   `-s -w` applied — DWARF + symbol tables
#                                 stripped; what `make build-release` produces.
#   cover    bin/<name>-cov       Same as debug + `-cover` instrumentation;
#                                 used by E2E to capture coverage from a real
#                                 running binary.
#
# Reads from env (set by the Makefile recipe):
#   VERSION_LDFLAGS  ldflags injecting Version / GitCommit / BuildTime
#   LDFLAGS          additional ldflags (typically empty for debug/cover)
#   TAGS             go build tags

set -euo pipefail

# shellcheck source=scripts/_colors.sh
. "$(dirname "$0")/_colors.sh"

name=${1:?binary name required}
variant=${2:?variant required (debug|release|cover)}

case "$variant" in
debug)
	label=$name
	ldflags="${LDFLAGS:-} ${VERSION_LDFLAGS:-}"
	build_flags=()
	output="bin/$name"
	;;
release)
	label="$name (release, stripped)"
	ldflags="-s -w ${VERSION_LDFLAGS:-}"
	build_flags=()
	output="bin/$name-release"
	;;
cover)
	label="$name (coverage instrumented)"
	ldflags="${LDFLAGS:-} ${VERSION_LDFLAGS:-}"
	build_flags=(-cover)
	output="bin/$name-cov"
	;;
*)
	printf '%sUnknown variant: %s%s\n' "$RED" "$variant" "$RESET" >&2
	exit 2
	;;
esac

mkdir -p bin
printf '%s==> Building %s...%s\n' "$CYAN" "$label" "$RESET"

start=$(date +%s)
# build_flags may be empty; use the ${arr[@]+"${arr[@]}"} idiom so the empty
# expansion is silent under `set -u` on bash 3.2 (macOS default).
CGO_ENABLED=0 go build \
	-tags="${TAGS:-}" \
	${build_flags[@]+"${build_flags[@]}"} \
	-ldflags="$ldflags" \
	-o "$output" \
	"./cmd/$name"
end=$(date +%s)

bytes=$(wc -c <"$output" | tr -d ' ')
size=$(awk -v b="$bytes" 'BEGIN {
	if (b > 1024*1024*1024) printf "%.1fG", b/(1024*1024*1024)
	else if (b > 1024*1024)  printf "%.1fM", b/(1024*1024)
	else if (b > 1024)       printf "%.1fK", b/1024
	else                     printf "%dB", b
}')
elapsed=$((end - start))
printf '%s✔%s %s (%s%ds%s, %s%s%s)\n' \
	"$GREEN" "$RESET" "$output" "$YELLOW" "$elapsed" "$RESET" "$YELLOW" "$size" "$RESET"

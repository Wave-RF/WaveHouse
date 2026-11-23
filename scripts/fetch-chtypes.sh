#!/usr/bin/env bash
# Prefetch the chtypes artifact for ClickHouse line(s) into a chtypes cache,
# with the SDK's own CLI at the version go.mod requires. Optional: an API
# process and the test helpers fetch a missing line themselves (autofetch).
# Callers: .github/actions/setup-env (once per CI job, before the suites), and
# anyone who wants the artifact before the first boot or test.
#
# There is no lock. Each run resolves the line's newest build on the registry
# and installs it unless the cache already holds it (two small requests when
# it does). chtypes verifies the build's signed statement on every install.
#
# Usage: scripts/fetch-chtypes.sh [<line> ...] [--platform <os-arch>] [--cache <dir>] [--offline] [--prune]
#        scripts/fetch-chtypes.sh --print-line | --where | --resolve [--platform <os-arch>]
#   <line>        ClickHouse major.minor line(s), e.g. 26.9. Default: the line
#                 of the pinned test ClickHouse (chversion.Test in
#                 internal/chversion/chversion.go).
#   --platform    os-arch to fetch for (default: $CHTYPES_TARGET, else this
#                 host), e.g. linux-amd64 for a container from a Mac.
#   --cache       cache directory (default: $CHTYPES_CACHE, else
#                 per-user cache; see --where).
#   --offline     no network: succeed only if the cache holds a build.
#   --prune       then delete every other build of the platform from the
#                 cache, so it holds exactly what this run fetched (needs jq).
#   --print-line  print the default line and exit.
#   --where       print the cache root the SDK would use (`chtypes where`,
#                 honouring --cache) and exit.
#   --resolve     print the manifest digest of the first line's current build
#                 for the platform and exit (`chtypes resolve --json`, honouring
#                 --cache; needs jq). Verified by the SDK and read from the
#                 channel the SDK itself fetches from, but installs nothing.
#
# Exit codes are the CLI's own: 0 ok, 2 usage, otherwise the failure's status
# (CHTYPES_ARTIFACT_*, CHTYPES_SOURCE_*).
set -euo pipefail

# shellcheck source=scripts/_colors.sh
. "$(dirname "$0")/_colors.sh"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIN_FILE="$REPO_ROOT/internal/chversion/chversion.go"
USAGE="Usage: $0 [<line> ...] [--platform <os-arch>] [--cache <dir>] [--offline] [--prune] | --print-line | --where | --resolve"

die() {
	printf '%s%s%s\n' "${RED}" "$*" "${RESET}" >&2
	exit 2
}

default_line() {
	local v
	v="$(sed -n 's/^const Test = "\([0-9][0-9.]*\)"$/\1/p' "$PIN_FILE")"
	[ -n "$v" ] || die "no 'const Test = \"<version>\"' in $PIN_FILE"
	printf '%s\n' "$v" | cut -d. -f1,2
}

platform=""
cache=""
offline=0
prune=0
mode=fetch
lines=()

while [ $# -gt 0 ]; do
	case "$1" in
	--platform)
		platform=${2:?--platform requires a value}
		shift 2
		;;
	--cache)
		cache=${2:?--cache requires a value}
		shift 2
		;;
	--offline)
		offline=1
		shift
		;;
	--prune)
		prune=1
		shift
		;;
	--print-line)
		mode=print-line
		shift
		;;
	--where)
		mode=where
		shift
		;;
	--resolve)
		mode=resolve
		shift
		;;
	-h | --help)
		printf '%s\n' "$USAGE"
		exit 0
		;;
	-*) die "unknown flag: $1" ;;
	*)
		lines+=("$1")
		shift
		;;
	esac
done

[ ${#lines[@]} -gt 0 ] || lines=("$(default_line)")
if [ "$mode" = print-line ]; then
	printf '%s\n' "${lines[0]}"
	exit 0
fi
[ -n "$platform" ] || platform="${CHTYPES_TARGET:-}"
[ -n "$platform" ] || platform="$(go env GOOS)-$(go env GOARCH)"

if [ -n "$cache" ]; then
	case "$cache" in /*) ;; *) cache="$PWD/$cache" ;; esac
fi

if [ "$mode" = where ]; then
	cd "$REPO_ROOT"
	if [ -n "$cache" ]; then
		exec go run github.com/wave-rf/chtypes/go/v2/cmd/chtypes where --cache "$cache"
	fi
	exec go run github.com/wave-rf/chtypes/go/v2/cmd/chtypes where
fi

if [ "$mode" = resolve ]; then
	command -v jq >/dev/null || die "--resolve needs jq"
	cd "$REPO_ROOT"
	# jq -e fails on no match, so a platform the build does not publish is an
	# error here rather than an empty key.
	go run github.com/wave-rf/chtypes/go/v2/cmd/chtypes resolve "${lines[0]}" --json ${cache:+--cache "$cache"} |
		jq -er --arg p "$platform" '.[] | select(.platform == $p) | .manifest' ||
		die "--resolve: no manifest for ${lines[0]} on $platform"
	exit 0
fi

args=(fetch "${lines[@]}" --platform "$platform")
[ "$offline" = 1 ] && args+=(--offline)
[ -z "$cache" ] || args+=(--cache "$cache")

printf '%s==> chtypes %s (%s)%s\n' "${CYAN}" "${args[*]}" "$platform" "${RESET}" >&2
# Through go.mod, so the CLI is the SDK version this repo builds against and a
# cached module makes no request of its own.
cd "$REPO_ROOT"
kept="$(go run github.com/wave-rf/chtypes/go/v2/cmd/chtypes "${args[@]}")"
printf '%s\n' "$kept"
[ "$prune" = 1 ] || exit 0

# --prune: drop every other install record of this platform, with its blobs
# (unless a kept record names them) and its index.json entry.
command -v jq >/dev/null || die "--prune needs jq"
root="$(go run github.com/wave-rf/chtypes/go/v2/cmd/chtypes where ${cache:+--cache "$cache"})"
keep_blobs=" "
while IFS= read -r dir; do
	keep_blobs+="$(jq -r '.digests[] // empty' "$dir/verified.json" | tr '\n' ' ')"
done <<<"$kept"
gone=()
for rec in "$root"/unpacked/sha256/*/verified.json; do
	[ -f "$rec" ] || continue
	dir="$(dirname "$rec")"
	if printf '%s\n' "$kept" | grep -qxF "$dir" || [ "$(jq -r .platform "$rec")" != "$platform" ]; then
		continue
	fi
	for d in $(jq -r '.digests[] // empty' "$rec"); do
		case "$keep_blobs" in *" $d "*) ;; *) rm -f "$root/blobs/sha256/${d#sha256:}" ;; esac
	done
	gone+=("sha256:$(basename "$dir")")
	rm -rf "$dir"
	printf '%s    pruned %s%s\n' "${YELLOW}" "$dir" "${RESET}" >&2
done
if [ ${#gone[@]} -gt 0 ] && [ -f "$root/index.json" ]; then
	tmp="$(mktemp "$root/.tmp-index-XXXXXX")"
	jq -c '.manifests |= map(select(.digest as $d | $ARGS.positional | any(. == $d) | not))' \
		--args "${gone[@]}" <"$root/index.json" >"$tmp"
	mv "$tmp" "$root/index.json"
fi

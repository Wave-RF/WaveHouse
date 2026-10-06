#!/usr/bin/env bash
# Fetch the chtypes artifact(s) this repo pins in chtypes.lock, via the
# published SDK CLI. One wrapper, three callers:
#   - deployments/Dockerfile / Dockerfile.goreleaser (bakes the artifact
#     into the runtime image at build time)
#   - .github/actions/setup-env (CI fetch, before Go test jobs)
#   - developers (`scripts/fetch-chtypes.sh` with no args pulls the host
#     platform's build of the e2e harness's pinned line)
#
# Always against chtypes.lock: this repo's lock is the only source of truth
# for which exact build gets installed (schema 3: OCI digests per line and
# platform). A line not in the lock, or a registry that no longer serves a
# locked digest, is a hard failure. The lock is refreshed deliberately, never
# implicitly (see .github/workflows/README.md, "chtypes artifacts").
#
# Usage: scripts/fetch-chtypes.sh [<line> ...] [--platform <os-arch>] [--cache <dir>] [--offline]
#   <line>      ClickHouse minor line(s) to fetch, e.g. 26.8. Defaults to
#               every line this repo needs today (LOCK_LINES below).
#   --platform  os-arch pair to fetch for (default: host platform, or
#               $CHTYPES_TARGET). Pass linux-amd64 / linux-arm64 to fetch a
#               foreign platform's artifact, e.g. to bake a Linux container
#               image from a non-Linux host.
#   --cache     artifact cache directory to fetch into (default:
#               $CHTYPES_CACHE, else ~/.cache/chtypes/v1; see `chtypes where`).
#   --offline   make zero network requests: succeed only if the cache already
#               holds the locked build (replaces --frozen). For a restored CI
#               cache; --frozen still downloads (Wave-RF/chtypes#414).
#
# Exit codes are the SDK CLI's own: 0 ok, 2 usage, otherwise the failure's
# status (CHTYPES_ARTIFACT_*, CHTYPES_SOURCE_*; docs/guides/fetch-v1.md in the SDK).
set -euo pipefail

# shellcheck source=scripts/_colors.sh
. "$(dirname "$0")/_colors.sh"

# Bump together with go.mod's `require github.com/wave-rf/chtypes/go` line.
CHTYPES_SDK_VERSION="v1.0.2"
CHTYPES_CLI="github.com/wave-rf/chtypes/go/cmd/chtypes@${CHTYPES_SDK_VERSION}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCK_FILE="${REPO_ROOT}/chtypes.lock"

# Lines every deployment of this repo needs today. The e2e harness
# (tests/integration/setup_test.go, scripts/orchestrator) and dev compose
# pin ClickHouse 26.8.15.10, the same patch as the locked 26.8.15.10
# build. A line request installs the newest patch the lock pins for that
# line, and the gateway asks the registry for the line, never a nearest
# line, so this is "26.8", not the exact patch. Widening this list is how a
# new line gets adopted: fetch it with --lock, add it here, commit the
# updated lock.
LOCK_LINES=(26.8)

platform=""
cache=""
offline=0
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
	-h | --help)
		printf 'Usage: %s [<line> ...] [--platform <os-arch>] [--cache <dir>] [--offline]\n' "$0"
		exit 0
		;;
	-*)
		printf '%sunknown flag: %s%s\n' "${RED}" "$1" "${RESET}" >&2
		exit 2
		;;
	*)
		lines+=("$1")
		shift
		;;
	esac
done

if [ ${#lines[@]} -eq 0 ]; then
	lines=("${LOCK_LINES[@]}")
fi

mode=--frozen
[ "$offline" = 1 ] && mode=--offline
args=(fetch "${lines[@]}" "$mode" --lock "$LOCK_FILE")
[ -n "$platform" ] && args+=(--platform "$platform")
if [ -n "$cache" ]; then
	case "$cache" in /*) ;; *) cache="$PWD/$cache" ;; esac
	args+=(--cache "$cache")
fi

printf '%s==> chtypes fetch %s%s %s (lock: %s)\n' "${CYAN}" "$mode" "${RESET}" "${lines[*]}" "$LOCK_FILE"
# Inside the repo the CLI resolves through go.mod, which makes no network
# request once the module is cached; `go run <module>@<version>` always asks the
# proxy (a deprecation lookup), so --offline could not be zero-request. Outside
# it (the goreleaser image's fetch stage ships no go.mod) fall back to @version.
if grep -qF "github.com/wave-rf/chtypes/go ${CHTYPES_SDK_VERSION}" "$REPO_ROOT/go.mod" 2>/dev/null; then
	cd "$REPO_ROOT"
	exec go run github.com/wave-rf/chtypes/go/cmd/chtypes "${args[@]}"
fi
exec go run "$CHTYPES_CLI" "${args[@]}"

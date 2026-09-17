#!/usr/bin/env bash
# Fetch the chtypes artifact(s) this repo pins in chtypes.lock, via the
# published SDK CLI. One wrapper, three callers:
#   - deployments/Dockerfile / Dockerfile.goreleaser (bakes the artifact
#     into the runtime image at build time)
#   - .github/actions/setup-env (CI cache-miss fetch, before Go test jobs)
#   - developers (`scripts/fetch-chtypes.sh` with no args pulls the host
#     platform's build of the e2e harness's pinned line)
#
# Always --frozen --lock chtypes.lock: this repo's lock is the only source
# of truth for which exact build gets installed — never the rolling
# `artifacts` release (see R3 §7 / docs/guides/artifacts.md upstream: "a
# chtypes.lock... refreshed deliberately, never regenerated implicitly").
# A line not in the lock, or a lock/registry mismatch, is a hard failure.
#
# Usage: scripts/fetch-chtypes.sh [<line> ...] [--platform <os-arch>] [--dest <dir>]
#   <line>      ClickHouse minor line(s) to fetch, e.g. 26.6. Defaults to
#               every line this repo needs today (LOCK_LINES below).
#   --platform  os-arch pair to fetch for (default: host platform, chosen
#               by the SDK's own HostPlatform()). Pass linux-amd64 /
#               linux-arm64 to fetch a foreign platform's artifact — used
#               when baking a Linux container image from a non-Linux host.
#   --dest      registry directory to fetch into (default: the SDK's own
#               default per-platform cache dir — see `chtypes where`).
#
# Exit codes are the SDK CLI's own: 0 ok, 1 verification/lock mismatch,
# 2 usage, 3 source unreachable, 4 not published.
set -euo pipefail

# shellcheck source=scripts/_colors.sh
. "$(dirname "$0")/_colors.sh"

# The SDK's own package is cgo-gated, so building the CLI below without a C
# compiler on PATH fails with a pile of "undefined: Registry" / "undefined:
# minorOf" errors from files the build tags excluded — which reads like a
# broken dependency, not a missing toolchain. Say so instead. (Measured in a
# bare ubuntu:24.04; every CI runner and golang:*-bookworm already ship one.)
if [ "$(go env CGO_ENABLED 2>/dev/null || echo 0)" != "1" ]; then
	printf '%sno C compiler: go env CGO_ENABLED is not 1%s\n' "${RED}" "${RESET}" >&2
	printf '  The chtypes SDK is cgo-only. Install a C toolchain (Debian/Ubuntu:\n' >&2
	printf '  apt-get install gcc; macOS: xcode-select --install) and retry.\n' >&2
	exit 2
fi

# Bump together with go.mod's `require github.com/wave-rf/chtypes/go` line.
CHTYPES_SDK_VERSION="v0.2.1"
CHTYPES_CLI="github.com/wave-rf/chtypes/go/cmd/chtypes@${CHTYPES_SDK_VERSION}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCK_FILE="${REPO_ROOT}/chtypes.lock"

# Lines every deployment of this repo needs today. The e2e harness
# (tests/integration/setup_test.go, scripts/orchestrator) and dev compose
# both pin ClickHouse 26.6.3.62; chtypes resolves by MINOR line (26.6),
# never nearest (R3 §6), so this is "26.6", not the exact patch. Widening
# this list is how a new line gets adopted: fetch it, add it here, commit
# the updated lock.
LOCK_LINES=(26.6)

platform=""
dest=""
lines=()

while [ $# -gt 0 ]; do
	case "$1" in
	--platform)
		platform=${2:?--platform requires a value}
		shift 2
		;;
	--dest)
		dest=${2:?--dest requires a value}
		shift 2
		;;
	-h | --help)
		printf 'Usage: %s [<line> ...] [--platform <os-arch>] [--dest <dir>]\n' "$0"
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

args=(fetch "${lines[@]}" --frozen --lock "$LOCK_FILE")
[ -n "$platform" ] && args+=(--platform "$platform")
[ -n "$dest" ] && args+=(--dest "$dest")

printf '%s==> chtypes fetch --frozen%s %s (lock: %s)\n' "${CYAN}" "${RESET}" "${lines[*]}" "$LOCK_FILE"
exec go run "$CHTYPES_CLI" "${args[@]}"

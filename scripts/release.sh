#!/usr/bin/env bash
# Cut a release by creating and pushing one tag. Nothing else — no version
# bump, no commit, no release PR.
#
# That is a property of the repo, not a shortcut this script takes: every
# component derives its version from the tag. The server gets it via
# GoReleaser's ldflags, the Go SDK because a Go module's version simply IS its
# tag, and @wavehouse/sdk because publish-npm.yml stamps package.json from the
# tag before publishing. Which is just as well — the branch ruleset forbids
# pushing to `main`, so a bump commit would need its own reviewed PR merged
# before every single release.
#
#   scripts/release.sh <component> <version>
#
#   server  -> v<version>              binaries + ghcr.io/wave-rf/wavehouse
#   ts      -> clients/ts/v<version>   @wavehouse/sdk on npm
#   go      -> clients/go/v<version>   go get .../clients/go@v<version>
#
# The `clients/<lang>/` prefix is required, not chosen: Go resolves a module in
# a subdirectory only against a tag carrying that subdirectory as a prefix
# (https://go.dev/ref/mod). The other clients follow the same shape so one
# convention covers all of them.
#
# Everything here is a preflight check plus two git commands. The point is that
# the checks are the part worth automating: each one is a mistake that is
# expensive to undo once a tag is public, because tags are immutable in
# practice (the ruleset blocks updates and deletes) and npm's unpublish window
# is 72 hours.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_colors.sh
. "$here/_colors.sh"

REMOTE="${WAVEHOUSE_RELEASE_REMOTE:-origin}"

die() {
  printf '%s✗%s %s\n' "$RED" "$RESET" "$1" >&2
  exit 1
}
ok() { printf '%s✓%s %s\n' "$GREEN" "$RESET" "$1"; }
info() { printf '  %s\n' "$1"; }

usage() {
  cat >&2 <<EOF
usage: $0 <server|ts|go> <version>

  $0 server 0.1.0     -> tag v0.1.0
  $0 ts     0.1.0     -> tag clients/ts/v0.1.0
  $0 go     0.1.0     -> tag clients/go/v0.1.0

Version is bare semver, with no leading 'v'. A prerelease suffix is allowed
(0.2.0-rc.1) and selects the rc/alpha/beta/next channel instead of latest.

Env:
  DRY_RUN=1    run every check and print the plan, but create no tag
EOF
  exit 1
}

component="${1:-}"
version="${2:-}"
[ -n "$component" ] && [ -n "$version" ] || usage

case "$component" in
  server) tag="v${version}"; desc="server (binaries + container image)"; guard="" ;;
  ts) tag="clients/ts/v${version}"; desc="@wavehouse/sdk (npm)"; guard="clients/ts/package.json" ;;
  go) tag="clients/go/v${version}"; desc="Go SDK (go get)"; guard="clients/go/go.mod" ;;
  *) die "unknown component '$component' (expected server, ts, or go)" ;;
esac

# Reject a version with a leading `v` up front rather than minting `vv0.1.0`.
case "$version" in
  v*) die "pass a bare version without the leading 'v' (e.g. ${version#v}, not ${version})" ;;
esac

# Reuse the publishers' own validator so a version this script accepts is
# exactly one they accept — and so the channel shown below is the real one.
channel="$("$here/ci/release-channel.sh" "$tag" 2>/dev/null)" \
  || die "'$version' is not a valid semver version (expected X.Y.Z with an optional -prerelease)"

printf '\n%sReleasing %s %s%s\n\n' "$BOLD" "$desc" "$version" "$RESET"

# --- preflight --------------------------------------------------------------

[ -z "$guard" ] || [ -f "$guard" ] \
  || die "$guard not found — is the $component client actually in this repo yet?"

git rev-parse --git-dir >/dev/null 2>&1 || die "not inside a git repository"

branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$branch" = "main" ] \
  || die "on branch '$branch' — releases are cut from main (the tag must be an ancestor of what CI tested)"
ok "on main"

[ -z "$(git status --porcelain)" ] \
  || die "working tree is dirty — commit or stash first, so the tag matches a reviewed commit"
ok "working tree clean"

git fetch --quiet "$REMOTE" main --tags
local_sha="$(git rev-parse HEAD)"
remote_sha="$(git rev-parse "$REMOTE/main")"
[ "$local_sha" = "$remote_sha" ] \
  || die "HEAD ($(git rev-parse --short HEAD)) != $REMOTE/main ($(git rev-parse --short "$REMOTE/main")) — pull, or push your merge, first"
ok "in sync with $REMOTE/main"

git rev-parse -q --verify "refs/tags/$tag" >/dev/null \
  && die "tag $tag already exists locally — releases are immutable; pick the next version"
git ls-remote --exit-code --tags "$REMOTE" "refs/tags/$tag" >/dev/null 2>&1 \
  && die "tag $tag already exists on $REMOTE — releases are immutable; pick the next version"
ok "tag $tag is free"

# CI green on this exact commit. `CI` is the aggregator job and the ruleset's
# sole required check, so its conclusion is the whole gate (see
# .github/workflows/README.md). Advisory: a missing gh or an API hiccup warns
# rather than blocks, since the tag is still recoverable at this point and a
# hard dependency on the API would make releasing impossible during an outage.
if command -v gh >/dev/null 2>&1; then
  conclusion="$(gh api "repos/{owner}/{repo}/commits/$local_sha/check-runs" \
    --jq '[.check_runs[] | select(.name == "CI")] | first | .conclusion // empty' 2>/dev/null || true)"
  case "$conclusion" in
    success) ok "CI is green on $(git rev-parse --short HEAD)" ;;
    "") printf '%s!%s could not read CI status for this commit — check it before continuing\n' "$YELLOW" "$RESET" ;;
    *) die "CI concluded '$conclusion' on $(git rev-parse --short HEAD) — do not release a red commit" ;;
  esac
else
  printf '%s!%s gh not installed — skipping the CI status check\n' "$YELLOW" "$RESET"
fi

# --- plan -------------------------------------------------------------------

printf '\n%sPlan%s\n' "$BOLD" "$RESET"
info "tag      $tag  ->  $(git rev-parse --short HEAD)"
info "channel  $channel"
case "$component" in
  server)
    info "publishes  GitHub Release (binaries + checksums, provenance-attested)"
    info "           ghcr.io/wave-rf/wavehouse:$tag and :$channel"
    ;;
  ts)
    info "publishes  @wavehouse/sdk@$version on npm under the '$channel' dist-tag"
    info "           + a GitHub Release"
    ;;
  go)
    info "publishes  go get github.com/Wave-RF/WaveHouse/clients/go@v$version"
    info "           (no build step — the Go module proxy serves the tag)"
    ;;
esac
[ "$channel" = "latest" ] \
  || info "NOTE: prerelease — this will NOT move 'latest'/':latest'"

if [ "${DRY_RUN:-}" = "1" ]; then
  printf '\n%sDRY_RUN=1 — no tag created.%s\n' "$YELLOW" "$RESET"
  exit 0
fi

# --- go ---------------------------------------------------------------------

printf '\n'
read -r -p "Create and push $tag? [y/N] " reply
case "$reply" in
  [yY] | [yY][eE][sS]) ;;
  *) die "aborted" ;;
esac

git tag -a "$tag" -m "$desc $version"
git push "$REMOTE" "refs/tags/$tag"

printf '\n%s✓ pushed %s%s\n\n' "$GREEN" "$tag" "$RESET"
info "watch:  gh run watch \$(gh run list --limit 1 --json databaseId --jq '.[0].databaseId')"
info "runs:   https://github.com/Wave-RF/WaveHouse/actions"

#!/usr/bin/env bash
# Cut a release by creating and pushing one tag. Nothing else — no version
# bump, no commit, no release PR.
#
# That is a property of the repo, not a shortcut this script takes: the server
# derives its version from the tag, through GoReleaser's ldflags. Which is just
# as well — the branch ruleset forbids pushing to `main`, so a bump commit
# would need its own reviewed PR merged before every single release.
#
#   scripts/release.sh server <version>   -> v<version>: binaries + ghcr.io/wave-rf/wavehouse
#
# The SDKs release from their own repositories, Wave-RF/wavehouse-sdk-js and
# Wave-RF/wavehouse-sdk-go.
#
# Everything here is a preflight check plus two git commands. The point is that
# the checks are the part worth automating: each one is a mistake that is
# expensive to undo once a tag is public, because tags are immutable in
# practice (the ruleset blocks updates and deletes).

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
usage: $0 server <version>

  $0 server 0.1.0     -> tag v0.1.0

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
  server) tag="v${version}"; desc="server (binaries + container image)" ;;
  *) die "unknown component '$component' (expected server; the SDKs release from their own repositories)" ;;
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
# .github/workflows/README.md).
#
# `check_name=CI&per_page=100` matters on both counts. Commits here carry
# 30-46 check runs and the endpoint defaults to per_page=30, so filtering
# client-side made it luck whether the aggregator was on the page at all —
# when it wasn't, this printed "could not read CI status" and waved the
# release through, reading like a transient API hiccup rather than a check
# that never ran. And a commit legitimately has TWO `CI` runs (the branch push
# and the merge_group), so `first` picked one arbitrarily; both must be green.
#
# Only "no runs at all" and "no gh" are advisory — a hard API dependency would
# make releasing impossible during a GitHub outage. A run that exists and is
# not green blocks.
if command -v gh >/dev/null 2>&1; then
  conclusions=()
  while IFS= read -r line; do
    [ -n "$line" ] && conclusions+=("$line")
  done < <(gh api "repos/{owner}/{repo}/commits/$local_sha/check-runs?check_name=CI&per_page=100" \
    --jq '.check_runs[] | .conclusion // "pending"' 2>/dev/null || true)

  if [ ${#conclusions[@]} -eq 0 ]; then
    printf '%s!%s no CI check run found for %s — verify it before continuing\n' \
      "$YELLOW" "$RESET" "$(git rev-parse --short HEAD)"
  else
    not_green=()
    for c in "${conclusions[@]}"; do
      [ "$c" = "success" ] || not_green+=("$c")
    done
    if [ ${#not_green[@]} -gt 0 ]; then
      die "CI on $(git rev-parse --short HEAD) is ${not_green[*]} — a release must be cut from a green commit"
    fi
    ok "CI is green on $(git rev-parse --short HEAD) (${#conclusions[@]} run(s))"
  fi
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

# Roll the local tag back if the push is rejected — tag creation is admin-only
# via the `release tag protection` ruleset, and any network failure lands here
# too. Left behind, the stray tag makes the NEXT attempt die on the
# "already exists locally — pick the next version" check above, which would be
# actively wrong advice: nothing was published, so the same version is still
# free.
if ! git push "$REMOTE" "refs/tags/$tag"; then
  git tag -d "$tag" >/dev/null
  die "push rejected — local tag $tag removed, nothing was published"
fi

printf '\n%s✓ pushed %s%s\n\n' "$GREEN" "$tag" "$RESET"
info "watch:  gh run watch \$(gh run list --limit 1 --json databaseId --jq '.[0].databaseId')"
info "runs:   https://github.com/Wave-RF/WaveHouse/actions"

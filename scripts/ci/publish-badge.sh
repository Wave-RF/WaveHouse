#!/usr/bin/env bash
# Publish a shields.io endpoint JSON to the orphan `badges` branch, which the
# README coverage badge reads over raw.githubusercontent.com (unrestricted now
# the repo is public — #133).
#
# Runs ONLY in the main-push `badge` job — the sole holder of contents:write —
# off a trusted-main checkout, so a push to this branch can never be influenced
# by PR code (.github/workflows/README.md, invariant 5).
#
# `badges` is a detached, single-purpose store with no shared history to
# protect: we fast-forward a tiny commit onto it (creating it as an orphan the
# first time), and no-op when the number hasn't changed. The workflow-level
# concurrency group serializes main-push runs, so the push never races itself.
#
# Usage: scripts/ci/publish-badge.sh <src.json> <dest-path-on-branch>
# Requires a checkout with push credentials (actions/checkout default).

set -euo pipefail

src="${1:?usage: publish-badge.sh <src.json> <dest-path-on-branch>}"
dest="${2:?usage: publish-badge.sh <src.json> <dest-path-on-branch>}"
branch="badges"

[ -f "$src" ] || { echo "::error::badge source not found: $src" >&2; exit 1; }

# A linked worktree keeps the job's main checkout untouched.
work="$(mktemp -d)"
trap 'git worktree remove --force "$work" 2>/dev/null || true' EXIT

if git fetch --depth=1 origin "$branch" 2>/dev/null; then
  git worktree add --force "$work" "origin/$branch"
  git -C "$work" checkout -B "$branch"
else
  echo "badges branch not found — creating it."
  git worktree add --force --detach "$work"
  git -C "$work" checkout --orphan "$branch"
  git -C "$work" rm -rf . >/dev/null 2>&1 || true
fi

mkdir -p "$work/$(dirname "$dest")"
cp "$src" "$work/$dest"
git -C "$work" add "$dest"

if git -C "$work" diff --cached --quiet; then
  echo "Coverage badge unchanged — nothing to publish."
  exit 0
fi

git -C "$work" \
  -c user.name="github-actions[bot]" \
  -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
  commit -q -m "chore(badge): update coverage badge"
git -C "$work" push origin "$branch"
echo "Published $dest to the $branch branch."

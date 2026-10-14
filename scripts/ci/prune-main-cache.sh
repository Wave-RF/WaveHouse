#!/usr/bin/env bash
# Delete main's superseded cache generations. A key ending in a 64-hex
# hash (gomod-v1, gobuild-v3, pnpm, playwright, astro, golangci,
# codeql-dependencies) gets a new hash on every dependency change, and the
# old entry lingers until eviction: two go.sum bumps in a day put three Go
# sets in the 10 GB budget. CodeQL's overlay base databases add one entry
# per language per main commit (`...-<commit sha>-<run id>-<attempt>`) and
# are restored by the prefix before the sha, so only the newest is read.
# In refs/heads/main this deletes every such entry but the newest of its
# family (the key minus that tail):
#   - each family has one writer whose main runs never overlap (ci.yml
#     queues them; publish-dev cancels the older), so the newest is the
#     generation main's HEAD uses;
#   - an entry goes only once a newer one of its family exists, which is
#     the one a restore-keys lookup would pick anyway.
# Three cases break "newest is HEAD's" and each costs one main run a cold
# or stale cache before the next prune repairs it: an older CodeQL
# analysis saving last, a re-run of an older main run after a dependency
# change (its old key was pruned, so it saves it again), and a revert
# landing while its bump's prune runs. A pull request still on an older
# go.sum restores the newest by prefix instead of its exact key. Other keys
# (smoke-node) are left alone.
#
# Usage: scripts/ci/prune-main-cache.sh [--dry-run] [--from <file>]
#   --dry-run    list what would be deleted; delete nothing
#   --from FILE  read the cache list from FILE (one JSON object per line,
#                as `gh api .../actions/caches --jq '.actions_caches[]'`
#                prints) instead of the API
# Env: GH_TOKEN, GH_REPO (owner/name).

set -euo pipefail

dry_run=0 from=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) dry_run=1; shift ;;
    --from) from="$2"; shift 2 ;;
    *) echo "prune-main-cache: unknown arg $1" >&2; exit 2 ;;
  esac
done

: "${GH_REPO:?GH_REPO (owner/name) is required}"

list="$(mktemp)"
trap 'rm -f "$list"' EXIT
if [ -n "$from" ]; then
  cp "$from" "$list"
else
  # A failed listing must not read as "nothing to prune". Oldest first, so
  # entries saved while it pages land on later pages.
  if ! gh api --paginate \
    "repos/$GH_REPO/actions/caches?ref=refs/heads/main&sort=created_at&direction=asc&per_page=100" \
    --jq '.actions_caches[]' >"$list"; then
    echo "::error::prune-main-cache: listing the caches failed" >&2
    exit 1
  fi
fi

# id, key, size — one line per entry to delete. Deduplicated first: paging
# can return an entry twice, and two copies of the newest would make `.[:-1]`
# delete it.
doomed="$(jq -rs '
  def family:
    if test("-[0-9a-f]{64}$") then sub("-[0-9a-f]{64}$"; "")
    elif test("^codeql-overlay-base-database-.*-[0-9a-f]{40}-[0-9]+-[0-9]+$")
    then sub("-[0-9a-f]{40}-[0-9]+-[0-9]+$"; "")
    else null end;
  map(select(.ref == "refs/heads/main") | .family = (.key | family) | select(.family != null))
  | unique_by(.id)
  | group_by(.family)
  | map(sort_by((.created_at | sub("\\.[0-9]+Z$"; "Z")), .id) | .[:-1][])[]
  | [.id, .key, .size_in_bytes] | @tsv
' "$list")"

if [ -z "$doomed" ]; then
  echo "prune-main-cache: nothing to delete"
  exit 0
fi

rc=0 freed=0
while IFS=$'\t' read -r id key size; do
  if [ "$dry_run" = 1 ]; then
    echo "would delete $id $key"
  elif out="$(gh api -X DELETE "repos/$GH_REPO/actions/caches/$id" 2>&1)"; then
    echo "deleted $id $key"
  elif grep -q 'HTTP 404' <<<"$out"; then
    # Evicted, or an overlapping prune got there first.
    echo "already gone $id $key"
    continue
  else
    echo "::error::prune-main-cache: deleting $id ($key) failed: $out" >&2
    rc=1
    continue
  fi
  freed=$((freed + size))
done <<<"$doomed"

echo "prune-main-cache: $( [ "$dry_run" = 1 ] && echo "would free" || echo "freed" ) $((freed / 1048576)) MiB"
exit "$rc"

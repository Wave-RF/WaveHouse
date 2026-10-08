#!/usr/bin/env bash
# Delete the Go build cache entries in pull-request scopes that no later
# run will read. setup-env lets a PR's long-pole jobs save their build
# cache into the PR's own scope when the key misses main's, i.e. the PR
# changes go.mod or go.sum (.github/workflows/README.md, invariant 6).
# Each later dependency change in the PR mints another key, and a run
# racing main's own save can store a copy of main's key, so in every
# refs/pull/* scope this deletes:
#   - an entry whose key and version main also holds: main's copy serves
#     it, and a PR whose dependencies match main's holds no build cache;
#   - every entry but the newest of its flavor (the key minus its hash):
#     restore-keys return the newest match, so the older ones are dead.
# It needs no PR number, so fork PRs are covered too.
#
# Usage: scripts/ci/prune-pr-build-cache.sh [--dry-run] [--from <file>]
#   --dry-run    list what would be deleted; delete nothing
#   --from FILE  read the cache list from FILE (one JSON object per line,
#                as `gh api .../actions/caches --jq '.actions_caches[]'`
#                prints) instead of the API
# Env: GH_TOKEN, GH_REPO (owner/name).

set -euo pipefail

# Must match the build cache key in .github/actions/setup-env/action.yml.
prefix="gobuild-v3-"

dry_run=0 from=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) dry_run=1; shift ;;
    --from) from="$2"; shift 2 ;;
    *) echo "prune-pr-build-cache: unknown arg $1" >&2; exit 2 ;;
  esac
done

if [ -z "$from" ] || [ "$dry_run" = 0 ]; then
  : "${GH_REPO:?GH_REPO (owner/name) is required}"
fi

list="$(mktemp)"
trap 'rm -f "$list"' EXIT
if [ -n "$from" ]; then
  cp "$from" "$list"
else
  # A failed listing must not read as "nothing to prune". Oldest first, so
  # entries saved while it pages land on later pages instead of shifting
  # the earlier ones.
  if ! gh api --paginate \
    "repos/$GH_REPO/actions/caches?key=$prefix&sort=created_at&direction=asc&per_page=100" \
    --jq '.actions_caches[]' >"$list"; then
    echo "::error::prune-pr-build-cache: listing the caches failed" >&2
    exit 1
  fi
fi

# id, ref, key, size, reason — one line per entry to delete.
# Deduplicated first: paging can return an entry twice, and two copies of
# a PR's newest entry would make `.[:-1]` delete it.
doomed="$(jq -rs --arg prefix "$prefix" '
  map(select(.key | startswith($prefix)))
  | unique_by(.id)
  | (map(select(.ref == "refs/heads/main") | [.key, .version])) as $main
  | map(select(.ref | startswith("refs/pull/")))
  | (map(select([.key, .version] as $k | any($main[]; . == $k)) | .why = "on main")
     + (group_by([.ref, (.key | sub("-[0-9a-f]{64}$"; ""))])
        | map(sort_by((.created_at | sub("\\.[0-9]+Z$"; "Z")), .id) | .[:-1][])
        | map(.why = "superseded")))
  | unique_by(.id)[]
  | [.id, .ref, .key, .size_in_bytes, .why] | @tsv
' "$list")"

if [ -z "$doomed" ]; then
  echo "prune-pr-build-cache: nothing to delete"
  exit 0
fi

rc=0 freed=0
while IFS=$'\t' read -r id ref key size why; do
  if [ "$dry_run" = 1 ]; then
    echo "would delete $id ($why) $ref $key"
  elif out="$(gh api -X DELETE "repos/$GH_REPO/actions/caches/$id" 2>&1)"; then
    echo "deleted $id ($why) $ref $key"
  elif grep -q 'HTTP 404' <<<"$out"; then
    # The PR's close cleanup got there first.
    echo "already gone $id $ref $key"
    continue
  else
    echo "::error::prune-pr-build-cache: deleting $id ($key on $ref) failed: $out" >&2
    rc=1
    continue
  fi
  freed=$((freed + size))
done <<<"$doomed"

echo "prune-pr-build-cache: $( [ "$dry_run" = 1 ] && echo "would free" || echo "freed" ) $((freed / 1048576)) MiB"
exit "$rc"

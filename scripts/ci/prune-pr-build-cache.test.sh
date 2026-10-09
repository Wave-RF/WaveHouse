#!/usr/bin/env bash
# Behavioral test for scripts/ci/prune-pr-build-cache.sh, which deletes Go
# build cache entries from pull-request scopes with a write token. Deleting
# a PR's newest entry, or anything of main's, would cost every later run its
# warm cache, so the selection is pinned here against fixtures, and the
# PR-state, delete and listing paths against a stub `gh`. Run by `make verify`
# (target: test-prune-pr-build-cache). Needs jq; no network.

set -uo pipefail
script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/prune-pr-build-cache.sh"
fails=0

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT

# A 64-hex hash made of one repeated digit.
h() { printf '%064d' 0 | tr 0 "$1"; }

# row <id> <ref> <key> <created_at> [version]
row() {
  printf '{"id":%s,"ref":"%s","key":"%s","version":"%s","created_at":"%s","size_in_bytes":1048576}\n' \
    "$1" "$2" "$3" "${5:-v1}" "$4"
}

app="gobuild-v3-Linux-go-integration-app"
pr2=refs/pull/2/merge
{
  row 1 refs/heads/main "$app-$(h 1)" 2026-10-01T00:00:00.1Z
  row 2 refs/heads/main "$app-$(h 2)" 2026-09-01T00:00:00Z
  # A copy of main's key and version: main's serves it.
  row 10 refs/pull/1/merge "$app-$(h 1)" 2026-10-02T00:00:00Z
  # Same key, another version: main's would not restore for it.
  row 11 refs/pull/3/merge "$app-$(h 1)" 2026-10-02T00:00:00Z v2
  # Superseded by the newest of the flavor (22).
  row 20 $pr2 "$app-$(h 3)" 2026-10-02T00:00:00Z
  row 21 $pr2 "$app-$(h 4)" 2026-10-02T12:00:00Z
  row 22 $pr2 "$app-$(h 5)" 2026-10-03T00:00:00Z
  # Its own flavor, though `-integration-` prefixes `-integration-app-`.
  row 23 $pr2 "gobuild-v3-Linux-go-integration-$(h 3)" 2026-10-01T00:00:00Z
  # Same second: the higher id is the newer, whatever the fractions or the
  # listing order say.
  row 41 refs/pull/4/merge "gobuild-v3-Linux-go-e2e-cov-$(h 7)" 2026-10-05T00:00:00.1Z
  row 40 refs/pull/4/merge "gobuild-v3-Linux-go-e2e-cov-$(h 6)" 2026-10-05T00:00:00.9Z
  # Paging returned the PR's only entry twice: still its newest.
  row 50 refs/pull/5/merge "$app-$(h 8)" 2026-10-06T00:00:00Z
  row 50 refs/pull/5/merge "$app-$(h 8)" 2026-10-06T00:00:00Z
  # Each the newest of its flavor and ref: all kept while PR 6 is open, all
  # deleted once it is closed, head ref included.
  row 80 refs/pull/6/merge "$app-$(h a)" 2026-10-07T00:00:00Z
  row 81 refs/pull/6/merge "gobuild-v3-Linux-go-e2e-cov-$(h a)" 2026-10-07T00:00:00Z
  row 82 refs/pull/6/head "$app-$(h b)" 2026-10-07T00:00:00Z
  # Not ours: other families, main's old generation, merge-queue refs.
  row 60 $pr2 "golangci-Linux-$(h 1)" 2026-10-01T00:00:00Z
  row 61 $pr2 "golangci-Linux-$(h 2)" 2026-10-02T00:00:00Z
  row 70 refs/heads/gh-readonly-queue/main/pr-9-abc "$app-$(h 1)" 2026-10-01T00:00:00Z
  row 71 refs/heads/gh-readonly-queue/main/pr-9-abc "$app-$(h 9)" 2026-10-02T00:00:00Z
} >"$dir/caches.jsonl"

# A stub gh: logs each call; answers a PR lookup `closed` for a number in
# STUB_CLOSED and `open` otherwise; fails the listing (STUB_LIST_FAIL), every
# PR lookup (STUB_PULLS_FAIL), or a delete of an id named in STUB_404 /
# STUB_403, as the real one does (message on stdout and stderr).
mkdir "$dir/bin"
cat >"$dir/bin/gh" <<'EOF'
#!/usr/bin/env bash
echo "$*" >>"$STUB_LOG"
case "$*" in
  *--paginate*)
    [ -n "${STUB_LIST_FAIL:-}" ] && { echo '{"message":"Bad credentials"}'; echo "gh: Bad credentials (HTTP 401)" >&2; exit 1; }
    cat "$STUB_LIST" ;;
  *"/pulls/"*)
    [ -n "${STUB_PULLS_FAIL:-}" ] && { echo "gh: Bad Gateway (HTTP 502)"; exit 1; }
    for a in "$@"; do case "$a" in */pulls/*) n="${a##*/}" ;; esac; done
    case " ${STUB_CLOSED:-} " in *" $n "*) echo closed ;; *) echo open ;; esac ;;
  *"-X DELETE"*)
    id="${!#}"; id="${id##*/}"
    case " ${STUB_404:-} " in *" $id "*) echo "gh: Not Found (HTTP 404)"; exit 1 ;; esac
    case " ${STUB_403:-} " in *" $id "*) echo "gh: Resource not accessible by integration (HTTP 403)"; exit 1 ;; esac ;;
esac
exit 0
EOF
chmod +x "$dir/bin/gh"

# selects <name> <want exit> <want ids, sorted> [env...]: a dry run's picks.
selects() {
  local name="$1" want_rc="$2" want="$3" rc got
  shift 3
  env "$@" PATH="$dir/bin:$PATH" GH_REPO=o/r STUB_LOG="$dir/log" \
    "$script" --dry-run --from "$dir/caches.jsonl" >"$dir/out" 2>&1
  rc=$?
  got="$(sed -n 's/^would delete \([0-9]*\) .*/\1/p' "$dir/out" | sort -n | tr '\n' ' ' | sed 's/ $//')"
  if [ "$rc" = "$want_rc" ] && [ "$got" = "$want" ]; then
    printf '  ok   %-28s exit %s, [%s]\n' "$name" "$rc" "$got"
  else
    printf '  FAIL %-28s want exit %s [%s], got exit %s [%s]\n' "$name" "$want_rc" "$want" "$rc" "$got" >&2
    sed 's/^/       /' "$dir/out" >&2
    fails=$((fails + 1))
  fi
}

# Open PRs: copies of main's (10), superseded (20 21), the same-second tie's
# older (40).
selects "selection, PRs open" 0 "10 20 21 40"
# A closed PR loses every build entry, under both refs; other families stay.
selects "closed PR: all entries" 0 "10 20 21 40 80 81 82" STUB_CLOSED=6
selects "closed PR: only build cache" 0 "10 20 21 22 23 40" STUB_CLOSED=2
# A failed lookup must not read as closed: nothing extra, and the run fails.
selects "PR lookup fails" 1 "10 20 21 40" STUB_CLOSED="2 6" STUB_PULLS_FAIL=1

# deletes <name> <want exit> <want DELETE ids, sorted> [env...]
deletes() {
  local name="$1" want_rc="$2" want="$3" rc got
  shift 3
  : >"$dir/log"
  env "$@" PATH="$dir/bin:$PATH" GH_REPO=o/r STUB_LOG="$dir/log" STUB_LIST="$dir/caches.jsonl" \
    "$script" >"$dir/out" 2>&1
  rc=$?
  got="$(sed -n 's|.*-X DELETE repos/o/r/actions/caches/||p' "$dir/log" | sort -n | tr '\n' ' ' | sed 's/ $//')"
  if [ "$rc" = "$want_rc" ] && [ "$got" = "$want" ]; then
    printf '  ok   %-28s exit %s, deleted [%s]\n' "$name" "$rc" "$got"
  else
    printf '  FAIL %-28s want exit %s [%s], got exit %s [%s]\n' "$name" "$want_rc" "$want" "$rc" "$got" >&2
    sed 's/^/       /' "$dir/out" >&2
    fails=$((fails + 1))
  fi
}

deletes "deletes the selection" 0 "10 20 21 40"
deletes "404 reads as already gone" 0 "10 20 21 40" STUB_404=20
# Fails, but only after trying the rest: one refused delete must not strand them.
deletes "403 fails after the rest" 1 "10 20 21 40" STUB_403=10
deletes "failed listing fails" 1 "" STUB_LIST_FAIL=1

: >"$dir/empty.jsonl"
if out="$(PATH="$dir/bin:$PATH" GH_REPO=o/r STUB_LOG="$dir/log" "$script" --dry-run --from "$dir/empty.jsonl" 2>&1)" &&
  [ "$out" = "prune-pr-build-cache: nothing to delete" ]; then
  printf '  ok   %-28s\n' "empty list"
else
  printf '  FAIL %-28s got: %s\n' "empty list" "$out" >&2
  fails=$((fails + 1))
fi

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "prune-pr-build-cache: all cases passed"

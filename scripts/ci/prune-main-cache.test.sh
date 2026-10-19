#!/usr/bin/env bash
# Behavioral test for scripts/ci/prune-main-cache.sh, which deletes main's
# superseded cache generations with a write token. Deleting the newest entry
# of a family would cost every later run its warm cache, so the selection is
# pinned here against fixtures, and the delete and listing paths against a
# stub `gh`. Run by `make verify` (target: test-prune-main-cache). Needs jq;
# no network.

set -uo pipefail
script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/prune-main-cache.sh"
fails=0

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT

# A 64-hex hash made of one repeated digit.
h() { printf '%064d' 0 | tr 0 "$1"; }

# row <id> <ref> <key> <created_at>
row() {
  printf '{"id":%s,"ref":"%s","key":"%s","version":"v1","created_at":"%s","size_in_bytes":1048576}\n' \
    "$1" "$2" "$3" "$4"
}

main=refs/heads/main
app="gobuild-v3-Linux-go-integration-app"
{
  # Three generations of one flavor: the two older go.
  row 1 $main "$app-$(h 1)" 2026-10-12T00:00:00Z
  row 2 $main "$app-$(h 2)" 2026-10-14T11:50:00Z
  row 3 $main "$app-$(h 3)" 2026-10-14T12:36:00.5Z
  # Its own flavor, though `-integration-` prefixes `-integration-app-`; its
  # only entry is its newest, hash shared or not.
  row 4 $main "gobuild-v3-Linux-go-integration-$(h 1)" 2026-10-12T00:00:00Z
  # gomod and pnpm generations, each family on its own.
  row 10 $main "gomod-v1-Linux-$(h 1)" 2026-10-12T00:00:00Z
  row 11 $main "gomod-v1-Linux-$(h 3)" 2026-10-14T12:34:00Z
  row 12 $main "pnpm-Linux-$(h 5)" 2026-10-13T00:00:00Z
  row 13 $main "pnpm-Linux-$(h 6)" 2026-10-14T12:56:00Z
  row 14 $main "codeql-dependencies-1-Linux-go-$(h 7)" 2026-10-12T00:00:00Z
  row 15 $main "codeql-dependencies-1-Linux-go-$(h 8)" 2026-10-14T12:33:00Z
  # Same second: the higher id is the newer, whatever the fractions or the
  # listing order say.
  row 21 $main "playwright-Linux-$(h 7)" 2026-10-14T00:00:00.1Z
  row 20 $main "playwright-Linux-$(h 6)" 2026-10-14T00:00:00.9Z
  # Paging returned a family's only entry twice: still its newest.
  row 30 $main "golangci-Linux-$(h 9)" 2026-10-13T00:00:00Z
  row 30 $main "golangci-Linux-$(h 9)" 2026-10-13T00:00:00Z
  # No trailing hash: never a generation.
  row 40 $main "smoke-node-v1-Linux-X64-22.0.0" 2026-10-01T00:00:00Z
  row 41 $main "smoke-node-v1-Linux-X64-22.1.0" 2026-10-02T00:00:00Z
  # Overlay databases: one family per language and CodeQL version, the
  # commit sha, run id and attempt dropped. Only the newest of each stays.
  ov="codeql-overlay-base-database-1-433570c23e836bda"
  row 42 $main "$ov-go-2.27.2-68578046021af94ca3564369127e82158d672191-38567452949-1" 2026-10-14T12:56:00Z
  row 43 $main "$ov-go-2.27.2-4ad8d8b5838b72c34a302e19ee7924e476d760ff-38565329743-1" 2026-10-14T12:40:00Z
  row 44 $main "$ov-go-2.27.2-8b37a2a2c49c4a405a0a56f50f8d84ca0db2b6d8-38564481869-2" 2026-10-14T12:33:00Z
  row 45 $main "$ov-javascript-2.27.2-4ad8d8b5838b72c34a302e19ee7924e476d760ff-38565329743-1" 2026-10-14T12:39:00Z
  row 46 $main "$ov-go-2.27.3-4ad8d8b5838b72c34a302e19ee7924e476d760ff-38565329743-1" 2026-10-14T12:00:00Z
  # A commit-sha tail outside the overlay family is not a generation.
  row 47 $main "other-4ad8d8b5838b72c34a302e19ee7924e476d760ff-38565329743-1" 2026-10-01T00:00:00Z
  row 48 $main "other-68578046021af94ca3564369127e82158d672191-38567452949-1" 2026-10-02T00:00:00Z
  # Not main's: PR and merge-queue scopes belong to the PR cleanup.
  row 50 refs/pull/1/merge "$app-$(h 1)" 2026-10-01T00:00:00Z
  row 51 refs/pull/1/merge "$app-$(h 2)" 2026-10-02T00:00:00Z
  row 52 refs/heads/gh-readonly-queue/main/pr-9-abc "astro-Linux-$(h 1)" 2026-10-01T00:00:00Z
  row 53 refs/heads/gh-readonly-queue/main/pr-9-abc "astro-Linux-$(h 2)" 2026-10-02T00:00:00Z
} >"$dir/caches.jsonl"

# A stub gh: logs each call; fails the listing (STUB_LIST_FAIL) or a delete
# of an id named in STUB_404 / STUB_403, as the real one does (message on
# stdout and stderr).
mkdir "$dir/bin"
cat >"$dir/bin/gh" <<'EOF'
#!/usr/bin/env bash
echo "$*" >>"$STUB_LOG"
case "$*" in
  *--paginate*)
    [ -n "${STUB_LIST_FAIL:-}" ] && { echo '{"message":"Bad credentials"}'; echo "gh: Bad credentials (HTTP 401)" >&2; exit 1; }
    cat "$STUB_LIST" ;;
  *"-X DELETE"*)
    id="${!#}"; id="${id##*/}"
    case " ${STUB_404:-} " in *" $id "*) echo "gh: Not Found (HTTP 404)"; exit 1 ;; esac
    case " ${STUB_403:-} " in *" $id "*) echo "gh: Resource not accessible by integration (HTTP 403)"; exit 1 ;; esac ;;
esac
exit 0
EOF
chmod +x "$dir/bin/gh"

want="1 2 10 12 14 20 43 44"

# selects <name> <want ids, sorted>: a dry run's picks.
selects() {
  local name="$1" want="$2" rc got
  PATH="$dir/bin:$PATH" GH_REPO=o/r STUB_LOG="$dir/log" \
    "$script" --dry-run --from "$dir/caches.jsonl" >"$dir/out" 2>&1
  rc=$?
  got="$(sed -n 's/^would delete \([0-9]*\) .*/\1/p' "$dir/out" | sort -n | tr '\n' ' ' | sed 's/ $//')"
  if [ "$rc" = 0 ] && [ "$got" = "$want" ]; then
    printf '  ok   %-28s [%s]\n' "$name" "$got"
  else
    printf '  FAIL %-28s want exit 0 [%s], got exit %s [%s]\n' "$name" "$want" "$rc" "$got" >&2
    sed 's/^/       /' "$dir/out" >&2
    fails=$((fails + 1))
  fi
}

selects "selection" "$want"

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

deletes "deletes the selection" 0 "$want"
deletes "404 reads as already gone" 0 "$want" STUB_404=2
# Fails, but only after trying the rest: one refused delete must not strand them.
deletes "403 fails after the rest" 1 "$want" STUB_403=1
deletes "failed listing fails" 1 "" STUB_LIST_FAIL=1

# The listing asks for main's scope only.
: >"$dir/log"
PATH="$dir/bin:$PATH" GH_REPO=o/r STUB_LOG="$dir/log" STUB_LIST="$dir/caches.jsonl" "$script" --dry-run >/dev/null 2>&1
if grep -q 'actions/caches?ref=refs/heads/main&' "$dir/log"; then
  printf '  ok   %-28s\n' "lists main's scope"
else
  printf '  FAIL %-28s got: %s\n' "lists main's scope" "$(cat "$dir/log")" >&2
  fails=$((fails + 1))
fi

: >"$dir/empty.jsonl"
if out="$(PATH="$dir/bin:$PATH" GH_REPO=o/r STUB_LOG="$dir/log" "$script" --dry-run --from "$dir/empty.jsonl" 2>&1)" &&
  [ "$out" = "prune-main-cache: nothing to delete" ]; then
  printf '  ok   %-28s\n' "empty list"
else
  printf '  FAIL %-28s got: %s\n' "empty list" "$out" >&2
  fails=$((fails + 1))
fi

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "prune-main-cache: all cases passed"

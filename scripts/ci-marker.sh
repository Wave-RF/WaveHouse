#!/usr/bin/env bash
# Pre-push CI marker logic. Called by Makefile (write) and .githooks/pre-push
# (path-for-commit). Tree-keyed: marker name encodes the tree SHA `make ci`
# validated, so commit-then-push doesn't invalidate it.

set -euo pipefail
cd "$(git rev-parse --show-toplevel)" 2>/dev/null || exit 1

usage() { echo "usage: $0 {write|path-for-commit <sha>}" >&2; exit 2; }

# Tree SHA of "what `git add -A && git commit` would produce now" — computed
# against a throwaway index so the real index/working dir stay untouched.
tree_of_working_dir() {
  local tmp_idx
  tmp_idx=$(mktemp); rm -f "$tmp_idx"
  trap "rm -f '$tmp_idx'" EXIT
  export GIT_INDEX_FILE="$tmp_idx"
  git read-tree HEAD
  git add -A
  git write-tree
}

case "${1:-}" in
  write)
    # CI runners don't push — skip the marker write there.
    [ -n "${CI:-}" ] && exit 0
    mkdir -p tmp
    touch "tmp/ci-passed-tree-$(tree_of_working_dir)"
    ;;
  path-for-commit)
    [ -n "${2:-}" ] || usage
    echo "tmp/ci-passed-tree-$(git rev-parse "$2^{tree}")"
    ;;
  *) usage ;;
esac

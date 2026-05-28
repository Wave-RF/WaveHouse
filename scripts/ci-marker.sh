#!/usr/bin/env bash
# Tree-keyed validation markers. Two flavors:
#   tmp/ci-passed-tree-<TREE>      — full `make ci` passed
#   tmp/verify-passed-tree-<TREE>  — just `make verify` passed (subset of ci)
# `make ci` writes both (it runs verify); `make verify` writes only the verify
# marker. Pre-commit skips re-running verify when the marker is current;
# pre-push consults the ci marker. Skipped on CI runners.

set -euo pipefail
cd "$(git rev-parse --show-toplevel)" 2>/dev/null || exit 1

usage() {
  echo "usage: $0 {write|write-verify|has-verify-marker|path-for-commit <sha>}" >&2
  exit 2
}

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
    [ -n "${CI:-}" ] && exit 0
    mkdir -p tmp
    tree=$(tree_of_working_dir)
    touch "tmp/ci-passed-tree-$tree" "tmp/verify-passed-tree-$tree"
    ;;
  write-verify)
    [ -n "${CI:-}" ] && exit 0
    mkdir -p tmp
    touch "tmp/verify-passed-tree-$(tree_of_working_dir)"
    ;;
  has-verify-marker)
    tree=$(tree_of_working_dir)
    [ -f "tmp/verify-passed-tree-$tree" ] && exit 0
    exit 1
    ;;
  path-for-commit)
    [ -n "${2:-}" ] || usage
    echo "tmp/ci-passed-tree-$(git rev-parse "$2^{tree}")"
    ;;
  *) usage ;;
esac

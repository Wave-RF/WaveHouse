#!/usr/bin/env bash
# Canonical "docs prose" resolver. TWO consumers depend on this list, so an
# exclusion added for one silently changes the other:
#
#   1. the docs-reviewer subagent — what it reviews and gates (accuracy-vs-code,
#      runnable examples, clarity, completeness, and code<->docs sync);
#   2. `make lint-prose` / `make fix-prose` — the misspell gate set (via
#      DOCS_PROSE in the Makefile) and the on-save hook (via `is-match`).
#
# So dropping a file here also drops it from spell-checking.
#
# DENYLIST model: every tracked *.md / *.mdx file IS docs prose UNLESS it
# matches an exclusion below — so newly-added docs are covered automatically
# (there is no allowlist to keep in sync). Keep the exclusions in lockstep
# with the THREE places that restate them: AGENTS.md §"Docs review",
# .github/prompts/docs-review.md, and .claude/agents/docs-reviewer.md (the
# gating subagent's own system prompt — easy to miss, and stale there means
# the reviewer scopes itself wrongly).
#
# Excluded (NOT user-facing prose):
#   .claude/** , .github/**     agent prompts, PR/issue templates, tooling
#   CHANGELOG.md                historical record, not instructions
#   AGENTS.md , CLAUDE.md       agent source-of-truth (the reviewer reads
#                               these AS truth to check the other docs)
#   *.draft.md , *.old.md       drafts / archives
#   PERF-CLAIMS-REVIEW.md       internal review artifact
#
# CODE_OF_CONDUCT.md and SUPPORT.md ARE docs prose (they appear in `all`), but
# the docs-reviewer only deep-reviews them when they changed or when a material
# change warrants it — that tiering lives in the agent, not here.
#
# Usage:
#   scripts/docs-prose.sh all              # every docs-prose file (the reading list)
#   scripts/docs-prose.sh changed [base]   # docs-prose changed in <base>...HEAD (default base: main)
#   scripts/docs-prose.sh is-match <path>  # exit 0 if <path> is docs prose, else 1

set -uo pipefail
cd "$(git rev-parse --show-toplevel)" 2>/dev/null || exit 1

# A path is docs prose iff it is *.md/*.mdx and not excluded. `case` patterns
# are glob-style and `*` spans `/`, so `.claude/*` matches at any depth and
# `*.md` matches nested files like docs/src/content/docs/api.md.
is_docs_prose() {
  case "$1" in
    .claude/*|.github/*)                                    return 1 ;;
    CHANGELOG.md|AGENTS.md|CLAUDE.md|PERF-CLAIMS-REVIEW.md) return 1 ;;
    *.draft.md|*.old.md)                                   return 1 ;;
    *.md|*.mdx)                                             return 0 ;;
    *)                                                      return 1 ;;
  esac
}

# Filter stdin (one path per line) down to the docs-prose paths. Always exits 0
# so a trailing non-match doesn't poison the pipe under `pipefail`.
filter_docs_prose() {
  local f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    is_docs_prose "$f" && printf '%s\n' "$f"
  done
  return 0
}

case "${1:-}" in
  all)
    git ls-files -- '*.md' '*.mdx' | filter_docs_prose
    ;;
  changed)
    base="${2:-main}"
    git diff --name-only "${base}...HEAD" -- '*.md' '*.mdx' | filter_docs_prose
    ;;
  is-match)
    [ -n "${2:-}" ] || { echo "usage: $0 is-match <path>" >&2; exit 2; }
    is_docs_prose "$2"
    ;;
  *)
    echo "usage: $0 {all | changed [base] | is-match <path>}" >&2
    exit 2
    ;;
esac

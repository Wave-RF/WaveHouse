#!/usr/bin/env bash
# PostToolUse hook: auto-fix Markdown/MDX after edits — the same fixers `make
# fix` runs, narrowed to the one file just written.
#
# Why a hook (vs leaving it to `make fix` or CI): these are deterministic,
# mechanical corrections — unwrapping hard-wrapped prose (WH001), inserting the
# blank line an MDX fence needs beside a JSX tag (WH002), common typos and
# US spelling. Catching them at write time means an agent's own output is
# already correct, instead of costing a lint failure and a second pass to fix
# by hand. Sibling of gofumpt-on-save.sh, which does the same for Go.
#
# Deliberately NOT wired into .githooks/pre-commit: a commit hook that rewrites
# and re-stages files silently changes what you reviewed and fights `git add -p`.
# The commit hook stays a check; this fixes early enough that it rarely fires.
#
# Order is load-bearing — see scripts/fix-mdx-fences.mjs for why the MDX
# structural pass must precede markdownlint.
#
# Safety: best-effort throughout. A missing tool, an unparseable file, or a
# lint error that has no fix leaves the file alone and never blocks the edit.

set -uo pipefail

input=$(cat)
file_path=$(echo "$input" | jq -r '.tool_input.file_path // empty' 2>/dev/null)

case "$file_path" in
  *.md | *.mdx) ;;
  *) exit 0 ;;
esac
[ -f "$file_path" ] || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

# markdownlint-cli2 resolves globs (and per-directory config) from the repo
# root, so hand it a repo-relative path.
rel="${file_path#"$PWD"/}"

# Phase 1: MDX structure. Must land before any generic fixer sees the file.
if [ "${rel##*.}" = "mdx" ]; then
  node scripts/fix-mdx-fences.mjs "$rel" >/dev/null 2>&1 || true
fi

# Phase 2: markdownlint (style + WH001 unwrapping). `--no-globs` keeps it to
# this one file instead of the whole repo; a nonzero exit just means something
# unfixable remains (e.g. a fence with no language), which CI will report.
#
# Twice, mirroring `fix:md` in package.json: WH001's insert carries the pre-fix
# text of the lines it joins, so another rule's fix for a joined line is dropped
# on the first pass. One pass would leave behind an issue `make fix` would have
# cleared — the exact round-trip this hook exists to avoid, in the common case
# where WH001 fires.
if [ -x node_modules/.bin/markdownlint-cli2 ]; then
  node_modules/.bin/markdownlint-cli2 --no-globs --fix ":$rel" >/dev/null 2>&1 || true
  node_modules/.bin/markdownlint-cli2 --no-globs --fix ":$rel" >/dev/null 2>&1 || true
fi

# Phase 3: spelling, over docs prose only — the same scope `make lint-prose`
# uses, resolved by the canonical script rather than a second copy of the list.
if scripts/docs-prose.sh is-match "$rel" 2>/dev/null; then
  for misspell in .bin/*/misspell-*; do
    [ -x "$misspell" ] || continue
    "$misspell" -locale US -source text -w "$rel" >/dev/null 2>&1 || true
    break
  done
fi

exit 0

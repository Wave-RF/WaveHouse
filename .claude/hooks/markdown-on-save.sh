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
# The two branches below are mutually exclusive by extension: markdownlint's
# generic fixers never see .mdx. See scripts/fix-mdx-fences.mjs for why.
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

# Resolve both sides to physical paths before comparing. A literal prefix strip
# is not a containment check: `<repo>/../notes.md` strips to `../notes.md`,
# which is not absolute and would sail past a `case */*` bail. Symlinks have the
# same problem in reverse. This repo's Markdown conventions have no business
# rewriting agent memory files under ~/.claude/, scratch notes in /tmp, or
# Markdown in an unrelated checkout — all of which a session routinely writes.
root=$(pwd -P) || exit 0
dir=$(cd "$(dirname "$file_path")" 2>/dev/null && pwd -P) || exit 0
case "$dir" in
  "$root" | "$root"/*) ;;
  *) exit 0 ;;
esac

# markdownlint-cli2 resolves globs (and per-directory config) from the repo
# root, so hand it a repo-relative path.
if [ "$dir" = "$root" ]; then
  rel=$(basename "$file_path")
else
  rel="${dir#"$root"/}/$(basename "$file_path")"
fi

# .mdx gets exactly one STRUCTURAL fixer, and it is ours (misspell below still
# corrects spelling there). The generic markdownlint rules are deliberately
# never run against MDX — markdownlint parses CommonMark, MDX
# does not, and where the two disagree a generic autofix rewrites the inside of
# a code block. fix-mdx-fences only ever inserts a blank line beside a JSX tag,
# so its worst failure is a render-neutral blank line. `make lint` still CHECKS
# .mdx; it just never acts on the disagreement. Mirrors `fix:md`.
if [ "${rel##*.}" = "mdx" ]; then
  node scripts/fix-mdx-fences.mjs "$rel" >/dev/null 2>&1 || true
elif [ -x node_modules/.bin/markdownlint-cli2 ]; then
  # Plain Markdown: markdownlint's parse IS authoritative, so the full fixer
  # chain is safe. `--no-globs` keeps it to this one file rather than the whole
  # repo; a nonzero exit only means something unfixable remains (e.g. a fence
  # with no language), which CI reports.
  #
  # Twice, mirroring `fix:md`: WH001's insert carries the pre-fix text of the
  # lines it joins, so another rule's fix for a joined line is dropped on the
  # first pass. One pass would leave behind exactly the issue this hook exists
  # to prevent, in the common case where WH001 fires.
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

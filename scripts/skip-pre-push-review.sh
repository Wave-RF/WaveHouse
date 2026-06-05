#!/usr/bin/env bash
# Record a DELIBERATE skip of a pre-push reviewer for the current HEAD.
#
# This is the SANCTIONED way for /prepush to skip a reviewer it judges
# out-of-lane for the diff (e.g. the code reviewer on a docs-only typo), so a
# trivial change doesn't pay for a full review. It:
#   1. writes the reviewer's marker tmp/<name>-passed-<HEAD>  (satisfies the
#      push gate in .claude/hooks/agent-bash-gate.sh), and
#   2. logs "<name>\t<reason>" to tmp/review-skips-<HEAD>.log for audit
#      (surfaced by the gate on push and by /prepush to the user).
#
# Use this instead of hand-touching a marker: the skip is then deliberate and
# recorded, not silent. The orchestrator's judgment is the gate here (a choice
# made deliberately — see AGENTS.md §"Don't bypass the gates"); when in doubt,
# RUN the reviewer instead of skipping it.
#
# Usage: scripts/skip-pre-push-review.sh <reviewer> "<reason>"

set -uo pipefail

name="${1:-}"
shift 2>/dev/null || true
reason="${*:-}"
[ -n "$reason" ] || reason="(no reason given)"

if [ -z "$name" ]; then
  echo "skip-pre-push-review: usage: $0 <reviewer> \"<reason>\"" >&2
  exit 2
fi

# Only a real gating reviewer can be skipped — refuse to mint a marker for an
# arbitrary name (that would just confuse the gate, never satisfy it).
list_script="scripts/pre-push-reviewers.sh"
if [ ! -f "$list_script" ] || ! bash "$list_script" 2>/dev/null | grep -Fxq -- "$name"; then
  echo "skip-pre-push-review: '$name' is not a reviewer in $list_script — refusing." >&2
  exit 2
fi

head_sha=$(git rev-parse HEAD 2>/dev/null) || {
  echo "skip-pre-push-review: not a git repo / no HEAD." >&2
  exit 2
}

# Resolve the review base the same way the push gate does (local main, else
# origin/main) for the advisory relevance check below.
base=""
for ref in main origin/main; do
  if git rev-parse --verify --quiet "$ref" >/dev/null 2>&1; then base="$ref"; break; fi
done

# marker scheme: tmp/<reviewer>-passed-<HEAD> — keep in sync with
# .claude/hooks/review-marker.sh and .claude/hooks/agent-bash-gate.sh.
marker="tmp/${name}-passed-${head_sha}"
skiplog="tmp/review-skips-${head_sha}.log"

# Advisory relevance check — NEVER blocks (judgment is the orchestrator's), but
# warns loudly when a skip looks wrong so an obvious mistake gets a second look.
warn=""
case "$name" in
  pre-push-reviewer)
    if [ -n "$base" ] && git diff --name-only "${base}...HEAD" 2>/dev/null | grep -qvE '\.mdx?$'; then
      warn="non-docs files changed on this branch — a code review is probably warranted"
    fi
    ;;
  docs-reviewer)
    if [ -x scripts/docs-prose.sh ] && [ -n "$(scripts/docs-prose.sh changed 2>/dev/null)" ]; then
      warn="docs-prose files changed on this branch — a docs review is probably warranted"
    fi
    ;;
esac

mkdir -p tmp || { echo "skip-pre-push-review: cannot create tmp/." >&2; exit 2; }
touch "$marker"  || { echo "skip-pre-push-review: cannot write $marker." >&2; exit 2; }
printf '%s\t%s\n' "$name" "$reason" >> "$skiplog"

[ -n "$warn" ] && echo "⚠️  skip-pre-push-review: skipping ${name}, but ${warn}. Skip recorded anyway." >&2
echo "⏭️  Skipped ${name} for HEAD ${head_sha:0:8} — marker written; reason: ${reason}" >&2

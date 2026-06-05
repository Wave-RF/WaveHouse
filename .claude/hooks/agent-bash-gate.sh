#!/usr/bin/env bash
# Agent PR workflow gate (PreToolUse Bash). Catches accidental violations of
# rules that have no human analog:
#   - gh pr create without --draft
#   - gh pr ready
#   - gh pr edit --add-reviewer / --add-assignee
#   - gh api .../requested_reviewers (write verbs)
#   - gh pr review --approve / --request-changes
#   - git push to a PR branch missing either pre-push review marker
#     (pre-push-reviewer code review + docs-reviewer docs review)
#
# Universal git checks (ci-passed marker, no-verify, etc.) live in .githooks/
# and apply to humans and agents equally. Bypass surface acknowledged: an
# agent that wants to bypass can edit this file or settings.json — these
# rules prevent accidental violations, not adversarial ones. See AGENTS.md
# §"Agent PR Discipline" for policy.

set -uo pipefail

block() {
  cat >&2 <<EOF

🛑 Claude PR discipline gate: $1

See AGENTS.md §"Agent PR Discipline".
EOF
  exit 2
}

if ! command -v jq >/dev/null 2>&1; then
  block "jq is required for this gate. Install jq or remove the PreToolUse hook."
fi

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""' 2>/dev/null) \
  || block "Could not parse hook payload as JSON."
[ -z "$cmd" ] && exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

# Strip quoted segments so commands that mention a blocked pattern in a
# string don't false-positive. Doesn't cover heredocs — pass long bodies via
# `-F <file>` if needed.
stripped=$(printf '%s' "$cmd" | sed -E "s/'[^']*'//g; s/\"[^\"]*\"//g")

git_subcmd() {
  printf '%s\n' "$stripped" | grep -qE "(^|[[:space:];|&]+)git[[:space:]]+$1\b"
}
git_subcmd_is_help() {
  printf '%s\n' "$stripped" | grep -qE "(^|[[:space:];|&]+)git[[:space:]]+$1([[:space:]]+[^;|&]*)?[[:space:]]+(-h|--help)([[:space:]]|\$|[;|&])"
}

# gh pr create requires --draft.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+create\b'; then
  printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--draft|-d)\b' \
    || block "Agent-opened PRs must use --draft. Only humans publish ready-for-review PRs."
fi

# gh pr ready is humans-only.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+ready\b'; then
  block "Only humans flip drafts to ready-for-review. Ask the user."
fi

# gh pr edit --add-reviewer / --add-assignee is humans-only.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+edit\b' \
   && printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])--(add|remove)-(reviewer|assignee)\b'; then
  block "Adding/removing reviewers is humans-only. Re-trigger bot reviewers via PR comment mention (e.g. @coderabbitai review)."
fi

# gh api .../requested_reviewers write verbs (API form of --add-reviewer).
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+api\b' \
   && printf '%s\n' "$stripped" | grep -qE 'requested_reviewers' \
   && printf '%s\n' "$stripped" | grep -qE '(-X[[:space:]]*(POST|PUT|PATCH)|--method[[:space:]]*(POST|PUT|PATCH)|[[:space:]]-f[[:space:]]+reviewers=|[[:space:]]-F[[:space:]]+reviewers=)'; then
  block "Reviewer-write requests are humans-only. Re-trigger bot reviewers via PR comment mention."
fi

# gh pr review --approve / --request-changes are humans-only.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+review\b'; then
  printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--approve|-a)\b' \
    && block "Only humans approve PRs."
  printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--request-changes|-r)\b' \
    && block "Agents post inline review comments instead of --request-changes."
fi

# git push from a non-main branch requires BOTH pre-push review markers for HEAD
# — the pre-push-reviewer (code) marker AND the docs-reviewer (docs) marker. Both
# are unconditional: even a code-only change goes through docs review, because
# catching "code changed but the docs should have and didn't" is the docs
# reviewer's job.
#
# We gate on "non-main branch with commits ahead of the base", NOT on "an OPEN PR
# exists". The agent flow is push-the-branch THEN open the draft PR, so keying on
# PR state let the FIRST push — the one that actually publishes the diff — skip
# review entirely. Trade-off: this also gates WIP/throwaway feature-branch pushes;
# that's intentional (agents review before sharing code; a human can push WIP from
# their own shell). The universal .githooks/pre-push handles ci-passed for everyone.
if git_subcmd 'push' && ! git_subcmd_is_help 'push'; then
  head_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
  branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")
  if [ -n "$head_sha" ] && [ -n "$branch" ] && [ "$branch" != "main" ]; then
    # Only gate when there's actually a diff to review: commits on HEAD not yet on
    # the base (local main, else origin/main). No base resolvable → fail safe and
    # gate. A branch with no delta vs the base has nothing for the reviewers.
    base=""
    for ref in main origin/main; do
      if git rev-parse --verify --quiet "$ref" >/dev/null 2>&1; then base="$ref"; break; fi
    done
    has_delta=1
    if [ -n "$base" ] && [ -z "$(git rev-list "${base}..HEAD" 2>/dev/null)" ]; then
      has_delta=0
    fi
    if [ "$has_delta" = "1" ]; then
      missing=""
      [ -f "tmp/review-passed-${head_sha}" ] \
        || missing="${missing}  - pre-push-reviewer (code)              -> tmp/review-passed-${head_sha:0:8}
"
      [ -f "tmp/docs-review-passed-${head_sha}" ] \
        || missing="${missing}  - docs-reviewer (docs prose + doc-sync) -> tmp/docs-review-passed-${head_sha:0:8}
"
      if [ -n "$missing" ]; then
        cat >&2 <<EOF

🛑 Claude PR discipline gate: missing pre-push review marker(s) for HEAD (${head_sha:0:8}) on branch '${branch}':

${missing}
Run /prepush — it launches both reviewers in parallel in fresh context and loops
to ship_it. Each writes its marker on VERDICT: ship_it, and the push then
succeeds. Both must reach ship_it (zero findings) — see AGENTS.md
§"Agent PR Discipline".
EOF
        exit 2
      fi
    fi
  fi
fi

exit 0

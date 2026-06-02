#!/usr/bin/env bash
# Agent PR workflow gate (PreToolUse Bash). Catches accidental violations of
# rules that have no human analog:
#   - gh pr create without --draft
#   - gh pr ready
#   - gh pr edit --add-reviewer / --add-assignee
#   - gh api .../requested_reviewers (write verbs)
#   - gh pr review --approve / --request-changes
#   - git push to a PR branch without a pre-push-reviewer review-passed marker
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

# git push to a PR branch requires a pre-push-reviewer review-passed marker.
# (The universal .githooks/pre-push handles ci-passed for everyone.)
if git_subcmd 'push' && ! git_subcmd_is_help 'push'; then
  head_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
  branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")
  if [ -n "$head_sha" ] && [ -n "$branch" ] && [ "$branch" != "main" ]; then
    if ! command -v gh >/dev/null 2>&1; then
      block "gh CLI required to detect PR state for '${branch}'. Install gh or push from main."
    fi
    pr_state=""
    if pr_view_out=$(gh pr view "$branch" --json state --jq .state 2>&1); then
      pr_state="$pr_view_out"
    elif ! printf '%s' "$pr_view_out" | grep -qiE 'no (open )?pull request'; then
      block "Could not determine PR state for '${branch}': ${pr_view_out}"
    fi
    if [ "$pr_state" = "OPEN" ] && [ ! -f "tmp/review-passed-${head_sha}" ]; then
      cat >&2 <<EOF

🛑 Claude PR discipline gate: no review marker for HEAD (${head_sha:0:8}) on PR branch '${branch}'.

Invoke the pre-push-reviewer subagent in fresh context before pushing. When
it returns VERDICT: ship_it, tmp/review-passed-${head_sha:0:8} is written
automatically and this push will succeed.
EOF
      exit 2
    fi
  fi
fi

exit 0

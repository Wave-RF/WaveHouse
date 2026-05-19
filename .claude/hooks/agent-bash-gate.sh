#!/usr/bin/env bash
# PreToolUse Bash gate — enforces AGENTS.md §"Agent PR Discipline".
#
# This hook is what the deny-list can't be (deny patterns are prefix-glob only).
# Here we regex over the whole command to catch:
#
#   1. --no-verify on git push/commit (no agent bypass; humans can use it intentionally)
#   2. gh pr create without --draft (only humans publish ready-for-review PRs)
#   3. gh pr ready (humans only — draft→ready is a deliberate human signal)
#   4. gh pr edit --add-reviewer / --add-assignee (human reviewer assignment is humans-only;
#      bot reviewers are re-triggered via PR comments, not the reviewer API)
#   5. gh api .../requested_reviewers POST (the API form of --add-reviewer)
#   6. gh pr review --approve / --request-changes (only humans take formal review actions;
#      bot reviewers use inline review comments + sticky summaries instead)
#   7. Direct marker forgery (touch / redirect / sh -c wrappers writing to
#      tmp/(ci|review)-passed-*)
#   8. git push without required markers (ci-passed always; review-passed on PR branches)
#
# Anything blocked here can typically be re-run by a human from terminal — these
# rules are for the agent path, not the underlying git/gh capabilities.

set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty' 2>/dev/null)
[ -z "$cmd" ] && exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

# --- Helper: block with a structured reason ---------------------------------
block() {
  local reason="$1"
  cat >&2 <<EOF

🛑 Claude PR discipline gate: ${reason}

See AGENTS.md §"Agent PR Discipline" for the full ruleset. If you genuinely
need to bypass, ask the human user to run the command themselves.
EOF
  exit 2
}

# Boundary helper: detects a `git <subcommand>` invocation anywhere in the
# command (including after && / ; / | / cd ... &&). Used by multiple checks.
git_subcmd() {
  printf '%s\n' "$cmd" | grep -qE "(^|[[:space:];|&]+)git[[:space:]]+$1\b"
}

# --- 1. --no-verify on git push/commit --------------------------------------
if git_subcmd 'push' || git_subcmd 'commit'; then
  if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:]])--no-verify\b'; then
    block "git push/commit with --no-verify is not permitted for agents. Run the gates."
  fi
fi

# --- 2. gh pr create without --draft ----------------------------------------
if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+create\b'; then
  if ! printf '%s\n' "$cmd" | grep -qE '(^|[[:space:]])(--draft|-d)\b'; then
    block "Agent-opened PRs must be created with --draft. Only humans publish ready-for-review PRs."
  fi
fi

# --- 3. gh pr ready ---------------------------------------------------------
if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+ready\b'; then
  block "Only humans transition PRs from draft to ready-for-review. Ask the user to do this manually when the PR is ready."
fi

# --- 4. gh pr edit with reviewer/assignee changes ---------------------------
if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+edit\b'; then
  if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:]])--(add|remove)-(reviewer|assignee)\b'; then
    block "Adding/removing reviewers or assignees is humans-only. To re-trigger bot reviewers, post a PR comment mentioning them (@coderabbitai review, @gemini-code-assist /gemini review, @claude / /review)."
  fi
fi

# --- 5. gh api .../requested_reviewers POST (humans-only reviewer-add API) --
if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+api\b' && \
   printf '%s\n' "$cmd" | grep -qE 'requested_reviewers'; then
  if printf '%s\n' "$cmd" | grep -qE '(-X[[:space:]]*POST|--method[[:space:]]*POST|[[:space:]]-f[[:space:]]+reviewers=|[[:space:]]-F[[:space:]]+reviewers=)'; then
    block "POST to /requested_reviewers is the API form of --add-reviewer; humans-only. For bot reviewers, post a PR comment mentioning them."
  fi
fi

# --- 6. gh pr review --approve / --request-changes --------------------------
if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+review\b'; then
  if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:]])(--approve|-a)\b'; then
    block "Only humans approve PRs (--approve)."
  fi
  if printf '%s\n' "$cmd" | grep -qE '(^|[[:space:]])(--request-changes|-r)\b'; then
    block "Agents don't use --request-changes. Post inline review comments via the GitHub inline-comment MCP tool, or use gh pr comment for top-level comments."
  fi
fi

# --- 7. Marker forgery ------------------------------------------------------
# Block any *write-like* operation targeting tmp/(ci|review)-passed-*.
# Read operations (ls/cat/find) on the markers are fine.
if printf '%s\n' "$cmd" | grep -qE 'tmp/(ci|review)-passed-'; then
  if printf '%s\n' "$cmd" | grep -qE '(touch|install)[[:space:]]+[^&|;]*tmp/(ci|review)-passed-'; then
    block "Direct marker creation is forbidden. Markers are written by 'make ci' (ci-passed) and the pre-push-reviewer agent's PostToolUse hook (review-passed)."
  fi
  if printf '%s\n' "$cmd" | grep -qE '(cp|mv)[[:space:]]+[^&|;]+[[:space:]]+[^&|;]*tmp/(ci|review)-passed-'; then
    block "Direct marker creation via cp/mv is forbidden."
  fi
  if printf '%s\n' "$cmd" | grep -qE '>[[:space:]]*tmp/(ci|review)-passed-'; then
    block "Direct marker creation via output redirect is forbidden."
  fi
  if printf '%s\n' "$cmd" | grep -qE '\b(sh|bash|zsh|env)\b[[:space:]]+-c[[:space:]]+.*tmp/(ci|review)-passed-'; then
    block "Wrapping marker creation in a shell -c to bypass the gate is forbidden."
  fi
fi

# --- 8. git push: check markers ---------------------------------------------
# Only on actual `git push` invocations (not `git push --help`, not `gh pr push`).
if git_subcmd 'push'; then
  head_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
  if [ -n "$head_sha" ]; then
    short_sha="${head_sha:0:8}"
    ci_marker="tmp/ci-passed-${head_sha}"
    review_marker="tmp/review-passed-${head_sha}"

    # 8a. ci-passed required for every push (mirrors the universal git pre-push hook;
    # firing here too gives Claude a more actionable error inside its session).
    if [ ! -f "$ci_marker" ]; then
      cat >&2 <<EOF

🛑 Claude PR discipline gate: 'make ci' has not been run for HEAD (${short_sha}).

Per AGENTS.md §"Local-First Validation", every push must have passing local CI
for the exact HEAD being published. Run:

    make ci

The 'ci' Makefile target writes tmp/ci-passed-${short_sha} on success. Then retry
'git push'.
EOF
      exit 2
    fi

    # 8b. review-passed required when HEAD branch has an open PR.
    branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")
    if [ -n "$branch" ] && [ "$branch" != "main" ]; then
      pr_state=$(gh pr view "$branch" --json state --jq .state 2>/dev/null || echo "")
      if [ "$pr_state" = "OPEN" ]; then
        if [ ! -f "$review_marker" ]; then
          cat >&2 <<EOF

🛑 Claude PR discipline gate: no review marker for HEAD (${short_sha}) on PR branch '${branch}'.

Invoke the pre-push-reviewer subagent in fresh context before pushing:

    Use the Agent tool with subagent_type="pre-push-reviewer" and a prompt
    asking it to review the current branch's full diff vs main, the latest
    commit, open PR comments, and CI status.

When the agent's response ends with "VERDICT: ship_it", a PostToolUse hook
auto-writes tmp/review-passed-${short_sha} and this push will succeed.

If the agent returns VERDICT: iterate or VERDICT: block, address the findings
and re-invoke the agent (always in fresh context — never the same session)
until ship_it.

Per AGENTS.md §"Agent PR Discipline", you cannot bypass this with --no-verify
or by writing the marker file directly. CI's claude-review will also fire on
push, but pre-push review catches issues before consuming shared capacity.
EOF
          exit 2
        fi
      fi
    fi
  fi
fi

exit 0

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
#   5. gh api .../requested_reviewers POST/PUT (the API form of --add-reviewer)
#   6. gh pr review --approve / --request-changes (only humans take formal review actions;
#      bot reviewers use inline review comments + sticky summaries instead)
#   7. git push without required markers (ci-passed always; review-passed on PR branches)
#
# Marker forgery (writing tmp/(ci|review)-passed-* by any means) is NOT blocked
# here. The .claude/settings.json deny list catches the obvious tool-level
# attempts (Bash(touch tmp/...:*), Write/Edit on the paths); everything else
# relies on the honest-agent model documented in AGENTS.md §"Agent PR
# Discipline" — Bash can write a file by a dozen paths and regex enforcement
# becomes a porous game of whack-a-mole that oversells what it delivers.
#
# Anything blocked here can typically be re-run by a human from terminal — these
# rules are for the agent path, not the underlying git/gh capabilities.

set -uo pipefail

# --- Helper: block with a structured reason ---------------------------------
# Declared early because the parse step below may need it.
block() {
  local reason="$1"
  cat >&2 <<EOF

🛑 Claude PR discipline gate: ${reason}

See AGENTS.md §"Agent PR Discipline" for the full ruleset. If you genuinely
need to bypass, ask the human user to run the command themselves.
EOF
  exit 2
}

# Fail-closed on missing jq or malformed JSON — silently exiting 0 here would
# disable every discipline check below, which is exactly what this hook is
# supposed to prevent. A valid Bash PreToolUse payload always has
# .tool_input.command, so a parse failure means something is wrong, not benign.
if ! command -v jq >/dev/null 2>&1; then
  block "jq is required for the PR discipline gate but is not installed. Install jq (brew install jq) or remove the PreToolUse hook from .claude/settings.json."
fi

input=$(cat)
if ! cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""' 2>/dev/null); then
  block "Could not parse hook payload as JSON; refusing to fail open."
fi
[ -z "$cmd" ] && exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

# Strip single- and double-quoted segments before matching, so legitimate
# commands that *mention* a blocked pattern inside a string (e.g.
# `gh pr comment -b "we will git push after CI"`, `echo "use --no-verify"`)
# don't false-positive. Same intent as commit 6c79315 (which fixed the
# no-verify regex specifically), generalized to every check below. Doesn't
# cover escaped quotes or heredocs — accept the corner case; agents
# constructing such commands are doing something weird.
stripped=$(printf '%s' "$cmd" | sed -E "s/'[^']*'//g; s/\"[^\"]*\"//g")

# Boundary helper: detects a `git <subcommand>` invocation anywhere in the
# (quote-stripped) command (including after && / ; / | / cd ... &&). Used
# by multiple checks.
git_subcmd() {
  printf '%s\n' "$stripped" | grep -qE "(^|[[:space:];|&]+)git[[:space:]]+$1\b"
}

# True if `git <subcommand>` is followed by -h / --help anywhere before a
# separator (so `git push --help`, `git push origin main --help`, `git push -h`
# all return true). Help invocations don't actually run the subcommand, so
# discipline gates should skip them.
git_subcmd_is_help() {
  printf '%s\n' "$stripped" | grep -qE "(^|[[:space:];|&]+)git[[:space:]]+$1([[:space:]]+[^;|&]*)?[[:space:]]+(-h|--help)([[:space:]]|\$|[;|&])"
}

# --- 1. --no-verify on git push/commit --------------------------------------
# Quote-stripping above already excludes false positives where the literal
# string sits inside a quoted commit-message body. Honest-agent defense, not
# adversarial — eval / sh -c wrappers around `git push --no-verify` could
# still bypass; AGENTS.md §"Agent PR Discipline" makes the rule explicit.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&])git[[:space:]]+(push|commit)\b[[:space:]][^&|;]*--no-verify\b'; then
  block "git push/commit with --no-verify is not permitted for agents. Run the gates."
fi

# --- 2. gh pr create without --draft ----------------------------------------
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+create\b'; then
  if ! printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--draft|-d)\b'; then
    block "Agent-opened PRs must be created with --draft. Only humans publish ready-for-review PRs."
  fi
fi

# --- 3. gh pr ready ---------------------------------------------------------
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+ready\b'; then
  block "Only humans transition PRs from draft to ready-for-review. Ask the user to do this manually when the PR is ready."
fi

# --- 4. gh pr edit with reviewer/assignee changes ---------------------------
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+edit\b'; then
  if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])--(add|remove)-(reviewer|assignee)\b'; then
    block "Adding/removing reviewers or assignees is humans-only. To re-trigger bot reviewers, post a PR comment mentioning them (@coderabbitai review, @gemini-code-assist /gemini review, @claude / /review)."
  fi
fi

# --- 5. gh api .../requested_reviewers any write verb (humans-only API) -----
# Both POST (add) and PUT (replace) on /pulls/<n>/requested_reviewers are
# reviewer-write operations. Neither has a legitimate agent use case — bot
# reviewers are re-triggered via PR comments. Match any reviewer-write idiom.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+api\b' && \
   printf '%s\n' "$stripped" | grep -qE 'requested_reviewers'; then
  if printf '%s\n' "$stripped" | grep -qE '(-X[[:space:]]*(POST|PUT|PATCH)|--method[[:space:]]*(POST|PUT|PATCH)|[[:space:]]-f[[:space:]]+reviewers=|[[:space:]]-F[[:space:]]+reviewers=)'; then
    block "Write requests to /requested_reviewers are the API form of --add-reviewer; humans-only. For bot reviewers, post a PR comment mentioning them."
  fi
fi

# --- 6. gh pr review --approve / --request-changes --------------------------
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+review\b'; then
  if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--approve|-a)\b'; then
    block "Only humans approve PRs (--approve)."
  fi
  if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--request-changes|-r)\b'; then
    block "Agents don't use --request-changes. Post inline review comments via the GitHub inline-comment MCP tool, or use gh pr comment for top-level comments."
  fi
fi

# --- 7. git push: check markers ---------------------------------------------
# Only on actual `git push` invocations (not `git push --help`, not `gh pr push`).
if git_subcmd 'push' && ! git_subcmd_is_help 'push'; then
  head_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
  if [ -n "$head_sha" ]; then
    short_sha="${head_sha:0:8}"
    ci_marker="tmp/ci-passed-${head_sha}"
    review_marker="tmp/review-passed-${head_sha}"

    # 7a. ci-passed required for every push (mirrors the universal git pre-push hook;
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

    # 7b. review-passed required when HEAD branch has an open PR.
    branch=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")
    if [ -n "$branch" ] && [ "$branch" != "main" ]; then
      if ! command -v gh >/dev/null 2>&1; then
        block "gh CLI is required to determine PR state for branch '${branch}' (needed to enforce pre-push review on PR branches). Install gh or push from main."
      fi
      # gh pr view exits 1 for both "no PR for this branch" (benign — no review-marker
      # enforcement needed) and "auth/network error" (NOT benign — would silently bypass
      # the review gate). Differentiate by capturing stderr and grepping for the
      # well-known "no pull requests found" message; anything else is treated as a
      # real failure and blocks.
      pr_state=""
      if pr_view_out=$(gh pr view "$branch" --json state --jq .state 2>&1); then
        pr_state="$pr_view_out"
      elif printf '%s' "$pr_view_out" | grep -qiE 'no (open )?pull request'; then
        pr_state=""
      else
        block "Could not determine PR state for branch '${branch}': ${pr_view_out}. Refusing to silently skip review-marker enforcement. Run 'gh auth status', 'gh auth login', or retry."
      fi
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

Per AGENTS.md §"Agent PR Discipline", agents do not bypass this with
--no-verify, and you do not write the marker file directly by any means —
the marker is wrong-shaped if you're the one writing it. CI's claude-review
will also fire on push, but pre-push review catches issues before consuming
shared capacity.
EOF
          exit 2
        fi
      fi
    fi
  fi
fi

exit 0

#!/usr/bin/env bash
# Agent PR workflow gate (PreToolUse Bash). Catches accidental violations of
# rules that have no human analog:
#   - gh pr create without --draft
#   - gh pr ready
#   - gh pr edit --add-reviewer / --add-assignee
#   - gh api .../requested_reviewers (write verbs)
#   - gh pr review --approve / --request-changes
#   - git push to a PR branch missing any pre-push review marker
#     (one per reviewer in scripts/pre-push-reviewers.sh — code, docs, …)
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

# gh pr create / gh pr edit --title: validate the title against the SAME
# Conventional-Commits rule the required `CI` check's `PR title` job enforces
# (scripts/lint-pr-title.sh is the shared rule) — so a too-long or wrong-format
# title is caught locally BEFORE the PR exists, not after the required check
# fails. Extract the quoted --title/-t value from the ORIGINAL command (the
# `stripped` copy has quotes removed). Fail-open: if no title is parseable
# (--fill, interactive, unquoted) or the validator is missing, fall through to
# the CI check rather than block a create we can't confidently judge.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+(create|edit)\b'; then
  pr_title=$(printf '%s' "$cmd" | sed -nE 's/.*(--title|-t)[[:space:]]+"([^"]*)".*/\2/p')
  [ -z "$pr_title" ] && pr_title=$(printf '%s' "$cmd" | sed -nE "s/.*(--title|-t)[[:space:]]+'([^']*)'.*/\2/p")
  if [ -n "$pr_title" ] && [ -x scripts/lint-pr-title.sh ]; then
    if ! reason=$(scripts/lint-pr-title.sh "$pr_title" 2>&1); then
      block "$reason"
    fi
  fi
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

# git push from a non-main branch requires a pre-push review marker for HEAD
# from EVERY reviewer in scripts/pre-push-reviewers.sh (the single source of
# truth — code, docs, and any future reviewer such as security). All are
# unconditional: even a code-only change goes through docs review, because
# catching "code changed but the docs should have and didn't" is the docs
# reviewer's job. The set is read at push time, so adding a reviewer there
# immediately makes it gate — no change needed here.
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
      # Resolve the gating reviewers from the single source of truth. If we
      # can't get a valid reviewer list — file missing, unreadable, empty, or
      # corrupt (a syntax error / conflict markers make `bash` error out to
      # empty stdout) — we can't know what to require, so fail CLOSED (block)
      # rather than silently let an un-reviewed push by. Capture once and
      # validate each name for filename-safety before building a marker path.
      reviewers_script="scripts/pre-push-reviewers.sh"
      reviewers_list=$(bash "$reviewers_script" 2>/dev/null | grep -E '^[A-Za-z0-9._-]+$')
      if [ -z "$reviewers_list" ]; then
        block "reviewer list '$reviewers_script' produced no valid reviewer names (missing, unreadable, empty, or corrupt) — can't tell which reviews gate this push. Fix it before pushing."
      fi
      missing=""
      for r in $reviewers_list; do
        [ -f "tmp/${r}-passed-${head_sha}" ] \
          || missing="${missing}  - ${r} -> tmp/${r}-passed-${head_sha:0:8}
"
      done
      if [ -n "$missing" ]; then
        cat >&2 <<EOF

🛑 Claude PR discipline gate: missing pre-push review marker(s) for HEAD (${head_sha:0:8}) on branch '${branch}':

${missing}
Run /prepush — it reads ${reviewers_script}, runs the reviewers this change needs
in parallel (fresh context), and skips the rest on the record. Each reviewer it
runs must reach VERDICT: ship_it (zero findings) to write its marker; a skipped
reviewer gets a logged marker via scripts/skip-pre-push-review.sh. The push
succeeds once every listed reviewer has a marker — see AGENTS.md §"Agent PR
Discipline".
EOF
        exit 2
      fi
      # All required markers present. Surface any reviewers that were skipped by
      # judgment (recorded by scripts/skip-pre-push-review.sh) so the skip is
      # visible at push time, not just in the /prepush conversation. Advisory.
      skiplog="tmp/review-skips-${head_sha}.log"
      if [ -f "$skiplog" ]; then
        echo "ℹ️  pre-push: reviewer(s) skipped by judgment for ${head_sha:0:8} (see ${skiplog}):" >&2
        sed 's/^/    ⏭️  /' "$skiplog" >&2
      fi
    fi
  fi
fi

exit 0

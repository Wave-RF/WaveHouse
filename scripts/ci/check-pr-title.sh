#!/usr/bin/env bash
# Validate a PR title against Conventional Commits — the blocking half of
# the title gate (ci.yml's `PR title` job). Delegates the rule to the
# shared scripts/lint-pr-title.sh; this wrapper handles the CI specifics:
#
#   - Reads the CURRENT title + author from the API, NOT the event payload.
#     Re-runs replay the original payload, so the "fix the title, re-run
#     the failed job" flow (automated by housekeeping.yml) would otherwise
#     keep judging the stale title.
#   - Exempts Dependabot from the length cap only (its grouped-update
#     titles routinely exceed 72 chars and the format isn't configurable;
#     the format check still applies).
#   - Emits a `::error::` annotation on failure.
#
# Runs from a checkout of the DEFAULT branch (the job's trust model) so a
# PR can't tamper with its own validator. Env: GH_TOKEN, GITHUB_REPOSITORY,
# PR_NUMBER.

set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

pr_json="$(gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}")"
title="$(jq -r '.title' <<<"$pr_json")"
author="$(jq -r '.user.login' <<<"$pr_json")"

if [[ "$author" == "dependabot[bot]" || "$author" == "app/dependabot" ]]; then
  export PR_TITLE_SKIP_LENGTH=1
fi

if reason="$(bash "$here/../lint-pr-title.sh" "$title" 2>&1)"; then
  echo "PR title OK: $title"
else
  printf '%s\n' "$reason"
  echo "::error::$(printf '%s' "$reason" | head -1)"
  exit 1
fi

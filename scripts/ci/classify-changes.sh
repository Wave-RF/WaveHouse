#!/usr/bin/env bash
# CI change-set classifier for ci.yml's `changes` job. Fetches the run's
# changed-file list from the GitHub API (CI checkouts are shallow, so the
# list can't come from `git diff`) and pipes it through the shared pure
# classifier, scripts/classify-paths.sh — the single source of truth for
# the code/docs allowlists, also usable from the local git hooks.
#
# This wrapper owns only the CI-specific policy on top of that:
#   - manual dispatch ⇒ run + deploy everything;
#   - an empty / errored file list (API hiccup, brand-new branch) ⇒ fail
#     closed and run everything;
#   - pushes to main and merge-group runs never skip code work — they're
#     the last gate before main and they warm the caches every PR inherits.
#
# Prints `code=…` / `docs=…` to stdout (the step redirects to
# $GITHUB_OUTPUT) and a one-line summary to stderr. Env in:
# GITHUB_EVENT_NAME, GITHUB_REPOSITORY, GH_TOKEN, PR_NUMBER (pull_request),
# PUSH_BEFORE + PUSH_SHA (push: before/after; merge_group: group base/head).

# -e/-pipefail: an unexpected failure aborts (and the job reds) instead of
# misclassifying; the two `gh api … || true` calls stay soft because their
# empty-output case already fails closed below.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

emit() { echo "code=$1"; echo "docs=$2"; echo "classified: code=$1 docs=$2" >&2; }

if [ "${GITHUB_EVENT_NAME}" = "workflow_dispatch" ]; then
  emit true true # manual → run + deploy everything
  exit 0
fi

if [ "${GITHUB_EVENT_NAME}" = "pull_request" ]; then
  files="$(gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}/files" \
    --paginate --jq '.[].filename' || true)"
else
  files="$(gh api "repos/${GITHUB_REPOSITORY}/compare/${PUSH_BEFORE}...${PUSH_SHA}" \
    --jq '.files[].filename' || true)"
fi

# Empty (API hiccup / new branch) → fail closed: run everything.
if [ -z "$files" ]; then
  emit true true
  exit 0
fi

# Pure classification of the file list, then the push/merge-group override.
#
# Capture the classifier's output and its EXIT STATUS before parsing. Reading it
# through process substitution (`done < <(…)`) discards the status, so a
# classifier that aborted mid-run left code/docs empty or half-set — every
# `needs.changes.outputs.code == 'true'` job would skip and the `CI` aggregator
# would go green having run nothing. Same fail-closed rule as the empty-list
# case above: if we cannot classify, run everything.
# Capture the status into rc first: `if ! cmd` inverts the test AND resets $?,
# so reading $? inside the branch always yields 0 — the diagnostic would lie
# about the very failure it exists to report.
classified=""; rc=0
classified="$(printf '%s\n' "$files" | "$here/../classify-paths.sh")" || rc=$?
if [ "$rc" -ne 0 ]; then
  echo "classify-changes: classifier failed (exit $rc) — failing closed, running everything" >&2
  emit true true
  exit 0
fi

code=""; docs=""
while IFS='=' read -r key value; do
  case "$key" in
    code) code="$value" ;;
    docs) docs="$value" ;;
  esac
done <<<"$classified"

# A partial or unparseable answer is as untrustworthy as a failed one.
if [ -z "$code" ] || [ -z "$docs" ]; then
  echo "classify-changes: incomplete classification (code='$code' docs='$docs') — failing closed" >&2
  emit true true
  exit 0
fi

if [ "${GITHUB_EVENT_NAME}" = "push" ] || [ "${GITHUB_EVENT_NAME}" = "merge_group" ]; then
  code=true
fi
emit "$code" "$docs"

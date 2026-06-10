#!/usr/bin/env bash
# Classify a CI run's change set for job gating. Single source of truth —
# used by ci.yml's `changes` job AND inlined into the e2e job (which
# classifies for itself so it can start without waiting on another job;
# the aggregator cross-checks the two classifications).
#
# Prints two `key=value` lines for $GITHUB_OUTPUT:
#   code=true|false  false only when EVERY changed file is prose/repo-meta
#                    (docs/, *.md, labels, templates) — then the Go/SDK
#                    test work skips. Fail-closed: push events, manual
#                    dispatches, API hiccups, and empty file lists all
#                    count as code changes.
#   docs=true|false  true when a docs-affecting file changed (docs site
#                    build inputs, including clients/ts — the landing
#                    page bundles @wavehouse/sdk).
#
# Computed from the GitHub API (CI checkouts are shallow, so `git diff`
# against the base can't see the change set). Env in: GITHUB_EVENT_NAME,
# GITHUB_REPOSITORY, GH_TOKEN, PR_NUMBER (pull_request), PUSH_BEFORE +
# PUSH_SHA (push).

set -u

if [ "${GITHUB_EVENT_NAME}" = "workflow_dispatch" ]; then
  echo "code=true" # manual → run + deploy everything
  echo "docs=true"
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
  echo "code=true"
  echo "docs=true"
  exit 0
fi
# Pushes to main always run the full suite (they also save the caches
# every PR inherits). For PRs, `code` flips false only if no file falls
# outside the prose/meta allowlist.
if [ "${GITHUB_EVENT_NAME}" = "push" ]; then
  code=true
elif printf '%s\n' "$files" | grep -qvE '^(docs/|.*\.md$|LICENSE|NOTICE|\.gitignore$|\.gitattributes$|\.github/labeler\.yml$|\.github/ISSUE_TEMPLATE/|\.github/pull_request_template|\.claude/|\.vscode/)'; then
  code=true
else
  code=false
fi
if printf '%s\n' "$files" | grep -qE '^(docs/|clients/ts/|pnpm-lock\.yaml|pnpm-workspace\.yaml|\.github/workflows/ci\.yml|\.github/actions/setup-env/)'; then
  docs=true
else
  docs=false
fi
echo "code=$code"
echo "docs=$docs"
echo "classified: code=$code docs=$docs" >&2

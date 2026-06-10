#!/usr/bin/env bash
# Poll the current workflow run until the named artifacts exist — the
# poll-not-`needs` pattern (.github/workflows/README.md): the consumer
# job does its own setup in parallel with the producer and only blocks
# right before the artifact is actually needed, instead of serializing
# whole jobs with a `needs` edge.
#
# Fails fast when a producer job already concluded without producing
# (failure / cancelled / skipped) — the aggregator is red from the
# producer itself in those cases; this just stops a pointless wait.
#
# Usage:
#   scripts/ci/wait-artifact.sh --artifacts <name>[,<name>...] \
#     --producers <job name>[,<job name>...] [--timeout <seconds>]
#
# Producer names must match the jobs' `name:` fields in ci.yml.
# Env: GH_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID.

set -euo pipefail

artifacts="" producers="" timeout=600
while [ $# -gt 0 ]; do
  case "$1" in
    --artifacts) artifacts="$2"; shift 2 ;;
    --producers) producers="$2"; shift 2 ;;
    --timeout) timeout="$2"; shift 2 ;;
    *) echo "wait-artifact: unknown arg $1" >&2; exit 2 ;;
  esac
done
[ -n "$artifacts" ] || { echo "wait-artifact: --artifacts is required" >&2; exit 2; }

# Note: `gh api --jq` accepts a bare expression only (no --arg), so the
# parameterized filters below pipe through standalone jq instead.
#
# Resilience: this polls for the WHOLE producer duration (e2e ≈ minutes),
# so it makes many API calls — a single transient 5xx / rate-limit must
# NOT red an otherwise-green run. A failed call just skips that iteration;
# only a PERSISTENT outage lasting past --timeout fails. So each `gh api`
# is guarded (its non-zero exit is caught, never aborts under `set -e`),
# and the producer fail-fast fires only on a SUCCESSFUL jobs query.
deadline=$(( $(date +%s) + timeout ))
while :; do
  if artifacts_json="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts?per_page=100" 2>/dev/null)"; then
    names="$(jq '[.artifacts[].name]' <<<"$artifacts_json" 2>/dev/null || echo '[]')"
    if jq -e --arg want "$artifacts" \
      '($want | split(",")) - . == []' >/dev/null <<<"$names"; then
      echo "All artifacts available: $artifacts"
      exit 0
    fi
    # Fail fast only on a successful jobs query that shows a producer
    # already concluded without producing; a transient jobs-API failure
    # just skips this check for the iteration.
    if [ -n "$producers" ] && jobs_json="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/jobs?per_page=100" 2>/dev/null)"; then
      bad="$(jq -r --arg jobs "$producers" \
        '[.jobs[] | select(.name as $n | ($jobs | split(",")) | index($n))
           | select(.conclusion == "failure" or .conclusion == "cancelled" or .conclusion == "skipped")
           | "\(.name) (\(.conclusion))"] | join(", ")' <<<"$jobs_json" 2>/dev/null || true)"
      if [ -n "$bad" ]; then
        echo "::error::Producer job(s) concluded without producing: $bad — cannot wait for: $artifacts"
        exit 1
      fi
    fi
    echo "Waiting for $artifacts (have: $names) — retrying in 5s."
  else
    echo "Artifacts API call failed (transient?) — retrying in 5s." >&2
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "::error::Timed out (${timeout}s) waiting for artifacts: $artifacts"
    exit 1
  fi
  sleep 5
done

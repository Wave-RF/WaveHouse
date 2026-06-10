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
deadline=$(( $(date +%s) + timeout ))
while :; do
  names="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts?per_page=100" \
    | jq '[.artifacts[].name]')"
  if jq -e --arg want "$artifacts" \
    '($want | split(",")) - . == []' >/dev/null <<<"$names"; then
    echo "All artifacts available: $artifacts"
    exit 0
  fi
  if [ -n "$producers" ]; then
    bad="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/jobs?per_page=100" \
      | jq -r --arg jobs "$producers" \
        '[.jobs[] | select(.name as $n | ($jobs | split(",")) | index($n))
           | select(.conclusion == "failure" or .conclusion == "cancelled" or .conclusion == "skipped")
           | "\(.name) (\(.conclusion))"] | join(", ")')"
    if [ -n "$bad" ]; then
      echo "::error::Producer job(s) concluded without producing: $bad — cannot wait for: $artifacts"
      exit 1
    fi
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "::error::Timed out (${timeout}s) waiting for artifacts: $artifacts (have: $names)"
    exit 1
  fi
  echo "Waiting for $artifacts (have: $names) — retrying in 5s."
  sleep 5
done

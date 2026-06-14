#!/usr/bin/env bash
# Emit a markdown wall-clock table for the current workflow run — which
# job started when (relative to run creation), how long it took, and how
# it concluded — so "where did the time go" is answerable from the run's
# Summary page without spelunking per-job logs. Written for
# $GITHUB_STEP_SUMMARY by ci.yml's non-gating "Timing summary" job.
#
# Env: GH_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID.

set -euo pipefail

run="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}")"
jobs="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/jobs?per_page=100")"

created="$(jq -r '.run_started_at' <<<"$run")"

echo "## ⏱ Wall-clock by job"
echo
echo "| Job | Started | Duration | Result |"
echo "|---|---:|---:|---|"
jq -r --arg t0 "$created" '
  ($t0 | fromdateiso8601) as $start |
  [.jobs[]
   | select(.name != "Timing summary")
   | . + {s: ((.started_at // empty | fromdateiso8601) // null),
          e: ((.completed_at // empty | fromdateiso8601) // null)}]
  | sort_by(.s // 0)[]
  | [ .name,
      (if .s then "+\(.s - $start)s" else "—" end),
      (if .s and .e then "\(.e - .s)s" else "running" end),
      (.conclusion // .status) ]
  | "| \(.[0]) | \(.[1]) | \(.[2]) | \(.[3]) |"
' <<<"$jobs"
echo
echo "_Durations include runner pickup-to-completion per job; the run's"
echo "wall-clock is the latest end above. Job graph + design rationale:"
echo "[.github/workflows/README.md](https://github.com/${GITHUB_REPOSITORY}/blob/main/.github/workflows/README.md)._"

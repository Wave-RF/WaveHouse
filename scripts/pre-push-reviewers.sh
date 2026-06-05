#!/usr/bin/env bash
# Single source of truth: the subagents that gate a PR-branch push.
#
# Prints one reviewer name per line. Each is a `.claude/agents/<name>.md`
# subagent that MUST reach `VERDICT: ship_it` before `git push` succeeds on a
# PR branch. Its marker is `tmp/<name>-passed-<HEAD-sha>`, written by
# `.claude/hooks/review-marker.sh` on ship_it and required (one per reviewer)
# by `.claude/hooks/agent-bash-gate.sh`.
#
# Consumed by both hooks above and by `/prepush`, which launches every listed
# reviewer in parallel. The set grows over time (code, docs, security, …) and
# nothing downstream hardcodes it — so add a reviewer in ONE place:
#
#   1. create `.claude/agents/<name>.md`  (the subagent itself), THEN
#   2. add `<name>` to the list below.
#
# Order matters: a name here with no matching agent file blocks every push
# until the agent exists. See AGENTS.md §"Adding a pre-push reviewer".
#
# Keep names filename-safe ([A-Za-z0-9._-]); consumers validate and silently
# drop anything else, so a malformed entry disables that reviewer's gate.

set -u

reviewers=(
  pre-push-reviewer   # code review — .github/prompts/pr-review.md
  docs-reviewer       # docs prose + code<->docs sync — .github/prompts/docs-review.md
)

printf '%s\n' "${reviewers[@]}"

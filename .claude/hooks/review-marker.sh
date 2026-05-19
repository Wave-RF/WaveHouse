#!/usr/bin/env bash
# PostToolUse Agent hook — writes the pre-push review marker.
#
# When the pre-push-reviewer subagent finishes its review and ends its response
# with `VERDICT: ship_it`, this hook writes tmp/review-passed-<HEAD-sha>. The
# pre-push gate (in .claude/hooks/agent-bash-gate.sh) reads that marker to allow
# the subsequent `git push`.
#
# Why this hook exists: the orchestrator agent is denied direct writes to
# tmp/(ci|review)-passed-* (permission deny list + agent-bash-gate). Hooks run at
# Claude Code privilege level, NOT subject to the permission system, so this
# is the only path to creating the marker. The subagent's verdict is the gate;
# the orchestrator can't fake it because the subagent runs in fresh context
# with the canonical system prompt from .claude/agents/pre-push-reviewer.md.

set -uo pipefail

input=$(cat)
agent_type=$(printf '%s' "$input" | jq -r '.tool_input.subagent_type // empty' 2>/dev/null)

# Only act on pre-push-reviewer completions
[ "$agent_type" = "pre-push-reviewer" ] || exit 0

response=$(printf '%s' "$input" | jq -r '.tool_response // empty' 2>/dev/null)
[ -z "$response" ] && exit 0

# Parse the parseable verdict line. Format (per .claude/agents/pre-push-reviewer.md):
#   VERDICT: ship_it    | VERDICT: iterate    | VERDICT: block
# Anchored to line start so an inline mention like "do not write VERDICT: ship_it"
# inside prose won't accidentally produce ship_it. We take the LAST matching
# line in the response (in case the agent emits the parseable line more than
# once for any reason). Case-insensitive on the keyword and value.
verdict=$(printf '%s\n' "$response" \
  | grep -iE '^[[:space:]]*VERDICT:[[:space:]]*(ship_it|iterate|block)[[:space:]]*$' \
  | tail -1 \
  | tr '[:upper:]' '[:lower:]' \
  | sed -E 's/^[[:space:]]*verdict:[[:space:]]*([a-z_]+)[[:space:]]*$/\1/')

[ "$verdict" = "ship_it" ] || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0
head_sha=$(git rev-parse HEAD 2>/dev/null)
[ -z "$head_sha" ] && exit 0

mkdir -p tmp
touch "tmp/review-passed-${head_sha}"
echo "📝 Pre-push review marker written: tmp/review-passed-${head_sha:0:8}" >&2

exit 0

#!/usr/bin/env bash
# SubagentStop hook — writes the pre-push review marker.
#
# When the pre-push-reviewer subagent finishes its review and its last assistant
# message ends with `VERDICT: ship_it`, this hook writes
# tmp/review-passed-<HEAD-sha>. The pre-push gate (in
# .claude/hooks/agent-bash-gate.sh) reads that marker to allow the subsequent
# `git push`.
#
# Why SubagentStop (not PostToolUse:Agent): the PostToolUse:Agent payload puts
# the subagent's final text in `.tool_response.content[].text` (array of
# content blocks), which is brittle to parse and to schema changes.
# SubagentStop exposes `.last_assistant_message` as a flat string, which is
# both stable and what we actually need. Both events do fire on subagent
# completion; we just use the one with the friendlier schema.
#
# Why this hook exists at all: the orchestrator agent is denied direct writes
# to tmp/(ci|review)-passed-* (permission deny list + agent-bash-gate). Hooks
# run at Claude Code privilege level, NOT subject to the permission system, so
# this is the only path to creating the marker. The subagent's verdict is the
# gate; the orchestrator can't fake it because the subagent runs in fresh
# context with the canonical system prompt from
# .claude/agents/pre-push-reviewer.md.

set -uo pipefail

input=$(cat)

# Failure modes for this hook (jq missing, malformed JSON) should leave
# stderr breadcrumbs rather than silently no-op — otherwise the orchestrator
# pushes, gets blocked by the missing review marker, and has no clue why.
# Mirrors agent-bash-gate.sh's posture, except this hook exits 0 on its own
# failures (it's the marker writer, not the push gate; the absence of a
# marker is itself the enforcement signal downstream).
if ! command -v jq >/dev/null 2>&1; then
  echo "review-marker: jq not found; cannot parse SubagentStop payload — no marker written." >&2
  exit 0
fi

# SubagentStop fires for every subagent completion (no matcher support per
# Claude Code docs), so we filter by `agent_type` in-script.
if ! agent_type=$(printf '%s' "$input" | jq -r '.agent_type // empty' 2>/dev/null); then
  echo "review-marker: malformed SubagentStop payload; could not parse .agent_type — no marker written." >&2
  exit 0
fi
[ "$agent_type" = "pre-push-reviewer" ] || exit 0

if ! response=$(printf '%s' "$input" | jq -r '.last_assistant_message // empty' 2>/dev/null); then
  echo "review-marker: malformed SubagentStop payload; could not parse .last_assistant_message — no marker written." >&2
  exit 0
fi
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

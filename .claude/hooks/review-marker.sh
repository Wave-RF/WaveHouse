#!/usr/bin/env bash
# SubagentStop hook — writes a pre-push review marker.
#
# Both review subagents gate a push, each with its own HEAD-keyed marker:
#   pre-push-reviewer → tmp/review-passed-<HEAD-sha>       (code review)
#   docs-reviewer     → tmp/docs-review-passed-<HEAD-sha>  (docs prose + code<->docs sync)
# When the subagent's last assistant message ends with `VERDICT: ship_it`, this
# hook writes the corresponding marker. The pre-push gate (in
# .claude/hooks/agent-bash-gate.sh) requires both markers to allow the
# subsequent `git push`.
#
# Why SubagentStop (not PostToolUse:Agent): the PostToolUse:Agent payload puts
# the subagent's final text in `.tool_response.content[].text` (array of
# content blocks), which is brittle to parse and to schema changes.
# SubagentStop exposes `.last_assistant_message` as a flat string, which is
# both stable and what we actually need. Both events do fire on subagent
# completion; we just use the one with the friendlier schema.
#
# Why this hook exists at all: the orchestrator agent must not hand-write
# tmp/(ci|review|docs-review)-passed-* (policy in AGENTS.md §"Don't bypass the
# gates"). Hooks run at Claude Code privilege level, NOT subject to the
# permission system, so this is the only honest path to creating a marker. The
# subagent's verdict is the gate; the orchestrator can't fake it because each
# subagent runs in fresh context with the canonical system prompt from
# .claude/agents/<agent>.md.

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
# Claude Code docs), so we filter by `agent_type` in-script and map it to the
# marker it gates. Any other subagent (Explore, Plan, …) is a no-op.
if ! agent_type=$(printf '%s' "$input" | jq -r '.agent_type // empty' 2>/dev/null); then
  echo "review-marker: malformed SubagentStop payload; could not parse .agent_type — no marker written." >&2
  exit 0
fi
# Map each reviewer to its own marker AND its sibling's, so we can both write
# this reviewer's marker and nudge about the other (both gate the push).
case "$agent_type" in
  pre-push-reviewer) marker_prefix="review-passed";      sibling_agent="docs-reviewer";     sibling_prefix="docs-review-passed" ;;
  docs-reviewer)     marker_prefix="docs-review-passed"; sibling_agent="pre-push-reviewer"; sibling_prefix="review-passed" ;;
  *) exit 0 ;;
esac

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

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0
head_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
[ -z "$head_sha" ] && exit 0

# On ship_it, write THIS reviewer's marker (the file the push gate checks for).
if [ "$verdict" = "ship_it" ]; then
  if mkdir -p tmp && touch "tmp/${marker_prefix}-${head_sha}"; then
    echo "📝 Pre-push marker written (${agent_type}): tmp/${marker_prefix}-${head_sha:0:8}" >&2
  else
    echo "review-marker: failed to write tmp/${marker_prefix}-${head_sha:0:8} — no marker written." >&2
  fi
fi

# Dual-review nudge. BOTH reviewers are mandatory, independent push gates
# (agent-bash-gate.sh requires both markers for HEAD). The recurring miss is
# running one and forgetting the other — most often docs-reviewer. So if the
# sibling reviewer has no ship_it marker for THIS HEAD, surface a reminder into
# the ORCHESTRATOR's context: a SubagentStop hook's hookSpecificOutput.additionalContext
# injects into the main session (not the finished subagent), per the Claude Code
# hooks docs. Exit 0 — this is advisory, never a block. Phrased defensively so
# it's a harmless no-op when the orchestrator already launched both in parallel
# (e.g. via /prepush) and the sibling simply hasn't finished writing yet.
if [ ! -f "tmp/${sibling_prefix}-${head_sha}" ]; then
  nudge="Reminder: ${sibling_agent} (the other mandatory pre-push reviewer) has no ship_it marker for HEAD ${head_sha:0:8} yet. Both reviewers gate the push, and ${sibling_agent} runs even on code-only changes. If you have not already started it this turn, run it in fresh context — best, run both in parallel via /prepush. Markers are HEAD-keyed, so re-run BOTH after any new commit."
  jq -n --arg ctx "$nudge" '{hookSpecificOutput:{hookEventName:"SubagentStop",additionalContext:$ctx}}'
fi

exit 0

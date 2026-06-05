#!/usr/bin/env bash
# SubagentStop hook — writes a pre-push review marker.
#
# The subagents that gate a push are listed in scripts/pre-push-reviewers.sh
# (the single source of truth — today code + docs review, tomorrow security or
# more). Each listed reviewer <name> gates the push with its own HEAD-keyed
# marker tmp/<name>-passed-<HEAD-sha>. When that subagent's last assistant
# message ends with `VERDICT: ship_it`, this hook writes the marker. The
# pre-push gate (.claude/hooks/agent-bash-gate.sh) requires a marker for EVERY
# listed reviewer before the subsequent `git push`. Nothing here hardcodes the
# set — add a reviewer by editing scripts/pre-push-reviewers.sh.
#
# Why SubagentStop (not PostToolUse:Agent): the PostToolUse:Agent payload puts
# the subagent's final text in `.tool_response.content[].text` (array of
# content blocks), which is brittle to parse and to schema changes.
# SubagentStop exposes `.last_assistant_message` as a flat string, which is
# both stable and what we actually need. Both events do fire on subagent
# completion; we just use the one with the friendlier schema.
#
# Why this hook exists at all: the orchestrator agent must not hand-write
# tmp/(ci|<reviewer>)-passed-* (policy in AGENTS.md §"Don't bypass the gates").
# Hooks run at Claude Code privilege level, NOT subject to the permission
# system, so this is the only honest path to creating a marker. The subagent's
# verdict is the gate; the orchestrator can't fake it because each reviewer
# runs in fresh context with the canonical system prompt from
# .claude/agents/<name>.md.

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
# Claude Code docs), so we filter by `agent_type` in-script and gate only the
# reviewers listed in the manifest. Any other subagent (Explore, Plan, …) is a
# no-op.
if ! agent_type=$(printf '%s' "$input" | jq -r '.agent_type // empty' 2>/dev/null); then
  echo "review-marker: malformed SubagentStop payload; could not parse .agent_type — no marker written." >&2
  exit 0
fi
[ -z "$agent_type" ] && exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || exit 0

# Resolve the gating reviewers from the single source of truth. Validate each
# name for filename-safety (it becomes part of a marker filename); a malformed
# manifest entry is dropped here rather than writing a junk marker path.
reviewers_script="scripts/pre-push-reviewers.sh"
reviewers=$([ -f "$reviewers_script" ] && bash "$reviewers_script" 2>/dev/null | grep -E '^[A-Za-z0-9._-]+$')

# Only the listed reviewers gate a push; bail for anything else.
is_reviewer=0
for r in $reviewers; do
  [ "$r" = "$agent_type" ] && { is_reviewer=1; break; }
done
[ "$is_reviewer" = 1 ] || exit 0

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

head_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
[ -z "$head_sha" ] && exit 0

# On ship_it, write THIS reviewer's marker (the file the push gate checks for).
if [ "$verdict" = "ship_it" ]; then
  if mkdir -p tmp && touch "tmp/${agent_type}-passed-${head_sha}"; then
    echo "📝 Pre-push marker written (${agent_type}): tmp/${agent_type}-passed-${head_sha:0:8}" >&2
  else
    echo "review-marker: failed to write tmp/${agent_type}-passed-${head_sha:0:8} — no marker written." >&2
  fi
fi

# Multi-review nudge. EVERY listed reviewer is a mandatory, independent push
# gate (agent-bash-gate.sh requires a marker for each). The recurring miss is
# running one and forgetting the rest. So if any OTHER gating reviewer has no
# ship_it marker for THIS HEAD, surface the list into the ORCHESTRATOR's
# context: a SubagentStop hook's hookSpecificOutput.additionalContext injects
# into the main session (not the finished subagent), per the Claude Code hooks
# docs. Exit 0 — advisory, never a block. Phrased defensively so it's a
# harmless no-op when the orchestrator already launched them all in parallel
# (e.g. via /prepush) and the others simply haven't finished writing yet.
missing_siblings=""
for r in $reviewers; do
  [ "$r" = "$agent_type" ] && continue
  [ -f "tmp/${r}-passed-${head_sha}" ] || missing_siblings="${missing_siblings}${r}, "
done
missing_siblings="${missing_siblings%, }"
if [ -n "$missing_siblings" ]; then
  nudge="Reminder: these mandatory pre-push reviewer(s) still have no marker for HEAD ${head_sha:0:8}: ${missing_siblings}. Every reviewer in scripts/pre-push-reviewers.sh gates the push — each needs a marker from a ship_it OR a deliberate skip. If you have not already handled them this turn, run /prepush: it runs the reviewers this change needs (fresh context, in parallel) and skips the rest on the record (scripts/skip-pre-push-review.sh). Markers are HEAD-keyed, so re-satisfy every reviewer after any new commit."
  jq -n --arg ctx "$nudge" '{hookSpecificOutput:{hookEventName:"SubagentStop",additionalContext:$ctx}}'
fi

exit 0

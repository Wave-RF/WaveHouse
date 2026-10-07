#!/usr/bin/env bash
# SubagentStop hook — writes a pre-push review marker.
#
# The subagents that gate a push are listed in scripts/pre-push-reviewers.sh
# (the single source of truth — today code + docs review, tomorrow security or
# more). Each listed reviewer <name> gates the push with its own commit-keyed
# marker tmp/<name>-passed-<sha>. The pre-push gate
# (.claude/hooks/agent-bash-gate.sh) requires a marker for EVERY listed
# reviewer before a `git push`. Nothing here hardcodes the set — add a reviewer
# by editing scripts/pre-push-reviewers.sh.
#
# A marker attests that a reviewer read exactly that commit, so this hook
# writes one only when all of these hold:
#   1. The reviewer's report ends with `VERDICT: ship_it` and names the commit
#      it reviewed on a `REVIEWED: <sha>` line (an unambiguous prefix will do).
#   2. Some worktree of the repository is on that commit now.
#   3. That worktree's HEAD was on that commit when the review began and has
#      not pointed anywhere else since (its HEAD reflog, against the first
#      timestamp of the reviewer's transcript). A commit made while a reviewer
#      runs therefore voids the review instead of inheriting its ship_it.
# The marker goes into each such worktree's tmp/, so a review of a sibling
# worktree marks that worktree, not the session's.
#
# The report is `.last_assistant_message` when that carries both lines. A
# subagent that delivers its report through the SubagentHandback tool leaves
# only its closing text there; the report is that tool call's `message`, read
# from the subagent's transcript (`.agent_transcript_path`).
#
# Every decision for a reviewer goes to stderr and to tmp/review-marker.log
# (gitignored), which the push gate prints when it blocks, so a missing marker
# comes with its reason.
#
# Why this hook exists at all: the orchestrator agent must not hand-write
# tmp/(ci|<reviewer>)-passed-* (policy in AGENTS.md §"Don't bypass the gates").
# Hooks run at Claude Code privilege level, NOT subject to the permission
# system, so this is the only honest path to creating a marker. The subagent's
# verdict is the gate; the orchestrator can't fake it because each reviewer
# runs in fresh context with the canonical system prompt from
# .claude/agents/<name>.md.
#
# This hook exits 0 on its own failures: it's the marker writer, not the push
# gate, and the absence of a marker is itself the enforcement signal.

set -uo pipefail

input=$(cat)

if ! command -v jq >/dev/null 2>&1; then
  echo "review-marker: jq not found; cannot parse SubagentStop payload — no marker written." >&2
  exit 0
fi

# SubagentStop fires for every subagent completion, so filter by `agent_type`
# in-script and act only for the reviewers listed in the manifest.
if ! agent_type=$(printf '%s' "$input" | jq -r '.agent_type // empty' 2>/dev/null); then
  echo "review-marker: malformed SubagentStop payload; could not parse .agent_type — no marker written." >&2
  exit 0
fi
[ -z "$agent_type" ] && exit 0

field() { printf '%s' "$input" | jq -r "$1 // empty" 2>/dev/null; }

# The repository the review belongs to: the hook's cwd, else the session's
# launch directory, whichever lists this agent as a reviewer. Markers go to
# whichever of its worktrees the review covered. A malformed manifest entry is
# dropped rather than written into a marker path.
reviewers_script="scripts/pre-push-reviewers.sh"
repo=""
for d in "$(field .cwd)" "${CLAUDE_PROJECT_DIR:-}"; do
  [ -n "$d" ] && top=$(git -C "$d" rev-parse --show-toplevel 2>/dev/null) && [ -f "$top/$reviewers_script" ] || continue
  if bash "$top/$reviewers_script" 2>/dev/null | grep -E '^[A-Za-z0-9._-]+$' | grep -Fxq -- "$agent_type"; then
    repo=$top
    break
  fi
done
[ -n "$repo" ] || exit 0

# note <worktree> <message>: report a decision on stderr and in that
# worktree's tmp/review-marker.log.
note() {
  echo "review-marker (${agent_type}): $2" >&2
  mkdir -p "$1/tmp" 2>/dev/null \
    && printf '%s %s: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$agent_type" "$2" >> "$1/tmp/review-marker.log"
}

transcript=$(field .agent_transcript_path)
case $transcript in \~/*) transcript="$HOME/${transcript#\~/}" ;; esac

# Anchored to line start so an inline mention like "do not write VERDICT: ship_it"
# inside prose can't count.
verdict_re='^[[:space:]]*VERDICT:[[:space:]]*(ship_it|iterate|block)[[:space:]]*$'
reviewed_re='^[[:space:]]*REVIEWED:[[:space:]]*([0-9A-Fa-f]+)[[:space:]]*$'

handback() {
  jq -Rrn '[inputs | fromjson? | select(.type == "assistant") | .message.content[]?
      | select(.type == "tool_use" and .name == "SubagentHandback") | .input.message // empty]
      | last // empty' "$transcript" 2>/dev/null
}

# verdict_of <report>: its VERDICT, lowercased. The LAST matching line wins, in
# case the agent emits it more than once.
verdict_of() {
  printf '%s\n' "$1" \
    | grep -iE "$verdict_re" \
    | tail -1 \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/^[[:space:]]*verdict:[[:space:]]*([a-z_]+)[[:space:]]*$/\1/'
}

# The report is the final message unless that has no VERDICT line, or says
# ship_it without naming the commit (closing text after a hand-back often
# repeats only the verdict); then it's the hand-back's message. A final iterate
# or block stands, whatever an earlier hand-back said. One retry covers a
# transcript not yet flushed.
report=$(field .last_assistant_message)
source="the final message"
verdict=$(verdict_of "$report")
if [ -z "$verdict" ] || { [ "$verdict" = ship_it ] && ! grep -qE "$reviewed_re" <<<"$report"; }; then
  hb=""
  if [ -f "$transcript" ]; then
    hb=$(handback)
    [ -n "$hb" ] || { sleep 1; hb=$(handback); }
  fi
  if [ -n "$hb" ]; then
    report=$hb
    source="the hand-back"
    verdict=$(verdict_of "$report")
  else
    source="the final message (the transcript has no hand-back)"
  fi
fi

if [ -z "$verdict" ]; then
  note "$repo" "no VERDICT line in ${source} (expected only for an advisory review) — no marker written."
  exit 0
fi
if [ "$verdict" != "ship_it" ]; then
  note "$repo" "VERDICT: ${verdict} — no marker written."
  exit 0
fi

reviewed=$(printf '%s\n' "$report" | sed -nE "s/${reviewed_re}/\1/p" | tail -1)
if [ -z "$reviewed" ]; then
  note "$repo" "ship_it, but ${source} has no 'REVIEWED: <sha>' line, so it can't tell which commit was reviewed — no marker written. A session started before the reviewers learned that line still runs their old definitions (they load at session start): restart it, or ask each reviewer in its prompt to end with 'REVIEWED: <the sha it pinned first>'."
  exit 0
fi
# An abbreviated sha counts when it names exactly one commit.
if ! full=$(git -C "$repo" rev-parse --verify -q "${reviewed}^{commit}" 2>/dev/null); then
  note "$repo" "ship_it, but 'REVIEWED: ${reviewed}' doesn't name exactly one commit in ${repo} — no marker written."
  exit 0
fi
reviewed=$full
short=${reviewed:0:8}

start=""
[ -f "$transcript" ] && start=$(jq -Rrn 'first(inputs | fromjson? | .timestamp // empty)
    | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601 | floor' "$transcript" 2>/dev/null)
case $start in
  '' | *[!0-9]*)
    note "$repo" "ship_it for ${short}, but can't tell when the review began (no timestamp in the subagent transcript '${transcript}') — no marker written."
    exit 0
    ;;
esac

# held_since <worktree>: true when its HEAD was on the reviewed commit when the
# review began and never pointed at another commit since. Same-commit entries
# (`git reset`, `git stash`, re-checking-out the branch) don't count as moves.
held_since() {
  local t h
  while read -r t h; do
    t=${t#HEAD@\{}
    t=${t%\}}
    [ "$h" = "$reviewed" ] || return 1
    [ "$t" -le "$start" ] 2>/dev/null && return 0
  done < <(git -C "$1" reflog --date=unix --format='%gd %H' HEAD 2>/dev/null)
  return 1
}

on_commit=0
wt=""
while IFS= read -r line; do
  case $line in
    "worktree "*) wt=${line#worktree } ;;
    "HEAD $reviewed")
      [ -d "$wt" ] || continue
      on_commit=1
      marker="tmp/${agent_type}-passed-${reviewed}"
      if ! held_since "$wt"; then
        note "$wt" "ship_it for ${short}, but HEAD of ${wt} moved while the review ran — no marker written; review the new HEAD."
      elif mkdir -p "$wt/tmp" && touch "$wt/$marker"; then
        note "$wt" "📝 marker written: ${wt}/tmp/${agent_type}-passed-${short}"
      else
        note "$wt" "failed to write ${wt}/tmp/${agent_type}-passed-${short} — no marker written."
      fi
      ;;
  esac
done < <(git -C "$repo" worktree list --porcelain 2>/dev/null)

if [ "$on_commit" = 0 ]; then
  note "$repo" "ship_it for ${short}, but no worktree is on that commit now (HEAD moved while the review ran?) — no marker written; review the new HEAD."
fi

# Writing the marker is this hook's only job — it intentionally does NOT nudge
# the orchestrator about other still-missing reviewers. A SubagentStop hook's
# additionalContext goes to the *finishing subagent*, not the main session, so
# it can't reach the orchestrator. The push gate lists every missing marker at
# push time, which is the right reminder at the right moment.

exit 0

#!/usr/bin/env bash
# Behavioral test for the hook commands in .claude/settings.json. Each command
# runs its script from $CLAUDE_PROJECT_DIR/.claude/hooks/ when the script is
# there and executable, exactly as a bare path would. When it isn't (the
# session's checkout was removed, say), the shell would exit 127, which Claude
# Code treats as a non-blocking error, so the push gate would stop gating
# (#787). Instead the PreToolUse command blocks anything that looks like a push
# or a PR change, the SubagentStop command reports the missing script without
# exit 2 (which would keep the subagent running), and the PostToolUse commands
# report it with exit 2, which shows Claude the message after the edit.
#
# Every command is read from the real settings.json and run the way Claude
# Code runs a shell-form hook: `sh -c`, the event JSON on stdin,
# CLAUDE_PROJECT_DIR in the environment; under dash and bash too when present.
# Run by `make verify` (target: test-hook-commands). Needs jq; no network.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1 # repo root (.claude/hooks/../..)
root=$PWD
settings=$root/.claude/settings.json
fails=0

command -v jq >/dev/null 2>&1 || { echo "hook-commands test: jq is required." >&2; exit 1; }

scratch=$(mktemp -d) || exit 1
trap 'rm -rf "$scratch"' EXIT
scratch=$(cd "$scratch" && pwd -P)

ok() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n' "$1" >&2
  [ -n "${2:-}" ] && printf '%s\n' "$2" | sed 's/^/         /' >&2
  fails=$((fails + 1))
}

# commands <event>: the event's command hooks, one JSON-encoded string a line.
commands() {
  jq -c --arg e "$1" '.hooks[$e][]? | .hooks[] | select(.type == "command") | .command' "$settings"
}

# The script each event runs: the contract this test holds settings.json to.
pre=$(jq -c '.hooks.PreToolUse[] | select(.matcher == "Bash") | .hooks[] | .command' "$settings")
stop=$(commands SubagentStop)
post=$(commands PostToolUse)
[ "$(printf '%s\n' "$pre" | grep -c .)" = 1 ] || fail "settings.json has one PreToolUse Bash command" "$pre"
[ "$(printf '%s\n' "$stop" | grep -c .)" = 1 ] || fail "settings.json has one SubagentStop command" "$stop"
[ "$(printf '%s\n' "$post" | grep -c .)" = 2 ] || fail "settings.json has two PostToolUse commands" "$post"
pre=$(jq -r . <<<"$pre")
stop=$(jq -r . <<<"$stop")
post_scripts=(gofumpt-on-save.sh markdown-on-save.sh)

shells=()
for s in sh dash bash; do
  p=$(command -v "$s") && shells+=("$p")
done

# A PATH with what the commands need but no jq.
nojq=$scratch/nojq
mkdir "$nojq"
for t in cat grep; do ln -s "$(command -v "$t")" "$nojq/$t"; done

gone=$scratch/gone # the project directory of a session whose checkout was removed
mkdir "$gone"
noexec=$scratch/noexec # one whose hook scripts are there but not executable
mkdir -p "$noexec/.claude/hooks"
for s in agent-bash-gate.sh review-marker.sh gofumpt-on-save.sh markdown-on-save.sh; do
  cp "$root/.claude/hooks/$s" "$noexec/.claude/hooks/$s"
  chmod a-x "$noexec/.claude/hooks/$s"
done

# run <shell> <command> <project-dir|-> <payload-file> [PATH]: run a hook
# command as Claude Code does, with CLAUDE_PROJECT_DIR unset for `-`; sets rc,
# out and err.
run() {
  local sh=$1 cmd=$2 dir=$3 payload=$4 path=${5:-$PATH}
  if [ "$dir" = - ]; then
    env -u CLAUDE_PROJECT_DIR PATH="$path" "$sh" -c "$cmd" <"$payload" >"$scratch/out" 2>"$scratch/err"
  else
    CLAUDE_PROJECT_DIR=$dir PATH=$path "$sh" -c "$cmd" <"$payload" >"$scratch/out" 2>"$scratch/err"
  fi
  rc=$?
  out=$(cat "$scratch/out")
  err=$(cat "$scratch/err")
}

# expect <name> <want-rc> <err-pattern> : check the last run. An empty pattern
# means stderr must be empty; a pattern led by `!` must not appear.
expect() {
  local name=$1 want=$2 pat=$3 bad=""
  [ "$rc" = "$want" ] || bad="exit $rc, want $want"
  case $pat in
    '') [ -z "$err" ] || bad="${bad:+$bad; }stderr not empty" ;;
    !*) case $err in *"${pat#!}"*) bad="${bad:+$bad; }stderr has '${pat#!}'" ;; esac ;;
    *) case $err in *"$pat"*) ;; *) bad="${bad:+$bad; }stderr lacks '$pat'" ;; esac ;;
  esac
  if [ -z "$bad" ]; then ok "$name"; else fail "$name: $bad" "$err"; fi
}

# Payloads, shaped like Claude Code's. The Bash description mentions a push on
# purpose: only the command may decide.
bash_payload() {
  jq -nc --arg c "$1" --arg cwd "$scratch" '{session_id: "t", transcript_path: "/dev/null", cwd: $cwd,
    hook_event_name: "PreToolUse", tool_name: "Bash",
    tool_input: {command: $c, description: "git push the branch, then gh pr create"}}' >"$scratch/payload"
}
stop_payload() {
  jq -nc --arg t "$1" '{session_id: "t", transcript_path: "/dev/null", hook_event_name: "SubagentStop",
    stop_hook_active: false, agent_id: "a1", agent_type: $t, agent_transcript_path: "/nonexistent",
    last_assistant_message: "REVIEWED: 0000000\nVERDICT: ship_it"}' >"$scratch/payload"
}
edit_payload() {
  jq -nc --arg f "$1" '{session_id: "t", transcript_path: "/dev/null", hook_event_name: "PostToolUse",
    tool_name: "Edit", tool_input: {file_path: $f, old_string: "a", new_string: "b"},
    tool_response: {filePath: $f, success: true}}' >"$scratch/payload"
}

pushes=(
  'git push'
  'git push -u origin feat'
  'cd sub && git -C ../wt push origin HEAD'
  "$(printf 'git commit -qm x\ngit push')"
  'gh pr create --draft --title "fix: x"'
  'gh pr new --title "fix: x"'
  'gh pr ready 12'
  'gh pr edit 12 --add-reviewer someone'
  'gh pr merge 12 --squash'
  'gh pr review 12 --approve'
  'gh api -X POST repos/o/r/pulls/1/requested_reviewers -f reviewers[]=someone'
  'gh api repos/o/r/pulls/1/reviews -f event=APPROVE'
  'gh api repos/o/r/pulls -f head=feat -f base=main -f title="fix: x"'
  'gh api -X PUT repos/o/r/pulls/1/merge'
  "gh api graphql -f query='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"x\"}) { clientMutationId } }'"
)
others=(
  'ls -la'
  'git status'
  'git log --oneline -5'
  'gh pr view 12'
  'gh pr checks 12'
  'gh issue edit 5 --title x'
  'gh api repos/o/r/issues/5/comments'
  'make verify'
)

for sh in "${shells[@]}"; do
  echo "── $sh"

  # Every command in settings.json, for every event: a script that is missing,
  # or there but not executable, must not end the hook the way the shell would
  # (126/127, or 0), and the hook must say what is wrong.
  while IFS= read -r line; do
    event=$(jq -r '.[0]' <<<"$line")
    cmd=$(jq -r '.[1]' <<<"$line")
    jq -nc --arg e "$event" '{hook_event_name: $e, tool_name: "Bash", tool_input: {command: "git push", file_path: "x.go"},
      agent_type: "pre-push-reviewer"}' >"$scratch/payload"
    for d in "$gone" "$noexec"; do
      what=missing
      [ "$d" = "$noexec" ] && what="not executable"
      run "$sh" "$cmd" "$d" "$scratch/payload"
      name="$event hook with its script $what says so (exit $rc, not 0/126/127)"
      if [ "$rc" != 0 ] && [ "$rc" != 126 ] && [ "$rc" != 127 ] && [[ $err == *"is missing or not executable"* ]]; then
        ok "$name"
      else
        fail "$name" "$err"
      fi
    done
  done < <(jq -c '.hooks | to_entries[] | .key as $e | .value[] | .hooks[] | select(.type == "command") | [$e, .command]' "$settings")

  # PreToolUse: the gate's script is gone.
  for c in "${pushes[@]}"; do
    bash_payload "$c"
    run "$sh" "$pre" "$gone" "$scratch/payload"
    expect "PreToolUse, gate missing: blocks '${c//$'\n'/; }'" 2 "agent-bash-gate.sh is missing or not executable"
  done
  if [[ $err == *"restart Claude Code from a live checkout"* ]]; then
    ok "PreToolUse, gate missing: says how to recover"
  else
    fail "PreToolUse, gate missing: says how to recover" "$err"
  fi
  for c in "${others[@]}"; do
    bash_payload "$c"
    run "$sh" "$pre" "$gone" "$scratch/payload"
    expect "PreToolUse, gate missing: lets '$c' through" 0 ""
  done
  bash_payload 'git push'
  run "$sh" "$pre" - "$scratch/payload"
  expect "PreToolUse, CLAUDE_PROJECT_DIR unset: blocks a push" 2 "is missing or not executable"
  bash_payload 'ls -la'
  run "$sh" "$pre" "$gone" "$scratch/payload" "$nojq"
  expect "PreToolUse, gate missing and no jq: blocks every command" 2 "without jq, every command is"
  printf 'not json' >"$scratch/payload"
  run "$sh" "$pre" "$gone" "$scratch/payload"
  expect "PreToolUse, gate missing: blocks an unreadable payload" 2 "is missing or not executable"
  bash_payload 'git push'
  run "$sh" "$pre" "$noexec" "$scratch/payload"
  expect "PreToolUse, gate not executable: blocks a push" 2 "is missing or not executable"
  bash_payload 'ls -la'
  run "$sh" "$pre" "$noexec" "$scratch/payload"
  expect "PreToolUse, gate not executable: lets 'ls -la' through" 0 ""

  for d in "$gone" "$noexec"; do
    what=missing
    [ "$d" = "$noexec" ] && what="not executable"

    # SubagentStop: visible, but never exit 2 (that keeps the subagent running).
    stop_payload pre-push-reviewer
    run "$sh" "$stop" "$d" "$scratch/payload"
    expect "SubagentStop, marker hook $what: reports it on stderr with exit 1" 1 "review-marker.sh is missing or not executable"

    # PostToolUse: exit 2 shows Claude the message; the edit already happened.
    i=0
    while IFS= read -r c; do
      c=$(jq -r . <<<"$c")
      edit_payload "$scratch/x.go"
      run "$sh" "$c" "$d" "$scratch/payload"
      expect "PostToolUse, ${post_scripts[i]} $what: tells Claude with exit 2" 2 "${post_scripts[i]} is missing or not executable"
      i=$((i + 1))
    done <<<"$post"
  done

  # A missing script's command still reads its whole payload: a hook that
  # exits without reading would leave a large write to its stdin unfinished.
  head -c 300000 /dev/zero | tr '\0' x | jq -Rsc '{hook_event_name: "PostToolUse", tool_name: "Write",
    tool_input: {file_path: "x.md", content: .}}' >"$scratch/big"
  drain=("PreToolUse" "$pre" "SubagentStop" "$stop" "PostToolUse" "$(jq -r . <<<"${post%%$'\n'*}")")
  for ((k = 0; k < ${#drain[@]}; k += 2)); do
    { cat "$scratch/big" && echo finished >"$scratch/writer"; } \
      | CLAUDE_PROJECT_DIR=$gone PATH=$nojq "$sh" -c "${drain[k + 1]}" >/dev/null 2>&1
    if [ "$(cat "$scratch/writer" 2>/dev/null)" = finished ]; then
      ok "${drain[k]}, script missing: reads a 300 KB payload to the end"
    else
      fail "${drain[k]}, script missing: reads a 300 KB payload to the end"
    fi
    rm -f "$scratch/writer"
  done

  # A live project directory: each command runs its own script, with the
  # payload byte for byte on stdin, no arguments, and nothing of its own on
  # stdout or stderr; the script's exit code is the hook's.
  live=$scratch/live
  rm -rf "$live"
  mkdir -p "$live/.claude/hooks"
  for s in agent-bash-gate.sh review-marker.sh "${post_scripts[@]}"; do
    # shellcheck disable=SC2016 # expanded by the stub
    printf '#!/bin/sh\ncat >"$0.stdin"\nprintf "%%s %%s\\n" "$#" "$0" >"$0.argv"\necho stub-out\necho stub-err >&2\nexit 42\n' >"$live/.claude/hooks/$s"
    chmod +x "$live/.claude/hooks/$s"
  done
  # shellcheck disable=SC2016 # a literal $(x), for the stub to receive as is
  printf '{"tool_input":{"command":"git push \\"$(x)\\" \\u00e9 é \\\\ \x27q\x27"}}' >"$scratch/odd" # no trailing newline
  i=0
  while IFS= read -r line; do
    s=$(jq -r '.[0]' <<<"$line")
    cmd=$(jq -r '.[1]' <<<"$line")
    rm -f "$live/.claude/hooks/$s".stdin "$live/.claude/hooks/$s".argv
    run "$sh" "$cmd" "$live" "$scratch/odd"
    name="live project dir: runs $s with its stdin, argv and exit code"
    if [ "$rc" = 42 ] && [ "$out" = stub-out ] && [ "$err" = stub-err ] \
      && cmp -s "$scratch/odd" "$live/.claude/hooks/$s.stdin" \
      && [ "$(cat "$live/.claude/hooks/$s.argv")" = "0 $live/.claude/hooks/$s" ]; then
      ok "$name"
    else
      fail "$name" "exit $rc; out: $out; err: $err; argv: $(cat "$live/.claude/hooks/$s.argv" 2>&1)"
    fi
    i=$((i + 1))
  done < <(jq -c '.hooks | to_entries[] | .value[] | .hooks[] | select(.type == "command") | .command
    | [(capture("/\\.claude/hooks/(?<s>[A-Za-z0-9_.-]+\\.sh)").s), .]' "$settings")
  [ "$i" = 4 ] || fail "live project dir: ran all four commands (ran $i)"
  spaced="$scratch/live project"
  rm -rf "$spaced"
  cp -R "$live" "$spaced"
  rm -f "$spaced"/.claude/hooks/*.stdin "$spaced"/.claude/hooks/*.argv
  run "$sh" "$pre" "$spaced" "$scratch/odd"
  if [ "$rc" = 42 ] && cmp -s "$scratch/odd" "$spaced/.claude/hooks/agent-bash-gate.sh.stdin"; then
    ok "live project dir with a space in its path: runs agent-bash-gate.sh"
  else
    fail "live project dir with a space in its path: runs agent-bash-gate.sh" "exit $rc; err: $err"
  fi

  # This checkout: the real scripts run, not the fallback.
  bash_payload 'gh pr ready 12'
  run "$sh" "$pre" "$root" "$scratch/payload"
  expect "this checkout: the real gate blocks gh pr ready" 2 "Only humans flip drafts"
  expect "this checkout: the real gate, not the fallback" 2 "!is missing or not executable"
  bash_payload 'ls -la'
  run "$sh" "$pre" "$root" "$scratch/payload"
  expect "this checkout: the real gate lets ls through" 0 ""
  stop_payload ""
  run "$sh" "$stop" "$root" "$scratch/payload"
  expect "this checkout: the real marker hook ignores a non-reviewer" 0 ""
  i=0
  while IFS= read -r c; do
    c=$(jq -r . <<<"$c")
    edit_payload "$scratch/notes.txt"
    run "$sh" "$c" "$root" "$scratch/payload"
    expect "this checkout: the real ${post_scripts[i]} skips a .txt file" 0 ""
    i=$((i + 1))
  done <<<"$post"
done

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "hook-commands: all cases passed"

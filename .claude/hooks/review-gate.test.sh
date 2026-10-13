#!/usr/bin/env bash
# Behavioral test for the pre-push review gate: review-marker.sh (SubagentStop,
# writes a reviewer's marker) and agent-bash-gate.sh (PreToolUse Bash, requires
# the markers on `git push`). Builds a scratch repository with sibling
# worktrees and feeds both hooks synthetic event JSON, with every reflog and
# transcript timestamp pinned so the "did HEAD move during the review" cases
# are deterministic. Markers come only from the hook under test or
# scripts/skip-pre-push-review.sh, never from the test itself. Run by
# `make verify` (target: test-review-gate). Needs git and jq; no network.

set -uo pipefail

# The hooks run under whichever bash `#!/usr/bin/env bash` finds first. macOS
# ships bash 3.2 as /bin/bash, and a Mac with no newer bash on PATH runs them
# under it, so when /bin/bash is another bash the suite runs a second time with
# it first on PATH. The test itself stays on this bash either way.
if [ -z "${HOOK_TEST_BASH:-}" ]; then
  self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  HOOK_TEST_BASH=$(command -v bash) "$BASH" "$self"
  rc=$?
  if [ -x /bin/bash ] && ! [ /bin/bash -ef "$(command -v bash)" ]; then
    shim=$(mktemp -d) && ln -s /bin/bash "$shim/bash" || exit 1
    echo "── the hooks again under /bin/bash $(/bin/bash -c 'echo "$BASH_VERSION"')"
    PATH="$shim:$PATH" HOOK_TEST_BASH=/bin/bash "$BASH" "$self" || rc=1
    rm -rf "$shim"
  fi
  exit "$rc"
fi

cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1 # repo root (.claude/hooks/../..)
root=$PWD
gate=$root/.claude/hooks/agent-bash-gate.sh
marker_hook=$root/.claude/hooks/review-marker.sh
fails=0

command -v jq >/dev/null 2>&1 || { echo "review-gate test: jq is required (the hooks under test parse JSON with it)." >&2; exit 1; }

scratch=$(mktemp -d) || exit 1
trap 'rm -rf "$scratch"' EXIT
scratch=$(cd "$scratch" && pwd -P)

# Run from a git hook (pre-commit runs make verify), GIT_DIR and friends point
# at this checkout: a `git init` here would re-initialize it (as a bare
# repository) instead of creating the scratch repo. Clear them, then refuse to
# write anything unless git sees the scratch directory as outside any repository
# and puts each new repository exactly where it was asked to. Every git call
# below names its scratch directory with -C.
# shellcheck disable=SC2046 # one variable name per word
unset $(git -C "$scratch" rev-parse --local-env-vars)
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
if git -C "$scratch" rev-parse --git-dir >/dev/null 2>&1; then
  echo "review-gate test: git resolves $scratch to an existing repository; refusing to run." >&2
  exit 1
fi
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@example.com GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@example.com

# scratch_init <dir>: create a repository at <dir>, or stop if git put it elsewhere.
scratch_init() {
  git -C "$scratch" init -q -b main "$1"
  if [ "$(git -C "$1" rev-parse --absolute-git-dir 2>/dev/null)" != "$1/.git" ]; then
    echo "review-gate test: git did not create $1/.git; refusing to go on." >&2
    exit 1
  fi
}

T=1700000000 # every timestamp below is T + an offset

# at <offset> <dir> <git args…>: run git in <dir> with its clock at T+offset.
at() {
  local t=$((T + $1)) dir=$2
  shift 2
  GIT_COMMITTER_DATE="@$t +0000" GIT_AUTHOR_DATE="@$t +0000" git -C "$dir" "$@"
}

ok() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n' "$1" >&2
  [ -n "${2:-}" ] && printf '%s\n' "$2" | sed 's/^/         /' >&2
  fails=$((fails + 1))
}

# ── Scratch repository ──
#   repo/  — main worktree on feat-a: the session's launch directory
#   wt-b/  — sibling worktree on feat-b
repo=$scratch/repo
wtb=$scratch/wt-b
scratch_init "$repo"
mkdir -p "$repo/scripts"
cp scripts/pre-push-reviewers.sh scripts/skip-pre-push-review.sh "$repo/scripts/"
git -C "$repo" add scripts
at 0 "$repo" commit -q -m base
at 10 "$repo" switch -q -c feat-a
at 10 "$repo" commit -q --allow-empty -m a1
at 20 "$repo" worktree add -q -b feat-b "$wtb" main
at 20 "$wtb" commit -q --allow-empty -m b1
A1=$(git -C "$repo" rev-parse HEAD)
B1=$(git -C "$wtb" rev-parse HEAD)
export CLAUDE_PROJECT_DIR=$repo

# other/ — an unrelated repository with no reviewer manifest
other=$scratch/other
scratch_init "$other"
at 30 "$other" commit -q --allow-empty -m base
at 30 "$other" switch -q -c x
at 30 "$other" commit -q --allow-empty -m x1

R1=pre-push-reviewer
R2=docs-reviewer

# ── review-marker.sh driver ──

# review <name> <agent_type> <cwd> <start-offset> <mode> <report>: write the
# subagent's transcript (its first entry stamped at T+start-offset) and run the
# SubagentStop hook. <mode>: text (the report is the last assistant message),
# handback (the report went through SubagentHandback and closing text
# followed), handback-only (nothing followed the hand-back), handback-verdict
# (the closing text repeats only the VERDICT line), handback-iterate (the
# closing text changes the verdict to iterate), notranscript.
review() {
  local name=$1 type=$2 cwd=$3 start=$4 mode=$5 report=$6 tr last=""
  tr=$scratch/transcript-$name.jsonl
  jq -nc --argjson t $((T + start)) '{type: "user", timestamp: ($t | todate | sub("Z$"; ".250Z")),
    message: {role: "user", content: "Review the branch."}}' > "$tr"
  case $mode in
    text) last=$report ;;
    handback | handback-only | handback-verdict | handback-iterate)
      jq -nc --arg m "$report" '{type: "assistant", message: {role: "assistant",
        content: [{type: "tool_use", id: "toolu_1", name: "SubagentHandback", input: {message: $m}}]}}' >> "$tr"
      jq -nc '{type: "user", message: {role: "user",
        content: [{type: "tool_result", tool_use_id: "toolu_1", content: "Report delivered to your caller."}]}}' >> "$tr"
      case $mode in
        handback) last="I sent the review to the agent that asked for it." ;;
        handback-verdict) last=$(printf 'Sent.\n\nVERDICT: ship_it') ;; # repeats the verdict, not the sha
        handback-iterate) last=$(printf 'One more finding after all.\n\nVERDICT: iterate') ;;
      esac
      [ -z "$last" ] || jq -nc --arg m "$last" '{type: "assistant", message: {role: "assistant", content: [{type: "text", text: $m}]}}' >> "$tr" ;;
    notranscript) last=$report; tr=$scratch/missing.jsonl ;;
  esac
  jq -nc --arg type "$type" --arg cwd "$cwd" --arg tr "$tr" --arg last "$last" \
    '{hook_event_name: "SubagentStop", cwd: $cwd, agent_id: "a1", agent_type: $type,
      agent_transcript_path: $tr, last_assistant_message: $last, stop_hook_active: false}' \
    | "$marker_hook" 2>"$scratch/marker.err"
}

# report <sha> [verdict]: a reviewer's closing section.
report() {
  printf '## Pre-push review\n\n### [MUST] Findings\n\n(none)\n\n## Verdict\n\n**Ship it** — nothing to fix.\n\nREVIEWED: %s\nVERDICT: %s\n' "$1" "${2:-ship_it}"
}

has_marker() { [ -f "$1/tmp/$2-passed-$3" ]; }

expect_marker() { # <name> <worktree> <reviewer> <sha>
  if has_marker "$2" "$3" "$4"; then ok "$1"; else fail "$1: no $3 marker for ${4:0:8} in $2" "$(cat "$scratch/marker.err")"; fi
}
expect_no_marker() { # <name> <worktree> <reviewer> <sha> [stderr substring]
  if has_marker "$2" "$3" "$4"; then
    fail "$1: unexpected $3 marker for ${4:0:8} in $2"
  elif [ -n "${5:-}" ] && ! grep -qF -- "$5" "$scratch/marker.err"; then
    fail "$1: stderr lacks '$5'" "$(cat "$scratch/marker.err")"
  else
    ok "$1"
  fi
}

# ── agent-bash-gate.sh driver ──

push() { # <cwd> <command>; exit status of the gate, stderr in gate.err
  jq -nc --arg cwd "$1" --arg c "$2" '{hook_event_name: "PreToolUse", tool_name: "Bash", cwd: $cwd, tool_input: {command: $c}}' \
    | "$gate" 2>"$scratch/gate.err"
}
expect_allow() { # <name> <cwd> <command> [stderr substring]
  if ! push "$2" "$3"; then
    fail "$1: blocked" "$(cat "$scratch/gate.err")"
  elif [ -n "${4:-}" ] && ! grep -qF -- "$4" "$scratch/gate.err"; then
    fail "$1: stderr lacks '$4'" "$(cat "$scratch/gate.err")"
  else
    ok "$1"
  fi
}
expect_block() { # <name> <cwd> <command> [stderr substring]
  local rc
  push "$2" "$3"; rc=$?
  if [ "$rc" != 2 ]; then
    fail "$1: not blocked (exit $rc)" "$(cat "$scratch/gate.err")"
  elif [ -n "${4:-}" ] && ! grep -qF -- "$4" "$scratch/gate.err"; then
    fail "$1: stderr lacks '$4'" "$(cat "$scratch/gate.err")"
  else
    ok "$1"
  fi
}

echo "review-marker.sh:"

# Each marker asserted present below is created by exactly one event, so an
# earlier misattributed marker can't make a later case pass.
review happy "$R1" "$repo" 100 text "$(report "$A1")"
expect_marker "ship_it marks the reviewed commit" "$repo" "$R1" "$A1"
expect_no_marker "…and not the sibling worktree" "$wtb" "$R1" "$A1"

# A report delivered through the hand-back tool, then closing text.
review handback "$R2" "$repo" 100 handback "$(report "$A1")"
expect_marker "a hand-back report followed by closing text writes the marker" "$repo" "$R2" "$A1"

# A review of the sibling worktree, run from the session's directory, marks the
# sibling's commit in the sibling's tmp/.
review sibling "$R1" "$repo" 100 text "$(report "$B1")"
expect_marker "a sibling worktree's review marks that worktree" "$wtb" "$R1" "$B1"
expect_no_marker "…and nothing in the session worktree" "$repo" "$R1" "$B1"

review handback-only "$R2" "$repo" 100 handback-only "$(report "$B1")"
expect_marker "a hand-back report with nothing after it writes the marker" "$wtb" "$R2" "$B1"

# A commit lands while a reviewer runs: the reviewer read a1, HEAD is now a2.
at 250 "$repo" commit -q --allow-empty -m a2
A2=$(git -C "$repo" rev-parse HEAD)
review moved-honest "$R1" "$repo" 200 text "$(report "$A1")"
expect_no_marker "a commit made mid-review gets no marker from the old review" "$repo" "$R1" "$A2" "no worktree is on that commit"
review moved-late "$R1" "$repo" 200 text "$(report "$A2")"
expect_no_marker "…even when the report names the new HEAD" "$repo" "$R1" "$A2" "moved while the review ran"
expect_block "the push of that commit is blocked" "$repo" "git push -u origin feat-a" "tmp/$R1-passed-${A2:0:8}"
expect_block "…and the block shows why the review wrote no marker" "$repo" "git push" "Latest review-marker decisions"

# Same-commit reflog entries are not moves.
at 310 "$repo" reset -q
at 311 "$repo" checkout -q feat-a
review churn "$R1" "$repo" 300 text "$(report "$A2")"
expect_marker "git reset / re-checkout of the same commit mid-review keeps the marker" "$repo" "$R1" "$A2"

# HEAD leaves the commit and comes back while the reviewer runs.
at 410 "$repo" checkout -q --detach HEAD~1
at 420 "$repo" checkout -q feat-a
review away-back "$R2" "$repo" 400 text "$(report "$A2")"
expect_no_marker "HEAD moved away and back mid-review: no marker" "$repo" "$R2" "$A2" "moved while the review ran"

review no-reviewed "$R2" "$repo" 500 text "$(printf 'Looks good.\n\nVERDICT: ship_it\n')"
expect_no_marker "ship_it without a REVIEWED line: no marker" "$repo" "$R2" "$A2" "the transcript has no hand-back"
expect_no_marker "…and the note points at a reviewer definition that predates the line" "$repo" "$R2" "$A2" "restart it"
review not-a-commit "$R2" "$repo" 500 text "$(report deadbeefdeadbeef)"
expect_no_marker "a REVIEWED sha that names no commit: no marker" "$repo" "$R2" "$A2" "doesn't name exactly one commit"
review iterate "$R2" "$repo" 500 text "$(report "$A2" iterate)"
expect_no_marker "VERDICT: iterate: no marker" "$repo" "$R2" "$A2" "VERDICT: iterate"
review inline "$R2" "$repo" 500 text "$(printf 'REVIEWED: %s\nDo not write VERDICT: ship_it yet.\n' "$A2")"
expect_no_marker "a VERDICT mentioned mid-sentence is not a verdict" "$repo" "$R2" "$A2" "no VERDICT line"
review notranscript "$R2" "$repo" 500 notranscript "$(report "$A2")"
expect_no_marker "no transcript to date the review: no marker" "$repo" "$R2" "$A2" "can't tell when the review began"
review handback-iterate "$R2" "$repo" 500 handback-iterate "$(report "$A2")"
expect_no_marker "a final VERDICT: iterate overrides an earlier ship_it hand-back" "$repo" "$R2" "$A2" "VERDICT: iterate"

lines_before=$(wc -l < "$repo/tmp/review-marker.log")
review other-agent Explore "$repo" 500 text "$(report "$A2")"
if has_marker "$repo" Explore "$A2" || [ "$(wc -l < "$repo/tmp/review-marker.log")" != "$lines_before" ]; then
  fail "a subagent that isn't a listed reviewer is ignored"
else
  ok "a subagent that isn't a listed reviewer is ignored"
fi

review abbreviated "$R2" "$repo" 500 text "$(report "${A2:0:12}")"
expect_marker "a fresh review naming the new HEAD by a short sha writes the full marker" "$repo" "$R2" "$A2"

echo "agent-bash-gate.sh:"

# timed <name> <max-seconds> <cwd> <command>: the gate answers within the limit.
timed() {
  local s=$SECONDS
  push "$3" "$4"
  if [ $((SECONDS - s)) -le "$2" ]; then ok "$1"; else fail "$1: took $((SECONDS - s))s"; fi
}

# State: repo/feat-a at a2 and wt-b's b1 are reviewed; b2 on top of b1 is not.
at 600 "$wtb" commit -q --allow-empty -m b2

# ── plain pushes are judged on the worktree they run in ──
expect_allow "plain push from the reviewed worktree" "$repo" "git push -u origin feat-a"
expect_allow "plain push, no refspec" "$repo" "git push"
expect_allow "push from a subdirectory of the worktree" "$repo/scripts" "git push"
expect_allow "a refspec resolved to a reviewed commit" "$repo" "git push origin HEAD:refs/heads/throwaway"
expect_allow "a refspec naming a commit that is no worktree's HEAD" "$repo" "git push origin feat-b~1:refs/heads/b1"
expect_allow "pushing main itself (no delta, no markers)" "$repo" "git push origin main"
expect_allow "2>&1 is not a refspec" "$repo" "git push origin feat-a 2>&1"
expect_allow "| tail -3" "$repo" "git push origin feat-a | tail -3"
expect_allow "2>&1 | tail -3" "$repo" "git push origin feat-a 2>&1 | tail -3"
expect_allow "git push --help" "$wtb" "git push --help"
expect_allow "git stash push is not a push" "$repo" "git stash push -m wip"
expect_allow "deleting a remote branch, no review" "$wtb" "git push origin --delete old-branch"
expect_allow "deleting by empty source, no review" "$wtb" "git push origin :old-branch"
expect_allow "a marker counts from whichever worktree holds it" "$wtb" "git push origin feat-a"

expect_block "plain push without markers" "$wtb" "git push origin feat-b" "missing pre-push review marker"
expect_block "…reporting the worktree it ran in" "$wtb" "git push origin feat-b" "pushed from $wtb"
expect_block "git -C <sibling worktree> push is judged on that worktree" "$repo" "git -C ../wt-b push" "missing pre-push review marker"
expect_block "git -C <absolute path> push" "$repo" "git -C $wtb push origin feat-b" "missing pre-push review marker"
expect_block "a refspec naming the sibling's branch" "$repo" "git push origin feat-b" "missing pre-push review marker"
expect_block "an unresolvable refspec" "$repo" "git push origin no-such-branch" "can't resolve"
expect_block "a glob refspec" "$repo" "git push origin 'refs/heads/feat-*'" "plain command"
expect_block "an unquoted glob refspec" "$repo" "git push origin refs/heads/feat-*" "plain command"
expect_block "--all" "$repo" "git push --all origin" "every branch"
expect_block "the matching refspec ':'" "$repo" "git push origin :" "every branch that exists on both sides"
expect_block "--force" "$repo" "git push --force origin feat-a" "Force-push"
expect_block "-f" "$repo" "git push -f origin feat-a" "Force-push"
expect_block "-uf" "$repo" "git push -uf origin feat-a" "Force-push"
expect_block "--force-with-lease" "$repo" "git push --force-with-lease origin feat-a" "Force-push"
expect_block "--force-if-includes" "$repo" "git push --force-if-includes origin feat-a" "Force-push"
expect_block "a +refspec, which force-updates its destination" "$repo" "git push origin +feat-a" "Force-push"
expect_block "a cwd outside any worktree" "$scratch" "git push" "not inside a git worktree"
if jq -nc --arg c "git push" '{tool_input: {command: $c}}' | "$gate" 2>"$scratch/gate.err"; then
  fail "no cwd in the payload: blocked" "$(cat "$scratch/gate.err")"
else
  ok "no cwd in the payload: blocked"
fi

# ── anything but a plain push is refused, with one message ──
expect_block "cd x && git push" "$repo" "cd ../wt-b && git push origin feat-b" "plain command"
expect_block "git commit … && git push" "$repo" "git commit --allow-empty -m x && git push" "plain command"
expect_block "git push; something" "$repo" "git push origin feat-a; echo done" "plain command"
expect_block "a push in \$(…)" "$repo" "out=\$(git push origin feat-b 2>&1)" "plain command"
expect_block "a push in bash -c" "$repo" "bash -c 'git push origin feat-b'" "plain command"
expect_block "a push behind a wrapper" "$repo" "timeout 60 git push" "plain command"
expect_block "a push in a heredoc" "$repo" "bash <<'EOF'
git push origin feat-b
EOF" "plain command"
expect_block "a push on the second line" "$repo" "echo hi
git push" "plain command"
expect_block "a push mentioned in a commit message" "$wtb" "git commit -m \"docs: explain git push\"" "plain command"
expect_block "a quoted refspec" "$repo" "git push origin \"feat-a\"" "plain command"
expect_block "a computed refspec" "$repo" "git push origin \"\$(git branch --show-current)\"" "plain command"
expect_block "a variable in -C" "$repo" "git -C \"\$WT\" push" "plain command"
expect_block "a filter that runs git" "$repo" "git push origin feat-a | git commit --allow-empty -m x" "plain command"
expect_block "a second pipe" "$repo" "git push origin feat-a | tail -3 | cat" "plain command"

# ── a repository without a reviewer manifest is out of scope ──
expect_allow "another repository without a manifest" "$other" "git push origin x"
expect_allow "…even pushed from the session's directory" "$repo" "git -C ../other push origin x"

# The base is origin/main when there is one: a commit made on local main by
# mistake and then branched from is on local main, but not yet reviewed.
acc=$scratch/acc
origin=$scratch/origin.git
git -C "$scratch" init -q --bare -b main "$origin"
if [ "$(git -C "$origin" rev-parse --absolute-git-dir 2>/dev/null)" != "$origin" ]; then
  echo "review-gate test: git did not create $origin; refusing to go on." >&2
  exit 1
fi
scratch_init "$acc"
mkdir -p "$acc/scripts"
cp scripts/pre-push-reviewers.sh scripts/skip-pre-push-review.sh "$acc/scripts/"
git -C "$acc" add scripts
at 900 "$acc" commit -q -m base
git -C "$acc" remote add origin "$origin"
git -C "$acc" push -q origin main
at 910 "$acc" commit -q --allow-empty -m "on main by mistake"
at 920 "$acc" switch -q -c feat
expect_block "commits made on local main, then pushed on a new branch" "$acc" "git push -u origin feat" "missing pre-push review marker"
expect_allow "…while a commit origin/main has needs no review" "$acc" "git push origin HEAD~1:refs/heads/copy"

# A worktree of the session's repository whose reviewer manifest is broken or
# gone fails closed.
wtc=$scratch/wt-c
at 700 "$repo" worktree add -q -b feat-c "$wtc" feat-a
printf '<<<<<<< broken\n' > "$wtc/scripts/pre-push-reviewers.sh"
at 700 "$wtc" commit -q -am c1
C1=$(git -C "$wtc" rev-parse HEAD)
expect_block "a broken reviewer manifest blocks" "$wtc" "git push" "reviewer list"

# The marker hook finds the reviewer manifest in the session's launch
# directory when the cwd is another repository.
review other-cwd "$R1" "$other" 800 text "$(report "$C1")"
expect_marker "a review stopped from another repository still marks the reviewed commit" "$wtc" "$R1" "$C1"
# The closing text repeats the verdict but not the sha: the hand-back is the report.
review handback-verdict "$R2" "$repo" 800 handback-verdict "$(report "$C1")"
expect_marker "a closing message with only the VERDICT line defers to the hand-back" "$wtc" "$R2" "$C1"

git -C "$wtc" rm -q scripts/pre-push-reviewers.sh
at 710 "$wtc" commit -q -m c2
expect_block "a deleted reviewer manifest blocks" "$wtc" "git push" "reviewer list"
expect_block "the block shows decisions logged in another worktree" "$repo" "git push origin feat-c" "$wtc/tmp/$R2-passed-"
expect_block "…and how to handle a reviewer definition older than REVIEWED:" "$repo" "git push origin feat-c" "restart it"

# A logged skip, recorded from a subdirectory, satisfies the gate and is echoed.
mkdir -p "$wtb/sub"
(cd "$wtb/sub" && ../scripts/skip-pre-push-review.sh "$R1" "test skip" \
  && ../scripts/skip-pre-push-review.sh "$R2" "test skip") 2>"$scratch/skip.err"
B2=$(git -C "$wtb" rev-parse HEAD)
if has_marker "$wtb" "$R1" "$B2" && has_marker "$wtb" "$R2" "$B2" && [ ! -d "$wtb/sub/tmp" ]; then
  ok "skip-pre-push-review.sh writes to the worktree root from a subdirectory"
else
  fail "skip-pre-push-review.sh writes to the worktree root from a subdirectory" "$(cat "$scratch/skip.err")"
fi
expect_allow "skipped reviewers satisfy the gate and are echoed" "$wtb" "git push origin feat-b" "skipped by judgment"

# Bash writes each of the manifest's names separately, so a reader that stops
# at the first match can exit before a later write, which then dies of SIGPIPE.
# Whether it does is up to the scheduler; a pause after each name gives the
# reader time to exit first.
slow=$scratch/slow
scratch_init "$slow"
mkdir -p "$slow/scripts"
cp scripts/pre-push-reviewers.sh "$slow/scripts/reviewers.sh"
cp scripts/skip-pre-push-review.sh "$slow/scripts/"
# shellcheck disable=SC2016 # the manifest's own code, expanded when it runs
printf '%s\n' '#!/usr/bin/env bash' 'bash "${0%/*}/reviewers.sh" | while read -r r; do echo "$r"; sleep 0.3; done' \
  > "$slow/scripts/pre-push-reviewers.sh"
git -C "$slow" add scripts
at 1000 "$slow" commit -q -m base
first=$(bash scripts/pre-push-reviewers.sh)
first=${first%%$'\n'*}
(cd "$slow" && scripts/skip-pre-push-review.sh "$first" "test skip") 2>"$scratch/skip.err"
if has_marker "$slow" "$first" "$(git -C "$slow" rev-parse HEAD)"; then
  ok "the first-listed reviewer can be skipped while the manifest is still writing"
else
  fail "the first-listed reviewer can be skipped while the manifest is still writing" "$(cat "$scratch/skip.err")"
fi

# ── gh: the humans-only PR actions, as plain text ──
# The title linter resolves from the session's checkout.
lint() { CLAUDE_PROJECT_DIR=$root "$@"; }

expect_block "gh pr create without --draft" "$repo" 'gh pr create --title "fix: x"' "must use --draft"
expect_block "gh pr new without --draft" "$repo" 'gh pr new --title "fix: x"' "must use --draft"
expect_block "gh --repo o/r pr create without --draft" "$repo" 'gh --repo o/r pr create --title "fix: x"' "must use --draft"
expect_block "gh pr create --draft=false" "$repo" 'gh pr create --draft=false --title "fix: x"' "must use --draft"
expect_allow "gh pr create --draft=true" "$repo" 'gh pr create --draft=true --title "fix: x"'
expect_allow "gh pr create -d" "$repo" 'gh pr create -d --title "fix: x"'
lint expect_allow "gh pr create --draft with a good title" "$repo" 'gh pr create --draft --title "feat(gate): add the thing" --body-file b.md'
lint expect_block "a bad double-quoted title" "$repo" 'gh pr create --draft --title "Bad title."' "PR title"
lint expect_block "a bad single-quoted title" "$repo" "gh pr create --draft -t 'Bad title.'" "PR title"
lint expect_block "a bad --title= title" "$repo" 'gh pr create --draft --title=Bad.' "PR title"
lint expect_block "a bad title on gh pr edit" "$repo" 'gh pr edit 12 --title "Bad title."' "PR title"
lint expect_allow "gh pr edit --title with a good title" "$repo" 'gh pr edit 12 --title "fix: add the thing"'
lint expect_allow "gh pr create --fill: no literal title, fail-open" "$repo" 'gh pr create --draft --fill'
expect_block "gh pr create --reviewer" "$repo" 'gh pr create --draft --title "feat: x" --reviewer someone' "Adding/removing reviewers"
expect_block "gh pr new --assignee=x" "$repo" 'gh pr new --draft --title "feat: x" --assignee=x' "Adding/removing reviewers"
expect_block "gh pr edit --add-reviewer" "$repo" 'gh pr edit 12 --add-reviewer someone' "Adding/removing reviewers"
expect_block "gh -R o/r pr edit --add-assignee=x" "$repo" 'gh -R o/r pr edit 12 --add-assignee=x' "Adding/removing reviewers"
expect_block "gh pr ready" "$repo" 'gh pr ready 12' "ready-for-review"
expect_block "gh pr review --approve" "$repo" 'gh pr review 12 --approve' "Only humans approve"
expect_block "gh pr review --request-changes" "$repo" 'gh pr review 12 --request-changes -b "see inline"' "instead of --request-changes"
expect_allow "gh pr review --comment" "$repo" 'gh pr review 12 --comment -b "noted"'

expect_block "REST approve" "$repo" 'gh api repos/o/r/pulls/12/reviews -f event=APPROVE' "Only humans approve"
expect_block "REST pending-review approve" "$repo" 'gh api repos/o/r/pulls/12/reviews/34/events -f event=APPROVE' "Only humans approve"
expect_block "REST request changes" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews -f event=REQUEST_CHANGES -f body='see inline'" "instead of --request-changes"
expect_block "GraphQL approve" "$repo" "gh api graphql -f query='mutation { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: APPROVE}) { clientMutationId } }'" "Only humans approve"
expect_block "GraphQL ready" "$repo" "gh api graphql -f query='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "ready-for-review"
expect_block "GraphQL request reviews" "$repo" "gh api graphql -f query='mutation { requestReviews(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "Reviewer-write"
expect_block "REST requested_reviewers" "$repo" "gh api repos/o/r/pulls/12/requested_reviewers -f 'reviewers[]=someone'" "Reviewer-write"
expect_block "REST merge" "$repo" 'gh api -X PUT repos/o/r/pulls/12/merge -f merge_method=squash' "don't merge"
expect_block "GraphQL merge" "$repo" "gh api graphql -f query='mutation { mergePullRequest(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "don't merge"
expect_block "GraphQL auto-merge" "$repo" "gh api graphql -f query='mutation { enablePullRequestAutoMerge(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "don't merge"
expect_block "GraphQL createPullRequest without draft" "$repo" "gh api graphql -f query='mutation { createPullRequest(input: {repositoryId: \"R_x\", baseRefName: \"main\", headRefName: \"feat\", title: \"fix: x\"}) { clientMutationId } }'" "must be drafts"
expect_block "REST create without draft" "$repo" 'gh api repos/o/r/pulls -f head=feat -f base=main -f title="fix: x"' "must be drafts"
lint expect_block "REST draft create gets the title lint" "$repo" 'gh api repos/o/r/pulls -f head=feat -f base=main -F draft=true -f title="Bad title."' "PR title"

expect_allow "gh pr view" "$repo" 'gh pr view 12'
expect_allow "gh pr list" "$repo" 'gh pr list --state open'
expect_allow "gh pr checks" "$repo" 'gh pr checks 12'
expect_allow "gh pr diff" "$repo" 'gh pr diff 12'
expect_allow "gh pr comment" "$repo" 'gh pr comment 12 --body "looks fine"'
expect_allow "gh pr merge is left to the settings.json deny rule" "$repo" 'gh pr merge 12 --squash'
expect_allow "gh api read of a PR" "$repo" 'gh api repos/o/r/pulls/12'
expect_allow "gh api list of PRs" "$repo" "gh api 'repos/o/r/pulls?state=open'"
expect_allow "gh api GET with fields" "$repo" 'gh api -X GET repos/o/r/pulls -f state=open'
expect_allow "gh api read of a PR's reviews" "$repo" 'gh api repos/o/r/pulls/12/reviews'
lint expect_allow "REST draft create with a good title" "$repo" 'gh api repos/o/r/pulls -f head=feat -f base=main -F draft=true -f title="fix: add the thing"'
expect_allow "GraphQL draft createPullRequest" "$repo" "gh api graphql -f query='mutation { createPullRequest(input: {repositoryId: \"R_x\", baseRefName: \"main\", headRefName: \"feat\", title: \"fix: x\", draft: true}) { clientMutationId } }'"
expect_allow "a COMMENT review through REST" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews -f event=COMMENT -f body='looks fine'"
expect_allow "a COMMENT review through GraphQL" "$repo" "gh api graphql -f query='mutation { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: COMMENT}) { clientMutationId } }'"
expect_allow "a review-thread reply" "$repo" "gh api repos/o/r/pulls/12/comments/34/replies -f body='fixed in abc123'"
expect_allow "an issue comment that mentions approve and merge" "$repo" "gh api -X POST repos/o/r/issues/12/comments -f body='approve it, then merge'"
expect_allow "no gh at all" "$repo" "echo hello; ls"

# The accepted false positive: the text is searched, not parsed. Put long text
# in a file (--body-file) to avoid it.
expect_block "a gh pr comment body that names markPullRequestReadyForReview IS blocked" "$repo" "gh pr comment 12 --body 'the markPullRequestReadyForReview mutation is humans-only'" "ready-for-review"

# A command longer than a pipe buffer (64 KB) is checked like any other, quickly.
big=$(printf 'line %05d of a long body\n' $(seq 1 4000))
timed "a 100 KB gh pr create is judged within 10 s" 10 "$repo" "gh pr create --draft --title \"fix: x\" --body \"\$(cat <<'EOF'
$big
EOF
)\""
expect_block "…and a 100 KB gh pr create without --draft is blocked" "$repo" "gh pr create --title \"fix: x\" --body \"\$(cat <<'EOF'
$big
EOF
)\"" "--draft"
expect_block "…and a push ahead of a 100 KB body is refused" "$repo" "git push origin feat-b && gh pr create --draft --body \"\$(cat <<'EOF'
$big
EOF
)\"" "plain command"

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "review-gate: all cases passed"

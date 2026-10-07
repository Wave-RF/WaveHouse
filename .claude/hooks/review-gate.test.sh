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

# State: repo/feat-a at a2 and wt-b's b1 are reviewed; b2 on top of b1 is not.
at 600 "$wtb" commit -q --allow-empty -m b2

expect_allow "push from the reviewed session worktree" "$repo" "git push -u origin feat-a"
expect_allow "push with no refspec" "$repo" "git push"
expect_allow "push HEAD to a new remote branch" "$repo" "git push origin HEAD:refs/heads/throwaway"
expect_allow "push from a subdirectory of the worktree" "$repo/scripts" "git push"
expect_allow "the fd number in 2>&1 is not a refspec" "$repo" "git push origin feat-a 2>&1 | tail -5"

# The session's launch directory (repo) is fully reviewed; the push runs elsewhere.
expect_block "a sibling worktree's unreviewed branch, pushed from there" "$wtb" "git push origin feat-b" "pushed from $wtb"
expect_block "…to a throwaway ref" "$wtb" "git push --dry-run origin HEAD:refs/heads/gate-probe-throwaway"
expect_block "cd into the sibling first" "$repo" "cd ../wt-b && git push origin feat-b"
expect_block "cd by absolute path" "$repo" "cd '$wtb' && git push"
expect_block "git -C the sibling" "$repo" "git -C ../wt-b push"
expect_block "git -C a quoted absolute path" "$repo" "git -C \"$wtb\" push origin feat-b"
expect_block "a refspec naming the sibling's branch" "$repo" "git push origin feat-b"
expect_block "…after a heredoc tag message with an apostrophe" "$wtb" "git tag -a v9 -m \"\$(cat <<'EOF'
Don't (really)
EOF
)\" && git push"
expect_allow "the same from the reviewed worktree" "$repo" "git tag -a v9 -m \"\$(cat <<'EOF'
Don't (really)
EOF
)\" && git push"
expect_allow "a push inside if … then" "$repo" "if git push origin feat-a; then echo ok; fi"
expect_block "a push inside a loop is gated, not refused" "$repo" "while false; do git push origin feat-b; done" "missing pre-push review marker"

# The gate runs before the command line does, so a push after a HEAD-moving git
# command would be judged on the commit before the move.
expect_block "git commit && git push" "$repo" "git commit --allow-empty -m x && git push" "separate command"
expect_block "git -C <path> switch; git push" "$repo" "git -C ../wt-b switch -q feat-a; git push origin feat-a" "separate command"
expect_allow "a marker counts from whichever worktree holds it" "$wtb" "git push origin feat-a"
expect_allow "…including for a commit that is no worktree's HEAD" "$repo" "git push origin feat-b~1:refs/heads/b1"
expect_allow "a subshell's cd doesn't leak" "$repo" "(cd ../wt-b && true) && git push"

# Fail closed when the gate can't tell.
expect_block "a cd to a computed path" "$repo" "cd \"\$WT\" && git push" "can't tell which repository"
expect_block "git -C a computed path" "$repo" "git -C \"\$WT\" push"
expect_block "cd -" "$repo" "cd - && git push"
expect_block "GIT_DIR in the environment" "$repo" "GIT_DIR=$wtb/.git git push"
expect_block "a computed refspec" "$repo" "git push origin \"\$(git branch --show-current)\"" "computed by the shell"
expect_block "a glob refspec" "$repo" "git push origin 'refs/heads/feat-*'" "glob refspec"
expect_block "--all" "$repo" "git push --all origin" "every branch"
expect_block "an unresolvable refspec" "$repo" "git push origin no-such-branch" "can't resolve"
expect_block "a wrapped push" "$repo" "timeout 60 git push" "can't follow"
expect_block "a push behind sudo" "$repo" "sudo git push origin feat-a" "can't follow"
expect_block "a push in a command substitution" "$repo" "out=\$(git push origin feat-b 2>&1)" "can't follow"
# A followed push on the line doesn't excuse an unfollowed one.
expect_block "a followed push, then a wrapped one" "$repo" "git push origin feat-a && timeout 60 git push origin feat-b" "can't follow"
expect_block "a followed push, then one in a substitution" "$repo" "git push origin feat-a; out=\$(git -C ../wt-b push 2>&1)" "can't follow"
expect_block "a followed push, then one behind sudo" "$repo" "git push origin feat-a && sudo git -C ../wt-b push" "can't follow"
expect_block "push --help, then a wrapped push" "$repo" "git push --help; timeout 5 git push origin feat-b" "can't follow"
expect_allow "two followed pushes" "$repo" "git push origin feat-a && git push origin feat-a:refs/heads/copy"
expect_block "export GIT_DIR before the push" "$repo" "export GIT_DIR=$wtb/.git; git push origin HEAD" "can't tell which repository"
expect_block "declare -x GIT_WORK_TREE before the push" "$repo" "declare -x GIT_WORK_TREE=$wtb; git push" "can't tell which repository"
expect_block "a bare GIT_DIR assignment before the push" "$repo" "GIT_DIR=$wtb/.git; git push" "can't tell which repository"
# An exported GIT_DIR or GIT_WORK_TREE outlives a later cd or git -C; only
# unset or the end of a subshell clears it.
expect_block "export GIT_DIR, then cd to a reviewed worktree" "$repo" "export GIT_DIR=$wtb/.git; cd $repo && git push" "can't tell which repository"
expect_block "export GIT_DIR, then git -C a reviewed worktree" "$repo" "export GIT_DIR=$wtb/.git; git -C $repo push" "can't tell which repository"
expect_block "export GIT_WORK_TREE, then cd" "$repo" "export GIT_WORK_TREE=$wtb; cd $repo; git push" "can't tell which repository"
expect_block "export GIT_DIR, then pushd" "$repo" "export GIT_DIR=$wtb/.git; pushd $repo >/dev/null; git push" "can't tell which repository"
expect_block "…and unsetting one of the two leaves the other" "$repo" "export GIT_DIR=$wtb/.git GIT_WORK_TREE=$wtb; unset GIT_DIR; git push" "can't tell which repository"
expect_block "git --git-dir, then -C a reviewed worktree" "$repo" "git --git-dir=$wtb/.git -C $repo push" "can't tell which repository"
expect_allow "export GIT_DIR, then unset it" "$repo" "export GIT_DIR=$wtb/.git; unset GIT_DIR; git push"
expect_allow "export GIT_DIR in a subshell" "$repo" "(export GIT_DIR=$wtb/.git; true) && git push"
# A HEAD-moving git command the walk doesn't run directly still moves HEAD.
expect_block "a wrapped commit, then a push" "$repo" "timeout 60 git commit -m x && git push" "separate command"
expect_block "a commit in a command substitution, then a push" "$repo" "out=\$(git commit --allow-empty -m x 2>&1) && git push" "separate command"
# Code handed to a shell or eval is not followed; a process substitution is.
expect_block "a push in bash -c" "$repo" "bash -c \"git push origin feat-b\"" "can't follow"
expect_block "a push in sh -c, after a cd" "$repo" "sh -c 'cd ../wt-b && git push'" "can't follow"
expect_block "a push in eval" "$repo" "eval \"git push origin feat-b\"" "can't follow"
expect_block "a push in a heredoc read by bash" "$repo" "bash <<'EOF'
cd ../wt-b
git push origin feat-b
EOF" "can't follow"
expect_block "a push in a here-string read by sh" "$repo" "sh <<< 'git -C ../wt-b push'" "can't follow"
expect_block "a push in an input process substitution" "$repo" "cat <(git push origin feat-b)" "missing pre-push review marker"
expect_block "a push in an output process substitution" "$repo" "tee >(git -C ../wt-b push)" "missing pre-push review marker"
expect_block "a push in a here-string read by sh, inside \$(…)" "$repo" "echo \"\$(sh <<<'git -C ../wt-b push')\"" "can't follow"
expect_block "a push in bash -c, inside \$(…)" "$repo" "out=\$(bash -c 'git -C ../wt-b push')" "can't follow"
expect_block "a push in eval, inside \$(…)" "$repo" "out=\$(eval 'git -C ../wt-b push')" "can't follow"
expect_block "a push with a quoted -C path, inside \$(…)" "$repo" "out=\$(git -C \"\$WT\" push)" "can't follow"
expect_block "a push in bash -c, inside \$'…' inside \$(…)" "$repo" "out=\$(bash -c \$'git -C ../wt-b push')" "can't follow"
# A substitution's output handed to a shell is code.
expect_block "a substitution's output handed to eval" "$repo" "eval \"\$(echo 'git -C ../wt-b push')\"" "can't follow"
expect_block "…to bash -c" "$repo" "bash -c \"\$(echo 'git -C ../wt-b push')\"" "can't follow"
expect_block "a push continued over a backslash-newline in bash -c" "$repo" "bash -c 'git -C ../wt-b \\
push'" "can't follow"
expect_block "…and in a heredoc read by bash" "$repo" "bash <<'EOF'
git -C ../wt-b \\
push
EOF" "can't follow"
# A substitution in a here-string or a redirect target runs like any other.
expect_block "a push in a here-string's substitution" "$repo" "grep -q rejected <<< \"\$(git -C ../wt-b push 2>&1)\"" "can't follow"
expect_block "a push in a redirect target's substitution" "$repo" "echo hi > \"\$(git -C ../wt-b push >/dev/null; echo /dev/null)\"" "can't follow"
expect_block "a commit in a here-string's substitution, then a push" "$repo" "grep -q x <<< \"\$(git commit --allow-empty -m x)\"; git push" "separate command"
expect_allow "a mention of git push beside a substitution" "$wtb" "echo \"\$(date): ran git push\""
# Quoted text inside a substitution is an argument, not code.
expect_allow "a substitution that greps for git push" "$wtb" "n=\$(grep -c 'git push' AGENTS.md)"
expect_allow "…looped over" "$wtb" "for f in \$(rg -l 'git push' docs); do echo \$f; done"
expect_allow "…double-quoted" "$wtb" "wc -l \$(grep -rl \"git push\" .claude)"
expect_allow "a commit message from a substitution that mentions git push" "$wtb" "git commit -m \"\$(echo 'docs: explain the git push gate')\""
expect_allow "a PR comment from printf that mentions git push" "$wtb" "gh pr comment 1 --body \"\$(printf 'Fixed; run git push to update.\\n')\""
expect_allow "a push, then a substitution that greps for git commit" "$repo" "git push origin feat-a; n=\$(grep -c 'git commit' AGENTS.md)"
expect_allow "a PR body heredoc that mentions github and pushed" "$wtb" "gh pr create --draft --title \"fix: x\" --body \"\$(cat <<'EOF'
The branch on github was pushed; it's ready.
EOF
)\""
expect_block "a push after an \$'…' string holding \\'" "$repo" "echo \$'it\\'s' && git push origin feat-b" "missing pre-push review marker"
expect_allow "…and the same push of a reviewed branch" "$repo" "echo \$'it\\'s' && git push origin feat-a"
expect_allow "…with the \$'…' string inside \$(…)" "$repo" "out=\$(printf \$'it\\'s pushed\\n') && git push origin feat-a"
expect_allow "…closed with EOF)\" on one line" "$wtb" "gh pr create --draft --title \"fix: x\" --body \"\$(cat <<'EOF'
The branch on github was pushed; it's ready.
EOF)\""
expect_allow "a script run by bash, then a push" "$repo" "bash scripts/pre-push-reviewers.sh && git push"
expect_allow "a heredoc read by bash that doesn't push, then a push" "$repo" "bash <<'EOF'
echo hi
EOF
git push"
# A command longer than a pipe buffer (64 KB) is checked like any other.
big=$(printf 'line %05d of a long body\n' $(seq 1 4000))
expect_block "a push ahead of a 100 KB body" "$repo" "git push origin feat-b && gh pr create --draft --body \"\$(cat <<'EOF'
$big
EOF
)\"" "missing pre-push review marker"
expect_block "a 100 KB gh pr create without --draft" "$repo" "gh pr create --title \"fix: x\" --body \"\$(cat <<'EOF'
$big
EOF
)\"" "--draft"
expect_block "the matching refspec ':'" "$repo" "git push origin :" "every branch that exists on both sides"
expect_block "…and '+:'" "$repo" "git push origin +:" "every branch that exists on both sides"
expect_block "an unterminated quote" "$repo" "git push origin 'feat-a" "can't parse"
if jq -nc --arg c "git push" '{tool_input: {command: $c}}' | "$gate" 2>"$scratch/gate.err"; then
  fail "no cwd in the payload: blocked" "$(cat "$scratch/gate.err")"
else
  ok "no cwd in the payload: blocked"
fi

# Not pushes, or nothing to review.
expect_allow "git stash push" "$repo" "git stash push -m wip"
expect_allow "git -C <path> stash push" "$repo" "git -C \"$wtb\" stash push -m wip"
expect_allow "git push --help" "$wtb" "git push --help"
expect_allow "a push mentioned in a string" "$wtb" "echo \"git push\""
# Mentions of a push in a heredoc body, a multi-line string or a comment, from
# the worktree whose branch is unreviewed: none of these pushes anything.
expect_allow "a push mentioned in a commit message heredoc" "$wtb" "git commit -m \"\$(cat <<'EOT'
fix: tidy

Then git push origin feat-b.
EOT
)\""
expect_allow "a push mentioned in a heredoc written to a file" "$wtb" "cat > notes.md <<'EOT'
run git push origin feat-b
EOT"
expect_allow "a push mentioned in a heredoc piped to gh" "$wtb" "gh pr comment 1 --body-file - <<'EOT'
Please git push origin feat-b
EOT"
expect_allow "a push mentioned in a multi-line quoted string" "$wtb" "gh issue comment 1 --body \"first line
git push origin feat-b
last line\""
expect_allow "a push mentioned in a comment" "$wtb" "git status # then git push"
expect_allow "deleting a remote branch" "$wtb" "git push origin --delete old-branch"
expect_allow "deleting by empty source" "$wtb" "git push origin :old-branch"
expect_allow "pushing main itself (no delta)" "$repo" "git push origin main"

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

expect_allow "another repository without a reviewer manifest is out of scope" "$other" "git push origin x"
expect_allow "…even pushed from the session's directory" "$repo" "git -C ../other push origin x"

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
(cd "$wtb/sub" && ../scripts/skip-pre-push-review.sh "$R1" "test skip" 2>/dev/null \
  && ../scripts/skip-pre-push-review.sh "$R2" "test skip" 2>/dev/null)
B2=$(git -C "$wtb" rev-parse HEAD)
if has_marker "$wtb" "$R1" "$B2" && [ ! -d "$wtb/sub/tmp" ]; then
  ok "skip-pre-push-review.sh writes to the worktree root from a subdirectory"
else
  fail "skip-pre-push-review.sh writes to the worktree root from a subdirectory"
fi
expect_allow "skipped reviewers satisfy the gate and are echoed" "$wtb" "git push origin feat-b" "skipped by judgment"

# The gh checks still resolve the title linter from the session's checkout.
CLAUDE_PROJECT_DIR=$root expect_block "gh pr create title lint" "$repo" "gh pr create --draft --title \"Bad title.\"" "PR title"
expect_block "gh pr create without --draft" "$repo" "gh pr create --title \"fix: x\""

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "review-gate: all cases passed"

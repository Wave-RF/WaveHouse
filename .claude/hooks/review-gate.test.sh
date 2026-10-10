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

# The gh checks still resolve the title linter from the session's checkout.
CLAUDE_PROJECT_DIR=$root expect_block "gh pr create title lint" "$repo" "gh pr create --draft --title \"Bad title.\"" "PR title"
expect_block "gh pr create without --draft" "$repo" "gh pr create --title \"fix: x\""

# The alias, REST and GraphQL forms of the humans-only gh pr actions (#790).
expect_block "gh pr new without --draft" "$repo" 'gh pr new --title "fix: x"' "must use --draft"
CLAUDE_PROJECT_DIR=$root expect_block "gh pr new title lint" "$repo" 'gh pr new --draft --title "Bad title."' "PR title"
CLAUDE_PROJECT_DIR=$root expect_block "an unquoted gh pr create title is linted" "$repo" 'gh pr create --draft --title Bad.' "PR title"
CLAUDE_PROJECT_DIR=$root expect_allow "gh pr new --draft with a good title" "$repo" 'gh pr new --draft --title "fix: add the thing"'
expect_block "approve through the REST API" "$repo" 'gh api repos/o/r/pulls/12/reviews -f event=APPROVE' "Only humans approve"
expect_block "request changes through the REST API" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews -f event=REQUEST_CHANGES -f body='see inline'" "instead of --request-changes"
expect_block "submit a pending review as an approval" "$repo" 'gh api repos/{owner}/{repo}/pulls/12/reviews/34/events -f event=APPROVE' "Only humans approve"
expect_block "open a PR through the REST API without draft" "$repo" 'gh api repos/o/r/pulls -f head=feat -f base=main -f title="fix: x"' "must be drafts"
CLAUDE_PROJECT_DIR=$root expect_block "a draft opened through the REST API gets the title lint" "$repo" 'gh api repos/o/r/pulls -f head=feat -f base=main -F draft=true -f title="Bad title."' "PR title"
CLAUDE_PROJECT_DIR=$root expect_block "a retitle through the REST API gets the title lint" "$repo" 'gh api --method=PATCH repos/o/r/pulls/12 -f "title=Bad title."' "PR title"
CLAUDE_PROJECT_DIR=$root expect_allow "a draft opened through the REST API with a good title" "$repo" 'gh api repos/o/r/pulls -f head=feat -f base=main -F draft=true -f title="fix: add the thing"'
expect_block "ready for review through GraphQL" "$repo" "gh api graphql -f query='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "ready-for-review"
expect_block "request reviewers with reviewers[]= and no -X" "$repo" "gh api repos/o/r/pulls/12/requested_reviewers -f 'reviewers[]=someone'" "Reviewer-write"
expect_block "remove a requested reviewer" "$repo" "gh api -X DELETE repos/o/r/pulls/12/requested_reviewers -f 'reviewers[]=someone'" "Reviewer-write"
expect_block "a quoted requested_reviewers path" "$repo" 'gh api -XPOST "repos/o/r/pulls/12/requested_reviewers" -f reviewers[]=someone' "Reviewer-write"
expect_block "request reviewers through GraphQL" "$repo" "gh api graphql -f query='mutation { requestReviews(input: {pullRequestId: \"PR_x\", userIds: [\"U_x\"]}) { clientMutationId } }'" "Reviewer-write"
expect_block "approve through GraphQL" "$repo" "gh api graphql -f query='mutation { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: APPROVE}) { clientMutationId } }'" "Only humans approve"
expect_block "open a non-draft PR through GraphQL" "$repo" "gh api graphql -f query='mutation { createPullRequest(input: {repositoryId: \"R_x\", baseRefName: \"main\", headRefName: \"feat\", title: \"fix: x\"}) { clientMutationId } }'" "must be drafts"
expect_block "merge through the REST API" "$repo" 'gh api -X PUT repos/o/r/pulls/12/merge -f merge_method=squash' "don't merge"
expect_block "merge through GraphQL" "$repo" "gh api graphql -f query='mutation { mergePullRequest(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "don't merge"
expect_block "auto-merge through GraphQL" "$repo" "gh api graphql -f query='mutation { enablePullRequestAutoMerge(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "don't merge"
# gh pr merge itself is left to the deny rule in .claude/settings.json: a
# person merges with it from shell mode, which this hook may see.
expect_allow "gh pr merge is left to the settings.json deny rule" "$repo" 'gh pr merge 12 --squash'

# Look-alikes that read, or don't touch a PR, go through.
expect_allow "read a PR through the REST API" "$repo" 'gh api repos/o/r/pulls/12'
expect_allow "list PRs" "$repo" "gh api 'repos/o/r/pulls?state=open'"
expect_allow "list PRs with -X GET and fields" "$repo" 'gh api -X GET repos/o/r/pulls -f state=open'
expect_allow "read a PR's reviews" "$repo" 'gh api repos/o/r/pulls/12/reviews'
expect_allow "read its requested reviewers" "$repo" 'gh api repos/o/r/pulls/12/requested_reviewers'
expect_allow "check whether it merged" "$repo" 'gh api repos/o/r/pulls/12/merge'
expect_allow "a COMMENT review through the REST API" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews -f event=COMMENT -f body='looks fine'"
expect_allow "a GraphQL query that reads reviews" "$repo" "gh api graphql -f query='query { repository(owner: \"o\", name: \"r\") { pullRequest(number: 12) { reviews(last: 5) { nodes { state } } } } }'"
expect_allow "a draft PR opened through GraphQL" "$repo" "gh api graphql -f query='mutation { createPullRequest(input: {repositoryId: \"R_x\", baseRefName: \"main\", headRefName: \"feat\", title: \"fix: x\", draft: true}) { clientMutationId } }'"
expect_allow "an issue comment that mentions approve and merge" "$repo" "gh api -X POST repos/o/r/issues/12/comments -f body='approve it, then merge'"
expect_allow "gh pr view" "$repo" 'gh pr view 12'

# A gh command only mentioned in a heredoc body, a comment or a quoted string
# isn't run; one a shell runs still is.
expect_allow "a gh pr ready in a comment inside a python heredoc" "$repo" "python3 - <<'EOF'
# gh pr ready is humans-only.
print('ok')
EOF"
expect_allow "gh pr merge and an API merge in a cat heredoc" "$repo" "cat <<'EOF' >notes.txt
gh pr merge 12 --squash
gh api -X PUT repos/o/r/pulls/12/merge
EOF"
expect_allow "a gh pr ready in a shell comment" "$repo" 'ls # then gh pr ready 12'
expect_allow "a gh pr ready in a quoted string" "$repo" 'echo "run gh pr ready 12 when done"'
expect_allow "a PR body heredoc that mentions gh pr ready" "$repo" "gh pr create --draft --title \"fix: x\" --body \"\$(cat <<'EOF'
A person runs gh pr ready 12 later.
EOF
)\""
expect_block "a gh pr ready run on its own line" "$repo" "cd .
gh pr ready 12" "ready-for-review"
expect_block "a gh pr ready behind a wrapper" "$repo" 'timeout 60 gh pr ready 12' "ready-for-review"
# shellcheck disable=SC2016 # a literal $(…), for the gate to read
expect_block "a gh pr ready in a \$(…)" "$repo" 'echo "$(gh pr ready 12)"' "ready-for-review"
expect_block "a gh pr ready handed to bash -c" "$repo" "bash -c 'gh pr ready 12'" "ready-for-review"
expect_block "a gh pr ready in a heredoc read by bash" "$repo" "bash <<'EOF'
gh pr ready 12
EOF" "ready-for-review"
expect_block "a gh pr ready in a heredoc piped into sh" "$repo" "cat <<'EOF' | sh
gh pr ready 12
EOF" "ready-for-review"

# gh api handed to a shell is judged like any other gh api call.
expect_block "an API merge handed to bash -c" "$repo" "bash -c 'gh api -X PUT repos/o/r/pulls/12/merge'" "don't merge"
expect_block "an API approval handed to sh -c" "$repo" 'sh -c "gh api repos/o/r/pulls/12/reviews -f event=APPROVE"' "Only humans approve"
expect_block "GraphQL ready handed to bash -lc" "$repo" "bash -lc \"gh api graphql -f query='mutation { markPullRequestReadyForReview(input: {pullRequestId: \\\"PR_x\\\"}) { clientMutationId } }'\"" "ready-for-review"
expect_block "an API merge handed to eval" "$repo" "eval 'gh api -X PUT repos/o/r/pulls/12/merge'" "don't merge"
expect_allow "a read handed to bash -c" "$repo" "bash -c 'gh api repos/o/r/pulls/12'"

# A boolean flag set with = counts as set when its value is true, and not when false.
expect_allow "gh pr create --draft=true" "$repo" 'gh pr create --draft=true --title "fix: x"'
expect_block "gh pr create --draft=false" "$repo" 'gh pr create --draft=false --title "fix: x"' "must use --draft"
expect_block "gh pr review --approve=true" "$repo" 'gh pr review 12 --approve=true' "Only humans approve"
expect_allow "gh pr review --approve=false --comment" "$repo" 'gh pr review 12 --approve=false --comment -b "noted"'
expect_block "gh pr review --request-changes=true" "$repo" 'gh pr review 12 --request-changes=true -b "see inline"' "instead of --request-changes"
expect_block "gh pr edit --add-reviewer=someone" "$repo" 'gh pr edit 12 --add-reviewer=someone' "Adding/removing reviewers"

# gh pr create / new can request reviewers or assign people at creation; on gh pr review -r / -a stay --request-changes / --approve.
for c in create new; do
  expect_block "gh pr $c --reviewer x" "$repo" "gh pr $c --draft --title \"feat: x\" --reviewer x" "Adding/removing reviewers"
  expect_block "gh pr $c --reviewer=x" "$repo" "gh pr $c --draft --title \"feat: x\" --reviewer=x" "Adding/removing reviewers"
  expect_block "gh pr $c -r x" "$repo" "gh pr $c --draft --title \"feat: x\" -r x" "Adding/removing reviewers"
  expect_block "gh pr $c -rx (glued)" "$repo" "gh pr $c --draft --title \"feat: x\" -rx" "Adding/removing reviewers"
  expect_block "gh pr $c --assignee x" "$repo" "gh pr $c --draft --title \"feat: x\" --assignee x" "Adding/removing reviewers"
  expect_block "gh pr $c --assignee=x" "$repo" "gh pr $c --draft --title \"feat: x\" --assignee=x" "Adding/removing reviewers"
  expect_block "gh pr $c -a x" "$repo" "gh pr $c --draft --title \"feat: x\" -a x" "Adding/removing reviewers"
  expect_block "gh pr $c -a=x (glued)" "$repo" "gh pr $c --draft --title \"feat: x\" -a=x" "Adding/removing reviewers"
done
expect_allow "a plain gh pr create --draft" "$repo" 'gh pr create --draft --title "feat: x"'
expect_block "gh pr review -a is still --approve" "$repo" 'gh pr review 12 -a' "Only humans approve"
expect_block "gh pr review -r is still --request-changes" "$repo" 'gh pr review 12 -r -b "see inline"' "instead of --request-changes"

# Each gh api call is judged by its own endpoint and fields, not by text in a
# field's value or in another command on the line.
expect_allow "a review-thread reply that quotes a merge endpoint" "$repo" "gh api repos/o/r/pulls/12/comments/34/replies -f body='Merging is PUT repos/o/r/pulls/12/merge and only a person runs it.'"
expect_allow "an issue comment that names requested_reviewers" "$repo" "gh api -X POST repos/o/r/issues/12/comments -f body='requested_reviewers is set by the ruleset'"
expect_allow "a comment body with event=APPROVE in it" "$repo" "gh api -X POST repos/o/r/issues/12/comments -f body='a review sends event=APPROVE; we do not'"
expect_allow "a GraphQL comment that mentions a mutation name" "$repo" "gh api graphql -f query='mutation(\$b: String!) { addComment(input: {subjectId: \"I_x\", body: \$b}) { clientMutationId } }' -f b='markPullRequestReadyForReview is for people'"
expect_allow "a reviewer read, then a comment, on one line" "$repo" 'gh api repos/o/r/pulls/12/requested_reviewers && gh api -X POST repos/o/r/issues/12/comments -f body=ping'
expect_block "a comment, then a reviewer write, on one line" "$repo" "gh api -X POST repos/o/r/issues/12/comments -f body=ping && gh api repos/o/r/pulls/12/requested_reviewers -f 'reviewers[]=someone'" "Reviewer-write"
expect_allow "a draft opened through the REST API whose body says draft=false" "$repo" "gh api repos/o/r/pulls -f head=feat -f base=main -F draft=true -f body='not draft=false'"
expect_block "a non-draft whose body says draft=true" "$repo" "gh api repos/o/r/pulls -f head=feat -f base=main -f body='draft=true'" "must be drafts"
expect_block "an API merge through a full URL" "$repo" 'gh api --method PUT https://api.github.com/repos/o/r/pulls/12/merge' "don't merge"
expect_block "an API merge with fields set by -F=" "$repo" 'gh api repos/o/r/pulls/12/merge --field=merge_method=squash' "don't merge"

# The gate follows 64 pieces of nested code; a gh command beyond them is refused.
# shellcheck disable=SC2016 # literal $(true)s, for the gate to read
pad62=$(printf ' $(true)%.0s' $(seq 62)) pad63=$(printf ' $(true)%.0s' $(seq 63))
expect_block "a gh pr ready in the 63rd substitution is judged" "$repo" "echo$pad62 \$(gh pr ready 12)" "ready-for-review"
expect_block "a gh pr ready past the 64 the gate follows is refused" "$repo" "echo$pad63 \$(gh pr ready 12)" "nests more code"
expect_allow "a gh read beside 70 substitutions with no gh in them" "$repo" "gh pr view 12 && echo$pad63 \$(true) \$(true) \$(true) \$(true) \$(true) \$(true) \$(true)"

# gh resolves -R/--repo before the command.
expect_block "gh -R o/r pr ready" "$repo" 'gh -R o/r pr ready 12' "ready-for-review"
expect_block "gh --repo o/r pr review --approve" "$repo" 'gh --repo o/r pr review 12 --approve' "Only humans approve"
expect_block "gh --repo=o/r pr create without --draft" "$repo" 'gh --repo=o/r pr create --title "fix: x"' "must use --draft"
expect_block "gh -Ro/r pr edit --add-reviewer" "$repo" 'gh -Ro/r pr edit 12 --add-reviewer someone' "Adding/removing reviewers"
expect_block "gh -R o/r api merge" "$repo" 'gh -R o/r api -X PUT repos/o/r/pulls/12/merge' "don't merge"
expect_allow "gh -R o/r pr view" "$repo" 'gh -R o/r pr view 12'

# A GraphQL query computed by a heredoc inside $(…) is read from that heredoc.
expect_block "GraphQL ready in a heredoc query" "$repo" "gh api graphql -f query=\"\$(cat <<'EOF'
mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }
EOF
)\"" "ready-for-review"
expect_allow "a GraphQL read in a heredoc query" "$repo" "gh api graphql -f query=\"\$(cat <<'EOF'
query { viewer { login } }
EOF
)\""

# A shell given options before -c still runs the code after them; the push
# check and the gh check agree on these forms.
expect_block "bash -e -c with a gh pr ready" "$repo" "bash -e -c 'gh pr ready 12'" "ready-for-review"
expect_block "bash -euo pipefail -c with a gh pr ready" "$repo" "bash -euo pipefail -c 'gh pr ready 12'" "ready-for-review"
expect_block "bash -o pipefail -c with a gh pr ready" "$repo" "bash -o pipefail -c 'gh pr ready 12'" "ready-for-review"
expect_block "bash +o posix -c with a gh pr ready" "$repo" "bash +o posix -c 'gh pr ready 12'" "ready-for-review"
expect_block "bash --noprofile --norc -c with an API merge" "$repo" "bash --noprofile --norc -c 'gh api -X PUT repos/o/r/pulls/12/merge'" "don't merge"
expect_block "bash -e -c with a push" "$repo" "bash -e -c 'git -C ../wt-b push'" "can't follow"
expect_block "bash -euo pipefail -c with a push" "$repo" "bash -euo pipefail -c 'git -C ../wt-b push'" "can't follow"
expect_block "bash --noprofile --norc -c with a push" "$repo" "bash --noprofile --norc -c 'git -C ../wt-b push'" "can't follow"
expect_allow "bash --norc running a script, not -c" "$repo" 'bash --norc scripts/pre-push-reviewers.sh'
expect_allow "bash -o pipefail -c with a read" "$repo" "bash -o pipefail -c 'gh pr view 12'"

# The code inside a $(…) is judged as written, its quotes kept.
expect_block "a GraphQL ready mutation captured by \$(…)" "$repo" "resp=\$(gh api graphql -f query='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }')" "ready-for-review"
# shellcheck disable=SC2016 # a literal $(…), for the gate to read
CLAUDE_PROJECT_DIR=$root expect_allow "a draft PR's URL captured by \$(…), two-word title" "$repo" 'url=$(gh pr create --draft --title "fix: add the thing" --body-file b.md)'
# shellcheck disable=SC2016
CLAUDE_PROJECT_DIR=$root expect_allow "a draft PR's URL captured by \$(…), scoped title" "$repo" 'url=$(gh pr create --draft --title "feat(gate): add the thing" --body-file b.md)'
# shellcheck disable=SC2016
CLAUDE_PROJECT_DIR=$root expect_block "a bad title inside \$(…) is still linted" "$repo" 'url=$(gh pr create --draft --title "Bad title." --body-file b.md)' "PR title"
expect_allow "a comment body with an apostrophe inside \$(…)" "$repo" "out=\$(gh pr comment 12 --body \"it's fixed\")"
expect_allow "a gh pr ready only grepped for inside \$(…)" "$repo" "n=\$(grep -c 'gh pr ready' AGENTS.md)"
expect_block "a gh pr ready in a heredoc read by bash inside \$(…)" "$repo" "out=\$(bash <<'EOF'
gh pr ready 12
EOF
)" "ready-for-review"

# gh takes -R/--repo between pr and its subcommand too.
expect_block "gh pr -R o/r ready" "$repo" 'gh pr -R o/r ready 12' "ready-for-review"
expect_block "gh pr --repo o/r create without --draft" "$repo" 'gh pr --repo o/r create --title "fix: x"' "must use --draft"
expect_block "gh pr --repo=o/r review --approve" "$repo" 'gh pr --repo=o/r review 12 --approve' "Only humans approve"
expect_block "gh pr -Ro/r edit --add-reviewer" "$repo" 'gh pr -Ro/r edit 12 --add-reviewer someone' "Adding/removing reviewers"
expect_allow "gh pr -R o/r view" "$repo" 'gh pr -R o/r view 12'

# A quoted gh command left past the 64 pieces is refused too, and a $(…) in a
# ${…} default is code like any other.
expect_block "a quoted gh pr ready past the 64 the gate follows is refused" "$repo" "echo$pad63 \$(bash -c 'gh pr ready 12')" "nests more code"
# shellcheck disable=SC2016 # literal ${…}s, for the gate to read
expect_block "a gh pr ready in a \${x:-\$(…)} default" "$repo" 'echo "${x:-$(gh pr ready 12)}"' "ready-for-review"
# shellcheck disable=SC2016
expect_block "…unquoted" "$repo" 'echo ${x:-$(gh pr ready 12)}' "ready-for-review"
# Expanded as in double quotes, a '…' in a default is literal, so its $(…) runs.
expect_block "a gh pr ready in '…' in a double-quoted \${x:-…} default" "$repo" "echo \"\${x:-'\$(gh pr ready 12)'}\"" "ready-for-review"
expect_block "…in an unquoted heredoc body" "$repo" "cat <<EOF
\${x:-'\$(gh pr ready 12)'}
EOF" "ready-for-review"
expect_block "…a push" "$repo" "echo \"\${x:-'\$(git -C ../wt-b push)'}\"" "can't follow"
expect_allow "…unquoted, where '…' quotes it" "$repo" "echo \${x:-'\$(gh pr ready 12)'}"

# A field read from stdin (-F key=@-, --input -) is read from a heredoc or
# here-string the gh command itself reads.
expect_block "GraphQL ready sent as -F query=@- from a heredoc" "$repo" "gh api graphql -F query=@- <<'EOF'
mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }
EOF" "ready-for-review"
expect_block "…from a here-string" "$repo" "gh api graphql -F query=@- <<< 'mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'" "ready-for-review"
expect_block "GraphQL ready sent through --input - from a heredoc" "$repo" "gh api graphql --input - <<'EOF'
{\"query\": \"mutation { markPullRequestReadyForReview(input: {pullRequestId: \\\"PR_x\\\"}) { clientMutationId } }\"}
EOF" "ready-for-review"
expect_block "an approval sent through --input - from a heredoc" "$repo" "gh api repos/o/r/pulls/12/reviews --input - <<'EOF'
{\"event\": \"APPROVE\"}
EOF" "Only humans approve"
expect_block "a non-draft PR opened through --input -" "$repo" "gh api repos/o/r/pulls --input - <<'EOF'
{\"head\": \"feat\", \"base\": \"main\", \"title\": \"fix: x\"}
EOF" "must be drafts"
CLAUDE_PROJECT_DIR=$root expect_allow "a draft PR opened through --input - with a good title" "$repo" "gh api repos/o/r/pulls --input - <<'EOF'
{\"head\": \"feat\", \"base\": \"main\", \"title\": \"fix: add the thing\", \"draft\": true}
EOF"
expect_allow "a GraphQL read sent as -F query=@-" "$repo" "gh api graphql -F query=@- <<'EOF'
query { viewer { login } }
EOF"
expect_allow "a COMMENT review sent through --input -" "$repo" "gh api repos/o/r/pulls/12/reviews --input - <<'EOF'
{\"event\": \"COMMENT\", \"body\": \"APPROVE is for people\"}
EOF"

# A << inside arithmetic is a shift, not a heredoc whose delimiter swallows
# the lines after it.
# shellcheck disable=SC2016 # literal $((…))s, for the gate to read
expect_block "a << shift in \$((…)), then a gh pr ready" "$repo" 'n=$(( 1 << 3 ))
gh pr ready 12' "ready-for-review"
expect_block "…in a (( … )) command" "$repo" '(( n = 1 << 3 ))
gh pr ready 12' "ready-for-review"
# shellcheck disable=SC2016
expect_block "…in a \$((…)) inside \$(…)" "$repo" 'x=$(echo $(( 1 << 3 )))
gh pr ready 12' "ready-for-review"
# shellcheck disable=SC2016
expect_block "…then a push" "$repo" 'n=$(( 1 << 3 ))
git push origin feat-c' "missing pre-push review marker"
# shellcheck disable=SC2016
expect_block "a gh pr ready in a \$(…) inside \$((…))" "$repo" 'n=$(( $(gh pr ready 12 | wc -l) + 1 ))' "ready-for-review"
expect_allow "a for (( … )) loop with << in it, then a gh read" "$repo" 'for (( i = 1; i < 1 << 3; i <<= 1 )); do :; done
gh pr view 12'
expect_block "a subshell in a process substitution is still a push" "$repo" "cat <((git push origin feat-c))" "missing pre-push review marker"
# The same for bash's older $[ … ] arithmetic.
# shellcheck disable=SC2016 # literal $[…]s, for the gate to read
expect_block "a << shift in \$[…], then a gh pr ready" "$repo" 'x=$[1<<3]
gh pr ready 12' "ready-for-review"
# shellcheck disable=SC2016
expect_block "…in a \$[…] inside \$(…)" "$repo" 'y=$(echo $[1<<3])
gh pr ready 12' "ready-for-review"
# shellcheck disable=SC2016
expect_block "…then a push" "$repo" 'x=$[1<<3]
git push origin feat-c' "missing pre-push review marker"
# shellcheck disable=SC2016
expect_block "a gh pr ready in a \$(…) inside \$[…]" "$repo" 'n=$[ $(gh pr ready 12 | wc -l) + 1 ]' "ready-for-review"
# shellcheck disable=SC2016
expect_block "a \$[…] with an array subscript, then a gh pr ready" "$repo" 'x=$[ a[1] << 2 ]
gh pr ready 12' "ready-for-review"

# The push check follows a heredoc a shell reads inside $(…), as the gh
# checks do.
expect_block "a push in a heredoc read by bash inside \$(…)" "$repo" "out=\$(bash <<'EOF'
git -C ../wt-b push
EOF
)" "can't follow"
expect_block "…piped into sh inside \$(…)" "$repo" "out=\$(cat <<'EOF' | sh
git -C ../wt-b push
EOF
)" "can't follow"

# An unquoted heredoc's body runs its $(…) and `…`; a quoted one's doesn't.
expect_block "a gh pr ready in a \$(…) in an unquoted heredoc body" "$repo" "cat <<EOF
Ready: \$(gh pr ready 12)
EOF" "ready-for-review"
expect_block "…in backticks" "$repo" "cat <<EOF
\`gh pr ready 12\`
EOF" "ready-for-review"
expect_block "…in a heredoc inside \$(…)" "$repo" "msg=\$(cat <<EOF
\$(gh pr ready 12)
EOF
)" "ready-for-review"
expect_block "a push in a \$(…) in an unquoted heredoc body" "$repo" "cat <<EOF
\$(git -C ../wt-b push)
EOF" "can't follow"
expect_block "…in a heredoc inside \$(…)" "$repo" "git commit --allow-empty -m \"\$(cat <<EOF
\$(git -C ../wt-b push)
EOF
)\"" "can't follow"
expect_block "…in a \$(…) that spans lines of the body" "$repo" "cat <<EOF
Ready: \$(
gh pr ready 12
)
EOF" "ready-for-review"
expect_block "…after one that spans lines, on the line where it ends" "$repo" "cat <<EOF
\$(
echo hi
) \$(gh pr ready 12)
EOF" "ready-for-review"
expect_allow "the same in a quoted heredoc is a mention" "$repo" "cat <<'EOF'
\$(gh pr ready 12) and \$(git -C ../wt-b push)
EOF"
expect_allow "an escaped \\\$(…) in an unquoted heredoc body is text" "$repo" "cat <<EOF
\\\$(gh pr ready 12) and \\\$(git -C ../wt-b push)
EOF"
expect_allow "a gh comment whose unquoted heredoc body runs a read" "$repo" "gh pr comment 12 --body-file - <<EOF
Fixed in \$(git rev-parse --short HEAD); run \`make ci\` first.
EOF"

# Inside `…`, a backslash quotes only $, ` and \, so \` nests a substitution.
# shellcheck disable=SC2016 # literal backticks, for the gate to read
expect_block "a gh pr ready in nested escaped backticks" "$repo" 'x=`echo \`gh pr ready 12\``' "ready-for-review"
# shellcheck disable=SC2016
expect_block "…a push" "$repo" 'x=`echo \`git -C ../wt-b push\``' "can't follow"
# shellcheck disable=SC2016
expect_allow "…and an escaped dollar sign is text" "$repo" 'x=`echo \$\(gh pr ready 12\)`'

# A computed query is matched against its own code, not another field's.
expect_allow "a computed query beside a body heredoc that names a mutation" "$repo" "gh api graphql -f query=\"\$(cat q.graphql)\" -f body=\"\$(cat <<'EOF'
markPullRequestReadyForReview is for people
EOF
)\""
expect_block "a query read from a variable is matched against the line that sets it" "$repo" "q='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'; gh api graphql -f query=\"\$q\"" "ready-for-review"
expect_block "…from inside a \$(…)" "$repo" "q='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'; resp=\$(gh api graphql -f query=\"\$q\")" "ready-for-review"
expect_block "…sent as -F query=@- from a here-string" "$repo" "q='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'; gh api graphql -F query=@- <<< \"\$q\"" "ready-for-review"
expect_block "…from an unquoted heredoc" "$repo" "q='mutation { markPullRequestReadyForReview(input: {pullRequestId: \"PR_x\"}) { clientMutationId } }'
gh api graphql -F query=@- <<EOF
\$q
EOF" "ready-for-review"
expect_block "…a GraphQL body sent through --input - from a here-string" "$repo" "b='{\"query\": \"mutation { markPullRequestReadyForReview(input: {pullRequestId: \\\"PR_x\\\"}) { clientMutationId } }\"}'; gh api graphql --input - <<< \"\$b\"" "ready-for-review"
# A GraphQL variable the query declares (\$id: ID!) isn't a shell variable.
expect_allow "a query with GraphQL variables beside a body heredoc that names a mutation" "$repo" "gh api graphql -f query=\"\$(cat <<'EOF'
mutation(\$id: ID!) { resolveReviewThread(input: {threadId: \$id}) { thread { id } } }
EOF
)\" -f id=PRRT_x -f body=\"\$(cat <<'EOF'
markPullRequestReadyForReview is for people
EOF
)\""
expect_allow "…sent from an unquoted heredoc, beside a variable that names one" "$repo" "note='markPullRequestReadyForReview is for people'
gh api graphql -F query=@- <<EOF
query(\\\$n: Int!) { viewer { repositories(first: \\\$n) { nodes { name } } } }
EOF"
# A quoted heredoc is static: a \$ in it is text, not a shell variable.
expect_allow "a quoted heredoc query with a \$ in a string, beside a variable that names a mutation" "$repo" "note='markPullRequestReadyForReview is for people'
gh api graphql -F query=@- <<'EOF'
query { repository(owner: \"o\", name: \"\$HOME\") { id } }
EOF"
expect_block "a shell variable beside GraphQL variables in an unquoted heredoc query" "$repo" "m=markPullRequestReadyForReview; gh api graphql -f query=\"\$(cat <<EOF
mutation(\\\$id: ID!) { \$m(input: {pullRequestId: \\\$id}) { clientMutationId } }
EOF
)\" -f id=PR_x" "ready-for-review"

# A review's event is APPROVE in the query or a field's whole value, not a word
# in a body, and the other addPullRequestReview… mutations take none.
expect_allow "a GraphQL thread reply whose body says APPROVE" "$repo" "gh api graphql -f query='mutation { addPullRequestReviewThreadReply(input: {pullRequestReviewThreadId: \"PRRT_x\", body: \"APPROVE once CI is green\"}) { comment { id } } }'"
expect_allow "…sent from stdin" "$repo" "gh api graphql -f query='mutation(\$t: ID!, \$b: String!) { addPullRequestReviewThreadReply(input: {pullRequestReviewThreadId: \$t, body: \$b}) { comment { id } } }' -f t=PRRT_x -F b=@- <<'EOF'
I would APPROVE this once CI is green.
EOF"
expect_allow "a GraphQL COMMENT review whose body from stdin says APPROVE" "$repo" "gh api graphql -f query='mutation(\$p: ID!, \$b: String!) { addPullRequestReview(input: {pullRequestId: \$p, event: COMMENT, body: \$b}) { pullRequestReview { id } } }' -f p=PR_x -F b=@- <<'EOF'
I would APPROVE this once CI is green.
EOF"
expect_block "a GraphQL approval with the event in a variable" "$repo" "gh api graphql -f query='mutation(\$e: PullRequestReviewEvent!) { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: \$e}) { clientMutationId } }' -f e=APPROVE" "Only humans approve"
expect_block "…changes requested, the event read from stdin" "$repo" "gh api graphql -f query='mutation(\$r: ID!, \$e: PullRequestReviewEvent!) { submitPullRequestReview(input: {pullRequestReviewId: \$r, event: \$e}) { clientMutationId } }' -f r=PRR_x -F e=@- <<< REQUEST_CHANGES" "instead of --request-changes"
# An event the shell computes can't be read, so it's refused; a literal one
# beside a computed body isn't.
expect_block "a GraphQL review with the event variable's value computed" "$repo" "ev=APPROVE; gh api graphql -f query='mutation(\$e: PullRequestReviewEvent!) { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: \$e}) { clientMutationId } }' -f e=\"\$ev\"" "can't be checked"
expect_block "…the value read from a here-string the shell expands" "$repo" "gh api graphql -f query='mutation(\$e: PullRequestReviewEvent!) { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: \$e}) { clientMutationId } }' -F e=@- <<< \"\$ev\"" "can't be checked"
expect_allow "a GraphQL COMMENT review in a variable, beside a computed body" "$repo" "gh api graphql -f query='mutation(\$e: PullRequestReviewEvent!, \$b: String!) { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: \$e, body: \$b}) { clientMutationId } }' -f e=COMMENT -f b=\"\$(cat notes.md)\""
expect_allow "a GraphQL review with a literal event, beside a computed body" "$repo" "gh api graphql -f query='mutation(\$b: String!) { addPullRequestReview(input: {pullRequestId: \"PR_x\", event: COMMENT, body: \$b}) { clientMutationId } }' -f b=\"\$(cat notes.md)\""
expect_block "a REST review with a computed event" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews -f event=\"\$ev\" -f body='looks fine'" "can't be checked"
expect_allow "a REST COMMENT review with a computed body" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews -f event=COMMENT -f body=\"\$(cat notes.md)\""
expect_block "a GraphQL review with a computed input[event] field" "$repo" "gh api graphql -f query='mutation(\$input: AddPullRequestReviewInput!) { addPullRequestReview(input: \$input) { clientMutationId } }' -f 'input[pullRequestId]=PR_x' -f \"input[event]=\$ev\"" "can't be checked"
expect_block "a GraphQL review payload built by jq, sent through --input -" "$repo" "payload=\$(jq -n --arg e \"\$ev\" '{query: \"mutation(\$e: PullRequestReviewEvent!) { addPullRequestReview(input: {pullRequestId: \\\"PR_x\\\", event: \$e}) { clientMutationId } }\", variables: {e: \$e}}'); gh api graphql --input - <<< \"\$payload\"" "can't be checked"
# A --input - body the shell expands, or that is JSON only once it has, is read
# from its text.
expect_block "a REST review whose expanded --input - body computes the event" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews --input - <<EOF
{\"event\": \"\$ev\", \"body\": \"looks fine\"}
EOF" "can't be checked"
expect_block "…an approval whose body is a \$(jq -Rs …)" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews --input - <<EOF
{\"event\": \"APPROVE\", \"body\": \$(jq -Rs . notes.md)}
EOF" "Only humans approve"
expect_allow "…a COMMENT review whose body is a \$(jq -Rs …)" "$repo" "gh api -X POST repos/o/r/pulls/12/reviews --input - <<EOF
{\"event\": \"COMMENT\", \"body\": \$(jq -Rs . notes.md)}
EOF"
CLAUDE_PROJECT_DIR=$root expect_allow "a draft PR whose expanded --input - body is a \$(jq -Rs …)" "$repo" "gh api -X POST repos/o/r/pulls --input - <<EOF
{\"title\": \"fix(ingest): drop duplicate events\", \"head\": \"b\", \"base\": \"main\", \"draft\": true, \"body\": \$(jq -Rs . body.md)}
EOF"
CLAUDE_PROJECT_DIR=$root expect_block "…with a title the lint refuses" "$repo" "gh api -X POST repos/o/r/pulls --input - <<EOF
{\"title\": \"Drop duplicate events.\", \"head\": \"b\", \"base\": \"main\", \"draft\": true, \"body\": \$(jq -Rs . body.md)}
EOF" "PR title"
expect_block "a GraphQL review whose expanded --input - body computes the event" "$repo" "gh api graphql --input - <<EOF
{\"query\": \"mutation(\\\$input: AddPullRequestReviewInput!) { addPullRequestReview(input: \\\$input) { clientMutationId } }\", \"variables\": {\"input\": {\"pullRequestId\": \"PR_x\", \"event\": \"\$ev\"}}}
EOF" "can't be checked"
expect_allow "…a COMMENT review whose body is a \$(jq -Rs …)" "$repo" "gh api graphql --input - <<EOF
{\"query\": \"mutation(\\\$input: AddPullRequestReviewInput!) { addPullRequestReview(input: \\\$input) { clientMutationId } }\", \"variables\": {\"input\": {\"pullRequestId\": \"PR_x\", \"event\": \"COMMENT\", \"body\": \$(jq -Rs . notes.md)}}}
EOF"
expect_block "…not a draft" "$repo" "gh api -X POST repos/o/r/pulls --input - <<EOF
{\"title\": \"fix(ingest): drop duplicate events\", \"head\": \"b\", \"base\": \"main\", \"body\": \$(jq -Rs . body.md)}
EOF" "must be drafts"

# Flags on gh pr are read as gh's parser reads them: a short-flag cluster
# letter by letter, and a flag's value never as a flag.
for c in create new; do
  expect_block "gh pr $c -fr x (fill, then a reviewer)" "$repo" "gh pr $c --draft -fr x" "Adding/removing reviewers"
  expect_block "gh pr $c -fa x (fill, then an assignee)" "$repo" "gh pr $c --draft -fa x" "Adding/removing reviewers"
done
expect_block "gh pr create -dfrx" "$repo" 'gh pr create -dfrx' "Adding/removing reviewers"
expect_allow "gh pr create -df (draft in a cluster)" "$repo" 'gh pr create -df'
CLAUDE_PROJECT_DIR=$root expect_block "a title given in a cluster is linted" "$repo" 'gh pr create -dt "Bad title."' "PR title"
expect_allow "a create body that starts with -a" "$repo" 'gh pr create --draft --title "feat: x" -b "-a thing"'
expect_allow "…a --body that starts with -r" "$repo" 'gh pr create --draft --title "feat: x" --body "-r x"'
expect_allow "a title that starts with -r is a title" "$repo" 'gh pr create --draft -t "-rx"'
expect_allow "a review body that says --approve" "$repo" 'gh pr review 12 --comment -b "--approve"'
expect_block "a review's -a in a cluster (-ab: approve, with a body)" "$repo" 'gh pr review 12 -ab "lgtm"' "Only humans approve"

# A heredoc opened inside a $(…) in an unquoted heredoc body ends inside that
# text: its delimiter must not stay pending for the rest of the command.
expect_block "a gh pr ready after a body whose \$(…) opens a heredoc, closing on its line" "$repo" "cat <<EOF
\$(cat <<X)
EOF
echo hi
gh pr ready 12" "ready-for-review"
expect_block "a push after the same body" "$repo" "cat <<EOF
\$(cat <<X)
EOF
echo hi
git push origin feat-c" "missing pre-push review marker"
expect_allow "a valid nested heredoc spanning lines in an unquoted body" "$repo" "cat <<EOF
\$(cat <<X
inner
X
)
EOF
echo hi
git push origin feat-a"
expect_block "…and a gh pr ready after it" "$repo" "cat <<EOF
\$(cat <<X
inner
X
)
EOF
gh pr ready 12" "ready-for-review"

# A large body is judged quickly under the slowest bash: 3.2's ${cmd//…/} was
# slow on a 60 KB command, and a hook timeout is non-blocking. Realistic text:
# this repo's own changelog.
big=""
while IFS= read -r l && [ "${#big}" -lt 60000 ]; do big+="$l"$'\n'; done <"$root/CHANGELOG.md"
timed() { # <name> <allow|block> <cwd> <command>
  local t0=$SECONDS took
  if [ "$2" = allow ]; then expect_allow "$1" "$3" "$4"; else expect_block "$1" "$3" "$4"; fi
  took=$((SECONDS - t0))
  if [ "$took" -ge 10 ]; then fail "$1: took ${took}s, over the 10s budget"; else echo "      (${#4} bytes in ${took}s)"; fi
}
timed "a 60 KB comment body from a heredoc" allow "$wtb" "gh pr comment 1 --body-file - <<'EOF'
${big}EOF"
timed "a 60 KB file heredoc, then a push from the reviewed worktree" allow "$repo" "cat > f <<'EOF'
${big}EOF
git push origin feat-a"
timed "a 60 KB file heredoc, then a push of the unreviewed branch" block "$repo" "cat > f <<'EOF'
${big}EOF
git push origin feat-c"
timed "a 60 KB --body from a heredoc inside \$(…)" allow "$wtb" "gh pr comment 1 --body \"\$(cat <<'EOF'
${big}EOF
)\""

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "review-gate: all cases passed"

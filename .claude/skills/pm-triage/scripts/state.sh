#!/usr/bin/env bash
# Local, private, cross-worktree state for the /pm-triage routine.
#
# State lives on a dedicated ORPHAN branch ("pm-triage-state") that shares NO history with
# main (its own thing), checked out in its own worktree at <main>/.worktrees/pm-triage-state.
# The skill does plain file I/O there and commits after meaningful runs, so you get a local
# audit trail ("see when it changed things"). The branch is LOCAL ONLY — never pushed by
# default, so teammates can't see it; flip publishing on later for cross-machine sync.
#
# No branch-switching ever happens in your working trees: the state worktree is a separate
# path, and the skill reads code/TODOs from origin/main by ref (git grep / git diff). The
# worktree path derives from the shared git common dir, so it's identical from every worktree.
#
# Files in the state worktree:
#   state.json   {schema,last_run_at,last_sha,filed:{<fingerprint>:<issue#>}}
#   pending.md   risky proposals awaiting a human decision (human-editable)
#   runs.log     append-only, one line per run
#
# Subcommands:
#   ensure          create the orphan branch + worktree if absent; echo the worktree path
#   path            echo the worktree path (empty if not set up yet)
#   read            echo state.json (or {} if absent / not set up)
#   write           read JSON from stdin, validate, atomically save as state.json
#   commit "<msg>"  stage all state files + commit in the worktree if anything changed
set -uo pipefail

BRANCH="${WH_PM_STATE_BRANCH:-pm-triage-state}"
# Dedicated identity for state commits: clearly attributable, never fails on a missing
# global git identity (matters for unattended routine runs). Local-only, so harmless.
AUTHOR_NAME="${WH_PM_STATE_AUTHOR:-pm-triage routine}"
AUTHOR_EMAIL="${WH_PM_STATE_EMAIL:-pm-triage@local}"
asbot=(-c "user.name=${AUTHOR_NAME}" -c "user.email=${AUTHOR_EMAIL}")

_main_root() {
  local common
  common="$(git rev-parse --git-common-dir 2>/dev/null)" || return 1
  [[ -n "$common" ]] || return 1
  common="$(cd "$common" && pwd)" || return 1   # normalise relative ".git" in the main worktree
  dirname "$common"                              # parent of <main>/.git  ==  <main>
}
_wt_path() { local r; r="$(_main_root)" || return 1; printf '%s\n' "$r/.worktrees/${BRANCH}"; }

# True iff $1 is the root of a real, resolvable linked worktree — NOT a stray dir, a dangling
# gitfile, or main's repo reached by walking UP (`$wt` is nested inside main, so a stray dir or
# empty `.git` there makes `--is-inside-work-tree` resolve to main). `--show-toplevel` of those
# resolves to main or errors; only a genuine worktree reports its own root. `-ef` (inode) is
# robust to any path-spelling difference between $1 and git's normalised toplevel.
_is_state_wt() {
  local top
  top="$(git -C "$1" rev-parse --show-toplevel 2>/dev/null)" || return 1
  [[ -n "$top" && "$top" -ef "$1" ]]
}

cmd="${1:-}"; shift || true
case "$cmd" in
  ensure)
    root="$(_main_root)" || { echo "not inside a git repo" >&2; exit 1; }
    wt="$root/.worktrees/${BRANCH}"
    # Already a healthy worktree rooted here? Return it. `_is_state_wt` asserts the resolved
    # toplevel IS $wt, so a dangling gitfile (move/re-clone) and a stray dir or empty `.git`
    # (which would otherwise resolve UP to the enclosing main worktree, risking add/commit
    # against MAIN) both fall through to the self-heal / stray-dir guard below.
    if _is_state_wt "$wt"; then printf '%s\n' "$wt"; exit 0; fi
    # Self-heal: if the worktree dir was deleted out from under us (manual rm -rf, a fresh
    # clone that didn't carry worktrees), the branch + a stale registration can survive and
    # `worktree add` then fails with "missing but already registered" — wedging the daily
    # routine until a human intervenes. `prune` clears ONLY provably-missing registrations
    # (healthy worktrees untouched), so we recover automatically.
    git -C "$root" worktree prune 2>/dev/null || true
    # A non-worktree directory squatting the target path can't be auto-resolved — fail loud
    # with an actionable message instead of git's raw "already exists" fatal.
    if [[ -e "$wt" ]]; then
      echo "state path '$wt' exists but isn't a valid worktree; move/remove it, then retry" >&2
      exit 1
    fi
    # Create the orphan branch (empty root commit, no parent) via plumbing so NO working
    # tree is touched. An existing branch (e.g. after a worktree-only loss) is reused, so its
    # committed state is preserved. Then check it out in its own worktree.
    if ! git -C "$root" show-ref --verify --quiet "refs/heads/${BRANCH}"; then
      empty_tree="$(git -C "$root" mktree </dev/null)" || { echo "mktree failed" >&2; exit 1; }
      root_commit="$(git -C "$root" "${asbot[@]}" commit-tree "$empty_tree" \
        -m "pm-triage state: init (orphan, local-only)")" || { echo "commit-tree failed" >&2; exit 1; }
      git -C "$root" branch "${BRANCH}" "$root_commit" || { echo "branch create failed" >&2; exit 1; }
    fi
    git -C "$root" worktree add "$wt" "${BRANCH}" >&2 || { echo "worktree add failed" >&2; exit 1; }
    printf '%s\n' "$wt"
    ;;
  path)
    wt="$(_wt_path)" || exit 1
    _is_state_wt "$wt" && printf '%s\n' "$wt" || printf '\n'
    ;;
  read)
    wt="$(_wt_path)" || exit 1
    if [[ -f "$wt/state.json" ]]; then cat "$wt/state.json"; else echo "{}"; fi
    ;;
  write)
    wt="$(_wt_path)" || exit 1
    _is_state_wt "$wt" || { echo "state worktree missing/invalid; run: state.sh ensure" >&2; exit 1; }
    tmp="$wt/.state.json.tmp"
    # `jq .` exits 0 on EMPTY stdin (0-byte file → would wipe state.json) and on a multi-value
    # stream — both silent corruption. Require exactly one non-empty JSON value before the mv.
    if jq . >"$tmp" 2>/dev/null && [[ "$(jq -s 'length' "$tmp" 2>/dev/null)" == "1" ]]; then
      mv "$tmp" "$wt/state.json"
    else
      rm -f "$tmp"; echo "stdin must be exactly one non-empty JSON value" >&2; exit 2
    fi
    ;;
  commit)
    msg="${1:-pm-triage: state update}"
    wt="$(_wt_path)" || exit 1
    _is_state_wt "$wt" || { echo "state worktree missing/invalid; run: state.sh ensure" >&2; exit 1; }
    git -C "$wt" add -A || { echo "git add failed in $wt" >&2; exit 1; }
    if git -C "$wt" diff --cached --quiet; then
      echo "nothing to commit"
    else
      git -C "$wt" "${asbot[@]}" commit -q -m "$msg" && git -C "$wt" rev-parse --short HEAD
    fi
    ;;
  *) echo "usage: state.sh {ensure|path|read|write|commit \"<msg>\"}" >&2; exit 2 ;;
esac

#!/usr/bin/env bash
# Agent PR workflow gate (PreToolUse Bash). Catches ACCIDENTAL violations of
# rules that have no human analog, with plain text checks over the whole command
# string (no shell parsing, so a command that only mentions a guarded action in
# a body or message can be blocked too; put long text in a file instead:
# --body-file, git commit -F <file>, gh api -F body=@file):
#   - gh pr create / new without --draft, or with a title the lint rejects
#   - adding reviewers or assignees; gh pr review --approve / --request-changes
#   - the gh api forms of ready, merge, approve, request-changes, reviewer
#     writes and non-draft PR creation (REST and GraphQL)
#   - git push of a commit missing any pre-push review marker (one per reviewer
#     in scripts/pre-push-reviewers.sh), when run as a plain `git push`
# gh pr merge, gh pr ready, gh pr review --approve and force-push are also
# denied by prefix in .claude/settings.json. Universal git checks live in
# .githooks/. Only plain forms are caught; any other form is still against the
# rules. See AGENTS.md §"Agent PR Discipline".

set -uo pipefail

block() {
  cat >&2 <<EOF

🛑 Claude PR discipline gate: $1

See AGENTS.md §"Agent PR Discipline".
EOF
  exit 2
}

if ! command -v jq >/dev/null 2>&1; then
  block "jq is required for this gate. Install jq or remove the PreToolUse hook."
fi

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // ""' 2>/dev/null) \
  || block "Could not parse hook payload as JSON."
[ -z "$cmd" ] && exit 0

# The gh checks resolve repo scripts from the session's checkout; the push
# check resolves everything from the worktree the push runs in.
project_dir="${CLAUDE_PROJECT_DIR:-.}"
reviewers_script="scripts/pre-push-reviewers.sh"

# lint_title <title>: validate a PR title with scripts/lint-pr-title.sh, the rule
# the required `CI` check's `PR title` job enforces. Fail-open: with no title to
# read or no validator, fall through to the CI check.
lint_title() {
  local reason
  [ -n "$1" ] && [ -x "$project_dir/scripts/lint-pr-title.sh" ] || return 0
  reason=$("$project_dir/scripts/lint-pr-title.sh" "$1" 2>&1) || block "$reason"
}

msg_draft="Agent-opened PRs must use --draft. Only humans publish ready-for-review PRs."
msg_ready="Only humans flip drafts to ready-for-review. Ask the user."
msg_reviewers="Adding/removing reviewers is humans-only. Re-trigger bot reviewers via PR comment mention (e.g. @coderabbitai review)."
msg_reviewers_api="Reviewer-write requests are humans-only. Re-trigger bot reviewers via PR comment mention."
msg_approve="Only humans approve PRs."
msg_changes="Agents post inline review comments instead of --request-changes."
merge_msg="Agents don't merge PRs; a person does. Ask the user."
msg_draft_api="Agent-opened PRs must be drafts: send draft=true (REST) or draft: true (GraphQL)."
msg_plain_push="run \`git push\` as a plain command on its own — \`git [-C <worktree>] push [<remote>] [<refspec>…]\`, optionally piped to a filter. Commit, cd and other commands go in separate tool calls."

# ── gh ──
# Every pattern is tested against the whole command, quoted or not.

# has <regex>: the command matches the (ERE) regex. both <re1> <re2>: some single
# line matches both. A pipe into >/dev/null, not grep -q, so pipefail can't see
# a SIGPIPE.
has() { [[ $cmd =~ $1 ]]; }
both() { printf '%s\n' "$cmd" | grep -E -e "$1" | grep -E -e "$2" >/dev/null; }

# lint_literal_title <regex>…: lint the first literal title any regex finds (its
# last capture group). Fail-open when none matches.
lint_literal_title() {
  local re
  for re in "$@"; do
    if [[ $cmd =~ $re ]]; then lint_title "${BASH_REMATCH[${#BASH_REMATCH[@]} - 1]}"; return 0; fi
  done
}

gh_re='(^|[^[:alnum:]_./-])gh[[:space:]]+((-R|--repo)(=|[[:space:]]*)[^[:space:]]+[[:space:]]+)*'
end='([[:space:]]|$)'
if has "${gh_re}(pr|api)${end}"; then
  create_re="${gh_re}pr[[:space:]]+(create|new)${end}"
  api_re="${gh_re}api${end}"
  word='(^|[^[:alnum:]_])'
  wend='($|[^[:alnum:]_])'

  if has "$create_re"; then
    has "(^|[[:space:]])(--draft(=(true|1|t|T|TRUE|True))?|-d)${end}" || block "$msg_draft"
    both "$create_re" '(^|[[:space:]])--(reviewer|assignee)([=[:space:]]|$)' && block "$msg_reviewers"
  fi
  if has "${gh_re}pr[[:space:]]+(create|new|edit)${end}"; then
    lint_literal_title '(^|[[:space:]])(--title|-t)[[:space:]]+"([^"]*)"' "(^|[[:space:]])(--title|-t)[[:space:]]+'([^']*)'" \
      '(^|[[:space:]])--title="([^"]*)"' "(^|[[:space:]])--title='([^']*)'" '(^|[[:space:]])--title=([^[:space:]"'"'"']+)'
  fi
  has '(^|[[:space:]])--(add|remove)-(reviewer|assignee)([=[:space:]]|$)' && block "$msg_reviewers"
  has "${gh_re}pr[[:space:]]+ready${end}" && block "$msg_ready"
  if has "${gh_re}pr[[:space:]]+review${end}"; then
    has '(^|[[:space:]])--approve([^[:alnum:]_-]|$)' && block "$msg_approve"
    has '(^|[[:space:]])--request-changes([^[:alnum:]_-]|$)' && block "$msg_changes"
  fi

  # The mutations and endpoints that are humans-only, wherever they appear.
  has 'markPullRequestReadyForReview' && block "$msg_ready"
  has 'mergePullRequest|enablePullRequestAutoMerge|pulls/[0-9]+/merge' && block "$merge_msg"
  has 'requestReviews|requested_reviewers' && block "$msg_reviewers_api"

  # gh api: the rest is judged on the line that carries the call.
  if has "$api_re"; then
    api_lines=$(printf '%s\n' "$cmd" | grep -E -e "$api_re")
    both "$api_re" "${word}APPROVE${wend}.*(reviews|PullRequestReview)|(reviews|PullRequestReview).*${word}APPROVE${wend}" && block "$msg_approve"
    both "$api_re" "${word}REQUEST_CHANGES${wend}.*(reviews|PullRequestReview)|(reviews|PullRequestReview).*${word}REQUEST_CHANGES${wend}" && block "$msg_changes"
    if [[ $api_lines == *createPullRequest* ]] && ! [[ $api_lines =~ draft:[[:space:]]*true ]]; then
      block "$msg_draft_api"
    fi
    # REST create: a repos/…/pulls endpoint with nothing after it, sending fields.
    if [[ $api_lines =~ (^|[[:space:]\"\'/])repos/[^[:space:]\"\'/]+/[^[:space:]\"\'/]+/pulls[\"\']?${end} ]] \
      && [[ $api_lines =~ (^|[[:space:]])(-f|-F|--field|--raw-field|--input)([=[:space:]]|$) ]] \
      && ! [[ $api_lines =~ (-X|--method)[=[:space:]]*GET ]]; then
      [[ $api_lines =~ draft=true|\"draft\":[[:space:]]*true ]] || block "$msg_draft_api"
      lint_literal_title "[\"']title=([^\"']*)[\"']" 'title="([^"]*)"' "title='([^']*)'" 'title=([^[:space:]"'"'"']+)'
    fi
  fi
fi

# ── git push: the commit each refspec publishes needs a review marker ──
# Only a plain `git [-C <path>] push …` on its own is judged (below), so the
# worktree is known without following cd or wrappers, and nothing earlier in the
# command can move HEAD. The commit each refspec publishes (HEAD when none)
# needs a marker from EVERY reviewer in scripts/pre-push-reviewers.sh, from any
# worktree of the repository. A commit already on the base (origin/main, else
# main) has nothing to review.

# gate_commit <worktree> <label> <sha>: block unless every reviewer has a marker
# for <sha> in some worktree of the repository.
gate_commit() {
  local top=$1 label=$2 sha=$3 base="" ref branch reviewers_list worktrees r wt found missing="" shown=0
  for ref in origin/main main; do
    if git -C "$top" rev-parse --verify --quiet "$ref" >/dev/null 2>&1; then base=$ref; break; fi
  done
  # No base resolvable → fail safe and gate.
  if [ -n "$base" ] && [ -z "$(git -C "$top" rev-list "${base}..${sha}" 2>/dev/null)" ]; then
    return 0
  fi

  # If the reviewer list is missing, unreadable, empty, or corrupt (a syntax
  # error or conflict markers make `bash` error out to empty stdout), we can't
  # know what to require, so fail CLOSED. Each name is validated for
  # filename-safety before it builds a marker path.
  reviewers_list=$(bash "$top/$reviewers_script" 2>/dev/null | grep -E '^[A-Za-z0-9._-]+$')
  if [ -z "$reviewers_list" ]; then
    block "reviewer list '$reviewers_script' in ${top} produced no valid reviewer names (missing, unreadable, empty, or corrupt) — can't tell which reviews gate this push. Fix it before pushing."
  fi

  worktrees=$(git -C "$top" worktree list --porcelain 2>/dev/null | sed -n 's/^worktree //p')
  for r in $reviewers_list; do
    found=0
    while IFS= read -r wt; do
      [ -n "$wt" ] && [ -f "$wt/tmp/${r}-passed-${sha}" ] && { found=1; break; }
    done <<<"$worktrees"
    [ "$found" = 1 ] || missing="${missing}  - ${r} -> tmp/${r}-passed-${sha:0:8}
"
  done

  if [ -n "$missing" ]; then
    branch=""
    if [ "$label" = HEAD ] && ref=$(git -C "$top" symbolic-ref --short -q HEAD 2>/dev/null); then
      branch=" (branch '$ref')"
    fi
    {
      echo
      echo "🛑 Claude PR discipline gate: missing pre-push review marker(s) for ${label}${branch} at ${sha:0:8}, pushed from ${top}:"
      echo
      printf '%s' "$missing"
      # Lines start with an ISO timestamp, so a stable sort on it merges every
      # worktree's log.
      decisions=$(while IFS= read -r wt; do
        [ -n "$wt" ] && [ -f "$wt/tmp/review-marker.log" ] && cat "$wt/tmp/review-marker.log"
      done <<<"$worktrees" | sort -s -k1,1 | tail -n 4)
      if [ -n "$decisions" ]; then
        echo
        echo "Latest review-marker decisions (tmp/review-marker.log, all worktrees):"
        printf '%s\n' "$decisions" | sed 's/^/    /'
      fi
      cat <<EOF

Run /prepush in that worktree — it reads ${reviewers_script}, runs the reviewers
this change needs in parallel (fresh context), and skips the rest on the record.
Each reviewer it runs must reach VERDICT: ship_it (zero findings) on a report
that names the commit it read (REVIEWED: <sha>) to write its marker, and HEAD
must not move while it runs; a skipped reviewer gets a logged marker via
scripts/skip-pre-push-review.sh. The push succeeds once every listed reviewer
has a marker for the pushed commit — see AGENTS.md §"Agent PR Discipline".
A session started before the reviewers learned the REVIEWED: line still runs
their old definitions (they load at session start): restart it, or ask each
reviewer in its prompt to end with REVIEWED: <the sha it pinned first>.
EOF
    } >&2
    exit 2
  fi

  # All required markers present. Surface any reviewers that were skipped by
  # judgment (recorded by scripts/skip-pre-push-review.sh) so the skip is
  # visible at push time, not just in the /prepush conversation. Advisory.
  while IFS= read -r wt; do
    [ -n "$wt" ] && [ -f "$wt/tmp/review-skips-${sha}.log" ] || continue
    [ "$shown" = 1 ] || echo "ℹ️  pre-push: reviewer(s) skipped by judgment for ${sha:0:8}:" >&2
    shown=1
    sed 's/^/    ⏭️  /' "$wt/tmp/review-skips-${sha}.log" >&2
  done <<<"$worktrees"
  return 0
}

# gate_push: cmd is a plain push (checked below); resolve what it publishes.
gate_push() {
  local dir=$1 j=$2 w opts=1 remote=0 delete=0 spec src sha top
  local -a specs=()
  while [ "$j" -lt "$nw" ]; do
    w=${words[j]}
    if [ "$opts" = 1 ]; then
      case $w in
        -h | --help) return 0 ;;
        --) opts=0; j=$((j + 1)); continue ;;
        --all | --branches | --mirror)
          block "\`git push $w\` publishes every branch at once, so the gate can't check each one's review. Push one branch at a time." ;;
        -d | --delete) delete=1; j=$((j + 1)); continue ;;
        --repo | -o | --push-option | --receive-pack | --exec) j=$((j + 2)); continue ;;
        -f | --force | --force-with-lease | --force-with-lease=* | --force-if-includes)
          block "Force-push is denied here. Push normally; ask the user if history must be rewritten." ;;
        --*) j=$((j + 1)); continue ;;
        -*f*) block "Force-push is denied here (combined short flags with -f). Push normally." ;;
        -*) j=$((j + 1)); continue ;;
      esac
    fi
    if [ "$remote" = 0 ]; then
      remote=1
    else
      if [ "$w" = tag ] && [ $((j + 1)) -lt "$nw" ]; then j=$((j + 1)); w="refs/tags/${words[j]}"; fi
      specs+=("$w")
    fi
    j=$((j + 1))
  done
  [ "$delete" = 1 ] && return 0

  top=$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null) \
    || block "can't tell which repository this \`git push\` runs in: '${dir}' is not inside a git worktree."

  # Another repository with no reviewer manifest isn't under this policy. The
  # session's own repository always is, so a missing manifest there blocks.
  if [ ! -f "$top/$reviewers_script" ] \
    && [ "$(git -C "$top" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" \
      != "$(git -C "$project_dir" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" ]; then
    return 0
  fi

  [ "${#specs[@]}" -gt 0 ] || specs=(HEAD)
  for spec in "${specs[@]}"; do
    spec=${spec#+}
    [ "$spec" = : ] && block "the refspec ':' pushes every branch that exists on both sides, so the gate can't check each one's review. Push one branch at a time."
    src=${spec%%:*}
    [ -z "$src" ] && continue # ":<dst>" deletes a remote ref; nothing is published
    case $src in *'*'*) block "the glob refspec '${spec}' publishes several refs at once, so the gate can't check each one's review. Push one branch at a time." ;; esac
    sha=$(git -C "$top" rev-parse --verify --quiet "${src}^{commit}" 2>/dev/null) \
      || block "can't resolve '${src}' to a commit in ${top}, so can't tell what this push publishes."
    gate_commit "$top" "$src" "$sha"
  done
}

# A command that mentions `git [global options] push` must BE a plain push:
# `git [-C <path>] push [args…]`, one line, optionally `2>&1` and one `| filter`.
push_re='(^|[^[:alnum:]_./-])git([[:space:]]+(-C[[:space:]]+[^[:space:]]+|-c[[:space:]]+[^[:space:]]+|-[a-zA-Z]|--[a-z-]+(=[^[:space:]]+)?))*[[:space:]]+push([^[:alnum:]_-]|$)'
if has "$push_re"; then
  t=$cmd filter=""
  if [[ $t == *'|'* ]]; then filter=${t#*|}; t=${t%%|*}; fi
  t=${t#"${t%%[![:space:]]*}"}; t=${t%"${t##*[![:space:]]}"}
  case $t in *' 2>&1') t=${t% 2>&1} ;; esac
  # shellcheck disable=SC2016 # a literal $( and backtick
  if [[ $filter == *'|'* || $filter == *';'* || $filter == *'&'* || $filter == *'$('* || $filter == *'`'* || $filter == *$'\n'* \
    || $filter =~ (^|[^[:alnum:]_])(git|gh)($|[^[:alnum:]_]) ]]; then
    block "$msg_plain_push"
  fi
  read -ra words <<<"$t"
  nw=${#words[@]}
  [[ $t == *$'\n'* ]] && block "$msg_plain_push"
  for w in "${words[@]}"; do
    [[ $w =~ ^[A-Za-z0-9._/:+@^~=,-]+$ ]] || block "$msg_plain_push"
  done
  first=1
  wdir=""
  if [ "${words[0]}" = git ] && [ "${words[1]:-}" = -C ] && [ "${words[3]:-}" = push ]; then
    wdir=${words[2]}; first=4
  elif [ "${words[0]}" = git ] && [ "${words[1]:-}" = push ]; then
    first=2
  else
    block "$msg_plain_push"
  fi
  hook_cwd=$(printf '%s' "$input" | jq -r '.cwd // empty' 2>/dev/null)
  [ -n "$hook_cwd" ] || block "can't tell which directory this \`git push\` runs in: the hook payload has no cwd."
  case $wdir in "") wdir=$hook_cwd ;; /*) ;; *) wdir="$hook_cwd/$wdir" ;; esac
  gate_push "$wdir" "$first"
fi

exit 0

#!/usr/bin/env bash
# Agent PR workflow gate (PreToolUse Bash). Catches accidental violations of
# rules that have no human analog:
#   - gh pr create (or its alias gh pr new) without --draft, or with a title
#     the PR-title lint rejects
#   - gh pr ready
#   - gh pr edit --add-reviewer / --add-assignee, and gh pr create / new
#     --reviewer / --assignee (-r / -a)
#   - gh pr review --approve / --request-changes
#   - the gh api forms of these, REST and GraphQL, and of a merge
#   - git push of a commit missing any pre-push review marker
#     (one per reviewer in scripts/pre-push-reviewers.sh — code, docs, …)
#
# Universal git checks (ci-passed marker, no-verify, etc.) live in .githooks/
# and apply to humans and agents equally. Bypass surface acknowledged: an
# agent that wants to bypass can edit this file or settings.json — these
# rules prevent accidental violations, not adversarial ones. See AGENTS.md
# §"Agent PR Discipline" for policy.

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

# ── gh: the PR actions that are humans-only, and the PR title ────────────────
#
# gate_gh runs from the bottom of this file, once the tokenizer below exists.
# Like the push check it reads the commands the line runs, so a gh command that
# is only mentioned, in a quoted string, a heredoc body or a comment
# (`python3 - <<'EOF'` with `# gh pr ready` in it), doesn't count, unless a
# shell runs it: a $(…) (in an argument, a ${…} default, arithmetic, $[ … ]
# included, or an unquoted heredoc's body), `bash -c '…'`, `eval`, or a
# heredoc or here-string a shell reads, directly or through a pipe; that code,
# as written, quotes and heredoc bodies included, is tokenized and judged the
# same way. Each gh command is judged on its own words: a gh api call by its
# endpoint (its first argument that isn't a flag) and its fields, never by text
# inside a field's value or elsewhere on the line (unless its GraphQL query
# reads a shell variable, which may be set there), so a comment body that names
# an endpoint is just a body.
#
# gh pr merge is not checked here: it is denied in .claude/settings.json for
# Claude's own commands, and a person merges with it in shell mode (`!`), which
# the docs don't say whether this hook sees. Its API forms, below, are blocked.

# lint_title <title>: validate a PR title against the SAME Conventional-Commits
# rule the required `CI` check's `PR title` job enforces (scripts/lint-pr-title.sh
# is the shared rule) — so a too-long or wrong-format title is caught locally
# BEFORE the PR exists, not after the required check fails. Fail-open: with no
# title to read (--fill, interactive, computed) or no validator, fall through
# to the CI check rather than block a create we can't confidently judge.
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
msg_event_dyn="A review event the shell computes can't be checked for APPROVE or REQUEST_CHANGES. Write it literally (event=COMMENT)."
merge_msg="Agents don't merge PRs; a person does. Ask the user."

# truthy <value>: the value turns a boolean flag on, as gh's flag parser reads it.
truthy() {
  case $1 in 1 | t | T | true | TRUE | True) return 0 ;; esac
  return 1
}

# gh_pr <i> <end>: judge `gh pr <TK_VAL[i]> …`, its words up to token <end>,
# reading its flags as gh's parser does: each letter of a short-flag cluster
# is a flag (-df is -d -f); the first that takes a value takes the rest of the
# word, after an optional "=", or else the next word (-fr x is --fill
# --reviewer x); and a flag's value is never read as a flag (-b "-a x" is a
# body).
gh_pr() {
  local i=$1 e=$2 j k w c v d sub=${TK_VAL[$1]} short long
  local draft=0 approve=0 changes=0 reviewer=0 people=0 title=""
  # The flags that take a value; any other is a switch.
  case $sub in
    create | new)
      short=aBbFHlmpRrTt
      long=" assignee attach base body body-file head label milestone project recover repo reviewer template title " ;;
    edit)
      short=BbFmRt
      long=" add-assignee add-label add-project add-reviewer attach base body body-file milestone remove-assignee remove-label remove-project remove-reviewer repo title " ;;
    review) short=bFR long=" body body-file repo " ;;
    ready) block "$msg_ready" ;;
    *) return 0 ;;
  esac
  for ((j = i + 1; j < e; j++)); do
    w=${TK_VAL[j]}
    case $w in
      --) break ;;
      --*=*) pr_flag "${w%%=*}" "${w#*=}" "${TK_DYN[j]}" 1 ;;
      --?*)
        case $long in
          *" ${w#--} "*)
            j=$((j + 1))
            [ "$j" -lt "$e" ] && pr_flag "$w" "${TK_VAL[j]}" "${TK_DYN[j]}" 1 ;;
          *) pr_flag "$w" "" 0 0 ;;
        esac ;;
      -?*)
        for ((k = 1; k < ${#w}; k++)); do
          c=${w:k:1}
          if [ "${w:k+1:1}" = = ]; then
            pr_flag "-$c" "${w:k+2}" "${TK_DYN[j]}" 1
            break
          fi
          case $short in
            *"$c"*)
              v=${w:k+1}; d=${TK_DYN[j]}
              if [ -z "$v" ] && [ $((j + 1)) -lt "$e" ]; then
                j=$((j + 1)); v=${TK_VAL[j]}; d=${TK_DYN[j]}
              fi
              pr_flag "-$c" "$v" "$d" 1
              break ;;
          esac
          pr_flag "-$c" "" 0 0
        done ;;
    esac
  done
  case $sub in
    create | new)
      [ "$people" = 0 ] || block "$msg_reviewers"
      [ "$draft" = 1 ] || block "$msg_draft"
      lint_title "$title" ;;
    edit)
      [ "$reviewer" = 0 ] || block "$msg_reviewers"
      lint_title "$title" ;;
    review)
      [ "$approve" = 0 ] || block "$msg_approve"
      [ "$changes" = 0 ] || block "$msg_changes" ;;
  esac
  return 0
}

# pr_flag <flag> <value> <value-is-dynamic> <value-given>: note, in gh_pr's
# variables, what one flag of its subcommand does. A short flag means what its
# long form on that subcommand does: -a is --assignee on create, --approve on
# review.
pr_flag() {
  local f=$1
  case $sub$1 in
    create-d | new-d) f=--draft ;;
    create-a | new-a) f=--assignee ;;
    create-r | new-r) f=--reviewer ;;
    create-t | new-t | edit-t) f=--title ;;
    review-a) f=--approve ;;
    review-r) f=--request-changes ;;
  esac
  case $f in
    --draft) { [ "$4" = 0 ] || truthy "$2"; } && draft=1 ;;
    --approve) { [ "$4" = 0 ] || truthy "$2"; } && approve=1 ;;
    --request-changes) { [ "$4" = 0 ] || truthy "$2"; } && changes=1 ;;
    --reviewer | --assignee) people=1 ;;
    --add-reviewer | --remove-reviewer | --add-assignee | --remove-assignee) reviewer=1 ;;
    --title) [ "$3" = 0 ] && title=$2 ;;
  esac
  return 0
}

# reads_var <text>: the text reads a shell variable: it has a ${ or a $name that
# isn't a GraphQL variable the text declares ($name: Type).
reads_var() {
  local v re
  while IFS= read -r v; do
    v=${v#?}
    [ "$v" != '{' ] || return 0
    re='[$]'$v'[[:space:]]*:'
    [[ $1 =~ $re ]] || return 0
  done < <(grep -oE "$var_re" <<<"$1" | sort -u)
  return 1
}

# gh_api <i> <end> <start>: judge `gh api …`, its words from token <i> up to
# <end>, in the simple command that starts at token <start>. The endpoint is
# its first word that isn't a flag or a flag's value; a field is the value of
# -f, -F, --field or --raw-field. A call writes when it names a method other
# than GET or, naming none, sends a field or --input, which makes gh POST. A
# value gh reads from a file (`-F key=@file`, `--input file`) isn't read; one it
# reads from stdin (`-F key=@-`, `--input -`) is read from the heredoc or
# here-string the command itself reads.
gh_api() {
  local i=$1 e=$2 j w f d ep="" method="" fields=0 write=0 path typed stdin="" sdyn=0 input=0
  local draft="" title="" event="" edyn=0 query="" qvar=0 values="" vset=0 vdyn=0
  for ((j = 0; j < ${#TK_IN[@]}; j++)); do
    [ "${TK_IN_AT[j]}" != "$3" ] || { stdin=${TK_IN[j]%$'\n'}; sdyn=${TK_IN_DYN[j]}; }
  done
  for ((j = i; j < e; j++)); do
    w=${TK_VAL[j]}
    f="" typed=0
    case $w in
      -X | --method) [ $((j + 1)) -lt "$e" ] && { j=$((j + 1)); method=${TK_VAL[j]}; } ;;
      -X?*) method=${w#-X} ;;
      --method=*) method=${w#--method=} ;;
      -f | --raw-field) [ $((j + 1)) -lt "$e" ] && { j=$((j + 1)); f=${TK_VAL[j]}; fields=1; } ;;
      -F | --field) [ $((j + 1)) -lt "$e" ] && { j=$((j + 1)); f=${TK_VAL[j]}; fields=1; typed=1; } ;;
      -f?*) f=${w#-f}; fields=1 ;;
      -F?*) f=${w#-F}; fields=1; typed=1 ;;
      --raw-field=*) f=${w#*=}; fields=1 ;;
      --field=*) f=${w#*=}; fields=1; typed=1 ;;
      --input)
        j=$((j + 1)); fields=1
        [ "$j" -lt "$e" ] && [ "${TK_VAL[j]}" = - ] && input=1 ;;
      --input=*) fields=1; [ "$w" = --input=- ] && input=1 ;;
      -H | --header | -q | --jq | -t | --template | -p | --preview | --hostname | --cache) j=$((j + 1)) ;;
      -*) ;;
      *) [ -n "$ep" ] || ep=$w ;;
    esac
    [ -n "$f" ] || continue
    d=${TK_DYN[j]}
    if [ "$typed" = 1 ]; then
      case ${f#*=} in
        @-) f=${f%%=*}=$stdin d=$sdyn ;;
        @*) continue ;; # a file's contents
      esac
    fi
    # A field that is a review event may be the GraphQL variable holding one;
    # a body that mentions APPROVE isn't. A field other than the query that the
    # shell computes may hold one too, unread.
    case ${f#*=} in
      APPROVE | REQUEST_CHANGES) values+=" ${f#*=}" vset=1 ;;
      COMMENT | DISMISS) vset=1 ;;
      *) [ "$d" = 0 ] || case ${f%%=*} in
        query) ;;
        event) vdyn=1 edyn=1 ;;
        *) vdyn=1 ;;
      esac ;;
    esac
    case $f in
      draft=*) draft=${f#draft=} ;;
      title=*) [ "$d" = 0 ] && title=${f#title=} ;;
      event=*) event=${f#event=} ;;
      query=*)
        query=${f#query=}
        [ "$d" = 1 ] && reads_var "$query" && qvar=1 ;;
    esac
  done
  if [ -n "$method" ]; then
    case $method in [gG][eE][tT]) ;; *) write=1 ;; esac
  else
    write=$fields
  fi
  path=${ep#https://api.github.com}
  path=${path#/}
  path=${path%%\?*}
  path=${path%/}
  # A body sent with --input - is the request's JSON: a GraphQL one is matched
  # as a query is, and a REST one's draft, event and title are read from it.
  if [ "$input" = 1 ] && [ -n "$stdin" ]; then
    if [ "$path" = graphql ]; then
      query+=" $stdin"
      [ "$sdyn" = 1 ] && reads_var "$stdin" && qvar=1
    else
      { read -r draft; read -r event; IFS= read -r title; } < <(jq -r \
        '(.draft | tostring), (.event // "" | tostring), (.title // "" | tostring | gsub("\n"; " "))' \
        <<<"$stdin" 2>/dev/null)
    fi
  fi
  # A computed query is matched against the code that computes it: its own
  # word, heredoc bodies included (`-f query="$(cat <<'EOF' … EOF)"`), and,
  # when it reads a variable, the whole line, where that may be set.
  [ "$qvar" = 0 ] || query+=" $cmd"
  if [ "$path" = graphql ]; then
    case $query in *markPullRequestReadyForReview*) block "$msg_ready" ;; esac
    case $query in *mergePullRequest* | *enablePullRequestAutoMerge*) block "$merge_msg" ;; esac
    case $query in *requestReviews*) block "$msg_reviewers_api" ;; esac
    # Not addPullRequestReviewThreadReply or …Comment, which take no event.
    case $query in
      *addPullRequestReview | *addPullRequestReview[!A-Za-z0-9_]* | *submitPullRequestReview*)
        case "$query$values" in *APPROVE*) block "$msg_approve" ;; esac
        case "$query$values" in *REQUEST_CHANGES*) block "$msg_changes" ;; esac
        # The event in a GraphQL variable no field gives literally.
        [[ $vset == 0 && $vdyn == 1 && $query =~ event[[:space:]]*:[[:space:]]*[$] ]] && block "$msg_event_dyn" ;;
    esac
    case $query in
      *createPullRequest*)
        # `draft: true` in the query, or `"draft": true` in a JSON body's variables.
        [[ $query =~ draft\"?[[:space:]]*:[[:space:]]*true || $draft == true ]] \
          || block "Agent-opened PRs must be drafts; in GraphQL, send draft: true. Only humans publish ready-for-review PRs." ;;
    esac
    return 0
  fi
  if [ "$write" = 1 ]; then
    case $path in
      repos/*/pulls/*/requested_reviewers) block "$msg_reviewers_api" ;;
      repos/*/pulls/*/merge) block "$merge_msg" ;;
      repos/*/pulls)
        [ "$draft" = true ] \
          || block "Agent-opened PRs must be drafts; through the API, send -F draft=true. Only humans publish ready-for-review PRs."
        lint_title "$title" ;;
      repos/*/pulls/*/*) ;;
      repos/*/pulls/*) lint_title "$title" ;;
    esac
  fi
  case $path in
    repos/*/pulls/*/reviews | repos/*/pulls/*/reviews/*/events)
      [ "$edyn" = 0 ] || block "$msg_event_dyn"
      case $event in
        [aA][pP][pP][rR][oO][vV][eE]) block "$msg_approve" ;;
        [rR][eE][qQ][uU][eE][sS][tT]_[cC][hH][aA][nN][gG][eE][sS]) block "$msg_changes" ;;
      esac ;;
  esac
  return 0
}

# shell_code_at <i> <end>: SHELL_CODE = the token index of the code a shell
# whose arguments start at token <i> runs with -c (its first argument after
# the options), or -1 when it isn't given -c. Steps over options and the value
# of -o, +o, -O, +O, --rcfile and --init-file; a short-option cluster that
# contains c (-c, -lc, -euc) counts as -c, a long option (--norc) doesn't.
shell_code_at() {
  local m=$1 e=$2 c=0
  SHELL_CODE=-1
  while [ "$m" -lt "$e" ]; do
    case ${TK_VAL[m]} in
      --) m=$((m + 1)); break ;;
      --rcfile | --init-file) m=$((m + 1)) ;;
      --*) ;;
      [-+]*[oO]*)
        case ${TK_VAL[m]} in -*c*) c=1 ;; esac
        m=$((m + 1)) ;;
      -*c*) c=1 ;;
      [-+]?*) ;;
      *) break ;;
    esac
    m=$((m + 1))
  done
  if [ "$c" = 1 ] && [ "$m" -lt "$e" ]; then SHELL_CODE=$m; fi
}

# past_repo <m> <end>: AT = the first token from <m> that isn't -R/--repo or
# its value.
past_repo() {
  AT=$1
  while [ "$AT" -lt "$2" ]; do
    case ${TK_VAL[AT]} in
      -R | --repo) AT=$((AT + 2)) ;;
      -R?* | --repo=*) AT=$((AT + 1)) ;;
      *) return 0 ;;
    esac
  done
}

# gh_scan: judge every gh pr and gh api command in the current tokens, and
# queue in GH_QUEUE the code a shell will run: each $(…) and `…`, the code a
# shell is handed with -c or eval, and a heredoc or here-string a shell reads,
# directly or through a pipe.
gh_scan() {
  local i j k m n=${#TK_VAL[@]} w line="" start=0 s
  local -a lines=() starts=() ends=()
  for ((i = 0; i <= n; i++)); do
    if [ "$i" -eq "$n" ] || [ "${TK_OP[i]}" = 1 ]; then
      lines+=("$line"); starts+=("$start"); ends+=("$i")
      line=""; start=$((i + 1))
      continue
    fi
    w=${TK_VAL[i]}
    case $w in *[[:space:]]*) w=Q ;; esac
    [ "${TK_DYN[i]}" = 0 ] || w=Q
    line+=" $w"
  done
  for ((k = 0; k < ${#lines[@]}; k++)); do
    for ((j = starts[k]; j < ends[k]; j++)); do
      w=${TK_VAL[j]##*/}
      if [ "$w" = gh ] && [ "${TK_DYN[j]}" = 0 ]; then
        # gh takes -R/--repo before the command, and before pr's subcommand:
        # `gh -R o/r pr ready 12`, `gh pr -R o/r ready 12`.
        past_repo $((j + 1)) "${ends[k]}"
        m=$AT
        if [ "$m" -lt "${ends[k]}" ]; then
          case ${TK_VAL[m]} in
            pr)
              past_repo $((m + 1)) "${ends[k]}"
              [ "$AT" -lt "${ends[k]}" ] && gh_pr "$AT" "${ends[k]}" ;;
            api) gh_api $((m + 1)) "${ends[k]}" "${starts[k]}" ;;
          esac
        fi
        break
      fi
      # Code handed to a shell: `bash -c '…'` (or -lc, -e -c, …), or eval's words.
      case $w in
        sh | bash | dash | zsh | ksh)
          shell_code_at $((j + 1)) "${ends[k]}"
          [ "$SHELL_CODE" -lt 0 ] || GH_QUEUE+=("${TK_VAL[SHELL_CODE]}") ;;
        eval)
          s=""
          for ((i = j + 1; i < ends[k]; i++)); do s+=" ${TK_VAL[i]}"; done
          GH_QUEUE+=("$s") ;;
      esac
    done
  done
  for ((j = 0; j < ${#TK_CODE[@]}; j++)); do
    GH_QUEUE+=("${TK_CODE[j]}")
  done
  for ((j = 0; j < ${#TK_IN[@]}; j++)); do
    for ((k = 0; k < ${#lines[@]}; k++)); do
      [ "${starts[k]}" = "${TK_IN_AT[j]}" ] || continue
      if [[ ${lines[k]} =~ $shell_re ]] \
        || { [ $((k + 1)) -lt "${#lines[@]}" ] && [[ ${TK_VAL[ends[k]]} == '|'* ]] && [[ ${lines[k + 1]} =~ $shell_re ]]; }; then
        GH_QUEUE+=("${TK_IN[j]}")
      fi
      break
    done
  done
}

# gate_gh: judge the line, then each piece of code it hands a shell, in turn.
# It follows 64 pieces, a bound on code that nests itself; past that, any piece
# left that mentions a gh pr or gh api command, quoted or not, is refused,
# unjudged. So is a line it can't parse that mentions one: it can't tell what
# runs.
gate_gh() {
  local q left
  grep -qE '(^|[^[:alnum:]_-])gh[[:space:]]' <<<"$cmd" || return 0
  GH_QUEUE=("$cmd")
  for ((q = 0; q < ${#GH_QUEUE[@]} && q < 64; q++)); do
    GH_CODE=${GH_QUEUE[q]}
    if tokenize "$GH_CODE"; then
      gh_scan
    elif grep -qE "$gh_cmd_re" <<<"$GH_CODE"; then
      block "can't parse this command (an unterminated quote or substitution?), so can't tell what it does to a PR. Fix the quoting, or run the gh command on its own."
    fi
  done
  # What's left, in one grep; not through a pipe, which pipefail fails when
  # grep -q stops reading early.
  [ "$q" -lt "${#GH_QUEUE[@]}" ] || return 0
  printf -v left '%s\n' "${GH_QUEUE[@]:q}"
  grep -qE "$gh_cmd_re" <<<"$left" \
    && block "this command nests more code than the gate follows (64 pieces), and some of what's left runs gh. Split it into separate commands."
  return 0
}

# ── git push: the commit each refspec publishes needs a review marker ────────
#
# The commit each refspec publishes (its tip; HEAD when there is no refspec)
# needs a marker from EVERY reviewer in
# scripts/pre-push-reviewers.sh (the single source of truth — code, docs, and
# any future reviewer such as security), read at push time, so adding a
# reviewer there immediately makes it gate. All are unconditional: even a
# code-only change goes through docs review, because catching "code changed
# but the docs should have and didn't" is the docs reviewer's job.
#
# The check follows the push to where it runs: the tool call's cwd, then any
# `cd`/`pushd` before it and any `git -C`, so a push from a sibling worktree is
# judged on that worktree, not on the one the session started in. A marker for
# that commit counts from any worktree of the repository: markers are keyed to
# the commit, which is what was reviewed. When the gate can't tell where a push
# runs or what it publishes, it blocks and says why. That includes a push in
# code handed to a shell (`bash -c`, `eval`, a heredoc read by `sh`), and a
# push that follows a HEAD-moving git command, run directly or not
# (`git commit … && git push`, `out=$(git commit …) && git push`): the gate runs
# before the command line does, so it would judge the commit before the move.
# It reads only the command line, so it catches accidental forms, not
# deliberate evasion; AGENTS.md lists the forms it doesn't follow.
#
# It gates any commit with a delta against the base (origin/main, else local
# main when there is no remote-tracking ref), NOT only commits on a branch with
# an open PR: the agent flow is push-the-branch THEN open the draft PR, so
# keying on PR state would let the first push — the one that publishes the
# diff — skip review. A commit already on the base (pushing main itself) has
# nothing for the reviewers. Local main would count commits made on it by
# mistake (commit, then `git switch -c feat`) as already on the base; a stale
# origin/main only gates more. The universal .githooks/pre-push handles
# ci-passed for everyone.

reviewers_script="scripts/pre-push-reviewers.sh"

# ── Shell tokenizer ──
# Splits a command line the way the shell would (quotes, escapes, operators,
# redirections, heredocs, arithmetic, $(…) and `…`) without expanding or
# running anything. A word that carries an expansion is flagged dynamic: its
# value is unknowable here, so a push that depends on one fails closed. Fills
# TK_VAL / TK_DYN / TK_OP, TK_IN / TK_IN_AT / TK_IN_DYN with each heredoc body
# and here-string outside a $(…), the token index of the simple command that
# reads it and whether the shell expands it, and TK_SUBS with the text of
# every $(…) and `…`, wherever it went (an argument, a here-string, a redirect
# target, an unquoted heredoc's body, arithmetic), each quoted part and
# heredoc body inside a $(…) wrapped in \002…\003, and TK_CODE with the code
# inside each one exactly as written, quotes and heredoc bodies included, for
# the gh checks to tokenize in turn; returns 1 on an unterminated quote or
# substitution.

tk_flush() {
  if [ "$_inw" = 1 ]; then
    # One pass with tr: bash 3.2's ${_w//…} is quadratic in the marker count.
    case $_w in *$'\002'* | *$'\003'*) _w=$(printf '%s.' "$_w" | tr -d '\002\003'); _w=${_w%.} ;; esac
    case $_skip in
      0) TK_VAL+=("$_w"); TK_DYN+=("$_dyn"); TK_OP+=(0) ;;
      2) TK_IN+=("$_w"); TK_IN_AT+=("$_cmd0"); TK_IN_DYN+=("$_dyn") ;;
    esac
    _skip=0
  fi
  _w=""; _inw=0; _dyn=0
}
# An operator ends any redirection: in `<(…)` and `>(…)` the "(" opens a
# process substitution, whose first word is a command, not a target.
tk_op() { tk_flush; _skip=0; TK_VAL+=("$1"); TK_DYN+=(0); TK_OP+=(1); _cmd0=${#TK_VAL[@]}; }

tk_squote() {
  local rest=${_s:_i+1} part
  case $rest in *"'"*) ;; *) return 1 ;; esac
  part=${rest%%"'"*}
  _inw=1; _i=$((_i + ${#part} + 2))
  [ "$_insub" = 0 ] || part=$'\002'$part$'\003'
  _w+=$part
}

# tk_ansi: a $'…' string, in which \' doesn't end the quote.
tk_ansi() {
  local c
  _inw=1; _i=$((_i + 2))
  [ "$_insub" = 0 ] || _w+=$'\002'
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      "'") _i=$((_i + 1)); [ "$_insub" = 0 ] || _w+=$'\003'; return 0 ;;
      \\) _w+=${_s:_i:2}; _i=$((_i + 2)) ;;
      *) _w+=$c; _i=$((_i + 1)) ;;
    esac
  done
  return 1
}

tk_dquote() {
  local c c2 rest part
  _inw=1; _i=$((_i + 1))
  [ "$_insub" = 0 ] || _w+=$'\002'
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      '"')
        _i=$((_i + 1))
        [ "$_insub" = 0 ] || _w+=$'\003'
        return 0 ;;
      \\)
        c2=${_s:_i+1:1}
        case $c2 in '$' | '`' | '"' | \\) _w+=$c2 ;; $'\n') ;; *) _w+=$c$c2 ;; esac
        _i=$((_i + 2)) ;;
      '$') tk_dollar q || return 1 ;;
      '`') tk_backtick || return 1 ;;
      *)
        # Everything up to the next character with a meaning here, in one step:
        # a PR body is often tens of KB of plain text in double quotes.
        rest=${_s:_i}
        part=${rest%%[\"\\\$\`]*}
        _w+=$part; _i=$((_i + ${#part})) ;;
    esac
  done
  return 1
}

# tk_dollar [q]: at a "$"; q when the text is expanded as in double quotes (a
# double-quoted string, an unquoted heredoc's body, arithmetic).
tk_dollar() {
  local rest part s0=${#_w} start=$_i
  _inw=1; _dyn=1
  case ${_s:_i+1:1} in
    '(')
      tk_arith && return 0
      _w+='$'; _i=$((_i + 1)); _insub=$((_insub + 1))
      tk_paren || return 1
      _insub=$((_insub - 1)); TK_SUBS+=("${_w:s0}")
      TK_CODE+=("${_s:start+2:_i-start-3}") ;;
    '[') tk_arith || return 1 ;;
    '{')
      rest=${_s:_i}
      case $rest in *'}'*) ;; *) return 1 ;; esac
      part=${rest%%'}'*}
      # A $(…) in a default (`${x:-$(…)}`) runs: the text inside the braces is
      # queued as code, for its substitution to be found when it's tokenized.
      # Expanded as in double quotes, a '…' in it is literal and its $(…) runs
      # (`"${x:-'$(…)'}"`), so it's queued double-quoted.
      # shellcheck disable=SC2016 # a literal $( or backtick in ${…:-…}
      case $part in
        *'$('* | *'`'*)
          TK_SUBS+=("$part}")
          if [ -n "${1:-}" ]; then TK_CODE+=("\"${part:2}\""); else TK_CODE+=("${part:2}"); fi ;;
      esac
      _w+="$part}"; _i=$((_i + ${#part} + 1)) ;;
    *) _w+='$'; _i=$((_i + 1)) ;;
  esac
}

# tk_arith: at "$((" or "((" whose parens close as "))", or at bash's older
# "$[ … ]", consume the arithmetic into the word, so a "<<" in it is a shift,
# not a heredoc, and record the substitutions in it. Returns 1, consuming
# nothing, when they don't close so (a "((" that is a subshell in a
# substitution or subshell, as bash reads it) or "((" follows "<" or ">" (a
# process substitution).
tk_arith() {
  local p=$_i from end depth=0 o='(' x=')' c rest part
  if [ "${_s:p:2}" = '$[' ]; then
    o='[' x=']'
  elif [ "${_s:p:1}" = '$' ]; then
    p=$((p + 1))
  elif [ "$p" -gt 0 ]; then
    case ${_s:p-1:1} in '<' | '>') return 1 ;; esac
  fi
  [ "$o" = '[' ] || [ "${_s:p:2}" = '((' ] || return 1
  from=$((p + 2))
  for ((p = from; p < _n; p++)); do
    c=${_s:p:1}
    case $c in
      "$o") depth=$((depth + 1)) ;;
      "$x")
        [ "$depth" -gt 0 ] || break
        depth=$((depth - 1)) ;;
      \\) p=$((p + 1)) ;;
      "'" | '"')
        rest=${_s:p+1}
        case $rest in *"$c"*) ;; *) return 1 ;; esac
        part=${rest%%"$c"*}; p=$((p + ${#part} + 1)) ;;
    esac
  done
  if [ "$o" = '(' ]; then
    [ "${_s:p:2}" = '))' ] || return 1
    end=$((p + 2))
  else
    [ "$p" -lt "$_n" ] || return 1
    end=$((p + 1))
  fi
  _w+=${_s:_i:end-_i}; _inw=1; _dyn=1; _i=$end
  tk_expand_subs "$from" "$p" || true # one that doesn't close: bash runs nothing
}

# tk_hd_restore: put back the pending heredocs tk_expand_subs saved (its
# hd_* locals). The "${a[@]+…}" form is for bash 3.2, whose "${a[@]}" on an
# empty array fails under set -u.
tk_hd_restore() {
  _hd_delim=(${hd_delim[@]+"${hd_delim[@]}"}); _hd_strip=(${hd_strip[@]+"${hd_strip[@]}"})
  _hd_at=(${hd_at[@]+"${hd_at[@]}"}); _hd_quoted=(${hd_quoted[@]+"${hd_quoted[@]}"})
}

# tk_expand_subs <from> <to>: text the shell expands as it does a double-quoted
# string, an unquoted heredoc's body or arithmetic, runs each $(…) and `…` in
# it; record them as a word's are, reading no further than <to>. Returns 1 on
# one that doesn't close by then. Each line is read as a string of its own:
# bash measures the whole string on every ${s:i:n}, so a character loop over a
# large body would be quadratic. A substitution that runs past its line is read
# from the whole text, and its last line from where it ends.
tk_expand_subs() {
  local ss=$_s si=$_i sn=$_n sw=$_w sinw=$_inw sdyn=$_dyn text=${_s:$1:$2-$1} line off=$1 next=$1 rc=0
  local -a hd_delim hd_strip hd_at hd_quoted
  case $text in *[\$\`]*) ;; *) return 0 ;; esac
  # A heredoc opened inside the text has its body inside it, so what a scan
  # leaves pending is dropped.
  hd_delim=(${_hd_delim[@]+"${_hd_delim[@]}"}); hd_strip=(${_hd_strip[@]+"${_hd_strip[@]}"})
  hd_at=(${_hd_at[@]+"${_hd_at[@]}"}); hd_quoted=(${_hd_quoted[@]+"${_hd_quoted[@]}"})
  while IFS= read -r line; do
    if [ $((off + ${#line})) -ge "$next" ]; then
      case $line in
        *[\$\`]*)
          _s=$line; _n=${#line}; _i=0
          [ "$next" -le "$off" ] || _i=$((next - off))
          if ! tk_scan_subs; then
            tk_hd_restore
            _s=$ss; _n=$2; _i=$((off + _i)); _w=""
            if [ "${_s:_i:1}" = '$' ]; then tk_dollar q; else tk_backtick; fi || { rc=1; break; }
            next=$_i
          fi
          tk_hd_restore ;;
      esac
    fi
    off=$((off + ${#line} + 1))
  done <<<"$text"
  _s=$ss; _i=$si; _n=$sn; _w=$sw; _inw=$sinw; _dyn=$sdyn
  return "$rc"
}

# tk_scan_subs: record each $(…) and `…` from _i to _n; on one that doesn't
# close by _n, return 1 with _i at its start.
tk_scan_subs() {
  local rest part from
  while :; do
    rest=${_s:_i}
    part=${rest%%[\\\$\`]*}
    _i=$((_i + ${#part}))
    [ "$_i" -lt "$_n" ] || return 0
    from=$_i; _w=""
    case ${_s:_i:1} in
      \\) _i=$((_i + 2)) ;;
      '$') tk_dollar q || { _i=$from; return 1; } ;;
      *) tk_backtick || { _i=$from; return 1; } ;;
    esac
  done
}

# tk_backtick: a `…`. Inside it a backslash quotes only $, ` and \, so the code
# it runs has \` as a backtick, which nests a substitution (`echo \`…\``).
tk_backtick() {
  local c code=""
  _inw=1; _dyn=1; _w+='`'; _i=$((_i + 1))
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      '`')
        _w+=$c; _i=$((_i + 1))
        TK_SUBS+=("\`$code\`"); TK_CODE+=("$code")
        return 0 ;;
      \\)
        _w+=${_s:_i:2}
        case ${_s:_i+1:1} in '$' | '`' | \\) code+=${_s:_i+1:1} ;; *) code+=${_s:_i:2} ;; esac
        _i=$((_i + 2)) ;;
      *) _w+=$c; code+=$c; _i=$((_i + 1)) ;;
    esac
  done
  return 1
}

# tk_paren: consume a $(…) from its "(" to the matching ")", nested quotes and
# heredocs included: a commit body passed as "$(cat <<'EOF' … EOF)" is common,
# and an apostrophe in it must not open a quote.
tk_paren() {
  local c depth=0
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      '(')
        # Its own "(" opens the substitution; a "((" after that is arithmetic.
        if [ "$depth" -eq 0 ] || ! tk_arith; then depth=$((depth + 1)); _w+=$c; _i=$((_i + 1)); fi ;;
      ')')
        depth=$((depth - 1)); _w+=$c; _i=$((_i + 1))
        [ "$depth" -eq 0 ] && return 0 ;;
      \\) _w+=${_s:_i:2}; _i=$((_i + 2)) ;;
      "'") tk_squote || return 1 ;;
      '"') tk_dquote || return 1 ;;
      '`') tk_backtick || return 1 ;;
      '$')
        if [ "${_s:_i+1:1}" = "'" ]; then
          tk_ansi || return 1
        elif ! tk_arith; then
          _w+=$c; _i=$((_i + 1))
        fi ;;
      '<')
        if [ "${_s:_i:3}" = '<<<' ]; then
          _w+='<<< '; _i=$((_i + 3))
        else
          tk_heredoc_op -1 || { _w+=$c; _i=$((_i + 1)); }
        fi ;;
      $'\n') _w+=$c; _i=$((_i + 1)); tk_heredoc_bodies || return 1 ;;
      *) _w+=$c; _i=$((_i + 1)) ;;
    esac
  done
  return 1
}

# tk_heredoc_op <at>: at "<<" or "<<-", register the heredoc's delimiter (its
# body starts after the next newline), whether any of it is quoted, and the
# token index of the command that reads it (-1 inside a substitution). Returns
# 1, consuming nothing, elsewhere.
tk_heredoc_op() {
  local strip=0 c d="" q=0
  case ${_s:_i:3} in
    '<<<') return 1 ;;
    '<<-') strip=1; _i=$((_i + 3)) ;;
    '<<'*) _i=$((_i + 2)) ;;
    *) return 1 ;;
  esac
  while c=${_s:_i:1}; [ "$c" = ' ' ] || [ "$c" = $'\t' ]; do _i=$((_i + 1)); done
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      ' ' | $'\t' | $'\n' | ';' | '|' | '&' | '<' | '>' | '(' | ')') break ;;
      "'" | '"' | \\) q=1 ;;
      *) d+=$c ;;
    esac
    _i=$((_i + 1))
  done
  _hd_delim+=("$d"); _hd_strip+=("$strip"); _hd_at+=("$1"); _hd_quoted+=("$q")
}

# tk_heredoc_bodies: just past a newline, step over the bodies of pending
# heredocs. A top-level one's body goes to TK_IN; one inside a $(…) joins the
# substitution's text as a quoted part, an argument unless the substitution
# hands it to a shell. Inside a $(…), a line that is the delimiter followed by
# ")" (`EOF)"`) also ends the body, and the ")" closes the substitution. When
# no part of the delimiter is quoted, the body runs its $(…) and `…`, recorded
# as a word's are; returns 1 on one that doesn't close in the body.
tk_heredoc_bodies() {
  local k nh=${#_hd_delim[@]} rest line start end
  local -a delim strip at quoted
  [ "$nh" -gt 0 ] || return 0
  delim=("${_hd_delim[@]}"); strip=("${_hd_strip[@]}"); at=("${_hd_at[@]}"); quoted=("${_hd_quoted[@]}")
  _hd_delim=(); _hd_strip=(); _hd_at=(); _hd_quoted=()
  for ((k = 0; k < nh; k++)); do
    start=$_i
    while end=$_i; [ "$_i" -lt "$_n" ]; do
      rest=${_s:_i}
      line=${rest%%$'\n'*}
      _i=$((_i + ${#line} + 1))
      [ "${strip[k]}" = 1 ] && line=${line#"${line%%[!$'\t']*}"}
      [ "$line" = "${delim[k]}" ] && break
      if [ "${at[k]}" = -1 ] && [ "${line#"${delim[k]})"}" != "$line" ]; then
        _i=$((_i - 1 - ${#line} + ${#delim[k]})); break
      fi
    done
    [ "$end" -le "$_n" ] || end=$_n
    if [ "${at[k]}" -ge 0 ]; then
      TK_IN+=("${_s:start:end-start}"); TK_IN_AT+=("${at[k]}"); TK_IN_DYN+=("$((1 - quoted[k]))")
    elif [ "$_insub" -gt 0 ]; then
      _w+=$'\002'${_s:start:end-start}$'\003'
    fi
    [ "${quoted[k]}" = 1 ] || tk_expand_subs "$start" "$end" || return 1
  done
}

# tk_redirect: at "<" or ">" (or the ">" of "&>"). An fd number glued to the
# operator (2>&1) belongs to it, and the word after it is a target, not an
# argument; a here-string's word goes to TK_IN.
tk_redirect() {
  if [ "$_inw" = 1 ]; then
    case $_w in '' | *[!0-9]*) tk_flush ;; *) _w=""; _inw=0; _dyn=0 ;; esac
  fi
  tk_heredoc_op "$_cmd0" && return 0
  if [ "${_s:_i:3}" = '<<<' ]; then
    _i=$((_i + 3)); _skip=2
  else
    _i=$((_i + 1)); _skip=1
    while case ${_s:_i:1} in '>' | '&' | '|') true ;; *) false ;; esac; do _i=$((_i + 1)); done
  fi
}

tokenize() {
  local LC_ALL=C c c2 rest line
  _s=$1; _n=${#1}; _i=0
  _w=""; _inw=0; _dyn=0; _skip=0; _cmd0=0; _insub=0
  TK_VAL=(); TK_DYN=(); TK_OP=(); TK_IN=(); TK_IN_AT=(); TK_IN_DYN=(); TK_SUBS=(); TK_CODE=()
  _hd_delim=(); _hd_strip=(); _hd_at=(); _hd_quoted=()
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      ' ' | $'\t') tk_flush; _i=$((_i + 1)) ;;
      $'\n') tk_op ';'; _i=$((_i + 1)); tk_heredoc_bodies || return 1 ;;
      \\)
        c2=${_s:_i+1:1}
        [ "$c2" = $'\n' ] || { _w+=$c2; _inw=1; }
        _i=$((_i + 2)) ;;
      "'") tk_squote || return 1 ;;
      '"') tk_dquote || return 1 ;;
      '$')
        if [ "${_s:_i+1:1}" = "'" ]; then tk_ansi || return 1; else tk_dollar || return 1; fi ;;
      '`') tk_backtick || return 1 ;;
      ';' | '|')
        c2=${_s:_i+1:1}
        if [ "$c2" = "$c" ] || [ "$c$c2" = '|&' ]; then
          tk_op "$c$c2"; _i=$((_i + 2))
        else
          tk_op "$c"; _i=$((_i + 1))
        fi ;;
      '&')
        case ${_s:_i+1:1} in
          '>') tk_flush; _i=$((_i + 1)); tk_redirect ;;
          '&') tk_op '&&'; _i=$((_i + 2)) ;;
          *) tk_op '&'; _i=$((_i + 1)) ;;
        esac ;;
      '(') tk_arith || { tk_op "$c"; _i=$((_i + 1)); } ;; # `(( n <<= 1 ))` is arithmetic
      ')') tk_op "$c"; _i=$((_i + 1)) ;;
      '<' | '>') tk_redirect ;;
      '#')
        if [ "$_inw" = 1 ]; then
          _w+=$c; _i=$((_i + 1))
        else
          rest=${_s:_i}; line=${rest%%$'\n'*}; _i=$((_i + ${#line}))
        fi ;;
      *) _w+=$c; _inw=1; _i=$((_i + 1)) ;;
    esac
  done
  tk_flush
  return 0
}

# ── Following the push ──

# join_path <base> <path>: where `cd <path>` from <base> lands.
join_path() {
  case $2 in
    /*) printf '%s' "$2" ;;
    \~) printf '%s' "$HOME" ;;
    \~/*) printf '%s/%s' "$HOME" "${2#\~/}" ;;
    *) printf '%s/%s' "$1" "$2" ;;
  esac
}

is_assignment() {
  case $1 in *=*) ;; *) return 1 ;; esac
  case ${1%%=*} in '' | [0-9]* | *[!A-Za-z0-9_]*) return 1 ;; esac
}

# walk_commands <dir>: run through the tokens, tracking the directory each
# simple command runs in (`why` is set once that can't be known) and whether
# GIT_DIR (`gd`) or GIT_WORK_TREE (`gw`) has been set, and gate every
# `git push` found. A later cd doesn't undo either variable; unset or the end
# of a subshell does.
walk_commands() {
  local dir=$1 why="" gd="" gw="" i=0 n=${#TK_VAL[@]} top cstart=0
  local -a words=() dyns=() sdir=() swhy=() sgd=() sgw=()
  while [ "$i" -le "$n" ]; do
    if [ "$i" -eq "$n" ] || [ "${TK_OP[i]}" = 1 ]; then
      [ "${#words[@]}" -gt 0 ] && run_simple
      words=(); dyns=()
      if [ "$i" -lt "$n" ]; then
        case ${TK_VAL[i]} in
          '(') sdir+=("$dir"); swhy+=("$why"); sgd+=("$gd"); sgw+=("$gw") ;;
          ')')
            if [ "${#sdir[@]}" -gt 0 ]; then
              top=$((${#sdir[@]} - 1))
              dir=${sdir[top]}; why=${swhy[top]}; gd=${sgd[top]}; gw=${sgw[top]}
              unset "sdir[top]" "swhy[top]" "sgd[top]" "sgw[top]"
            fi ;;
        esac
      fi
    else
      [ "${#words[@]}" -gt 0 ] || cstart=$i
      words+=("${TK_VAL[i]}"); dyns+=("${TK_DYN[i]}")
    fi
    i=$((i + 1))
  done
}

# run_simple: one simple command (words/dyns of walk_commands); follows a cd,
# notes GIT_DIR / GIT_WORK_TREE and HEAD-moving git commands, and gates a git
# push.
run_simple() {
  local k=0 j nw=${#words[@]} w a d dwhy ewhy sub="" agd="" agw="" line
  while [ "$k" -lt "$nw" ]; do
    w=${words[k]}
    if is_assignment "$w"; then
      case ${w%%=*} in
        GIT_DIR) agd="\`GIT_DIR=\`" ;;
        GIT_WORK_TREE) agw="\`GIT_WORK_TREE=\`" ;;
      esac
    else
      case $w in
        command | builtin | exec | nohup | time | env | '{' | '!') ;;
        if | then | elif | else | do | while | until) ;;
        *) break ;;
      esac
    fi
    k=$((k + 1))
  done
  # A bare assignment sets GIT_DIR / GIT_WORK_TREE for the rest of the line.
  if [ "$k" -ge "$nw" ]; then
    gd=${agd:-$gd}; gw=${agw:-$gw}
    return 0
  fi

  case ${words[k]} in
    export | declare | typeset)
      for ((j = k + 1; j < nw; j++)); do
        case ${words[j]} in
          GIT_DIR | GIT_DIR=*) gd="\`${words[k]} GIT_DIR\`" ;;
          GIT_WORK_TREE | GIT_WORK_TREE=*) gw="\`${words[k]} GIT_WORK_TREE\`" ;;
        esac
      done ;;
    unset)
      for ((j = k + 1; j < nw; j++)); do
        case ${words[j]} in GIT_DIR) gd="" ;; GIT_WORK_TREE) gw="" ;; esac
      done ;;
    cd | pushd)
      k=$((k + 1))
      while [ "$k" -lt "$nw" ]; do
        case ${words[k]} in -L | -P | -e | -@) ;; --) k=$((k + 1)); break ;; *) break ;; esac
        k=$((k + 1))
      done
      if [ "$k" -ge "$nw" ]; then
        dir=$HOME; why=""
        return 0
      fi
      a=${words[k]}
      if [ "${dyns[k]}" = 1 ]; then
        why="\`cd $a\`"
        return 0
      fi
      case $a in
        - | [+-][0-9]*) why="\`cd $a\`"; return 0 ;;
        /* | \~ | \~/*) why="" ;;
      esac
      dir=$(join_path "$dir" "$a") ;;
    popd) why="\`popd\`" ;;
    eval | bash | sh | zsh | dash | ksh | */bash | */sh | */zsh | */dash | */ksh)
      # Code handed to a shell (`-c '…'`, eval's words, a heredoc or
      # here-string it reads) isn't followed, so a push in it blocks.
      line="${words[*]:k+1}"
      for ((j = 0; j < ${#TK_IN_AT[@]}; j++)); do
        [ "${TK_IN_AT[j]}" != "$cstart" ] || line="$line
${TK_IN[j]}"
      done
      line=${line//$'\\\n'/}
      head_move "$line"
      ! grep -qE "$push_re" <<<"$line" || block "$cant_follow" ;;
    git)
      d=$dir; dwhy=$why; ewhy=${agd:-${agw:-${gd:-$gw}}}
      k=$((k + 1))
      while [ "$k" -lt "$nw" ]; do
        w=${words[k]}
        case $w in
          -C)
            k=$((k + 1))
            [ "$k" -lt "$nw" ] || return 0
            a=${words[k]}
            if [ "${dyns[k]}" = 1 ]; then
              dwhy="\`git -C $a\`"
            else
              case $a in /* | \~ | \~/*) dwhy="" ;; esac
              d=$(join_path "$d" "$a")
            fi ;;
          -c | --namespace) k=$((k + 1)) ;;
          --git-dir | --work-tree) ewhy="\`git $w\`"; k=$((k + 1)) ;;
          --git-dir=* | --work-tree=*) ewhy="\`git ${w%%=*}\`" ;;
          -*) ;;
          *) sub=$w; break ;;
        esac
        k=$((k + 1))
      done
      case $sub in
        commit | merge | rebase | reset | checkout | switch | cherry-pick | revert | am | pull) HEAD_MOVER=$sub ;;
        push)
          if [ "${dyns[k]}" = 0 ]; then
            FOLLOWED="$FOLLOWED$cstart "
            gate_push "$d" "${ewhy:-$dwhy}" $((k + 1))
          fi ;;
      esac ;;
    *)
      # A git command behind a wrapper (`timeout 60 git commit`) still moves HEAD.
      line=""
      for ((j = k; j < nw; j++)); do
        w=${words[j]}
        case $w in *[[:space:]]*) w=Q ;; esac
        [ "${dyns[j]}" = 0 ] || w=Q
        line="$line $w"
      done
      head_move "$line" ;;
  esac
}

# head_move <text>: note a HEAD-moving `git …` in text the walk doesn't follow.
head_move() {
  if [[ $1 =~ $head_re ]]; then HEAD_MOVER=${BASH_REMATCH[4]}; fi
}

# gate_push <dir> <why-unknown> <first-arg-index>: resolve what a `git push`
# publishes and require every reviewer's marker for each commit.
gate_push() {
  local dir=$1 why=$2 j=$3 w opts=1 remote=0 delete=0 dynspec="" spec src sha top
  local -a specs=()
  while [ "$j" -lt "$nw" ]; do
    w=${words[j]}
    if [ "$opts" = 1 ] && [ "${dyns[j]}" = 0 ]; then
      case $w in
        -h | --help) return 0 ;;
        --) opts=0; j=$((j + 1)); continue ;;
        --all | --branches | --mirror)
          block "\`git push $w\` publishes every branch at once, so the gate can't check each one's review. Push one branch at a time." ;;
        -d | --delete) delete=1; j=$((j + 1)); continue ;;
        --repo | -o | --push-option | --receive-pack | --exec) j=$((j + 2)); continue ;;
        -*) j=$((j + 1)); continue ;;
      esac
    fi
    if [ "$remote" = 0 ]; then
      remote=1
    else
      if [ "$w" = tag ] && [ "${dyns[j]}" = 0 ] && [ $((j + 1)) -lt "$nw" ]; then
        j=$((j + 1)); w="refs/tags/${words[j]}"
      fi
      [ "${dyns[j]}" = 1 ] && dynspec=$w
      specs+=("$w")
    fi
    j=$((j + 1))
  done
  [ "$delete" = 1 ] && return 0

  [ -z "$why" ] || block "can't tell which repository this \`git push\` runs in (${why} comes before it). Run it from inside the worktree, or as \`git -C <worktree> push …\` with a literal path."
  top=$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null) \
    || block "can't tell which repository this \`git push\` runs in: '${dir}' is not inside a git worktree."

  # Another repository with no reviewer manifest isn't under this policy. The
  # session's own repository always is, so a missing manifest there blocks.
  if [ ! -f "$top/$reviewers_script" ] \
    && [ "$(git -C "$top" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" \
      != "$(git -C "$project_dir" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" ]; then
    return 0
  fi

  [ -z "$HEAD_MOVER" ] || block "this command runs \`git ${HEAD_MOVER}\` before \`git push\`, and the gate runs before either, so it would judge the commit as it was before the ${HEAD_MOVER}. Run the push as a separate command."
  [ -z "$dynspec" ] || block "can't tell what \`git push … ${dynspec}\` publishes: the refspec is computed by the shell. Name the branch or commit literally."
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

# `git [global options] push` and the HEAD-moving subcommands (BASH_REMATCH[4]),
# as regexes over a line of words.
# shellcheck disable=SC2016 # the backticks are literal: a push inside `…`
git_re='(^|[[:space:];|&(`])git([[:space:]]+-[^[:space:]]+([[:space:]]+[^-[:space:];|&][^[:space:];|&]*)?)*[[:space:]]+'
# shellcheck disable=SC2016
end_re='([[:space:]]|$|[;|&)`])'
push_re=${git_re}push$end_re
# A gh pr or gh api command, -R/--repo before it allowed, anywhere in a text,
# quoted or not (`bash -c 'gh pr ready 12'`): for code the gate can't parse or
# doesn't reach, where it can't tell a mention from a command.
gh_cmd_re='(^|[^[:alnum:]_.-])gh[[:space:]]+((-R|--repo)(=|[[:space:]]*)[^[:space:]]+[[:space:]]+)*(pr|api)([^[:alnum:]_-]|$)'
head_re=${git_re}'(commit|merge|rebase|reset|checkout|switch|cherry-pick|revert|am|pull)'$end_re
var_re='[$]([A-Za-z_][A-Za-z0-9_]*|[{])' # a $name or ${
# shellcheck disable=SC2016
shell_re='(^|[[:space:];|&(`])(eval|([^[:space:];|&]*/)?(ba|z|da|k)?sh)([[:space:]<]|$)'
cant_follow="this command runs \`git push\` in a form the gate can't follow (behind a wrapper such as timeout, sudo or env -C, in a command substitution, or in code handed to a shell or eval). Run \`git push\` directly, or as \`git -C <worktree> push …\`."

# unquote <text>: UNQ = <text> with each \002…\003 (quoted) part as Q.
unquote() {
  local LC_ALL=C t=$1 pre c depth=0
  UNQ=""
  while :; do
    pre=${t%%[$'\002\003']*}
    [ "$depth" -gt 0 ] || UNQ+=$pre
    [ "${#pre}" -lt "${#t}" ] || return 0
    c=${t:${#pre}:1}; t=${t:${#pre}+1}
    if [ "$c" = $'\002' ]; then
      [ "$depth" -gt 0 ] || UNQ+=Q
      depth=$((depth + 1))
    elif [ "$depth" -gt 0 ]; then
      depth=$((depth - 1))
    fi
  done
}

# sub_scan: note a HEAD move in any command substitution, and set SUB_PUSH when
# one holds a `git … push`. Quoted text in it is an argument, not code
# (`$(grep -c 'git push' f)`), unless the substitution hands it to a shell.
sub_scan() {
  local j s
  SUB_PUSH=0
  for ((j = 0; j < ${#TK_SUBS[@]}; j++)); do
    s=${TK_SUBS[j]//$'\\\n'/}
    unquote "$s"
    if [[ $UNQ =~ $shell_re ]]; then UNQ=${s//$'\002'/}; UNQ=${UNQ//$'\003'/}; fi
    head_move "$UNQ"
    if [[ $UNQ =~ $push_re ]]; then SUB_PUSH=1; fi
  done
}

# unfollowed_push: true when a command substitution anywhere holds a
# `git … push`, or a simple command the walk didn't gate (FOLLOWED holds the
# first-token index of each one it did) holds one behind a wrapper it doesn't
# know (`timeout 60 git push`). A push only mentioned in a quoted string, a
# heredoc body or a comment is one word or no token at all, so it doesn't count.
unfollowed_push() {
  local i n=${#TK_VAL[@]} w line="" start=0
  [ "$SUB_PUSH" = 0 ] || return 0
  for ((i = 0; i <= n; i++)); do
    if [ "$i" -eq "$n" ] || [ "${TK_OP[i]}" = 1 ]; then
      case $FOLLOWED in
        *" $start "*) ;;
        *) grep -qE "$push_re" <<<"$line" && return 0 ;;
      esac
      line=""; start=$((i + 1))
      continue
    fi
    w=${TK_VAL[i]}
    case $w in *[[:space:]]*) w=Q ;; esac
    [ "${TK_DYN[i]}" = 0 ] || w=Q
    line="$line $w"
  done
  return 1
}

gate_gh

# Cheap filter first, so most commands are never tokenized: quoted strings on a
# line become a placeholder word (`git -C "<path>" stash push` reads as a
# stash). It may still match a push that is only mentioned, say in a heredoc
# body; the tokenizer then finds no push and lets the command through. Code
# handed to a shell or eval, and a substitution, are often quoted, so a line
# that has one and mentions git and push anywhere goes through too.
bsnl=$'\\\n' # outside the $(…): bash 3.2 misreads the pattern inside it and is slow
squashed=$(printf '%s' "${cmd//"$bsnl"/}" | sed -E "s/'[^']*'/Q/g; s/\"[^\"]*\"/Q/g")
# shellcheck disable=SC2016 # a literal $( or backtick
if grep -qE "$push_re" <<<"$squashed" \
  || { [[ $cmd == *git*push* ]] && { [[ $cmd == *'$('* || $cmd == *'`'* ]] || grep -qE "$shell_re" <<<"$squashed"; }; }; then
  hook_cwd=$(printf '%s' "$input" | jq -r '.cwd // empty' 2>/dev/null)
  [ -n "$hook_cwd" ] || block "can't tell which directory this \`git push\` runs in: the hook payload has no cwd."
  tokenize "$cmd" || block "can't parse this command (an unterminated quote or substitution?), so can't tell what it pushes."
  FOLLOWED=" "
  # A substitution runs before the command it's in, so a HEAD move in one
  # counts against every push on the line.
  HEAD_MOVER=""
  sub_scan
  walk_commands "$hook_cwd"
  if unfollowed_push; then
    block "$cant_follow"
  fi
fi

exit 0

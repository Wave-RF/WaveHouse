#!/usr/bin/env bash
# Agent PR workflow gate (PreToolUse Bash). Catches accidental violations of
# rules that have no human analog:
#   - gh pr create (or its alias gh pr new) without --draft, or with a title
#     the PR-title lint rejects
#   - gh pr ready
#   - gh pr edit --add-reviewer / --add-assignee
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
# shell runs it: a $(…), `bash -c '…'`, or a heredoc or here-string a shell
# reads, directly or through a pipe. A heredoc read by a shell inside a $(…) is
# not followed.
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

# gh_scan: from the tokens, GH_VIEW = the commands the line runs, one simple
# command a line, each word that has a space in it or is computed read as Q
# (a quoted string); plus the text of each $(…) and `…` (its quoted parts as
# Q), and, word for word, the code a shell is handed and any heredoc or
# here-string a shell reads. GH_API = the gh api calls among them, word for
# word. GH_TITLE = the --title/-t of a gh pr create, new or edit; GH_API_TITLE
# = the title= field of a gh api call; each empty when absent or computed.
gh_scan() {
  local i j k n=${#TK_VAL[@]} w line="" raw="" start=0 s
  local -a lines=() raws=() starts=() ends=()
  GH_VIEW=""; GH_API=""; GH_TITLE=""; GH_API_TITLE=""
  for ((i = 0; i <= n; i++)); do
    if [ "$i" -eq "$n" ] || [ "${TK_OP[i]}" = 1 ]; then
      lines+=("$line"); raws+=("$raw"); starts+=("$start"); ends+=("$i")
      line=""; raw=""; start=$((i + 1))
      continue
    fi
    w=${TK_VAL[i]}
    raw+=" $w"
    case $w in *[[:space:]]*) w=Q ;; esac
    [ "${TK_DYN[i]}" = 0 ] || w=Q
    line+=" $w"
  done
  for ((k = 0; k < ${#lines[@]}; k++)); do
    GH_VIEW+="${lines[k]}"$'\n'
    if [[ ${lines[k]} =~ $shell_re ]]; then GH_VIEW+="${raws[k]}"$'\n'; fi
    if [[ ${lines[k]} =~ ${gh_re}pr[[:space:]]+(create|new|edit)$end_re ]]; then
      for ((j = starts[k]; j < ends[k]; j++)); do
        case ${TK_VAL[j]} in
          --title | -t)
            [ $((j + 1)) -lt "${ends[k]}" ] && [ "${TK_DYN[j + 1]}" = 0 ] && GH_TITLE=${TK_VAL[j + 1]} ;;
          --title=*) [ "${TK_DYN[j]}" = 0 ] && GH_TITLE=${TK_VAL[j]#--title=} ;;
        esac
      done
    fi
    if [[ ${lines[k]} =~ ${gh_re}api$end_re ]]; then
      GH_API+="${raws[k]}"$'\n'
      for ((j = starts[k]; j < ends[k]; j++)); do
        case ${TK_VAL[j]} in
          title=@*) ;; # -F title=@file: the file's contents
          title=*) [ "${TK_DYN[j]}" = 0 ] && GH_API_TITLE=${TK_VAL[j]#title=} ;;
        esac
      done
    fi
  done
  for s in "${TK_SUBS[@]}"; do
    unquote "$s"
    GH_VIEW+="$UNQ"$'\n'
    s=${s//$'\002'/}; s=${s//$'\003'/}
    if [[ $s =~ ${gh_re}api$end_re ]]; then GH_API+="$s"$'\n'; fi
  done
  # A heredoc or here-string counts when its reader is a shell, or pipes into one.
  for ((j = 0; j < ${#TK_IN[@]}; j++)); do
    for ((k = 0; k < ${#lines[@]}; k++)); do
      [ "${starts[k]}" = "${TK_IN_AT[j]}" ] || continue
      if [[ ${lines[k]} =~ $shell_re ]] \
        || { [ $((k + 1)) -lt "${#lines[@]}" ] && [[ ${TK_VAL[ends[k]]} == '|'* ]] && [[ ${lines[k + 1]} =~ $shell_re ]]; }; then
        GH_VIEW+="${TK_IN[j]}"$'\n'
        if [[ ${TK_IN[j]} =~ ${gh_re}api$end_re ]]; then GH_API+="${TK_IN[j]}"$'\n'; fi
      fi
      break
    done
  done
}

# api_title: the title= field of a gh api call in an unparsed line.
api_title() {
  local t
  t=$(sed -nE 's/.*[[:space:]](title="([^"]*)"|"title=([^"]*)").*/\2\3/p' <<<"$cmd")
  [ -n "$t" ] || t=$(sed -nE "s/.*[[:space:]](title='([^']*)'|'title=([^']*)').*/\2\3/p" <<<"$cmd")
  printf '%s' "$t"
}

gate_gh() {
  local pr method api_write=0 pulls stripped
  pr="${gh_re}pr[[:space:]]+"
  grep -qE '(^|[^[:alnum:]_-])gh[[:space:]]' <<<"$cmd" || return 0
  if tokenize "$cmd"; then
    gh_scan
  else
    # An unterminated quote or substitution: read the line with its quoted
    # segments removed, heredoc bodies and comments included.
    stripped=$(printf '%s' "$cmd" | sed -E "s/'[^']*'//g; s/\"[^\"]*\"//g")
    GH_VIEW=$stripped; GH_API=""; GH_TITLE=""; GH_API_TITLE=""
    if grep -qE "${gh_re}api$end_re" <<<"$stripped"; then
      GH_API=$(printf '%s' "$cmd" | tr -d "'\"")
      GH_API_TITLE=$(api_title)
    fi
    GH_TITLE=$(printf '%s' "$cmd" | sed -nE 's/.*(--title|-t)[[:space:]]+"([^"]*)".*/\2/p')
    [ -n "$GH_TITLE" ] || GH_TITLE=$(printf '%s' "$cmd" | sed -nE "s/.*(--title|-t)[[:space:]]+'([^']*)'.*/\2/p")
  fi

  # gh pr create (and its alias gh pr new) requires --draft.
  if grep -qE "${pr}(create|new)$end_re" <<<"$GH_VIEW"; then
    grep -qE '(^|[[:space:]])(--draft|-d)([[:space:]]|$)' <<<"$GH_VIEW" \
      || block "Agent-opened PRs must use --draft. Only humans publish ready-for-review PRs."
  fi

  # gh pr create / new / edit --title: the PR-title lint.
  if grep -qE "${pr}(create|new|edit)$end_re" <<<"$GH_VIEW"; then
    lint_title "$GH_TITLE"
  fi

  # gh pr ready is humans-only.
  if grep -qE "${pr}ready$end_re" <<<"$GH_VIEW"; then
    block "Only humans flip drafts to ready-for-review. Ask the user."
  fi

  # gh pr edit --add-reviewer / --add-assignee is humans-only.
  if grep -qE "${pr}edit$end_re" <<<"$GH_VIEW" \
    && grep -qE '(^|[[:space:]])--(add|remove)-(reviewer|assignee)([[:space:]=]|$)' <<<"$GH_VIEW"; then
    block "Adding/removing reviewers is humans-only. Re-trigger bot reviewers via PR comment mention (e.g. @coderabbitai review)."
  fi

  # gh pr review --approve / --request-changes are humans-only.
  if grep -qE "${pr}review$end_re" <<<"$GH_VIEW"; then
    grep -qE '(^|[[:space:]])(--approve|-a)([[:space:]]|$)' <<<"$GH_VIEW" \
      && block "Only humans approve PRs."
    grep -qE '(^|[[:space:]])(--request-changes|-r)([[:space:]]|$)' <<<"$GH_VIEW" \
      && block "Agents post inline review comments instead of --request-changes."
  fi

  # gh api: the REST and GraphQL forms of the actions above, and of a merge.
  # Like the rest of the gate this reads the command line only: fields sent
  # through --input or `-F field=@file` go unseen. A call writes when it names
  # a method other than GET or, naming none, sends fields (-f, -F, --field,
  # --raw-field, --input), which make gh POST.
  [ -n "$GH_API" ] || return 0
  method=$(sed -nE 's/.*[[:space:]](-X|--method)[[:space:]=]*([A-Za-z]+).*/\2/p' <<<"$GH_API" | tr '[:lower:]' '[:upper:]')
  if [ -n "$method" ]; then
    case $method in GET) ;; *) api_write=1 ;; esac
  elif grep -qE '[[:space:]](-[fF]|--field|--raw-field|--input)' <<<"$GH_API"; then
    api_write=1
  fi
  pulls='repos/[^/[:space:]]+/[^/[:space:]]+/pulls'
  if [ "$api_write" = 1 ]; then
    # POST or DELETE …/requested_reviewers: --add-reviewer / --remove-reviewer.
    grep -q 'requested_reviewers' <<<"$GH_API" \
      && block "Reviewer-write requests are humans-only. Re-trigger bot reviewers via PR comment mention."
    # PUT …/pulls/N/merge.
    grep -qE "$pulls/[^/[:space:]]+/merge([?[:space:]]|\$)" <<<"$GH_API" && block "$merge_msg"
    # POST …/pulls is gh pr create: a draft.
    if grep -qE "$pulls([?[:space:]]|\$)" <<<"$GH_API"; then
      grep -qE '(^|[[:space:]])draft=true([[:space:]]|$)' <<<"$GH_API" \
        || block "Agent-opened PRs must be drafts; through the API, send -F draft=true. Only humans publish ready-for-review PRs."
    fi
    # POST …/pulls or PATCH …/pulls/N with a title: the PR-title lint.
    if grep -qE "$pulls(/[^/[:space:]?]+)?([?[:space:]]|\$)" <<<"$GH_API"; then
      lint_title "$GH_API_TITLE"
    fi
  fi
  # …/pulls/N/reviews (or a pending review's /events) with a verdict.
  if grep -qE "$pulls/[^[:space:]]*/reviews" <<<"$GH_API"; then
    grep -qiE '(^|[[:space:]])event=APPROVE([[:space:]]|$)' <<<"$GH_API" && block "Only humans approve PRs."
    grep -qiE '(^|[[:space:]])event=REQUEST_CHANGES([[:space:]]|$)' <<<"$GH_API" \
      && block "Agents post inline review comments instead of --request-changes."
  fi
  # The GraphQL mutations that do the same.
  if grep -qE '[[:space:]]graphql([[:space:]]|$)' <<<"$GH_API"; then
    grep -q 'markPullRequestReadyForReview' <<<"$GH_API" \
      && block "Only humans flip drafts to ready-for-review. Ask the user."
    grep -qE 'mergePullRequest|enablePullRequestAutoMerge' <<<"$GH_API" && block "$merge_msg"
    grep -q 'requestReviews' <<<"$GH_API" \
      && block "Reviewer-write requests are humans-only. Re-trigger bot reviewers via PR comment mention."
    if grep -qE '(add|submit)PullRequestReview' <<<"$GH_API"; then
      grep -q 'APPROVE' <<<"$GH_API" && block "Only humans approve PRs."
      grep -q 'REQUEST_CHANGES' <<<"$GH_API" \
        && block "Agents post inline review comments instead of --request-changes."
    fi
    if grep -q 'createPullRequest' <<<"$GH_API"; then
      grep -qE 'draft:[[:space:]]*true' <<<"$GH_API" \
        || block "Agent-opened PRs must be drafts; in GraphQL, send draft: true. Only humans publish ready-for-review PRs."
    fi
  fi
  return 0
}
merge_msg="Agents don't merge PRs; a person does. Ask the user."

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
# redirections, heredocs, $(…) and `…`) without expanding or running anything.
# A word that carries an expansion is flagged dynamic: its value is unknowable
# here, so a push that depends on one fails closed. Fills TK_VAL / TK_DYN /
# TK_OP, TK_IN / TK_IN_AT with each heredoc body and here-string and the token
# index of the simple command that reads it, and TK_SUBS with the text of every
# $(…) and `…`, wherever its word went (an argument, a here-string, a redirect
# target), each quoted part inside a $(…) wrapped in \002…\003; returns 1 on an
# unterminated quote or substitution.

tk_flush() {
  if [ "$_inw" = 1 ]; then
    # One pass with tr: bash 3.2's ${_w//…} is quadratic in the marker count.
    case $_w in *$'\002'* | *$'\003'*) _w=$(printf '%s.' "$_w" | tr -d '\002\003'); _w=${_w%.} ;; esac
    case $_skip in
      0) TK_VAL+=("$_w"); TK_DYN+=("$_dyn"); TK_OP+=(0) ;;
      2) TK_IN+=("$_w"); TK_IN_AT+=("$_cmd0") ;;
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
      '$') tk_dollar || return 1 ;;
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

tk_dollar() {
  local rest part s0=${#_w}
  _inw=1; _dyn=1
  case ${_s:_i+1:1} in
    '(')
      _w+='$'; _i=$((_i + 1)); _insub=$((_insub + 1))
      tk_paren || return 1
      _insub=$((_insub - 1)); TK_SUBS+=("${_w:s0}") ;;
    '{')
      rest=${_s:_i}
      case $rest in *'}'*) ;; *) return 1 ;; esac
      part=${rest%%'}'*}
      # shellcheck disable=SC2016 # a literal $( or backtick in ${…:-…}
      case $part in *'$('* | *'`'*) TK_SUBS+=("$part}") ;; esac
      _w+="$part}"; _i=$((_i + ${#part} + 1)) ;;
    *) _w+='$'; _i=$((_i + 1)) ;;
  esac
}

tk_backtick() {
  local c s0=${#_w}
  _inw=1; _dyn=1; _w+='`'; _i=$((_i + 1))
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      '`') _w+=$c; _i=$((_i + 1)); TK_SUBS+=("${_w:s0}"); return 0 ;;
      \\) _w+=${_s:_i:2}; _i=$((_i + 2)) ;;
      *) _w+=$c; _i=$((_i + 1)) ;;
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
      '(') depth=$((depth + 1)); _w+=$c; _i=$((_i + 1)) ;;
      ')')
        depth=$((depth - 1)); _w+=$c; _i=$((_i + 1))
        [ "$depth" -eq 0 ] && return 0 ;;
      \\) _w+=${_s:_i:2}; _i=$((_i + 2)) ;;
      "'") tk_squote || return 1 ;;
      '"') tk_dquote || return 1 ;;
      '`') tk_backtick || return 1 ;;
      '$')
        if [ "${_s:_i+1:1}" = "'" ]; then tk_ansi || return 1; else _w+=$c; _i=$((_i + 1)); fi ;;
      '<')
        if [ "${_s:_i:3}" = '<<<' ]; then
          _w+='<<< '; _i=$((_i + 3))
        else
          tk_heredoc_op -1 || { _w+=$c; _i=$((_i + 1)); }
        fi ;;
      $'\n') _w+=$c; _i=$((_i + 1)); tk_heredoc_bodies ;;
      *) _w+=$c; _i=$((_i + 1)) ;;
    esac
  done
  return 1
}

# tk_heredoc_op <at>: at "<<" or "<<-", register the heredoc's delimiter (its
# body starts after the next newline) and the token index of the command that
# reads it (-1 inside a substitution: the body isn't kept). Returns 1,
# consuming nothing, elsewhere.
tk_heredoc_op() {
  local strip=0 c d=""
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
      "'" | '"' | \\) ;;
      *) d+=$c ;;
    esac
    _i=$((_i + 1))
  done
  _hd_delim+=("$d"); _hd_strip+=("$strip"); _hd_at+=("$1")
}

# tk_heredoc_bodies: just past a newline, step over the bodies of pending
# heredocs, keeping each top-level one in TK_IN. Inside a $(…), a line that is
# the delimiter followed by ")" (`EOF)"`) also ends the body, and the ")" closes
# the substitution.
tk_heredoc_bodies() {
  local k rest line start
  [ "${#_hd_delim[@]}" -gt 0 ] || return 0
  for ((k = 0; k < ${#_hd_delim[@]}; k++)); do
    start=$_i
    while [ "$_i" -lt "$_n" ]; do
      rest=${_s:_i}
      line=${rest%%$'\n'*}
      _i=$((_i + ${#line} + 1))
      [ "${_hd_strip[k]}" = 1 ] && line=${line#"${line%%[!$'\t']*}"}
      [ "$line" = "${_hd_delim[k]}" ] && break
      if [ "${_hd_at[k]}" = -1 ] && [ "${line#"${_hd_delim[k]})"}" != "$line" ]; then
        _i=$((_i - 1 - ${#line} + ${#_hd_delim[k]})); break
      fi
    done
    if [ "${_hd_at[k]}" -ge 0 ]; then
      TK_IN+=("${_s:start:_i-start}"); TK_IN_AT+=("${_hd_at[k]}")
    fi
  done
  _hd_delim=(); _hd_strip=(); _hd_at=()
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
  TK_VAL=(); TK_DYN=(); TK_OP=(); TK_IN=(); TK_IN_AT=(); TK_SUBS=(); _hd_delim=(); _hd_strip=(); _hd_at=()
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      ' ' | $'\t') tk_flush; _i=$((_i + 1)) ;;
      $'\n') tk_op ';'; _i=$((_i + 1)); tk_heredoc_bodies ;;
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
      '(' | ')') tk_op "$c"; _i=$((_i + 1)) ;;
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
# shellcheck disable=SC2016
gh_re='(^|[[:space:];|&(`])gh[[:space:]]+'
head_re=${git_re}'(commit|merge|rebase|reset|checkout|switch|cherry-pick|revert|am|pull)'$end_re
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
squashed=$(printf '%s' "${cmd//$'\\\n'/}" | sed -E "s/'[^']*'/Q/g; s/\"[^\"]*\"/Q/g")
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

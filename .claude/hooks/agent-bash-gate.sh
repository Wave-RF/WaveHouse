#!/usr/bin/env bash
# Agent PR workflow gate (PreToolUse Bash). Catches accidental violations of
# rules that have no human analog:
#   - gh pr create without --draft
#   - gh pr ready
#   - gh pr edit --add-reviewer / --add-assignee
#   - gh api .../requested_reviewers (write verbs)
#   - gh pr review --approve / --request-changes
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

# Strip quoted segments so commands that mention a blocked pattern in a
# string don't false-positive. Doesn't cover heredocs — pass long bodies via
# `-F <file>` if needed.
stripped=$(printf '%s' "$cmd" | sed -E "s/'[^']*'//g; s/\"[^\"]*\"//g")

# The gh checks resolve repo scripts from the session's checkout; the push
# check resolves everything from the worktree the push runs in.
project_dir="${CLAUDE_PROJECT_DIR:-.}"

# gh pr create requires --draft.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+create\b'; then
  printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--draft|-d)\b' \
    || block "Agent-opened PRs must use --draft. Only humans publish ready-for-review PRs."
fi

# gh pr create / gh pr edit --title: validate the title against the SAME
# Conventional-Commits rule the required `CI` check's `PR title` job enforces
# (scripts/lint-pr-title.sh is the shared rule) — so a too-long or wrong-format
# title is caught locally BEFORE the PR exists, not after the required check
# fails. Extract the quoted --title/-t value from the ORIGINAL command (the
# `stripped` copy has quotes removed). Fail-open: if no title is parseable
# (--fill, interactive, unquoted) or the validator is missing, fall through to
# the CI check rather than block a create we can't confidently judge.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+(create|edit)\b'; then
  pr_title=$(printf '%s' "$cmd" | sed -nE 's/.*(--title|-t)[[:space:]]+"([^"]*)".*/\2/p')
  [ -z "$pr_title" ] && pr_title=$(printf '%s' "$cmd" | sed -nE "s/.*(--title|-t)[[:space:]]+'([^']*)'.*/\2/p")
  if [ -n "$pr_title" ] && [ -x "$project_dir/scripts/lint-pr-title.sh" ]; then
    if ! reason=$("$project_dir/scripts/lint-pr-title.sh" "$pr_title" 2>&1); then
      block "$reason"
    fi
  fi
fi

# gh pr ready is humans-only.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+ready\b'; then
  block "Only humans flip drafts to ready-for-review. Ask the user."
fi

# gh pr edit --add-reviewer / --add-assignee is humans-only.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+edit\b' \
   && printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])--(add|remove)-(reviewer|assignee)\b'; then
  block "Adding/removing reviewers is humans-only. Re-trigger bot reviewers via PR comment mention (e.g. @coderabbitai review)."
fi

# gh api .../requested_reviewers write verbs (API form of --add-reviewer).
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+api\b' \
   && printf '%s\n' "$stripped" | grep -qE 'requested_reviewers' \
   && printf '%s\n' "$stripped" | grep -qE '(-X[[:space:]]*(POST|PUT|PATCH)|--method[[:space:]]*(POST|PUT|PATCH)|[[:space:]]-f[[:space:]]+reviewers=|[[:space:]]-F[[:space:]]+reviewers=)'; then
  block "Reviewer-write requests are humans-only. Re-trigger bot reviewers via PR comment mention."
fi

# gh pr review --approve / --request-changes are humans-only.
if printf '%s\n' "$stripped" | grep -qE '(^|[[:space:];|&]+)gh[[:space:]]+pr[[:space:]]+review\b'; then
  printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--approve|-a)\b' \
    && block "Only humans approve PRs."
  printf '%s\n' "$stripped" | grep -qE '(^|[[:space:]])(--request-changes|-r)\b' \
    && block "Agents post inline review comments instead of --request-changes."
fi

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
# runs or what it publishes, it blocks and says why. That includes a push that
# follows a HEAD-moving git command (`git commit … && git push`): the gate runs
# before the command line does, so it would judge the commit before the move.
#
# It gates any commit with a delta against the base (local main, else
# origin/main), NOT only commits on a branch with an open PR: the agent flow is
# push-the-branch THEN open the draft PR, so keying on PR state would let the
# first push — the one that publishes the diff — skip review. A commit already
# on the base (pushing main itself) has nothing for the reviewers. The universal
# .githooks/pre-push handles ci-passed for everyone.

reviewers_script="scripts/pre-push-reviewers.sh"

# ── Shell tokenizer ──
# Splits a command line the way the shell would (quotes, escapes, operators,
# redirections, heredocs, $(…) and `…`) without expanding or running anything.
# A word that carries an expansion is flagged dynamic: its value is unknowable
# here, so a push that depends on one fails closed. Fills TK_VAL / TK_DYN /
# TK_OP; returns 1 on an unterminated quote or substitution.

tk_flush() {
  if [ "$_inw" = 1 ]; then
    if [ "$_skip" = 0 ]; then
      TK_VAL+=("$_w"); TK_DYN+=("$_dyn"); TK_OP+=(0)
    fi
    _skip=0
  fi
  _w=""; _inw=0; _dyn=0
}
tk_op() { tk_flush; TK_VAL+=("$1"); TK_DYN+=(0); TK_OP+=(1); }

tk_squote() {
  local rest=${_s:_i+1} part
  case $rest in *"'"*) ;; *) return 1 ;; esac
  part=${rest%%"'"*}
  _w+=$part; _inw=1; _i=$((_i + ${#part} + 2))
}

tk_dquote() {
  local c c2
  _inw=1; _i=$((_i + 1))
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      '"') _i=$((_i + 1)); return 0 ;;
      \\)
        c2=${_s:_i+1:1}
        case $c2 in '$' | '`' | '"' | \\) _w+=$c2 ;; $'\n') ;; *) _w+=$c$c2 ;; esac
        _i=$((_i + 2)) ;;
      '$') tk_dollar || return 1 ;;
      '`') tk_backtick || return 1 ;;
      *) _w+=$c; _i=$((_i + 1)) ;;
    esac
  done
  return 1
}

tk_dollar() {
  local rest part
  _inw=1; _dyn=1
  case ${_s:_i+1:1} in
    '(') _w+='$'; _i=$((_i + 1)); tk_paren ;;
    '{')
      rest=${_s:_i}
      case $rest in *'}'*) ;; *) return 1 ;; esac
      part=${rest%%'}'*}
      _w+="$part}"; _i=$((_i + ${#part} + 1)) ;;
    *) _w+='$'; _i=$((_i + 1)) ;;
  esac
}

tk_backtick() {
  local c
  _inw=1; _dyn=1; _w+='`'; _i=$((_i + 1))
  while [ "$_i" -lt "$_n" ]; do
    c=${_s:_i:1}
    case $c in
      '`') _w+=$c; _i=$((_i + 1)); return 0 ;;
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
      '<')
        if [ "${_s:_i:3}" = '<<<' ]; then
          _w+='<<<'; _i=$((_i + 3))
        else
          tk_heredoc_op || { _w+=$c; _i=$((_i + 1)); }
        fi ;;
      $'\n') _w+=$c; _i=$((_i + 1)); tk_heredoc_bodies ;;
      *) _w+=$c; _i=$((_i + 1)) ;;
    esac
  done
  return 1
}

# tk_heredoc_op: at "<<" or "<<-", register the heredoc's delimiter (its body
# starts after the next newline). Returns 1, consuming nothing, elsewhere.
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
  _hd_delim+=("$d"); _hd_strip+=("$strip")
}

# tk_heredoc_bodies: just past a newline, skip the bodies of pending heredocs.
tk_heredoc_bodies() {
  local k rest line
  [ "${#_hd_delim[@]}" -gt 0 ] || return 0
  for ((k = 0; k < ${#_hd_delim[@]}; k++)); do
    while [ "$_i" -lt "$_n" ]; do
      rest=${_s:_i}
      line=${rest%%$'\n'*}
      _i=$((_i + ${#line} + 1))
      [ "${_hd_strip[k]}" = 1 ] && line=${line#"${line%%[!$'\t']*}"}
      [ "$line" = "${_hd_delim[k]}" ] && break
    done
  done
  _hd_delim=(); _hd_strip=()
}

# tk_redirect: at "<" or ">" (or the ">" of "&>"). An fd number glued to the
# operator (2>&1) belongs to it, and the word after it is a target, not an
# argument.
tk_redirect() {
  if [ "$_inw" = 1 ]; then
    case $_w in '' | *[!0-9]*) tk_flush ;; *) _w=""; _inw=0; _dyn=0 ;; esac
  fi
  tk_heredoc_op && return 0
  if [ "${_s:_i:3}" = '<<<' ]; then
    _i=$((_i + 3))
  else
    _i=$((_i + 1))
    while case ${_s:_i:1} in '>' | '&' | '|') true ;; *) false ;; esac; do _i=$((_i + 1)); done
  fi
  _skip=1
}

tokenize() {
  local LC_ALL=C c c2 rest line
  _s=$1; _n=${#1}; _i=0
  _w=""; _inw=0; _dyn=0; _skip=0
  TK_VAL=(); TK_DYN=(); TK_OP=(); _hd_delim=(); _hd_strip=()
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
      '$') tk_dollar || return 1 ;;
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
# simple command runs in (`why` is set once that can't be known), and gate
# every `git push` found.
walk_commands() {
  local dir=$1 why="" i=0 n=${#TK_VAL[@]} top cstart=0
  local -a words=() dyns=() sdir=() swhy=()
  while [ "$i" -le "$n" ]; do
    if [ "$i" -eq "$n" ] || [ "${TK_OP[i]}" = 1 ]; then
      [ "${#words[@]}" -gt 0 ] && run_simple
      words=(); dyns=()
      if [ "$i" -lt "$n" ]; then
        case ${TK_VAL[i]} in
          '(') sdir+=("$dir"); swhy+=("$why") ;;
          ')')
            if [ "${#sdir[@]}" -gt 0 ]; then
              top=$((${#sdir[@]} - 1))
              dir=${sdir[top]}; why=${swhy[top]}
              unset "sdir[top]" "swhy[top]"
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

# run_simple: one simple command (words/dyns of walk_commands); follows a cd
# and gates a git push.
run_simple() {
  local k=0 j nw=${#words[@]} w a d dwhy sub="" gitenv=""
  while [ "$k" -lt "$nw" ]; do
    w=${words[k]}
    if is_assignment "$w"; then
      case ${w%%=*} in GIT_DIR | GIT_WORK_TREE) gitenv="${w%%=*}=" ;; esac
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
    [ -z "$gitenv" ] || why="\`${gitenv}\`"
    return 0
  fi

  case ${words[k]} in
    export | declare | typeset)
      for ((j = k + 1; j < nw; j++)); do
        case ${words[j]} in GIT_DIR | GIT_DIR=* | GIT_WORK_TREE | GIT_WORK_TREE=*) why="\`${words[k]} ${words[j]%%=*}\`" ;; esac
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
    git)
      d=$dir; dwhy=$why
      [ -z "$gitenv" ] || dwhy="\`${gitenv}\`"
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
              case $a in /* | \~ | \~/*) [ -n "$gitenv" ] || dwhy="" ;; esac
              d=$(join_path "$d" "$a")
            fi ;;
          -c | --namespace) k=$((k + 1)) ;;
          --git-dir | --work-tree) dwhy="\`git $w\`"; k=$((k + 1)) ;;
          --git-dir=* | --work-tree=*) dwhy="\`git ${w%%=*}\`" ;;
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
            gate_push "$d" "$dwhy" $((k + 1))
          fi ;;
      esac ;;
  esac
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
  for ref in main origin/main; do
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

# `git [global options] push`, as a regex over a line of words.
# shellcheck disable=SC2016 # the backticks are literal: a push inside `…`
push_re='(^|[[:space:];|&(`])git([[:space:]]+-[^[:space:]]+([[:space:]]+[^-[:space:];|&][^[:space:];|&]*)?)*[[:space:]]+push([[:space:]]|$|[;|&)`])'

# unfollowed_push: true when a simple command the walk didn't gate (FOLLOWED
# holds the first-token index of each one it did) holds a `git … push`: behind
# a wrapper it doesn't know (`timeout 60 git push`) or inside a command
# substitution. A push only mentioned in a quoted string, a heredoc body or a
# comment is one word or no token at all, so it doesn't count.
unfollowed_push() {
  local i n=${#TK_VAL[@]} w line="" dyn="" start=0
  for ((i = 0; i <= n; i++)); do
    if [ "$i" -eq "$n" ] || [ "${TK_OP[i]}" = 1 ]; then
      case $FOLLOWED in
        *" $start "*) ;;
        *) printf '%s\n%s\n' "$line" "$dyn" | grep -qE "$push_re" && return 0 ;;
      esac
      line=""; dyn=""; start=$((i + 1))
      continue
    fi
    w=${TK_VAL[i]}
    if [ "${TK_DYN[i]}" = 1 ]; then
      dyn="$dyn$w
"
      w=Q
    fi
    case $w in *[[:space:]]*) w=Q ;; esac
    line="$line $w"
  done
  return 1
}

# Cheap filter first, so most commands are never tokenized: quoted strings on a
# line become a placeholder word (`git -C "<path>" stash push` reads as a
# stash). It may still match a push that is only mentioned, say in a heredoc
# body; the tokenizer then finds no push and lets the command through.
squashed=$(printf '%s' "${cmd//$'\\\n'/}" | sed -E "s/'[^']*'/Q/g; s/\"[^\"]*\"/Q/g")
if printf '%s\n' "$squashed" | grep -qE "$push_re"; then
  hook_cwd=$(printf '%s' "$input" | jq -r '.cwd // empty' 2>/dev/null)
  [ -n "$hook_cwd" ] || block "can't tell which directory this \`git push\` runs in: the hook payload has no cwd."
  tokenize "$cmd" || block "can't parse this command (an unterminated quote or substitution?), so can't tell what it pushes."
  FOLLOWED=" "
  HEAD_MOVER=""
  walk_commands "$hook_cwd"
  if unfollowed_push; then
    block "this command runs \`git push\` in a form the gate can't follow (behind a wrapper such as timeout, sudo or env -C, or in a command substitution). Run \`git push\` directly, or as \`git -C <worktree> push …\`."
  fi
fi

exit 0

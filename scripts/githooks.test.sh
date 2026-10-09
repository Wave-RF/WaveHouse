#!/usr/bin/env bash
# Behavioral test for the git hooks in .githooks/. Builds a scratch repository
# whose core.hooksPath names this checkout's .githooks/ by absolute path, the
# setup that runs them in every linked worktree, and commits and pushes from
# worktrees with and without code: a worktree whose branch has no Makefile must
# commit and push its own branch (#678), while code still needs its markers and
# a missing scripts/ci-marker.sh is reported, not swallowed. The repository's
# Makefile is a stub whose `verify` only says it ran. Markers come only from
# scripts/ci-marker.sh, never from the test itself. Run by `make verify`
# (target: test-githooks). Needs git and make; no network.

set -uo pipefail

# The git hooks run under whichever bash `#!/usr/bin/env bash` finds first.
# macOS ships bash 3.2 as /bin/bash, so when that is another bash the suite runs
# a second time with it first on PATH (review-gate.test.sh does the same).
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

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1 # repo root (scripts/..)
root=$PWD
fails=0

scratch=$(mktemp -d) || exit 1
trap 'rm -rf "$scratch"' EXIT
scratch=$(cd "$scratch" && pwd -P)

# Run from a git hook (pre-commit runs make verify), GIT_DIR and friends point
# at this checkout: a `git init` here would re-initialize it instead of creating
# the scratch repository. Clear them, then refuse to write anything unless git
# sees the scratch directory as outside any repository and puts the new
# repository exactly where it was asked to. Every git call below names its
# directory with -C. The outer make's MAKEFLAGS would hand the stub make its
# jobserver, so clear those too.
# shellcheck disable=SC2046 # one variable name per word
unset $(git -C "$scratch" rev-parse --local-env-vars) MAKEFLAGS MAKELEVEL MFLAGS
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
if git -C "$scratch" rev-parse --git-dir >/dev/null 2>&1; then
  echo "githooks test: git resolves $scratch to an existing repository; refusing to run." >&2
  exit 1
fi
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@example.com GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@example.com

repo=$scratch/repo
git -C "$scratch" init -q -b main "$repo"
if [ "$(git -C "$repo" rev-parse --absolute-git-dir 2>/dev/null)" != "$repo/.git" ]; then
  echo "githooks test: git did not create $repo/.git; refusing to go on." >&2
  exit 1
fi

ok() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n' "$1" >&2
  [ -n "${2:-}" ] && printf '%s\n' "$2" | sed 's/^/         /' >&2
  fails=$((fails + 1))
}

# expect <name> <want-exit> <pattern>... -- <dir> <git args>: run git in <dir>
# and check its exit code and combined output (a hook's output reaches git's
# stderr). A pattern is a fixed string the output must contain, or, led by
# `!`, one it must not.
expect() {
  local name=$1 want=$2 got out p bad=""
  shift 2
  local pats=()
  while [ "$1" != -- ]; do pats+=("$1"); shift; done
  shift
  local dir=$1
  shift
  out=$(git -C "$dir" "$@" 2>&1)
  got=$?
  [ "$got" = "$want" ] || bad="exit $got, want $want"
  for p in "${pats[@]}"; do
    case $p in
      !*) case $out in *"${p#!}"*) bad="${bad:+$bad; }output has '${p#!}'" ;; esac ;;
      *) case $out in *"$p"*) ;; *) bad="${bad:+$bad; }output lacks '$p'" ;; esac ;;
    esac
  done
  if [ -z "$bad" ]; then ok "$name"; else fail "$name: $bad" "$out"; fi
}

# in_wt <dir> <command>…: run a command from the top of the scratch worktree <dir>,
# or stop if git doesn't see <dir> as one: scripts/ci-marker.sh writes its
# markers into whichever worktree it runs in.
in_wt() {
  local dir=$1
  shift
  if [ "$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null)" != "$dir" ]; then
    echo "githooks test: $dir is not a scratch worktree; refusing to go on." >&2
    exit 1
  fi
  (cd "$dir" && "$@")
}

# ── Scratch repository ──
#   repo/  — main worktree on main: a stub Makefile and the real marker and
#            classifier scripts
#   code/  — linked worktree on feat
#   state/ — linked worktree on the orphan branch `state`: three text files,
#            no Makefile, no scripts
mkdir -p "$repo/scripts"
cp scripts/ci-marker.sh scripts/classify-paths.sh "$repo/scripts/"
# shellcheck disable=SC2016 # make's $$, not the shell's
printf 'verify:\n\t@echo "stub verify ran"\n\t@test -z "$$STUB_VERIFY_FAIL"\n' > "$repo/Makefile"
printf 'tmp/\n' > "$repo/.gitignore"
git -C "$repo" add -A
git -C "$repo" commit -q -m base
code=$scratch/code
state=$scratch/state
git -C "$repo" worktree add -q -b feat "$code" main
git -C "$repo" worktree add -q --detach "$state" main
git -C "$state" checkout -q --orphan state
in_wt "$state" git rm -rqf .
printf '{}\n' > "$state/state.json"
git -C "$state" add state.json
git -C "$state" commit -q -m 'state: first run'
# The remote has main alone, so every branch pushed below is new to it.
git -C "$scratch" init -q --bare remote.git
git -C "$scratch/remote.git" fetch -q "$repo" main:main
git -C "$repo" remote add origin "$scratch/remote.git"
git -C "$repo" fetch -q origin

# Hooks from here on: this checkout's, by absolute path, in every worktree.
git -C "$repo" config core.hooksPath "$root/.githooks"

# ── pre-commit ──
printf 'run 2\n' > "$state/runs.log"
git -C "$state" add runs.log
expect "pre-commit: a worktree with no Makefile commits" 0 "no Makefile" "!make verify" -- "$state" commit -q -m 'state: run 2'

printf 'package x\n' > "$code/x.go"
git -C "$code" add x.go
expect "pre-commit: code with no verify marker runs make verify" 0 "stub verify ran" "!missing" "!cached" -- "$code" commit -q -m x

printf 'package y\n' > "$code/y.go"
git -C "$code" add y.go
in_wt "$code" env -u CI scripts/ci-marker.sh write-verify
expect "pre-commit: a verify marker for the tree skips make verify" 0 "(cached)" "!stub verify ran" -- "$code" commit -q -m y

printf 'package z\n' > "$code/z.go"
git -C "$code" add z.go
export STUB_VERIFY_FAIL=1
expect "pre-commit: a failing make verify blocks the commit" 1 "stub verify ran" "'make verify' failed" -- "$code" commit -q -m z
unset STUB_VERIFY_FAIL
git -C "$code" commit -q -m z >/dev/null 2>&1 || fail "setup: commit z"

printf '#!/usr/bin/env bash\necho "marker store unreadable" >&2\nexit 3\n' > "$code/scripts/ci-marker.sh"
printf 'package w\n' > "$code/w.go"
git -C "$code" add -A
expect "pre-commit: a failing marker script is reported, then make verify runs" 0 "has-verify-marker failed" "marker store unreadable" "stub verify ran" -- "$code" commit -q -m w
git -C "$code" rm -q scripts/ci-marker.sh
expect "pre-commit: a missing marker script is reported, then make verify runs" 0 "scripts/ci-marker.sh is missing" "stub verify ran" -- "$code" commit -q -m 'drop the marker script'

# ── pre-push ──
expect "pre-push: a branch with no Makefile pushes" 0 "has no Makefile" -- "$state" push -q origin state
expect "pre-push: code pushed from a worktree without the marker script is blocked" 1 "scripts/ci-marker.sh is missing" -- "$state" push -q origin main:fresh
expect "pre-push: code pushed from a worktree whose marker script is gone is blocked" 1 "scripts/ci-marker.sh is missing" -- "$code" push -q origin feat
git -C "$repo" switch -q -c feat2 main
printf 'package v\n' > "$repo/v.go"
git -C "$repo" add v.go
git -C "$repo" commit -q -m v >/dev/null 2>&1 || fail "setup: commit v"
expect "pre-push: code with no make ci marker is blocked" 1 "'make ci' has not been run" -- "$repo" push -q origin feat2
in_wt "$repo" env -u CI scripts/ci-marker.sh write
expect "pre-push: code with a make ci marker for its tree pushes" 0 "!pre-push:" -- "$repo" push -q origin feat2

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "githooks: all cases passed"

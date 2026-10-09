#!/usr/bin/env bash
# Behavioral test for scripts/ci/remote-ci.sh (`make ci-remote`) and the
# provenance line .githooks/pre-push prints for its marker. Runs the script in
# a scratch repository with stand-in `ssh` and `docker` commands on PATH: they
# record how they were called, then run the host side for real in a scratch
# home against a stub `make` and `uname`, so no host, network or Docker is
# needed. Markers come only from the script under test. Run by `make verify`
# (target: test-remote-ci). Needs git.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1 # repo root (scripts/ci/../..)
fails=0

scratch=$(mktemp -d) || exit 1
trap 'rm -rf "$scratch"' EXIT
scratch=$(cd "$scratch" && pwd -P)

# Run from a git hook (pre-commit runs make verify), GIT_DIR and friends point
# at this checkout, so a `git init` here would re-initialize it. Clear them,
# refuse to run unless git sees the scratch directory as outside any
# repository, and name every repository with -C.
# shellcheck disable=SC2046 # one variable name per word
unset $(git -C "$scratch" rev-parse --local-env-vars)
# Hiding untracked files from `git status` leaves the scripts' own
# --untracked-files=normal as the only thing that finds them.
printf '[status]\n\tshowUntrackedFiles = no\n' > "$scratch/gitconfig"
export GIT_CONFIG_GLOBAL=$scratch/gitconfig GIT_CONFIG_NOSYSTEM=1
if git -C "$scratch" rev-parse --git-dir >/dev/null 2>&1; then
  echo "remote-ci test: git resolves $scratch to an existing repository; refusing to run." >&2
  exit 1
fi
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@example.com GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@example.com

repo=$scratch/repo
git -C "$scratch" init -q -b main "$repo"
if [ "$(git -C "$repo" rev-parse --absolute-git-dir 2>/dev/null)" != "$repo/.git" ]; then
  echo "remote-ci test: git did not create $repo/.git; refusing to go on." >&2
  exit 1
fi
mkdir -p "$repo/scripts/ci" "$repo/.githooks"
cp scripts/ci-marker.sh scripts/classify-paths.sh "$repo/scripts/"
cp scripts/ci/remote-ci.sh "$repo/scripts/ci/"
cp .githooks/pre-push "$repo/.githooks/"
echo 'tmp/' > "$repo/.gitignore"
# A code repository has a Makefile; the pre-push hook passes a commit without one.
printf 'ci:\n' > "$repo/Makefile"
git -C "$repo" add -A
git -C "$repo" commit -q -m base
git -C "$repo" update-ref refs/remotes/origin/main HEAD
echo 'package main' > "$repo/main.go"
git -C "$repo" add main.go
git -C "$repo" commit -q -m code

# ── Stand-ins ──
#   bin/ssh, bin/docker  record their arguments, then run the host side in
#                        a process group of its own, as sshd would
#   host-bin/make        records each call with the tree of the checkout it
#                        runs in; `make ci` exits with the code in make-exit
#                        after running the during-ci script, if there is one
#   host-bin/uname       a Linux amd64 host
#   host-bin/git         git, except that a checkout lands on the commit
#                        named in wrong-commit, if there is one
mkdir -p "$scratch/bin" "$scratch/host-bin"
for cmd in ssh docker; do
  cat > "$scratch/bin/$cmd" << EOF
#!/usr/bin/env bash
echo "$cmd \$*" >> "$scratch/transport.log"
cd "$scratch/home" || exit 1
set -m
HOME="$scratch/home" PATH="$scratch/host-bin:\$PATH" bash -s &
wait \$!
EOF
done
cat > "$scratch/host-bin/make" << EOF
#!/usr/bin/env bash
echo "make \$* in \$(git rev-parse 'HEAD^{tree}')" >> "$scratch/make.log"
[ "\$1" = ci ] || exit 0
echo "stub make ci output"
[ ! -f "$scratch/during-ci" ] || bash "$scratch/during-ci"
exit "\$(cat "$scratch/make-exit")"
EOF
printf '#!/bin/sh\necho "Linux x86_64"\n' > "$scratch/host-bin/uname"
cat > "$scratch/host-bin/git" << EOF
#!/usr/bin/env bash
if [ -f "$scratch/wrong-commit" ] && [[ " \$* " == *" checkout "* ]]; then
  set -- "\${@:1:\$#-1}" "\$(cat "$scratch/wrong-commit")"
fi
exec "$(command -v git)" "\$@"
EOF
chmod +x "$scratch/bin/"* "$scratch/host-bin/"*

ok() { printf '  ok   %s\n' "$1"; }
fail() {
  printf '  FAIL %s\n' "$1" >&2
  [ -z "${2:-}" ] || printf '%s\n' "$2" | sed 's/^/         /' >&2
  fails=$((fails + 1))
}

# reset: a clean slate for the next case.
reset() {
  rm -rf "$repo/tmp" "${scratch:?}/home" "$scratch/runs" "$scratch/during-ci" "$scratch/wrong-commit"
  mkdir -p "$scratch/home"
  : > "$scratch/transport.log"
  : > "$scratch/make.log"
  echo 0 > "$scratch/make-exit"
}

# start <host> [VAR=value…]: the script under test, in the scratch repository,
# in the background; $! is the script's own PID.
start() {
  local host=$1
  shift
  (cd "$repo" && exec env PATH="$scratch/bin:$PATH" "$@" scripts/ci/remote-ci.sh "$host") > "$scratch/out" 2>&1 &
}

# run <host> [VAR=value…]: start, then wait. A run that hangs is stopped after
# 60s, so a regression fails here instead of stalling `make verify`.
run() {
  local pid dog rc
  start "$@"
  pid=$!
  (sleep 60 && kill -TERM "$pid") > /dev/null 2>&1 &
  dog=$!
  wait "$pid"
  rc=$?
  kill "$dog" 2> /dev/null
  return "$rc"
}

tree() { git -C "$repo" rev-parse 'HEAD^{tree}'; }
marker() { echo "$repo/tmp/ci-passed-tree-$1"; }
prov() { sed -n "s/^$1=//p" "$(marker "$2").provenance"; }
leftover_runs() { find "$scratch/home" "$scratch/runs" -name 'run.*' -prune 2>/dev/null; }

expect_marker() { # <name> <tree>
  if [ -f "$(marker "$2")" ] && [ -f "$repo/tmp/verify-passed-tree-$2" ] && [ -f "$(marker "$2").provenance" ]; then
    ok "$1"
  else
    fail "$1: no marker and provenance for tree ${2:0:8}" "$(cat "$scratch/out")"
  fi
}
expect_nothing() { # <name> <stderr substring> <tree…>
  local name=$1 want=$2 t
  shift 2
  for t in "$@"; do
    if [ -e "$(marker "$t")" ] || [ -e "$(marker "$t").provenance" ] || [ -e "$repo/tmp/verify-passed-tree-$t" ]; then
      fail "$name: wrote a marker for tree ${t:0:8}"
      return
    fi
  done
  if ! grep -qF -- "$want" "$scratch/out"; then
    fail "$name: output lacks '$want'" "$(cat "$scratch/out")"
  else
    ok "$name"
  fi
}
expect_eq() { # <name> <got> <want>
  if [ "$2" = "$3" ]; then ok "$1"; else fail "$1" "got:  $2"$'\n'"want: $3"; fi
}

# ── A passing run over ssh ──
reset
T=$(tree)
C=$(git -C "$repo" rev-parse HEAD)
if run devbox; then
  expect_marker "a passing run writes the markers and provenance" "$T"
else
  fail "a passing run exits 0" "$(cat "$scratch/out")"
fi
expect_eq "…over ssh, in a login shell" "$(cat "$scratch/transport.log")" "ssh -T devbox bash -l -s"
expect_eq "…running make tools, then make ci, in a checkout of HEAD's tree" "$(cat "$scratch/make.log")" "make tools in $T"$'\n'"make ci in $T"
expect_eq "…recording the host" "$(prov host "$T")" devbox
expect_eq "…its OS and architecture as the host reports them" "$(prov os "$T")/$(prov arch "$T")" "Linux/x86_64"
expect_eq "…the commit and tree" "$(prov commit "$T") $(prov tree "$T")" "$C $T"
iso='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
if [[ $(prov started "$T") =~ $iso ]] && [[ $(prov finished "$T") =~ $iso ]]; then
  ok "…the start and end times"
else
  fail "…the start and end times" "$(cat "$(marker "$T").provenance")"
fi
log=$repo/$(prov log "$T")
if [[ $(head -n 1 "$log") == "ci-remote["*"] uname Linux x86_64" ]] && grep -qx "stub make ci output" "$log"; then
  ok "…and a local copy of the log, which opens with the host's uname"
else
  fail "…and a local copy of the log, which opens with the host's uname" "$(cat "$log")"
fi
expect_eq "…and removes the run directory from the host" "$(leftover_runs)" ""

# ── KEEP and REMOTE_DIR ──
reset
if run devbox KEEP=1 REMOTE_DIR="$scratch/runs"; then
  dir=$(prov remote_dir "$T")
  if [ "$(prov kept "$T")" = 1 ] && [ "${dir#"$scratch/runs/run."}" != "$dir" ] && [ -d "$dir/src" ]; then
    ok "KEEP=1 leaves the run directory under REMOTE_DIR"
  else
    fail "KEEP=1 leaves the run directory under REMOTE_DIR" "$(cat "$(marker "$T").provenance")"
  fi
else
  fail "KEEP=1 run exits 0" "$(cat "$scratch/out")"
fi

# ── Failures write nothing ──
reset
echo 2 > "$scratch/make-exit"
if run devbox; then fail "a failing make ci exits non-zero"; fi
expect_nothing "a failing make ci writes nothing" "make ci failed on devbox (exit 2)" "$T"
expect_eq "…and still cleans up the host" "$(leftover_runs)" ""

reset
cat > "$scratch/during-ci" << EOF
echo change > "$repo/main.go"
git -C "$repo" commit -qam 'commit during the run'
EOF
if run devbox; then fail "HEAD moving during the run exits non-zero"; fi
expect_nothing "a tree mismatch (HEAD moved during the run) writes nothing" "HEAD moved to tree" "$T" "$(tree)"
git -C "$repo" reset -q --hard "$C"

reset
git -C "$repo" rev-parse HEAD~1 > "$scratch/wrong-commit"
if run devbox; then fail "a host that tested another tree exits non-zero"; fi
expect_nothing "a tree mismatch (the host tested another tree) writes nothing" "reported testing tree" "$T"

reset
git -C "$repo" rev-parse HEAD~1 > "$scratch/wrong-commit"
echo "echo 'ci-remote[000000000000] tree $T'" > "$scratch/during-ci"
if run devbox; then fail "a report line forged by make's output exits non-zero"; fi
expect_nothing "a report line forged by make's output doesn't count" "reported testing tree" "$T"

reset
echo "touch '$repo/new-file'" > "$scratch/during-ci"
if run devbox; then fail "a worktree dirtied during the run exits non-zero"; fi
expect_nothing "a worktree dirtied during the run writes nothing" "the worktree changed during the run" "$T"
rm -f "$repo/new-file"

reset
echo "touch generated-file" > "$scratch/during-ci" # cwd: the host's checkout
if run devbox; then fail "make ci dirtying the host checkout exits non-zero"; fi
expect_nothing "make ci leaving the host checkout dirty writes nothing" "make ci changed the checkout" "$T"

# ── Interrupted: the host stops the run and cleans up ──
# The host's make ci records its PID, then waits.
hold_ci() { echo "echo \$\$ > '$scratch/ci.pid'; exec sleep 60" > "$scratch/during-ci"; }
wait_for_ci() { for _ in $(seq 100); do [ -s "$scratch/ci.pid" ] && break; sleep 0.1; done; }
expect_host_stopped() { # <name>
  local pid
  pid=$(cat "$scratch/ci.pid" 2> /dev/null)
  for _ in $(seq 100); do
    [ -n "$pid" ] && [ -z "$(leftover_runs)" ] && ! kill -0 "$pid" 2> /dev/null && break
    sleep 0.1
  done
  if [ -n "$pid" ] && [ -z "$(leftover_runs)" ] && ! kill -0 "$pid" 2> /dev/null; then
    ok "$1"
  else
    fail "$1" "${pid:-make ci never started}; left: $(leftover_runs)"
    [ -z "$pid" ] || kill "$pid" 2> /dev/null
  fi
  rm -f "$scratch/ci.pid"
}

reset
hold_ci
set -m
run devbox &
local_side=$!
set +m
wait_for_ci
kill -TERM -- "-$local_side" 2> /dev/null # what Ctrl-C does: the whole local process group
wait "$local_side" 2> /dev/null
expect_host_stopped "stopping the local process group stops make ci on the host and removes its run directory"
expect_nothing "…and writes nothing" "make ci on devbox" "$T"

reset
hold_ci
start devbox
script=$!
wait_for_ci
kill -TERM "$script" # the script alone: its pipeline is left running
wait "$script" 2> /dev/null
expect_host_stopped "signaling only the script stops make ci on the host and removes its run directory"
expect_nothing "…and writes nothing" "make ci on devbox" "$T"

# ── Refusals: nothing reaches the host ──
reset
echo change > "$repo/main.go"
if run devbox; then fail "a modified file is refused"; fi
expect_nothing "a modified tracked file is refused" "uncommitted or untracked changes" "$T"
git -C "$repo" checkout -q -- main.go
touch "$repo/untracked"
if run devbox; then fail "an untracked file is refused"; fi
expect_nothing "an untracked file is refused" "uncommitted or untracked changes" "$T"
rm -f "$repo/untracked"
for bad in "" "docker:" "-oProxyCommand=sh" "docker:-it" "dev box"; do
  if run "$bad"; then fail "HOST '$bad' is refused"; fi
done
expect_eq "…and so are unusable HOST values, none of them contacting the host" "$(cat "$scratch/transport.log")" ""

# ── A container ──
reset
if run docker:wh-dev; then
  expect_marker "docker:<container> writes the markers" "$T"
else
  fail "docker:<container> run exits 0" "$(cat "$scratch/out")"
fi
expect_eq "…through docker exec, in the container's own environment" "$(cat "$scratch/transport.log")" "docker exec -i wh-dev bash -s"
expect_eq "…recording the container as the host" "$(prov host "$T")" "docker:wh-dev"

# ── Two runs of one tree keep their own logs ──
reset
run devbox
log_a=$(prov log "$T")
run docker:wh-dev
log_b=$(prov log "$T")
if [ "$log_a" != "$log_b" ] && [ -f "$repo/$log_a" ] && [ -f "$repo/$log_b" ] \
  && [ "$(head -n 1 "$repo/$log_a")" != "$(head -n 1 "$repo/$log_b")" ]; then
  ok "two runs of the same tree write separate logs, and the provenance names this run's"
else
  fail "two runs of the same tree write separate logs, and the provenance names this run's" "$log_a"$'\n'"$log_b"
fi

# ── The pre-push hook names where make ci passed ──
zero=0000000000000000000000000000000000000000
push() { (cd "$repo" && echo "refs/heads/main $C refs/heads/main $zero" | .githooks/pre-push origin url) > "$scratch/out" 2>&1; }
if push && grep -qF "make ci passed on docker:wh-dev (Linux/x86_64)" "$scratch/out"; then
  ok "pre-push accepts the marker and names the host"
else
  fail "pre-push accepts the marker and names the host" "$(cat "$scratch/out")"
fi
rm -f "$(marker "$T").provenance"
if push && ! grep -qF "passed on" "$scratch/out"; then
  ok "…and says nothing extra for a marker without provenance"
else
  fail "…and says nothing extra for a marker without provenance" "$(cat "$scratch/out")"
fi
rm -f "$(marker "$T")"
if push; then fail "pre-push blocks a code push with no marker"; else ok "pre-push blocks a code push with no marker"; fi

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "remote-ci: all cases passed"

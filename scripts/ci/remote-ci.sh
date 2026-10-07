#!/usr/bin/env bash
# Run `make ci` for HEAD on another machine and, when it passes there for
# exactly this tree, write the markers a local `make ci` would
# (tmp/ci-passed-tree-<tree> and the verify marker), plus a provenance file
# beside them that .githooks/pre-push prints. `make ci-remote` calls this.
#
# Usage: scripts/ci/remote-ci.sh <host>
#   <host>       an ssh destination (user@box, a ~/.ssh/config alias, ...), or
#                docker:<container> for a running local container
#   KEEP=1       leave the run directory on the host
#   REMOTE_DIR   base directory for per-run checkouts on the host; a relative
#                path is under the host's home (default .cache/wavehouse-ci)
#
# The host needs bash, git, GNU make 4+, and what `make ci` itself needs: Go,
# Node and pnpm for `make tools`, and a Docker daemon for the integration and
# e2e suites. Over ssh the commands run in a login shell, so PATH comes from
# the host's profile; in a container it is the container's own.
#
# Nothing is pushed. HEAD travels as a git bundle on the transport's stdin and
# is checked out in a fresh directory under REMOTE_DIR, which is removed after
# the run unless KEEP=1. The markers are written only if the host exits 0, the
# tree it reports having tested is HEAD^{tree} here, and this worktree is still
# clean.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

die() {
  printf '🛑 ci-remote: %s\n' "$*" >&2
  exit 1
}

# Runs on the host. bash reads a script from a pipe one byte at a time, so
# after it reads the call below, the bundle that follows is left for `cat`.
# Report lines carry this run's nonce, which make's output cannot know.
remote_main() {
  set -eu
  exec 2>&1
  say() { printf 'ci-remote[%s] %s\n' "$nonce" "$*"; }
  case $base in
    /*) ;;
    *) base=$HOME/${base#"~/"} ;;
  esac
  mkdir -p "$base"
  dir=$(mktemp -d "$base/run.XXXXXX")
  trap 'if [ "$keep" = 1 ]; then say "kept $dir"; else rm -rf "$dir"; fi' EXIT
  say "uname $(uname -sm)"
  say "dir $dir"
  cat > "$dir/head.bundle"
  exec < /dev/null
  git clone -q --no-checkout "$dir/head.bundle" "$dir/src"
  cd "$dir/src"
  git -c advice.detachedHead=false checkout -q --detach "$commit"
  say "tree $(git rev-parse 'HEAD^{tree}')"
  if [ -n "$no_color" ]; then export NO_COLOR="$no_color"; fi
  make tools
  make ci
  # A local `make ci` keys its marker on the tree it leaves behind.
  if [ -n "$(git status --porcelain)" ]; then
    echo "make ci changed the checkout:"
    git status --short
    exit 1
  fi
}

host=${1:-}
case $host in
  docker:*) target=${host#docker:} ;;
  *) target=$host ;;
esac
case $target in
  "") die "usage: make ci-remote HOST=<ssh destination>|docker:<container>" ;;
  -* | *[[:space:][:cntrl:]]*) die "not a usable host: '$host'" ;;
esac
case $host in
  docker:*) transport=(docker exec -i "$target" bash -s) ;;
  *) transport=(ssh -T "$target" bash -l -s) ;;
esac

clean() {
  local s
  s=$(git status --porcelain --untracked-files=normal) && [ -z "$s" ]
}
clean || die "the worktree has uncommitted or untracked changes, and the host only gets HEAD. Commit them first."

commit=$(git rev-parse --verify HEAD) || die "no HEAD commit"
tree=$(git rev-parse "$commit^{tree}") || die "can't resolve the tree of ${commit:0:8}"
keep=0
[ "${KEEP:-}" = 1 ] && keep=1
base=${REMOTE_DIR:-.cache/wavehouse-ci}
nonce=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
[ -n "$nonce" ] || die "can't read /dev/urandom"

mkdir -p tmp
log=tmp/ci-remote-$tree.log
printf '==> make ci on %s for %s (tree %s), log in %s\n' "$host" "${commit:0:8}" "${tree:0:8}" "$log" >&2
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
{
  printf 'commit=%q base=%q keep=%q nonce=%q no_color=%q\n' "$commit" "$base" "$keep" "$nonce" "${NO_COLOR:-}"
  declare -f remote_main
  echo remote_main
  git bundle create -q - HEAD
} | "${transport[@]}" 2>&1 | tee "$log"
status=("${PIPESTATUS[@]}")
finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)

report() {
  awk -v p="ci-remote[$nonce] $1 " 'index($0, p) == 1 { print substr($0, length(p) + 1); exit }' "$log"
}
uname_sm=$(report uname)
remote_dir=$(report dir)
tested=$(report tree)

[ "${status[1]}" = 0 ] || die "make ci failed on $host (exit ${status[1]}); no marker written. Log: $log"
[ "${status[0]}" = 0 ] && [ "${status[2]}" = 0 ] || die "the transfer or the log failed (exit ${status[*]}); no marker written."
[ "$tested" = "$tree" ] || die "$host reported testing tree '${tested:0:8}', not ${tree:0:8}; no marker written."
now=$(git rev-parse "HEAD^{tree}") || die "can't resolve HEAD"
[ "$now" = "$tree" ] || die "HEAD moved to tree ${now:0:8} during the run, which tested ${tree:0:8}; no marker written. Rerun."
clean || die "the worktree changed during the run; no marker written. Commit, then rerun."

marker=$(scripts/ci-marker.sh path-for-commit "$commit") || die "can't resolve the marker path"
verify_marker=$(scripts/ci-marker.sh verify-path-for-commit "$commit") || die "can't resolve the marker path"
prov=$marker.provenance
{
  echo "host=$host"
  echo "os=${uname_sm%% *}"
  echo "arch=${uname_sm##* }"
  echo "commit=$commit"
  echo "tree=$tree"
  echo "started=$started"
  echo "finished=$finished"
  echo "log=$log"
  echo "remote_dir=$remote_dir"
  echo "kept=$keep"
} > "$prov.tmp" || die "can't write $prov"
mv "$prov.tmp" "$prov" || die "can't write $prov"
touch "$marker" "$verify_marker" || die "can't write $marker"
printf '✔ ci-remote: make ci passed on %s (%s) for %s; wrote %s\n' "$host" "$uname_sm" "${commit:0:8}" "$marker" >&2

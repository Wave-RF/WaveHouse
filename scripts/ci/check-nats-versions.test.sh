#!/usr/bin/env bash
# Behavioral test for scripts/ci/check-nats-versions.sh, which requires the five
# places naming the NATS server line to agree. Run by `make verify` (target:
# check-nats-versions). Dependency-free.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1

script="$PWD/scripts/ci/check-nats-versions.sh"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
fails=0

# tree <go.mod> <image> <helm> <recommended> <docs>: each a MAJOR.MINOR, writes
# a synthetic repo under $dir/repo.
tree() {
  rm -rf "$dir/repo"
  mkdir -p "$dir/repo/tests/integration" "$dir/repo/deployments/nats" "$dir/repo/internal/mq" "$dir/repo/docs/src/content/docs"
  printf 'module m\n\nrequire (\n\tgithub.com/nats-io/nats-server/v2 v%s.0 // indirect\n\tgithub.com/nats-io/nats.go v1.99.0\n)\n' "$1" >"$dir/repo/go.mod"
  printf 'package x\n\n// natsImage is a comment.\nconst natsImage = "nats:%s.6-alpine"\n' "$2" >"$dir/repo/tests/integration/setup_test.go"
  printf 'container:\n  image:\n    tag: %s.6-alpine # note\n  env:\n    A: b\n' "$3" >"$dir/repo/deployments/nats/values.yaml"
  printf 'package mq\n\nconst recommendedNATSMinor = "%s."\n' "$4" >"$dir/repo/internal/mq/nats_topology.go"
  printf '1. **Run NATS 2.14 or later** with JetStream. %s.x, the line the external-NATS tests run against, is recommended.\n' "$5" >"$dir/repo/docs/src/content/docs/deployment.md"
}

# check <name> <expect: pass|fail> <five lines>
check() {
  local name="$1" want="$2" got=pass
  shift 2
  tree "$@"
  "$script" "$dir/repo" >/dev/null 2>"$dir/err" || got=fail
  if [ "$got" = "$want" ]; then
    printf '  ok   %-34s %s\n' "$name" "$want"
  else
    printf '  FAIL %-34s wanted %s, got %s\n' "$name" "$want" "$got" >&2
    fails=$((fails + 1))
  fi
}

check "all agree" pass 2.15 2.15 2.15 2.15 2.15
check "all agree, other line" pass 2.14 2.14 2.14 2.14 2.14
check "go.mod ahead" fail 2.16 2.15 2.15 2.15 2.15
check "test image behind" fail 2.15 2.14 2.15 2.15 2.15
check "Helm tag behind" fail 2.15 2.15 2.14 2.15 2.15
check "recommendedNATSMinor behind" fail 2.15 2.15 2.15 2.14 2.15
check "deployment.md behind" fail 2.15 2.15 2.15 2.15 2.14
check "major differs" fail 3.15 2.15 2.15 2.15 2.15
check "minor 2.1 vs 2.15 not a prefix match" fail 2.1 2.15 2.15 2.15 2.15

# A disagreement names every source, its line and the fix.
tree 2.16 2.15 2.15 2.15 2.15
"$script" "$dir/repo" >/dev/null 2>"$dir/err"
for want in "embedded server" "external-NATS test image" "Helm values tag" "recommendedNATSMinor" "deployment.md tested line" "2.16" "go.mod" "Move every source"; do
  if grep -qF -- "$want" "$dir/err"; then
    printf '  ok   %-34s in message\n' "$want"
  else
    printf '  FAIL message lacks %s\n' "$want" >&2
    fails=$((fails + 1))
  fi
done

# Fail closed: a source whose line is gone, doubled or reworded is an error.
tree 2.15 2.15 2.15 2.15 2.15
printf 'no version here\n' >"$dir/repo/docs/src/content/docs/deployment.md"
if "$script" "$dir/repo" >/dev/null 2>&1; then
  printf '  FAIL docs sentence reworded passed\n' >&2
  fails=$((fails + 1))
else
  printf '  ok   %-34s fail\n' "docs sentence reworded"
fi
tree 2.15 2.15 2.15 2.15 2.15
printf '    tag: 2.15.6-alpine\n' >>"$dir/repo/deployments/nats/values.yaml"
if "$script" "$dir/repo" >/dev/null 2>&1; then
  printf '  FAIL two Helm tags passed\n' >&2
  fails=$((fails + 1))
else
  printf '  ok   %-34s fail\n' "two Helm tags"
fi
tree 2.15 2.15 2.15 2.15 2.15
rm "$dir/repo/go.mod"
if "$script" "$dir/repo" >/dev/null 2>&1; then
  printf '  FAIL missing go.mod passed\n' >&2
  fails=$((fails + 1))
else
  printf '  ok   %-34s fail\n' "missing file"
fi

# The real repository must agree, or the gate is already red.
if "$script" >/dev/null 2>"$dir/err"; then
  printf '  ok   %-34s agrees\n' "repo"
else
  cat "$dir/err" >&2
  printf '  FAIL the repository sources disagree\n' >&2
  fails=$((fails + 1))
fi

if [ "$fails" -ne 0 ]; then
  printf '%d case(s) failed\n' "$fails" >&2
  exit 1
fi

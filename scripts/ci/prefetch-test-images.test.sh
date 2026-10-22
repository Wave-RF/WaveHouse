#!/usr/bin/env bash
# Behavioral test for scripts/ci/prefetch-test-images.sh. The image lists are
# read from the real Go sources, so a change to how a suite declares its
# images, or a suite moving directories, fails here instead of silently
# prefetching nothing. The pulls run against a stub `docker`. Run by
# `make verify` (target: test-prefetch-test-images); no network.

set -uo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$root/scripts/ci/prefetch-test-images.sh"
fails=0
fail() { printf '  FAIL %s\n' "$*" >&2; fails=$((fails + 1)); }
ok() { printf '  ok   %s\n' "$*"; }

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
cd "$root" || exit 1

# has <suite> <image name without tag>...: the suite's list holds each name.
has() {
  local suite="$1" got name missing=""
  shift
  got="$("$script" --list "$suite")" || { fail "$suite: --list exited $?"; return; }
  for name in "$@"; do grep -q "^$name:" <<<"$got" || missing+=" $name"; done
  if [ -z "$missing" ]; then ok "$suite lists $*"; else fail "$suite is missing$missing; got: $(tr '\n' ' ' <<<"$got")"; fi
}
has app clickhouse/clickhouse-server amazon/dynamodb-local nats redis
has backends clickhouse/clickhouse-server redis valkey/valkey docker.dragonflydb.io/dragonflydb/dragonfly
has e2e clickhouse/clickhouse-server redis

# Every integration part CI runs has a case in the script.
parts="$(scripts/ci/integration-parts.sh | sed -n 's/^integration-parts=//p' | tr -d '[]"' | tr ',' ' ')"
[ -n "$parts" ] || fail "integration-parts.sh listed no parts"
for p in $parts; do
  if "$script" --list "$p" >/dev/null 2>&1; then ok "part $p is known"; else fail "part $p has no case in prefetch-test-images.sh"; fi
done

if "$script" --list nope >/dev/null 2>&1; then fail "an unknown suite should exit non-zero"; else ok "unknown suite exits non-zero"; fi

# Pulls: the script returns while they run, and each logs its result.
mkdir "$dir/bin"
cat >"$dir/bin/docker" <<'EOF'
#!/usr/bin/env bash
sleep 3
case "$*" in *redis*) echo "Error response from daemon: unauthorized: authentication required" >&2; exit 1 ;; esac
exit 0
EOF
chmod +x "$dir/bin/docker"
start=$(date +%s)
PATH="$dir/bin:$PATH" PREFETCH_LOG="$dir/log" "$script" e2e >"$dir/out" 2>&1
rc=$?
took=$(($(date +%s) - start))
if [ "$rc" = 0 ] && [ "$took" -lt 2 ]; then ok "returns before the pulls finish"; else fail "exit $rc after ${took}s; want 0 before the 3s stub pulls end"; fi
for _ in $(seq 20); do [ "$(grep -c . "$dir/log" 2>/dev/null)" -ge 2 ] && break; sleep 0.5; done
if grep -q "clickhouse/clickhouse-server:.* pulled in" "$dir/log" &&
  grep -q "redis:.* FAILED (Error response from daemon: unauthorized: authentication required)" "$dir/log"; then
  ok "logs each pull's result"
else
  fail "log: $(cat "$dir/log" 2>/dev/null)"
fi

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "prefetch-test-images: all cases passed"

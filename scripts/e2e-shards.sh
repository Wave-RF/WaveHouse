#!/usr/bin/env bash
# Run the E2E suite as N concurrent orchestrator shards, then fold the
# per-shard TS coverage back into the standard tmp/coverage/ts-e2e/ layout
# so `cov ts-merge` (and everything downstream) is untouched.
#
# WHY shards: within one server the vitest files must run sequentially
# (shared global policy state — see tests/e2e/sdk/vitest.config.ts and
# #214), so the suite's wall-clock is the SUM of its files. Each shard
# gets its own ClickHouse testcontainer + wavehouse-cov instance (random
# ports, per-shard scratch paths via E2E_SHARD), which removes the shared
# state, so the wall-clock becomes the slowest shard instead.
#
# Shard map: balanced by measured file durations (2026-06-10 CI runner:
# ingest 36s · ndjson 25s · cache 16s · query 8s · batching 6s · dlq 6s ·
# streaming/stress/admin/auth ≲1s). ingest.test.ts is the floor — no
# split below its duration without splitting the file (test changes are
# out of bounds). Rebalance here when file timings drift; every
# *.test.ts in tests/e2e/sdk MUST appear in exactly one shard (guarded
# below, so a new test file fails loudly instead of silently not running).
#
# Go covdata: all shards share GOCOVERDIR (covcounters are pid-stamped;
# covmeta is identical for the same binary). TS coverage: per-shard dirs,
# nyc-merged into ts-e2e/coverage-final.json at the end.
#
# Usage: scripts/e2e-shards.sh   (env: TS_E2E_COVERAGE_DIR optional,
#        E2E_KEEP_CH forwarded to each orchestrator)
# Bash 3.2-compatible (macOS /bin/bash): plain indexed arrays only.

set -u

cd "$(git rev-parse --show-toplevel)" || exit 1

SHARDS=(
  "ingest.test.ts"
  "ndjson.test.ts query.test.ts streaming.test.ts"
  "cache.test.ts batching.test.ts dlq.test.ts stress.test.ts admin.test.ts auth.test.ts"
)

# Completeness guard: every test file on disk must be in exactly one shard.
all_listed=" $(printf '%s ' "${SHARDS[@]}")"
missing=""
for f in tests/e2e/sdk/*.test.ts; do
  base="$(basename "$f")"
  case "$all_listed" in
    *" $base "*) ;;
    *) missing="$missing $base" ;;
  esac
done
if [ -n "$missing" ]; then
  echo "✗ e2e-shards: test file(s) not assigned to any shard:$missing" >&2
  echo "  Add them to a shard in scripts/e2e-shards.sh." >&2
  exit 1
fi

ts_cov_root="${TS_E2E_COVERAGE_DIR:-$PWD/tmp/coverage/ts-e2e}"

# Precompile once so N concurrent `go run`s don't each pay the compile.
orch_bin="tmp/e2e-orchestrator"
go build -o "$orch_bin" ./scripts/orchestrator || exit 1

# Line-buffered pure-bash prefixer (BSD sed has no -u) so interleaved
# shard logs stay attributable.
prefix_lines() {
  while IFS= read -r line; do printf '[shard %s] %s\n' "$1" "$line"; done
}

pids=""
i=0
for files in "${SHARDS[@]}"; do
  i=$((i + 1))
  shard_dir="${ts_cov_root}-shard-$i"
  rm -rf "$shard_dir" && mkdir -p "$shard_dir"
  # Subshell with pipefail: $! must reflect the ORCHESTRATOR's exit, not
  # the prefixer's (a bare `cmd | filter &` waits on the filter only).
  (
    set -o pipefail
    E2E_SHARD="$i" \
      E2E_VITEST_FILES="$files" \
      TS_E2E_COVERAGE_DIR="$shard_dir" \
      VITEST_CACHE_DIR="$PWD/tmp/vite-cache-shard-$i" \
      "$orch_bin" 2>&1 | prefix_lines "$i"
  ) &
  pids="$pids $!"
done

rc=0
for pid in $pids; do
  wait "$pid" || rc=1
done
if [ "$rc" -ne 0 ]; then
  echo "✗ e2e-shards: at least one shard failed (see [shard N] output above)" >&2
  exit 1
fi

# Fold shard TS coverage into the standard single-suite layout. nyc merges
# every *.json under a directory, so stage the shard jsons under one dir.
stage="${ts_cov_root}-shard-input"
rm -rf "$stage" "$ts_cov_root" && mkdir -p "$stage" "$ts_cov_root"
for d in "${ts_cov_root}"-shard-[0-9]*; do
  [ -f "$d/coverage-final.json" ] || { echo "✗ e2e-shards: $d produced no coverage-final.json" >&2; exit 1; }
  cp "$d/coverage-final.json" "$stage/$(basename "$d").json"
done
pnpm exec nyc merge "$stage" "$ts_cov_root/coverage-final.json" >/dev/null || exit 1
echo "✓ e2e-shards: ${#SHARDS[@]} shards passed; TS coverage merged → $ts_cov_root/coverage-final.json"

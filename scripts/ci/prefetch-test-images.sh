#!/usr/bin/env bash
# Start pulling a suite's test container images in the background at the top
# of its CI job, so the pulls overlap the cache restores and builds. Otherwise
# testcontainers pulls an image when a test first asks for it: in
# tests/integration, TestMain pulls dynamodb-local (~230 MiB compressed)
# before any test runs, on the run's longest job. A prefetch that fails costs
# nothing, because the test pulls the image itself as before, so a flaky
# registry pull also gets a second try. A concurrent pull of the same tag is
# safe: dockerd dedupes layer downloads.
#
# The images are read from the suite's Go sources (`fooImage = "<ref>"` and
# `Image: "<ref>"`), so no tag is written down twice. ClickHouse is the
# exception no suite spells out as a literal: every suite runs the one version
# internal/chversion pins, so its tag is read from there.
#
# Usage: scripts/ci/prefetch-test-images.sh [--list] <suite>
#   <suite>  an integration part (app, backends) or e2e
#   --list   print the images, one per line, and pull nothing
# Each pull's result goes to $PREFETCH_LOG (default /tmp/prefetch-images.log).

set -euo pipefail

list=0
if [ "${1:-}" = --list ]; then list=1; shift; fi

# Where each suite's tests and harness live: the Makefile's INTEGRATION_*_PKGS
# for the parts, the orchestrator for e2e. The test holds every part in
# INTEGRATION_PARTS to a case here.
case "${1:-}" in
  app) dirs=(tests/integration) ;;
  backends) dirs=(internal/cache internal/mq) ;;
  e2e) dirs=(scripts/orchestrator) ;;
  *) echo "prefetch-test-images: unknown suite '${1:-}'" >&2; exit 2 ;;
esac

images="$(grep -rhoE --include='*.go' '[A-Za-z]*Image[[:space:]]*[:=][[:space:]]*"[^"]+"' "${dirs[@]}" |
  sed -E 's/.*"([^"]+)"$/\1/' |
  grep -E '^[a-z0-9.-]+(/[a-z0-9._-]+)*:[A-Za-z0-9._-]+$' | sort -u || true)"
clickhouse="$(sed -nE 's#^const Test = "([^"]+)"$#clickhouse/clickhouse-server:\1#p' internal/chversion/chversion.go)"
images="$(printf '%s\n%s\n' "$images" "$clickhouse" | sed '/^$/d' | sort -u)"

if [ "$list" = 1 ]; then
  [ -n "$images" ] && echo "$images"
  exit 0
fi
if [ -z "$images" ]; then
  echo "prefetch-test-images: no images found for $1"
  exit 0
fi

log=${PREFETCH_LOG:-/tmp/prefetch-images.log}
for img in $images; do
  echo "prefetch-test-images: pulling $img in the background"
  (
    start=$(date +%s)
    if out=$(docker pull -q "$img" 2>&1); then r=pulled; else r="FAILED (${out##*$'\n'})"; fi
    echo "$(date -u +%H:%M:%SZ) $img $r in $(($(date +%s) - start))s"
  ) </dev/null >>"$log" 2>&1 &
done

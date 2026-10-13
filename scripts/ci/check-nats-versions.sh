#!/usr/bin/env bash
# Fails unless every place that names the NATS server line names the same
# MAJOR.MINOR. Five sources, each read by one explicit pattern:
#
#   go.mod                                     github.com/nats-io/nats-server/v2 vX.Y.Z (the embedded server)
#   tests/integration/setup_test.go            natsImage = "nats:X.Y.Z-alpine" (the external-NATS test image)
#   deployments/nats/values.yaml               the one `tag: X.Y.Z-alpine` (the Helm chart's server image)
#   internal/mq/nats_topology.go               recommendedNATSMinor = "X.Y." (what boot recommends)
#   docs/src/content/docs/deployment.md        "X.Y.x, the line the external-NATS tests run against"
#
# Dependabot moves the first one alone; this fails until the other four follow.
# It fails closed: a source whose pattern no longer matches exactly once is an
# error, not a pass, so a reworded line cannot silently drop out of the check.
# Run by `make verify` (target: check-nats-versions).
#
# Usage: check-nats-versions.sh [repo-root]   (default: this repository)

set -euo pipefail

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"

names=()
files=()
lines=()
bad=0

# add <name> <file, relative to root> <grep -E pattern selecting the line>
#     <sed -E expression printing its MAJOR.MINOR>
# The pattern must match exactly one line.
add() {
  local name="$1" file="$2" pattern="$3" extract="$4" hits version
  if [ ! -f "$root/$file" ]; then
    echo "check-nats-versions: $file ($name) is missing" >&2
    bad=$((bad + 1))
    return
  fi
  hits="$(grep -E -- "$pattern" "$root/$file" || true)"
  if [ "$(grep -c . <<<"$hits")" -ne 1 ]; then
    echo "check-nats-versions: expected exactly one line in $file ($name) matching: $pattern; found ${hits:-none}. If the line was reworded, update this check." >&2
    bad=$((bad + 1))
    return
  fi
  version="$(sed -E -n -- "$extract" <<<"$hits")"
  if ! [[ "$version" =~ ^[0-9]+\.[0-9]+$ ]]; then
    echo "check-nats-versions: could not read a MAJOR.MINOR from the $name line in $file: $hits" >&2
    bad=$((bad + 1))
    return
  fi
  names+=("$name")
  files+=("$file")
  lines+=("$version")
}

add "embedded server" go.mod \
  '^[[:space:]]*github\.com/nats-io/nats-server/v2[[:space:]]+v[0-9]+\.[0-9]+\.[0-9]+' \
  's/.*nats-server\/v2[[:space:]]+v([0-9]+\.[0-9]+)\.[0-9]+.*/\1/p'
add "external-NATS test image" tests/integration/setup_test.go \
  '^const natsImage = "nats:[0-9]+\.[0-9]+\.[0-9]+' \
  's/.*"nats:([0-9]+\.[0-9]+)\.[0-9]+.*/\1/p'
add "Helm values tag" deployments/nats/values.yaml \
  '^[[:space:]]+tag:[[:space:]]*[0-9]+\.[0-9]+\.[0-9]+' \
  's/.*tag:[[:space:]]*([0-9]+\.[0-9]+)\.[0-9]+.*/\1/p'
add "recommendedNATSMinor" internal/mq/nats_topology.go \
  '^const recommendedNATSMinor = "[0-9]+\.[0-9]+\."' \
  's/.*"([0-9]+\.[0-9]+)\.".*/\1/p'
add "deployment.md tested line" docs/src/content/docs/deployment.md \
  '[0-9]+\.[0-9]+\.x, the line the external-NATS tests run against' \
  's/.*[^0-9.]([0-9]+\.[0-9]+)\.x, the line the external-NATS tests run against.*/\1/p'

if [ "$bad" -ne 0 ]; then
  exit 1
fi

first="${lines[0]}"
differ=0
for v in "${lines[@]}"; do
  [ "$v" = "$first" ] || differ=1
done
if [ "$differ" -eq 0 ]; then
  exit 0
fi

echo "check-nats-versions: the NATS server line differs between sources:" >&2
for i in "${!lines[@]}"; do
  printf '  %-26s %-6s %s\n' "${names[$i]}" "${lines[$i]}" "${files[$i]}" >&2
done
echo "Move every source to one MAJOR.MINOR: the embedded server's line (go.mod, moved by Dependabot) is the one to follow, or bump go.mod with 'go get github.com/nats-io/nats-server/v2@vX.Y.Z' to match the others." >&2
exit 1

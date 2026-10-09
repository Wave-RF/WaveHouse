#!/usr/bin/env bash
# Fails unless every `golang:` base image in the Dockerfile is exactly the Go
# version go.mod pins. The image sets GOTOOLCHAIN=local, so go.mod's version is
# not enforced there by the toolchain's own fetch: a stale tag would build with
# an older Go, or refuse to build, depending on which side is behind. The pin
# comes from scripts/ci/go-toolchain.sh, the one derivation the Makefile and CI
# share. A Dockerfile with no `golang:` FROM line fails too, so a refactor
# cannot make this pass vacuously. Run by `make verify` (target:
# check-dockerfile-go).
#
# Usage: check-dockerfile-go.sh [Dockerfile] [go.mod]
#   (defaults: deployments/Dockerfile and go.mod at the repo root)

set -euo pipefail

abs() { printf '%s/%s\n' "$(cd "$(dirname "$1")" && pwd)" "$(basename "$1")"; }

dockerfile="${1:-deployments/Dockerfile}"
gomod="${2:-go.mod}"
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
dockerfile="$(abs "$dockerfile")"
gomod="$(abs "$gomod")"

pin="$(scripts/ci/go-toolchain.sh "$gomod")"

# FROM [--flag=value ...] golang:<tag>[@sha256:<digest>] [AS name]
from_re='^[[:space:]]*[Ff][Rr][Oo][Mm][[:space:]]+(--[^[:space:]]+[[:space:]]+)*golang:([^[:space:]@]+)(@[^[:space:]]+)?([[:space:]]|$)'

found=0
bad=0
while IFS= read -r line; do
  [[ "$line" =~ $from_re ]] || continue
  found=$((found + 1))
  tag="${BASH_REMATCH[2]}"
  want="${pin#go}"
  # The version is the tag up to its first '-' (1.26.9-alpine -> 1.26.9).
  if [ "${tag%%-*}" != "$want" ]; then
    echo "check-dockerfile-go: $dockerfile has 'FROM golang:$tag' but go.mod pins $pin; set the tag to golang:$want-<variant> (or bump go.mod with 'go get go@1.N.P' and the tag together)" >&2
    bad=$((bad + 1))
  fi
done <"$dockerfile"

if [ "$found" -eq 0 ]; then
  echo "check-dockerfile-go: no 'FROM golang:' line in $dockerfile, so there is nothing to check; if the build image moved, update this check" >&2
  exit 1
fi
[ "$bad" -eq 0 ]

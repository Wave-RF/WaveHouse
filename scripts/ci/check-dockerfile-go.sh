#!/usr/bin/env bash
# Fails unless every `golang:` base image in the Dockerfile is exactly the Go
# version go.mod pins. The image sets GOTOOLCHAIN=local, so go.mod's version is
# not enforced there by the toolchain's own fetch: a stale tag would build with
# an older Go, or refuse to build, depending on which side is behind. The pin
# comes from scripts/ci/go-toolchain.sh, the one derivation the Makefile and CI
# share. It fails closed: a Dockerfile with no `golang:` FROM line, or a FROM
# whose image is an ARG (`FROM ${IMAGE}`, `golang:${V}-alpine`), cannot be
# verified, so it is an error rather than a pass. Run by `make verify` (target:
# check-dockerfile-go).
#
# Usage: check-dockerfile-go.sh [Dockerfile] [go.mod]
#   Relative paths are relative to the caller's directory; the defaults are
#   deployments/Dockerfile and go.mod at the repo root.

set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
abs() { case "$1" in /*) printf '%s\n' "$1" ;; *) printf '%s/%s\n' "$PWD" "$1" ;; esac; }
dockerfile="$(abs "${1:-$root/deployments/Dockerfile}")"
gomod="$(abs "${2:-$root/go.mod}")"

pin="$("$root/scripts/ci/go-toolchain.sh" "$gomod")"
want="${pin#go}"

# One image per FROM: continuation lines joined, comments dropped, flags
# (--platform=...) skipped. awk also reads a last line with no newline.
images="$(awk '
  function emit(l,   f, n, i) {
    n = split(l, f, /[ \t]+/); i = 1
    while (i <= n && f[i] == "") i++
    if (tolower(f[i]) != "from") return
    for (i++; i <= n && f[i] ~ /^--/; i++) ;
    print f[i]
  }
  { sub(/\r$/, "") }
  /^[ \t]*#/ { next }
  { buf = buf $0 }
  buf ~ /\\[ \t]*$/ { sub(/\\[ \t]*$/, "", buf); buf = buf " "; next }
  { emit(buf); buf = "" }
  END { if (buf != "") emit(buf) }
' "$dockerfile")"

found=0
bad=0
while IFS= read -r image; do
  [ -n "$image" ] || continue
  if [[ "$image" == *'$'* ]]; then
    echo "check-dockerfile-go: $dockerfile has 'FROM $image', an ARG-built image this check cannot verify; write the golang:$want-<variant> tag literally" >&2
    bad=$((bad + 1))
    continue
  fi
  base="${image%%@*}" # drop a digest
  base="${base##*/}"  # drop a registry host[:port]/path prefix
  [[ "$base" == golang || "$base" == golang:* ]] || continue
  found=$((found + 1))
  tag="${base#golang}"
  tag="${tag#:}"
  # The tag is the version, or the version and a letter-led variant (-alpine),
  # not a pre-release (-rc1, -beta2).
  if [ "$tag" != "$want" ] && { [[ "$tag" != "$want"-[A-Za-z]* ]] || [[ "$tag" == "$want"-@(rc|beta|alpha)[0-9]* ]]; }; then
    echo "check-dockerfile-go: $dockerfile has 'FROM $image' but go.mod pins $pin; set the tag to golang:$want-<variant> (or bump go.mod with 'go get go@1.N.P' and the tag together)" >&2
    bad=$((bad + 1))
  fi
done <<<"$images"

if [ "$found" -eq 0 ]; then
  echo "check-dockerfile-go: no 'FROM golang:' line in $dockerfile, so there is nothing to check; if the build image moved, update this check" >&2
  exit 1
fi
[ "$bad" -eq 0 ]

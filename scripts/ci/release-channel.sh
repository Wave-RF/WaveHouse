#!/usr/bin/env bash
# Map a release tag to its distribution channel — the MOVING pointer that
# should follow this release. One rule, every publisher:
#
#   release.yml     -> WAVEHOUSE_CHANNEL, the second GHCR tag in
#                      .goreleaser.yaml's dockers_v2 block
#   publish-npm.yml -> the `npm publish --tag` dist-tag
#
# so `ghcr.io/wave-rf/wavehouse:alpha` and `@wavehouse/sdk@alpha` can never
# mean different things. Every release also gets its immutable `vX.Y.Z` /
# exact-version reference; this is only the pointer on top of it.
#
# The rule: a stable release owns `latest`; a prerelease owns the channel
# named by its prerelease identifier. That is what keeps `v1.3.0-rc.1` from
# taking `:latest` away from the shipped `v1.2.0` — the failure mode this
# script exists to prevent.
#
# Tag shapes, one per releasable component (see scripts/release.sh):
#
#   v1.2.3               the server (root Go module)
#   clients/ts/v1.2.3    @wavehouse/sdk
#   clients/go/v1.2.3    the Go SDK
#
# The `clients/<lang>/` prefix is not a style choice: Go requires a module in
# a subdirectory to be tagged with that subdirectory as a prefix, or
# `go get .../clients/go@v1.2.3` cannot resolve (https://go.dev/ref/mod).
# Every other client follows the same shape so one rule covers all of them.
#
# Usage:  scripts/ci/release-channel.sh v1.2.3               -> latest
#         scripts/ci/release-channel.sh clients/ts/v1.2.3-rc.1 -> rc
#         scripts/ci/release-channel.sh 0.1.0-alpha.1        -> alpha
# Exit:   0 with the channel on stdout; 1 on a tag that isn't semver-shaped
#         (fail loud — a typo'd tag must not silently publish as `latest`).

set -euo pipefail

tag="${1:-}"
if [ -z "$tag" ]; then
  echo "usage: $0 <tag|version>" >&2
  exit 1
fi

# Drop any component prefix, then the `v`. `${tag##*/}` takes everything after
# the last slash, so it handles `clients/ts/v1.2.3` and a bare `v1.2.3` alike
# and needs no per-component list.
version="${tag##*/}"
version="${version#v}"

# Full SemVer 2.0.0 grammar, not a loose approximation — this gate is the last
# thing standing between a typo and an immutable tag, and scripts/release.sh
# trusts it. The official regex, transcribed to POSIX ERE (bash has no
# non-capturing groups): numeric identifiers may not have leading zeros, and
# dot-separated identifiers may not be empty. So `v01.2.3`, `v1.2.3-01`, and
# `v1.2.3-alpha..1` are rejected where a `[0-9A-Za-z.-]+` blob accepted them.
semver='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)(\.(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*))*))?(\+([0-9a-zA-Z-]+(\.[0-9a-zA-Z-]+)*))?$'
if ! [[ "$version" =~ $semver ]]; then
  echo "::error::'$tag' is not a semver release tag (expected vX.Y.Z or clients/<lang>/vX.Y.Z, with an optional -prerelease suffix)." >&2
  exit 1
fi

# Strip any build metadata before looking for the prerelease marker, so a
# `+alpha` build tag on a stable release can't be read as a prerelease.
version="${version%%+*}"

# No prerelease at all -> stable -> `latest`.
case "$version" in
  *-*) ;;
  *) echo latest; exit 0 ;;
esac

# Match the FIRST prerelease identifier EXACTLY, not as a substring. A glob
# (`*-alpha*`) reads `v1.2.3-alphafoo` as alpha and `v1.2.3-preview-rc.1` as rc,
# which would point a channel other people consume at an unrelated release.
# Anything that isn't exactly alpha/beta/rc is some other prerelease and belongs
# on `next`. Mirrors publish-npm.yml's dist-tag map.
prerelease="${version#*-}"
case "${prerelease%%.*}" in
  alpha) echo alpha ;;
  beta) echo beta ;;
  rc) echo rc ;;
  *) echo next ;;
esac

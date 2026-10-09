#!/bin/sh
# Prints the Go toolchain go.mod pins (e.g. go1.26.6): its `toolchain` line if it
# has one, else its `go` line. Exits non-zero, with one message, unless that is
# a full x.y.z version: `go mod edit -go=1.27` writes `go 1.27`, which Go
# rejects as a toolchain name. The Makefile, the setup-env action and
# deployments/Dockerfile all call this, so local runs, CI and the image build
# cannot derive the pin differently. It is POSIX sh so the Alpine builder (no
# bash) can run it; shellcheck flags any bashism a later edit adds.
#
# Usage: go-toolchain.sh [path/to/go.mod]   (default: go.mod at the repo root)

set -eu
cd "$(dirname "$0")/../.."

mod="${1:-go.mod}"
pin="$(awk '/^[ \t]*toolchain[ \t]/{t=$2} /^[ \t]*go[ \t]/{g="go"$2} END{print (t != "") ? t : g}' "$mod")"

if ! printf '%s\n' "$pin" | grep -Eq '^go[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "go-toolchain: $mod needs a go line of the form x.y.z (or a toolchain line), got '${pin:-nothing}'; bump with 'go get go@1.N.P'" >&2
  exit 1
fi
printf '%s\n' "$pin"

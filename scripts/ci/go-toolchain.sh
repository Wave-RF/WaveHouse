#!/usr/bin/env bash
# Prints the Go toolchain go.mod pins (e.g. go1.26.6): its `toolchain` line if it
# has one, else its `go` line. Exits non-zero, with one message, unless that is
# a full x.y.z version: `go mod edit -go=1.27` writes `go 1.27`, which Go
# rejects as a toolchain name. The Makefile and the setup-env action both call
# this, so local runs and CI cannot derive the pin differently.
#
# Usage: go-toolchain.sh [path/to/go.mod]   (default: go.mod at the repo root)

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

mod="${1:-go.mod}"
pin="$(awk '/^toolchain[ \t]/{t=$2} /^go[ \t]/{g="go"$2} END{print (t != "") ? t : g}' "$mod")"

if [[ ! "$pin" =~ ^go[0-9]+\.[0-9]+\.[0-9]+ ]]; then
  echo "go-toolchain: $mod needs a go line of the form x.y.z (or a toolchain line), got '${pin:-nothing}'; bump with 'go get go@1.N.P'" >&2
  exit 1
fi
printf '%s\n' "$pin"

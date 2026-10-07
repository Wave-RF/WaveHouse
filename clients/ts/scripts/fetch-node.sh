#!/usr/bin/env bash
# Prints the path to a Node binary of exactly the given version, downloaded from
# nodejs.org into <dest> on first use and checked against a sha256 pinned here
# (not one fetched from the same origin). The version comes from engines.node,
# so raising its floor needs new pins: take them from
# https://nodejs.org/dist/v<version>/SHASUMS256.txt.
# Usage: fetch-node.sh <x.y.z> <dest-dir>
set -euo pipefail

version=${1:?usage: fetch-node.sh <x.y.z> <dest-dir>}
dest=${2:?usage: fetch-node.sh <x.y.z> <dest-dir>}

case "$(uname -s)" in
  Darwin) os=darwin ;;
  Linux) os=linux ;;
  *) echo "fetch-node: unsupported OS $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) arch=x64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) echo "fetch-node: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

case "${version}.${os}-${arch}" in
  22.0.0.darwin-arm64) want=ea96d349cfaa67aa87ceeaa3e5b52c9167f7ac302fd8d1ff162d0785e9dc0785 ;;
  22.0.0.darwin-x64) want=422a3887ff5418f0a4552d89cf99346ab8ab51bb5d384660baa88b8444d2c111 ;;
  22.0.0.linux-arm64) want=1d3547226be7e59aceee5c7d01a9f8fc18de67e015c5a15d8cf385b6e02d062b ;;
  22.0.0.linux-x64) want=74bb0f3a80307c529421c3ed84517b8f543867709f41e53cd73df99e6442af4d ;;
  *)
    echo "fetch-node: no pinned sha256 for node ${version} on ${os}-${arch};" >&2
    echo "add it to this script from https://nodejs.org/dist/v${version}/SHASUMS256.txt" >&2
    exit 1
    ;;
esac

name="node-v${version}-${os}-${arch}"
node="${dest}/${name}/bin/node"
if [ ! -x "$node" ]; then
  base="https://nodejs.org/dist/v${version}"
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  curl -sSfL --retry 3 -o "${tmp}/${name}.tar.gz" "${base}/${name}.tar.gz" >&2
  if command -v sha256sum >/dev/null; then
    got=$(sha256sum "${tmp}/${name}.tar.gz" | awk '{ print $1 }')
  else
    got=$(shasum -a 256 "${tmp}/${name}.tar.gz" | awk '{ print $1 }')
  fi
  [ "$got" = "$want" ] || { echo "fetch-node: checksum mismatch for ${name}.tar.gz" >&2; exit 1; }
  mkdir -p "$dest"
  tar -xzf "${tmp}/${name}.tar.gz" -C "$tmp"
  rm -rf "${dest:?}/${name}"
  mv "${tmp}/${name}" "${dest}/${name}"
fi
echo "$node"

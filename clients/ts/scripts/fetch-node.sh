#!/usr/bin/env bash
# Prints the path to a Node binary of exactly the given version, downloading it
# from nodejs.org into <dest> on first use and verifying it against the release's
# SHASUMS256.txt. Usage: fetch-node.sh <x.y.z> <dest-dir>
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

name="node-v${version}-${os}-${arch}"
node="${dest}/${name}/bin/node"
if [ ! -x "$node" ]; then
  base="https://nodejs.org/dist/v${version}"
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  curl -sSfL --retry 3 -o "${tmp}/${name}.tar.gz" "${base}/${name}.tar.gz" >&2
  curl -sSfL --retry 3 -o "${tmp}/SHASUMS256.txt" "${base}/SHASUMS256.txt" >&2
  want=$(awk -v f="${name}.tar.gz" '$2 == f { print $1 }' "${tmp}/SHASUMS256.txt")
  [ -n "$want" ] || { echo "fetch-node: no checksum for ${name}.tar.gz" >&2; exit 1; }
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

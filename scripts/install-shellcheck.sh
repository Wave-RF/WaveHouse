#!/usr/bin/env bash
# Fetch the pinned shellcheck release binary, checksum-verified, into the
# path given as $1 (the Makefile's $(SHELLCHECK) — version encoded in the
# path so a version bump triggers reinstall). shellcheck is a Haskell
# binary, so the repo's `go install`-to-.bin pattern doesn't apply;
# official static release tarballs + pinned sha256 per platform instead.
#
# Usage: scripts/install-shellcheck.sh <target-path>

set -euo pipefail

VERSION="v0.11.0"
target="${1:?usage: install-shellcheck.sh <target-path>}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$os.$arch" in
  linux.x86_64)   plat="linux.x86_64";   sha="8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198" ;;
  linux.aarch64 | linux.arm64) plat="linux.aarch64"; sha="12b331c1d2db6b9eb13cfca64306b1b157a86eb69db83023e261eaa7e7c14588" ;;
  darwin.arm64)   plat="darwin.aarch64"; sha="56affdd8de5527894dca6dc3d7e0a99a873b0f004d7aabc30ae407d3f48b0a79" ;;
  darwin.x86_64)  plat="darwin.x86_64";  sha="3c89db4edcab7cf1c27bff178882e0f6f27f7afdf54e859fa041fca10febe4c6" ;;
  *) echo "install-shellcheck: unsupported platform $os/$arch" >&2; exit 1 ;;
esac

url="https://github.com/koalaman/shellcheck/releases/download/${VERSION}/shellcheck-${VERSION}.${plat}.tar.xz"
tmp="$(mktemp -d)"
# shellcheck disable=SC2064  # expand NOW on purpose: $tmp must be the value at trap-set time
trap "rm -rf '$tmp'" EXIT

echo "==> Downloading shellcheck ${VERSION} (${plat})..."
curl -sSfL -o "$tmp/sc.tar.xz" "$url"
echo "${sha}  $tmp/sc.tar.xz" | shasum -a 256 -c - >/dev/null
tar -xJf "$tmp/sc.tar.xz" -C "$tmp"
mkdir -p "$(dirname "$target")"
install -m 0755 "$tmp/shellcheck-${VERSION}/shellcheck" "$target"
echo "==> Installed: $target"

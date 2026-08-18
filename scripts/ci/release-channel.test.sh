#!/usr/bin/env bash
# Behavioral test for scripts/ci/release-channel.sh — the single rule that maps
# a release tag to its moving channel pointer. Three publishers depend on it
# (release.yml's GHCR tag, publish-npm.yml's dist-tag, release.sh's preflight),
# and its whole job is to keep `v1.3.0-rc.1` from taking `:latest`/`@latest`
# away from a shipped stable release — so the fail-closed cases below matter as
# much as the happy path. Run by `make verify` (target: test-release-channel),
# so it gates in CI exactly like a unit test. Dependency-free.

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1 # repo root (scripts/ci/../..)

script=scripts/ci/release-channel.sh
fails=0

# ok <tag> <expected-channel>
ok() {
  local tag="$1" want="$2" got
  if got="$("$script" "$tag" 2>/dev/null)" && [ "$got" = "$want" ]; then
    printf '  ok   %-28s -> %s\n' "$tag" "$got"
  else
    printf '  FAIL %-28s want %s, got %s (exit %s)\n' "$tag" "$want" "${got:-<none>}" "$?" >&2
    fails=$((fails + 1))
  fi
}

# rejects <tag> — must exit non-zero. A tag this script cannot classify must
# never fall through to `latest`; that is the fail-closed contract its header
# promises and the reason every publisher calls it before doing any work.
rejects() {
  local tag="$1"
  if "$script" "$tag" >/dev/null 2>&1; then
    printf '  FAIL %-28s should have been rejected\n' "${tag:-<empty>}" >&2
    fails=$((fails + 1))
  else
    printf '  ok   %-28s rejected\n' "${tag:-<empty>}"
  fi
}

# --- stable -> latest, for every tag family --------------------------------
ok "v1.2.3" latest
ok "clients/ts/v1.2.3" latest
ok "clients/go/v1.2.3" latest
ok "1.2.3" latest # bare version (release.sh passes the constructed tag)

# --- prereleases own their own channel, never `latest` ---------------------
ok "v0.1.0-alpha.1" alpha
ok "v0.1.0-beta.2" beta
ok "v1.3.0-rc.1" rc
ok "clients/ts/v0.2.0-rc.1" rc
ok "clients/go/v2.0.0-beta.3" beta
# Any other prerelease form lands on the catch-all rather than `latest`.
ok "v1.0.0-canary.1" next
ok "v1.0.0-next.4" next
ok "v1.0.0-0" next

# --- build metadata is stripped before the prerelease check ----------------
# `+alpha` is build metadata, NOT a prerelease: this must stay `latest`.
ok "v1.0.0+alpha" latest
ok "v1.0.0+build.5" latest
ok "v1.0.0-alpha+meta" alpha

# --- ONLY an exact first prerelease identifier picks a named channel --------
# A substring match would point a channel real consumers install from at an
# unrelated release: `-alphafoo` is not the alpha line, and `-preview-rc.1` is
# not the rc line. Everything that isn't exactly alpha/beta/rc is `next`.
ok "v1.2.3-alphafoo" next
ok "v1.2.3-preview-rc.1" next
ok "v1.2.3-betamax" next
ok "v1.2.3-rcfoo" next
ok "clients/ts/v1.2.3-alpha-extra" next
# ...while the real channels still resolve, with or without a numeric part.
ok "v1.2.3-alpha" alpha
ok "v1.2.3-beta" beta
ok "v1.2.3-rc" rc

# --- fail closed on anything not semver-shaped -----------------------------
# SemVer forbids leading zeros on numeric identifiers and empty dot-separated
# identifiers; a loose `[0-9A-Za-z.-]+` blob accepted all of these, and
# scripts/release.sh trusts this gate before creating an immutable tag.
rejects "v01.2.3"
rejects "v1.02.3"
rejects "v1.2.03"
rejects "v1.2.3-01"
rejects "v1.2.3-alpha..1"
rejects "v1.2.3-"
rejects "v1.2.3+"
rejects "v1.2.3-alpha+"
rejects "v1.2"
rejects "v1.2.3.4"
rejects "latest"
rejects "clients/ts/latest"
rejects "dev"
rejects ""
rejects "v"
rejects "vX.Y.Z"

if [ "$fails" -gt 0 ]; then
  printf '\n%d case(s) failed\n' "$fails" >&2
  exit 1
fi
echo "release-channel: all cases passed"

#!/usr/bin/env bash
# The integration suite's parts, read from the Makefile's INTEGRATION_PARTS so
# CI runs every part `make test-integration` runs: a part added there gets its
# own job with no ci.yml edit, and cannot be left out of CI. Prints, for
# $GITHUB_OUTPUT (ci.yml's `changes` job):
#
#   integration-parts=["app","backends"]          the integration job's matrix
#   integration-fragments=coverage-integration-app,...
#   integration-producers=Integration tests (app),...
#
# The last two are what the coverage job waits for, so they must match the
# integration job's artifact `name:` and job `name:` in ci.yml.
#
# The list must also match the Makefile's test-integration-<part> targets, one
# for one (test-integration-parts, the fixtures' own target, is not a part): a
# target left out of the list would run in no CI job, and a part without one
# would fail every run.
#
#   scripts/ci/integration-parts.sh --check <part>...
#       Fail unless the parts read here are exactly <part>..., make's own
#       $(INTEGRATION_PARTS) as check-integration-parts passes it, so a list
#       this reading would get wrong fails every part, locally too.

set -euo pipefail

# One plain `:=` line, or nothing CI can trust: a `+=`, a second assignment or
# an override changes make's list without changing this one.
assignments="$(grep -E '^[[:space:]]*((override|export)[[:space:]]+)*INTEGRATION_PARTS[[:space:]]*[:+?!]*=' Makefile || true)"
if [ "$(grep -c . <<<"$assignments")" -ne 1 ] ||
  ! [[ "$assignments" =~ ^INTEGRATION_PARTS[[:space:]]*:=([^#]*) ]]; then
  echo "integration-parts: the Makefile must assign INTEGRATION_PARTS once, as 'INTEGRATION_PARTS := ...'; found:" >&2
  echo "${assignments:-  nothing}" >&2
  exit 1
fi
read -ra list <<<"${BASH_REMATCH[1]}"
if [ "${#list[@]}" -eq 0 ]; then
  echo "integration-parts: INTEGRATION_PARTS is empty" >&2
  exit 1
fi

for part in "${list[@]}"; do
  if ! [[ "$part" =~ ^[a-z0-9-]+$ ]]; then
    echo "integration-parts: part '$part' is not lowercase letters, digits and dashes" >&2
    exit 1
  fi
done

targets="$(grep -oE '^test-integration-[a-z0-9-]+:' Makefile | sed 's/^test-integration-//; s/:$//' | grep -vx parts | sort -u || true)"
listed="$(printf '%s\n' "${list[@]}" | sort -u)"
if [ "$targets" != "$listed" ]; then
  echo "integration-parts: INTEGRATION_PARTS and the test-integration-<part> targets differ:" >&2
  echo "  in INTEGRATION_PARTS only: $(comm -13 <(echo "$targets") <(echo "$listed") | paste -sd' ' -)" >&2
  echo "  targets only (run by no CI job): $(comm -23 <(echo "$targets") <(echo "$listed") | paste -sd' ' -)" >&2
  exit 1
fi

if [ "${1:-}" = --check ]; then
  shift
  if [ "${list[*]}" != "$*" ]; then
    echo "integration-parts: CI would run the parts '${list[*]}', make runs '$*'" >&2
    exit 1
  fi
  exit 0
fi

json="" fragments="" producers=""
for part in "${list[@]}"; do
  json+="${json:+,}\"$part\""
  fragments+="${fragments:+,}coverage-integration-$part"
  producers+="${producers:+,}Integration tests ($part)"
done

echo "integration-parts=[$json]"
echo "integration-fragments=$fragments"
echo "integration-producers=$producers"

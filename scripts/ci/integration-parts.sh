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

set -euo pipefail

parts="$(sed -n 's/^INTEGRATION_PARTS[[:space:]]*:=[[:space:]]*//p' Makefile)"
read -ra list <<<"$parts"
if [ "${#list[@]}" -eq 0 ]; then
  echo "integration-parts: no INTEGRATION_PARTS := ... line in the Makefile" >&2
  exit 1
fi

json="" fragments="" producers=""
for part in "${list[@]}"; do
  if ! [[ "$part" =~ ^[a-z0-9-]+$ ]]; then
    echo "integration-parts: part '$part' is not lowercase letters, digits and dashes" >&2
    exit 1
  fi
  json+="${json:+,}\"$part\""
  fragments+="${fragments:+,}coverage-integration-$part"
  producers+="${producers:+,}Integration tests ($part)"
done

echo "integration-parts=[$json]"
echo "integration-fragments=$fragments"
echo "integration-producers=$producers"

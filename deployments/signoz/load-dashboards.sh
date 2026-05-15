#!/usr/bin/env bash
#
# Upsert dashboards/*.json into a running SigNoz instance, matching by title.
# Auth + first-run flow: see deployments/signoz/dashboards/README.md.
set -euo pipefail

SIGNOZ_URL="${SIGNOZ_URL:-http://localhost:3301}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/dashboards"

command -v jq   >/dev/null 2>&1 || { echo "error: jq is required (brew install jq | apt-get install jq)" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "error: curl is required" >&2; exit 1; }

TOKEN="${SIGNOZ_TOKEN:-}"
if [ -z "$TOKEN" ]; then
  cat >&2 <<EOF
error: SIGNOZ_TOKEN is required.

  1. Sign in at ${SIGNOZ_URL} (create your account on first visit).
  2. DevTools → Application → Local Storage → ${SIGNOZ_URL} → copy AUTH_TOKEN.
  3. Re-run: SIGNOZ_TOKEN='eyJ...' $0
EOF
  exit 1
fi
AUTH=(-H "Authorization: Bearer ${TOKEN}")

# `--max-time 30` on every call: SigNoz is local so 30s is generous, and
# `make dev-obs`'s `|| echo "failed; continuing"` only catches non-zero
# exits — a hung curl would block the whole dev session.
# --- existing dashboards: title -> id ---------------------------------------
existing="$(curl -fsS --max-time 30 "${AUTH[@]}" "${SIGNOZ_URL}/api/v1/dashboards")"

shopt -s nullglob
files=("${DIR}"/*.json)
[ "${#files[@]}" -gt 0 ] || { echo "error: no dashboard JSON files in ${DIR}" >&2; exit 1; }

# Reject duplicate titles across the local JSON files. `existing` is snapshot
# once above, so two files sharing a .title would both miss the lookup and
# silently POST — producing duplicate dashboards instead of an upsert.
dupe="$(jq -r '.title' "${files[@]}" | sort | uniq -d | head -1)"
[ -z "$dupe" ] || { echo "error: duplicate dashboard .title '$dupe' across local JSON files — titles must be unique (used as the upsert key)" >&2; exit 1; }

for f in "${files[@]}"; do
  title="$(jq -r '.title' "$f")"
  if [ -z "$title" ] || [ "$title" = "null" ]; then
    echo "error: dashboard file '$f' must contain a non-empty .title" >&2
    exit 1
  fi
  id="$(printf '%s' "$existing" | jq -r --arg t "$title" 'first(.data[] | select(.data.title == $t) | .id) // empty')"
  if [ -n "$id" ]; then
    echo "↻ updating  ${title}  (${id})"
    curl -fsS --max-time 30 -o /dev/null -X PUT  "${AUTH[@]}" -H 'Content-Type: application/json' \
      "${SIGNOZ_URL}/api/v1/dashboards/${id}" --data-binary @"$f"
  else
    echo "＋ creating  ${title}"
    curl -fsS --max-time 30 -o /dev/null -X POST "${AUTH[@]}" -H 'Content-Type: application/json' \
      "${SIGNOZ_URL}/api/v1/dashboards" --data-binary @"$f"
  fi
done

echo "done — ${SIGNOZ_URL}/dashboard"

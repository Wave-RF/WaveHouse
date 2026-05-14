#!/usr/bin/env bash
#
# Load (upsert) the WaveHouse SigNoz dashboards in ./dashboards/*.json into a
# running SigNoz instance. Existing dashboards are matched by title and updated
# in place (so bookmarked URLs survive); new ones are created. Re-run any time
# the JSON files change.
#
# SigNoz OSS has no on-disk dashboard provisioning (unlike Grafana) and disables
# self-registration, so this can't be folded into `docker compose up` — run it
# once after you've created your SigNoz account in the UI.
#
# Auth: SIGNOZ_TOKEN (the JWT used by the SPA itself).
#   1. Sign in at ${SIGNOZ_URL} (default http://localhost:3301).
#   2. DevTools → Application → Local Storage → http://localhost:3301 → copy
#      the value of `AUTH_TOKEN`.
#   3. Export it: `export SIGNOZ_TOKEN='eyJ...'`.
#
# (SigNoz v0.122.0 moved username/password login to `/api/v2/sessions/email_password`
# and requires an org UUID that isn't externally discoverable, so we no longer
# offer email/password login here — the token is one click away.)
#
# Optional:
#   SIGNOZ_URL=http://localhost:3301                      (default)
#
# Requires: curl, jq.
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

# --- existing dashboards: title -> id ---------------------------------------
existing="$(curl -fsS --max-time 30 "${AUTH[@]}" "${SIGNOZ_URL}/api/v1/dashboards")"

shopt -s nullglob
files=("${DIR}"/*.json)
[ "${#files[@]}" -gt 0 ] || { echo "error: no dashboard JSON files in ${DIR}" >&2; exit 1; }

for f in "${files[@]}"; do
  title="$(jq -r '.title' "$f")"
  if [ -z "$title" ] || [ "$title" = "null" ]; then
    echo "error: dashboard file '$f' must contain a non-empty .title" >&2
    exit 1
  fi
  id="$(printf '%s' "$existing" | jq -r --arg t "$title" 'first(.data[] | select(.data.title == $t) | .id) // empty')"
  if [ -n "$id" ]; then
    echo "↻ updating  ${title}  (${id})"
    curl -fsS -o /dev/null -X PUT  "${AUTH[@]}" -H 'Content-Type: application/json' \
      "${SIGNOZ_URL}/api/v1/dashboards/${id}" --data-binary @"$f"
  else
    echo "＋ creating  ${title}"
    curl -fsS -o /dev/null -X POST "${AUTH[@]}" -H 'Content-Type: application/json' \
      "${SIGNOZ_URL}/api/v1/dashboards" --data-binary @"$f"
  fi
done

echo "done — ${SIGNOZ_URL}/dashboard"

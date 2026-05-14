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
# Auth — provide ONE of:
#   SIGNOZ_TOKEN=<jwt>                                    bearer token; in the SigNoz UI it's
#                                                         localStorage['AUTH_TOKEN']
#   SIGNOZ_EMAIL=you@example.com SIGNOZ_PASSWORD='...'    logs in for you via /api/v1/login
# Optional:
#   SIGNOZ_URL=http://localhost:3301                      (default)
#
# Requires: curl, jq.
#
# Examples:
#   SIGNOZ_EMAIL=you@example.com SIGNOZ_PASSWORD='hunter2' deployments/signoz/load-dashboards.sh
#   SIGNOZ_TOKEN="$(pbpaste)" deployments/signoz/load-dashboards.sh
set -euo pipefail

SIGNOZ_URL="${SIGNOZ_URL:-http://localhost:3301}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/dashboards"

command -v jq   >/dev/null 2>&1 || { echo "error: jq is required (brew install jq | apt-get install jq)" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "error: curl is required" >&2; exit 1; }

# --- obtain a bearer token --------------------------------------------------
TOKEN="${SIGNOZ_TOKEN:-}"
if [ -z "$TOKEN" ]; then
  if [ -z "${SIGNOZ_EMAIL:-}" ] || [ -z "${SIGNOZ_PASSWORD:-}" ]; then
    cat >&2 <<EOF
error: no SigNoz credentials.

  1. Open ${SIGNOZ_URL} and sign in (create the account on first visit).
  2. Re-run with either:
       SIGNOZ_EMAIL=you@example.com SIGNOZ_PASSWORD='...' $0
     or paste the JWT from the browser (localStorage AUTH_TOKEN):
       SIGNOZ_TOKEN='eyJ...' $0
EOF
    exit 1
  fi
  TOKEN="$(curl -fsS -X POST "${SIGNOZ_URL}/api/v1/login" \
            -H 'Content-Type: application/json' \
            -d "$(jq -nc --arg e "$SIGNOZ_EMAIL" --arg p "$SIGNOZ_PASSWORD" '{email:$e,password:$p}')" \
          | jq -r '.accessJwt // .data.accessJwt // empty')"
  [ -n "$TOKEN" ] || { echo "error: login failed — check SIGNOZ_EMAIL / SIGNOZ_PASSWORD (or pass SIGNOZ_TOKEN instead)" >&2; exit 1; }
fi
AUTH=(-H "Authorization: Bearer ${TOKEN}")

# --- existing dashboards: title -> id ---------------------------------------
existing="$(curl -fsS "${AUTH[@]}" "${SIGNOZ_URL}/api/v1/dashboards")"

shopt -s nullglob
files=("${DIR}"/*.json)
[ "${#files[@]}" -gt 0 ] || { echo "error: no dashboard JSON files in ${DIR}" >&2; exit 1; }

for f in "${files[@]}"; do
  title="$(jq -r '.title' "$f")"
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

#!/usr/bin/env bash
set -euo pipefail

NAME="otel-lgtm"
PORT="3000"

cleanup() {
    echo -e "\n==> Stopping and removing $NAME..."
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> Starting $NAME (this may take a few seconds to boot)..."
docker run --name "$NAME" -d \
  -p "$PORT:3000" \
  -p 4317:4317 \
  -e GF_AUTH_ANONYMOUS_ENABLED=true \
  -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
  -e GF_AUTH_DISABLE_LOGIN_FORM=true \
  grafana/otel-lgtm:latest >/dev/null

echo "==> Dashboard running at: http://localhost:$PORT"
(sleep 4 && (command -v open >/dev/null && open "http://localhost:$PORT" || xdg-open "http://localhost:$PORT" 2>/dev/null)) &

echo "==> Streaming logs (Press Ctrl+C to exit)..."
docker logs -f "$NAME"

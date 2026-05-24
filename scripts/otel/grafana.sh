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
# Auto-open unless under CI.
if [ -z "${CI:-}" ]; then
  # Open browser once the dashboard is actually accepting connections
  (
    for i in $(seq 1 30); do
      if curl -sf "http://localhost:${PORT}/" >/dev/null 2>&1; then
        if command -v open >/dev/null 2>&1; then
          open "http://localhost:${PORT}"
        elif command -v xdg-open >/dev/null 2>&1; then
          xdg-open "http://localhost:${PORT}"
        fi
        break
      fi
      sleep 1
    done
  ) &
fi

echo "==> Streaming logs (Press Ctrl+C to exit)..."
docker logs -f "$NAME"

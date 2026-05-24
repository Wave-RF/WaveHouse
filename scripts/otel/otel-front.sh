#!/usr/bin/env bash
set -euo pipefail

NAME="otel-front"
PORT="8000"

cleanup() {
    echo -e "\n==> Stopping and removing $NAME..."
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> Starting $NAME..."
docker run --name "$NAME" -d \
  -p "$PORT:8000" \
  -p 4317:4317 \
  -p 4318:4318 \
  ghcr.io/mesaglio/otel-front:latest >/dev/null

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

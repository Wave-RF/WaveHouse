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
(sleep 2 && (command -v open >/dev/null && open "http://localhost:$PORT" || xdg-open "http://localhost:$PORT" 2>/dev/null)) &

echo "==> Streaming logs (Press Ctrl+C to exit)..."
docker logs -f "$NAME"

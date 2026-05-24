#!/usr/bin/env bash
set -euo pipefail

NAME="aspire-dashboard"
PORT="18888"

# Automatically clean up the container when you press Ctrl+C
cleanup() {
    echo -e "\n==> Stopping and removing $NAME..."
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Ensure clean slate before starting
docker rm -f "$NAME" >/dev/null 2>&1 || true

echo "==> Starting $NAME..."
docker run --name "$NAME" -d \
  -p "$PORT:18888" \
  -p 4317:18889 \
  -e DOTNET_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS=true \
  mcr.microsoft.com/dotnet/aspire-dashboard:latest >/dev/null

echo "==> Dashboard running at: http://localhost:$PORT"
(sleep 2 && (command -v open >/dev/null && open "http://localhost:$PORT" || xdg-open "http://localhost:$PORT" 2>/dev/null)) &

echo "==> Streaming logs (Press Ctrl+C to exit)..."
docker logs -f "$NAME"

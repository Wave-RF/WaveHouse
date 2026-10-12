#!/usr/bin/env bash
# Point this runner's Docker daemon at a Docker Hub mirror (#805). Docker Hub
# limits anonymous pulls per IP address, hosted runners share addresses, and
# the suites pull every image anonymously (ClickHouse, Redis, Valkey, NATS,
# dynamodb-local and testcontainers' own Ryuk), so a job could fail before any
# test ran.
#
# The daemon tries the mirror first and falls back to Docker Hub when the
# mirror lacks an image or refuses the pull: mirror.gcr.io caches only images
# that are pulled often, and a miss can hit its own upstream quota. With the
# mirror down, a pull is no worse off than with no mirror. Image names stay
# Docker Hub names everywhere, so nothing else in the repo knows the mirror.
#
# Run it before the job's first pull. It needs a running dockerd and root
# (through sudo when not root). dockerd reloads registry-mirrors on SIGHUP, so
# nothing restarts.
#
# Env: DOCKER_HUB_MIRROR overrides the mirror (default https://mirror.gcr.io).

set -euo pipefail

mirror=${DOCKER_HUB_MIRROR:-https://mirror.gcr.io}
conf=/etc/docker/daemon.json

as_root() {
  if [ "$(id -u)" -eq 0 ]; then "$@"; else sudo "$@"; fi
}

current='{}'
if as_root test -s "$conf"; then current=$(as_root cat "$conf"); fi
as_root mkdir -p "${conf%/*}"
jq --arg m "$mirror" '."registry-mirrors" = [$m] + ((."registry-mirrors" // []) - [$m])' <<<"$current" |
  as_root tee "$conf.new" >/dev/null
as_root mv "$conf.new" "$conf"
as_root pkill -HUP -x dockerd

# dockerd reports each mirror with a trailing slash.
want="${mirror%/}/"
for _ in $(seq 50); do
  if docker info --format '{{range .RegistryConfig.Mirrors}}{{println .}}{{end}}' | grep -qxF "$want"; then
    echo "docker-hub-mirror: Docker Hub pulls try $mirror first"
    exit 0
  fi
  sleep 0.2
done
echo "docker-hub-mirror: dockerd did not take $mirror as a registry mirror" >&2
exit 1

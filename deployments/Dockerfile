# syntax=docker/dockerfile:1
# Stage 1: Build
#
# bookworm (glibc), not alpine (musl): the chtypes SDK's dlopen path needs
# cgo (dlfcn), and the image the SDK builds its own artifacts against — plus
# the artifact's own glibc floor (2.17, 2.29 for the 24.8/25.3 lines) — is
# glibc, not musl. The non-alpine golang image bundles gcc out of the box,
# unlike alpine (which needs an explicit apk add build-base for cgo).
FROM golang:1.27-bookworm AS builder
WORKDIR /src

# Pass build tags as an argument (defaults to empty)
ARG BUILD_TAGS=""

# Download dependencies first into the module cache. Splitting this into its
# own layer means dependency changes (go.mod / go.sum) don't bust the
# source-code COPY layer's cache.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy source code
COPY . .

# Build with both BuildKit cache mounts: /go/pkg/mod for the resolved module
# tree (read by `go build` for imports) and /root/.cache/go-build for the
# compiled-package cache (Go's content-addressed build cache). Without both,
# every `docker build` is a cold compile.
#
# CGO_ENABLED=1 is unconditional (see the bookworm comment above).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -tags="${BUILD_TAGS}" -ldflags="-s -w" -o /bin/wavehouse ./cmd/wavehouse

# Bake the pinned chtypes artifact(s) into the image so the container has no
# runtime network dependency and no first-request download stall. chtypes.lock
# is this repo's pin (schema-1, exact file + sha256 per platform/line);
# scripts/fetch-chtypes.sh wraps the SDK's own CLI with --frozen, which
# refuses anything the lock doesn't name. `go run <module>@<version>`
# resolves and builds the SDK's CLI straight from its module proxy — it
# needs no entry in THIS repo's go.mod/go.sum, so this works even before
# `go mod tidy` has picked up the chtypes/go dependency.
COPY chtypes.lock ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    mkdir -p /opt/chtypes/artifacts && \
    scripts/fetch-chtypes.sh --dest /opt/chtypes/artifacts

# Create the parent state and settings directories owned by the nonroot
# user (UID 65532 in distroless). The binary creates `nats/` and `pebble/`
# subdirectories itself when NATS/Pebble open their stores — pre-creating
# them here would buy nothing and obscures intent. Named-volume copy-up runs
# against `/app/data` regardless; bind mounts mask the image dir entirely
# and rely on the host directory being writable by UID 65532.
#
# /app/settings is the settings-directory mount point, pre-created but
# deliberately EMPTY: the image ships no settings, same as it ships no
# policy. The operator bind-mounts a directory there, written on the host
# with `wavehouse bootstrap` (a bind mount, not a named volume: the image
# is distroless, with no shell to edit files inside a volume), and a
# missing mount refuses to boot instead of running on defaults nobody
# chose. Not a VOLUME either (unlike /app/data): it's config, not state,
# and an anonymous volume would hide a forgotten mount.
RUN mkdir -p /app/data /app/settings && chown -R 65532:65532 /app

# Stage 2: Final minimal image
#
# distroless/cc, not distroless/static: chtypes' dlopen path needs glibc's
# dynamic loader plus libstdc++ (the artifact is a C++-built .so) — the
# "cc" variant bundles exactly that (glibc, libgcc1, libstdc++6) and nothing
# else, still with no shell or package manager.
FROM gcr.io/distroless/cc-debian12

WORKDIR /app
USER nonroot:nonroot
ENV WH_SETTINGS_DIR=/app/settings
# The SDK's own registry search path (R3 §7) reads this directly — no
# WaveHouse config change is needed to find what the image already baked
# in. WH_CHTYPES_REGISTRY (internal/config) is for an operator who wants to
# point at a different, bind-mounted registry directory instead.
ENV CHTYPES_REGISTRY=/opt/chtypes/artifacts

COPY --from=builder --chown=nonroot:nonroot /app /app
COPY --from=builder --chown=nonroot:nonroot /opt/chtypes /opt/chtypes
COPY --from=builder --chown=nonroot:nonroot /bin/wavehouse /app/wavehouse

# OCI image metadata. Static labels live here; per-build dynamic ones
# (revision, version, created) are added by the workflow at build time
# via `docker build --label`.
LABEL org.opencontainers.image.title="WaveHouse"
LABEL org.opencontainers.image.description="Schema-aware real-time API gateway for ClickHouse"
LABEL org.opencontainers.image.url="https://github.com/Wave-RF/WaveHouse"
LABEL org.opencontainers.image.source="https://github.com/Wave-RF/WaveHouse"
LABEL org.opencontainers.image.logo="https://raw.githubusercontent.com/Wave-RF/WaveHouse/refs/heads/main/docs/public/apple-touch-icon.png"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.vendor="Wave RF"

# more custom labels for the WaveHouse project, not part of the OCI spec
LABEL dev.orbstack.icon="https://raw.githubusercontent.com/Wave-RF/WaveHouse/refs/heads/main/docs/public/apple-touch-icon.png"
LABEL com.docker.extension.icon="https://raw.githubusercontent.com/Wave-RF/WaveHouse/refs/heads/main/docs/public/apple-touch-icon.png"

# Declares /app/data as a managed mount point. If the operator runs the
# container without an explicit volume or bind mount, Docker creates an
# anonymous volume here automatically — preventing JetStream/Pebble state
# from landing on the writable overlay layer (where it'd be wiped on
# every restart). Doesn't override or interact with explicit `-v` /
# compose `volumes:` declarations; those still take precedence.
#
# The settings directory (/app/settings) is intentionally NOT a VOLUME — it's
# operator config the process only reads, and an anonymous volume would hide
# a missing mount instead of refusing to boot. See docs/deployment.md.
VOLUME ["/app/data"]

# Default listen port. ENV is just a default — compose `environment:` or
# `docker run -e` overrides it normally — and EXPOSE is image-metadata
# documentation only, not a port publish. If you set WH_SERVER_PORT to
# something else, also update your compose `ports:` mapping accordingly;
# Docker can't recompute EXPOSE at runtime.
ENV WH_SERVER_PORT=8080
EXPOSE 8080

# Liveness probe via the binary's built-in `health` subcommand — does an
# HTTP GET to /livez on localhost. Distroless has no shell or curl, so
# the subcommand approach is the standard pattern here.
#
# --interval=10s: probe every 10 seconds (default 30s is conservative
#   for a service this lightweight)
# --timeout=3s:   give the probe 3s before considering it failed (the
#   in-process check itself caps at 2s — see runHealthCheck)
# --start-period=15s: grace window for schema discovery + NATS init;
#   failures during this period don't count toward retries
# --retries=3:    three consecutive failures before marking unhealthy
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD ["/app/wavehouse", "health"]

ENTRYPOINT ["/app/wavehouse"]

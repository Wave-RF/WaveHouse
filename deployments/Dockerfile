# syntax=docker/dockerfile:1
# Stage 1: Build
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Pass build tags as an argument (defaults to empty)
ARG BUILD_TAGS=""

# Download dependencies first into the module cache. The `golang:*-alpine`
# image sets GOPATH=/go, so `go mod download` writes to /go/pkg/mod — that's
# the cache mount target. Splitting this into its own layer means dependency
# changes (go.mod / go.sum) don't bust the source-code COPY layer's cache.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy source code
COPY . .

# Build with both BuildKit cache mounts: /go/pkg/mod for the resolved module
# tree (read by `go build` for imports) and /root/.cache/go-build for the
# compiled-package cache (Go's content-addressed build cache). Without both,
# every `docker build` is a cold compile.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -tags="${BUILD_TAGS}" -ldflags="-s -w" -o /bin/wavehouse ./cmd/wavehouse

# Create the parent state and pipes-bootstrap directories owned by the nonroot
# user (UID 65532 in distroless). The binary creates `nats/` and `pebble/`
# subdirectories itself when NATS/Pebble open their stores — pre-creating
# them here would buy nothing and obscures intent. Named-volume copy-up runs
# against `/app/data` regardless; bind mounts mask the image dir entirely
# and rely on host-side ownership matching UID 65532.
RUN mkdir -p /app/data /app/pipes && chown -R 65532:65532 /app

# Stage 2: Final minimal image
FROM gcr.io/distroless/static-debian12

WORKDIR /app
USER nonroot:nonroot

COPY --from=builder --chown=nonroot:nonroot /app /app
COPY --from=builder --chown=nonroot:nonroot /bin/wavehouse /app/wavehouse

# OCI image metadata. Static labels live here; per-build dynamic ones
# (revision, version, created) are added by the workflow at build time
# via `docker build --label`.
LABEL org.opencontainers.image.title="WaveHouse"
LABEL org.opencontainers.image.description="Schema-aware real-time API gateway for ClickHouse"
LABEL org.opencontainers.image.url="https://github.com/Wave-RF/WaveHouse"
LABEL org.opencontainers.image.source="https://github.com/Wave-RF/WaveHouse"
LABEL org.opencontainers.image.logo="https://raw.githubusercontent.com/Wave-RF/WaveHouse/refs/heads/main/docs/public/apple-touch-icon.png"
LABEL org.opencontainers.image.licenses="MIT"
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
# Pipes (/app/pipes) is intentionally NOT a VOLUME — it's a read-only
# bootstrap source, not state. See docs/deployment.md.
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

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

# Default listen port. ENV is just a default — compose `environment:` or
# `docker run -e` overrides it normally — and EXPOSE is image-metadata
# documentation only, not a port publish. If you set WH_SERVER_PORT to
# something else, also update your compose `ports:` mapping accordingly;
# Docker can't recompute EXPOSE at runtime.
ENV WH_SERVER_PORT=8080
EXPOSE 8080

ENTRYPOINT ["/app/wavehouse"]

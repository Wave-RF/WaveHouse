# syntax=docker/dockerfile:1
# Stage 1: Build
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Pass build tags as an argument (defaults to empty)
ARG BUILD_TAGS=""

# Download dependencies first and cache them using BuildKit cache mounts
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

# Copy source code
COPY . .

# Build all binaries and cache the build output using BuildKit cache mounts
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -tags="${BUILD_TAGS}" -ldflags="-s -w" -o /bin/wavehouse ./cmd/wavehouse

# Create directories for state and config, and set ownership to nonroot (UID 65532 in distroless)
RUN mkdir -p /app/data/nats /app/data/pebble /app/pipes && chown -R 65532:65532 /app

# Stage 2: Final minimal image
FROM gcr.io/distroless/static-debian12

WORKDIR /app
USER nonroot:nonroot

COPY --from=builder --chown=nonroot:nonroot /app /app
COPY --from=builder --chown=nonroot:nonroot /bin/wavehouse /app/wavehouse

# Explicitly expose server port
EXPOSE 8080

# Set standalone as the default if no command is passed
ENTRYPOINT ["/app/wavehouse"]

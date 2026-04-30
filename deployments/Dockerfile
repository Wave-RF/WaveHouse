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

# Build all three binaries and cache the build output using BuildKit cache mounts
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -tags="${BUILD_TAGS}" -ldflags="-s -w" -o /bin/wavehouse ./cmd/wavehouse

# Stage 2: Final minimal image
FROM gcr.io/distroless/static-debian12

# Set user and group to non-root for security
USER nonroot:nonroot

# Copy binaries from builder
COPY --from=builder --chown=nonroot:nonroot /bin/wavehouse /usr/local/bin/wavehouse

# Set standalone as the default if no command is passed
ENTRYPOINT ["/usr/local/bin/wavehouse"]

# Stage 1: Build
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Download dependencies first (caching optimization)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Pass build tags as an argument (defaults to empty)
ARG BUILD_TAGS=""

# Build all three binaries
RUN CGO_ENABLED=0 go build -tags="${BUILD_TAGS}" -ldflags="-s -w" -o /bin/wavehouse ./cmd/wavehouse
RUN CGO_ENABLED=0 go build -tags="${BUILD_TAGS}" -ldflags="-s -w" -o /bin/wavehouse-api ./cmd/wavehouse-api
RUN CGO_ENABLED=0 go build -tags="${BUILD_TAGS}" -ldflags="-s -w" -o /bin/wavehouse-worker ./cmd/wavehouse-worker

# Stage 2: Final minimal image
FROM gcr.io/distroless/static-debian12

# Copy binaries from builder
COPY --from=builder /bin/wavehouse /usr/local/bin/wavehouse
COPY --from=builder /bin/wavehouse-api /usr/local/bin/wavehouse-api
COPY --from=builder /bin/wavehouse-worker /usr/local/bin/wavehouse-worker

# Set standalone as the default if no command is passed
ENTRYPOINT ["/usr/local/bin/wavehouse"]

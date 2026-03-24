# Deployment Guide

## Standalone (Single Binary)

The standalone mode runs everything in a single process with embedded NATS and Pebble. The only external dependency is ClickHouse.

### Docker Compose (Recommended)

```bash
docker compose -f deployments/compose/standalone.yaml up -d
```

This starts:

- **ClickHouse** on ports 8123 (HTTP) and 9000 (native)
- **BeachHouse** on port 8080

### Binary

```bash
# Build
make build

# Run (ensure ClickHouse is available)
./bin/beachhouse
```

Or with environment variables:

```bash
BH_CH_ADDR=clickhouse.example.com:9000 \
BH_AUTH_JWT_SECRET=my-secret \
./bin/beachhouse
```

## Clustered (Distributed)

Clustered mode separates the API servers from the workers and uses external infrastructure for message queuing, caching, and deduplication.

### Components

| Component | Purpose | Scaling |
| --------- | ------- | ------- |
| `beachhouse-api` | HTTP API (ingest, query, stream) | Horizontal — add more instances behind a load balancer |
| `beachhouse-worker` | Batch consumer + replay buffer | Horizontal — NATS consumer groups distribute work |
| ClickHouse | Analytics storage | Per ClickHouse docs |
| NATS | Durable event streaming (JetStream) | NATS cluster |
| Redis | L2 shared query cache | Redis cluster/sentinel |
| ScyllaDB | Distributed deduplication | ScyllaDB cluster |

### Docker Compose

```bash
# Start all infrastructure + 2 API servers + 2 workers + Caddy LB
docker compose -f deployments/compose/cluster.yaml up -d
```

This starts:

- **Caddy** reverse proxy on port 80 (round-robin load balancing with health checks)
- **ClickHouse** on ports 8123/9000
- **NATS** with JetStream on port 4222
- **Redis** on port 6379
- **ScyllaDB** on port 9042
- **2x beachhouse-api** instances
- **2x beachhouse-worker** instances

### Infrastructure Only

For local development against clustered infrastructure:

```bash
docker compose -f deployments/compose/dependencies.yaml up -d
```

This starts ClickHouse, NATS, Redis, and ScyllaDB without any BeachHouse processes.

## Docker Images

### Building

```bash
make docker
```

This builds three images:

- `beachhouse:latest`
- `beachhouse-api:latest`
- `beachhouse-worker:latest`

All images use multi-stage builds (Go Alpine builder → distroless runtime) for minimal attack surface.

### Registry

Production images are published to GitHub Container Registry via GoReleaser:

```text
ghcr.io/wave-rf/beachhouse:<tag>
ghcr.io/wave-rf/beachhouse-api:<tag>
ghcr.io/wave-rf/beachhouse-worker:<tag>
```

## Releases

Releases are built with [GoReleaser](https://goreleaser.com/). The configuration is in `.goreleaser.yaml`.

### Supported Platforms

| OS | Architecture |
| -- | ----------- |
| Linux | amd64, arm64 |
| macOS | amd64, arm64 |
| Windows | amd64, arm64 |

### Creating a Release

Tag and push to trigger the release workflow:

```bash
git tag v0.1.0
git push origin v0.1.0
```

This will:

1. Build binaries for all platforms
2. Build and push Docker images
3. Create a GitHub release with archives

## Environment Variables

All configuration can be set via environment variables. This is the recommended approach for container deployments. See [Configuration Reference](configuration.md) for the full list.

Key variables for production:

```bash
# Required
BH_AUTH_JWT_SECRET=<strong-random-secret>
BH_CH_ADDR=clickhouse:9000

# Clustered mode
BH_MODE=clustered
BH_MQ_URL=nats://nats:4222
BH_CACHE_REDIS_URL=redis://redis:6379
BH_DEDUPE_SCYLLA_HOSTS=scylla-1:9042,scylla-2:9042
BH_DEDUPE_SCYLLA_KEYSPACE=beachhouse
```

## Health Checks

Both API servers and standalone mode expose health endpoints:

- `GET /health` — Liveness probe. Returns 200 if the process is running.
- `GET /ready` — Readiness probe. Returns 200 if ClickHouse is reachable, 503 otherwise.

Configure your load balancer or orchestrator to use these endpoints.

## ClickHouse Schema

BeachHouse expects an `events` table in ClickHouse. Create it before starting:

```sql
CREATE TABLE IF NOT EXISTS events (
    tenant_id   String,
    event_id    String,
    timestamp   DateTime64(3, 'UTC'),
    event_type  String,
    data        Map(String, String)
) ENGINE = MergeTree()
ORDER BY (tenant_id, timestamp, event_id);
```

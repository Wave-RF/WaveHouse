# Deployment Guide

## Standalone (Single Binary)

The standalone mode runs everything in a single process with embedded NATS and optional Pebble dedup. The only external dependency is ClickHouse.

### Quick Start with Docker Compose

```bash
# Start ClickHouse + BeachHouse
docker compose -f deployments/compose/standalone.yaml up -d

# Create your tables in ClickHouse (BeachHouse discovers schemas automatically)
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS clicks (
      page String,
      button String,
      score Float64,
      received_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
    ) ENGINE = MergeTree()
    ORDER BY (page)
  "

# Ingest data (no auth required by default)
curl -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
```

This starts:

- **ClickHouse** on ports 8123 (HTTP) and 9000 (native)
- **BeachHouse** on port 8080

### Binary

```bash
# Build
make build

# Run standalone (uses config.yaml in current directory by default)
./bin/beachhouse
```

Or override any config with environment variables:

```bash
BH_CH_ADDR=clickhouse.example.com:9000 \
BH_SCHEMA_REFRESH_INTERVAL=30 \
./bin/beachhouse
```

## Clustered (Distributed)

Clustered mode separates the API servers from the workers and uses external infrastructure for message queuing, caching, and optional deduplication.

### Components

| Component | Purpose | Scaling |
| --------- | ------- | ------- |
| `beachhouse-api` | HTTP API (ingest, query, stream, schema) | Horizontal — add more instances behind a load balancer |
| `beachhouse-worker` | Batch consumer + Active Sweeper + DLQ | Horizontal — NATS consumer groups distribute work |
| ClickHouse | Analytics storage + schema source of truth | Per ClickHouse docs |
| NATS | Durable event streaming (JetStream) | NATS cluster |
| Redis | L2 shared query cache | Redis cluster/sentinel |
| ScyllaDB | Optional distributed deduplication | ScyllaDB cluster |

### Quick Start with Docker Compose (Clustered)

```bash
# Start all infrastructure + 2 API servers + 2 workers + Caddy LB
docker compose -f deployments/compose/cluster.yaml up -d

# Create your tables in ClickHouse
docker compose -f deployments/compose/cluster.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS clicks (
      page String,
      button String,
      score Float64,
      received_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
    ) ENGINE = MergeTree()
    ORDER BY (page)
  "

# BeachHouse API is available via Caddy on port 80
curl -X POST http://localhost/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
```

This starts:

- **Caddy** reverse proxy on port 80 (round-robin load balancing with health checks)
- **ClickHouse** on ports 8123/9000
- **NATS** with JetStream on port 4222
- **Redis** on port 6379
- **ScyllaDB** on port 9042 (for optional deduplication)
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

## Environment Variables

All configuration can be set via environment variables. This is the recommended approach for container deployments. See [Configuration Reference](configuration.md) for the full list.

Key variables for production:

```bash
# Required
BH_CH_ADDR=clickhouse:9000

# Schema discovery
BH_SCHEMA_REFRESH_INTERVAL=60      # Seconds between schema refreshes

# Optional auth
BH_AUTH_ENABLED=true
BH_AUTH_JWT_SECRET=<strong-random-secret>

# Optional dedup
BH_DEDUPE_ENABLED=true
BH_DEDUPE_ID_FIELD=event_id

# Standalone tuning
BH_MQ_GAP_WINDOW_MINUTES=15       # Minutes of NATS history for SSE/WS gap-fill
BH_MQ_MAX_BYTES_GB=50              # Max NATS JetStream disk usage (triggers backpressure)

# DLQ
BH_DLQ_ENABLED=true                # Dead Letter Queue for failed inserts

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

BeachHouse uses a **Bring Your Own Schema** model. You create your tables in ClickHouse with whatever columns and engines you need. BeachHouse discovers the schemas automatically via `system.columns` and validates ingest data against them.

Example table:

```sql
CREATE TABLE IF NOT EXISTS clicks (
    page              String,
    button            String,
    score             Float64,
    received_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (page);
```

BeachHouse discovers this schema on startup and refreshes it every `schema.refresh_interval` seconds (default: 60). You can also trigger an immediate refresh via `POST /v1/schema/refresh`.

## ScyllaDB Schema (Clustered Mode, Dedup Enabled)

When deduplication is enabled in clustered mode, ScyllaDB stores the dedup state:

```sql
CREATE KEYSPACE IF NOT EXISTS beachhouse
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

CREATE TABLE IF NOT EXISTS beachhouse.dedupe (
    event_hash text,
    created_at timestamp,
    PRIMARY KEY (event_hash)
);
```

## Dead Letter Queue (DLQ)

When `dlq.enabled` is `true` (default), failed batch inserts are published to the `BEACHHOUSE_DLQ` NATS stream under subjects `dlq.{table}`. This prevents infinite retry loops. Monitor DLQ depth via `GET /v1/dlq/stats`.

## Resetting Data in Development

### Option 1: Drop and Recreate Tables

```bash
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "DROP TABLE IF EXISTS clicks"

# Recreate the table, then restart BeachHouse to re-discover schemas
docker compose -f deployments/compose/standalone.yaml restart beachhouse
```

### Option 2: Full Reset (Clean Slate)

```bash
docker compose -f deployments/compose/standalone.yaml down -v
docker compose -f deployments/compose/standalone.yaml up -d
```

### Option 3: Reset for Local Binary Development

```bash
rm -rf data/         # Removes embedded NATS + Pebble data
make clean           # Removes bin/, tmp/, data/
make build && ./bin/beachhouse
```

# Deployment Guide

## Standalone (Single Binary)

The standalone mode runs everything in a single process with embedded NATS and Pebble. The only external dependency is ClickHouse.

### Quick Start with Docker Compose

```bash
# Start ClickHouse + BeachHouse
docker compose -f deployments/compose/standalone.yaml up -d

# Test it (the events table is auto-created at startup)
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"tenant_id": "550e8400-e29b-41d4-a716-446655440000", "exp": 9999999999}')
curl -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "660e8400-e29b-41d4-a716-446655440001", "table_name": "click", "data": {"page": "/home"}}'
```

> **Note:** In standalone mode, `clickhouse.auto_migrate` defaults to `true`, so the `events` table is created automatically. Set `BH_CH_AUTO_MIGRATE=false` to manage the schema yourself.

This starts:

- **ClickHouse** on ports 8123 (HTTP) and 9000 (native)
- **BeachHouse** on port 8080

### Binary

```bash
# Build
make build

# Run standalone (uses config.yaml in current directory by default)
# The events table is auto-created in ClickHouse on startup.
./bin/beachhouse
```

Or override any config with environment variables:

```bash
BH_CH_ADDR=clickhouse.example.com:9000 \
BH_AUTH_JWT_SECRET=my-secret \
BH_MQ_GAP_WINDOW_MINUTES=30 \
BH_MQ_MAX_BYTES_GB=100 \
./bin/beachhouse
```

## Clustered (Distributed)

Clustered mode separates the API servers from the workers and uses external infrastructure for message queuing, caching, and deduplication.

### Components

| Component | Purpose | Scaling |
| --------- | ------- | ------- |
| `beachhouse-api` | HTTP API (ingest, query, stream) | Horizontal — add more instances behind a load balancer |
| `beachhouse-worker` | Batch consumer + Active Sweeper | Horizontal — NATS consumer groups distribute work |
| ClickHouse | Analytics storage | Per ClickHouse docs |
| NATS | Durable event streaming (JetStream) | NATS cluster |
| Redis | L2 shared query cache | Redis cluster/sentinel |
| ScyllaDB | Distributed deduplication | ScyllaDB cluster |

### Quick Start with Docker Compose

```bash
# Start all infrastructure + 2 API servers + 2 workers + Caddy LB
docker compose -f deployments/compose/cluster.yaml up -d

# Create the events table in ClickHouse
# (In clustered mode, auto_migrate defaults to false — create schemas manually
# or set BH_CH_AUTO_MIGRATE=true and BH_DEDUPE_AUTO_MIGRATE=true)
docker compose -f deployments/compose/cluster.yaml exec clickhouse \
  clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS events (
      tenant_id UUID,
      event_id UUID,
      received_timestamp DateTime64(3, 'UTC'),
      ingested_timestamp DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
      timestamp DateTime64(3, 'UTC'),
      table_name String,
      str_data Map(String, String),
      num_data Map(String, Float64),
      bool_data Map(String, Bool)
    ) ENGINE = ReplacingMergeTree(ingested_timestamp)
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (tenant_id, table_name, toDate(timestamp), event_id)
  "

# Create the ScyllaDB keyspace and table
docker compose -f deployments/compose/cluster.yaml exec scylladb \
  cqlsh -e "
    CREATE KEYSPACE IF NOT EXISTS beachhouse
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    CREATE TABLE IF NOT EXISTS beachhouse.dedupe (
      tenant_id text, event_hash text, created_at timestamp,
      PRIMARY KEY (tenant_id, event_hash)
    );
  "

# BeachHouse API is available via Caddy on port 80
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"tenant_id": "550e8400-e29b-41d4-a716-446655440000", "exp": 9999999999}')
curl -X POST http://localhost/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "660e8400-e29b-41d4-a716-446655440001", "table_name": "click", "data": {"page": "/home"}}'
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

# Standalone tuning
BH_MQ_GAP_WINDOW_MINUTES=15       # Minutes of NATS history for SSE/WS gap-fill
BH_MQ_MAX_BYTES_GB=50              # Max NATS JetStream disk usage (triggers backpressure)

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

BeachHouse expects an `events` table in ClickHouse. In standalone mode, the table is **auto-created at startup** (controlled by `clickhouse.auto_migrate`, which defaults to `true`). In clustered mode, auto-migrate defaults to `false` — create it manually or set `BH_CH_AUTO_MIGRATE=true`:

```sql
CREATE TABLE IF NOT EXISTS events (
    tenant_id            UUID,
    event_id             UUID,
    received_timestamp   DateTime64(3, 'UTC'),
    ingested_timestamp   DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    timestamp            DateTime64(3, 'UTC'),
    table_name           String,
    str_data             Map(String, String),
    num_data             Map(String, Float64),
    bool_data            Map(String, Bool)
) ENGINE = ReplacingMergeTree(ingested_timestamp)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tenant_id, table_name, toDate(timestamp), event_id);
```

> **Note:** The table uses `ReplacingMergeTree(ingested_timestamp)` for deduplication at the storage level. The `table_name` field is included in the ORDER BY as a pseudo-table concept. Data is stored in three typed Map columns: `str_data`, `num_data`, and `bool_data` — not parallel arrays. Both `tenant_id` and `event_id` are UUID type. `ingested_timestamp` is auto-populated by ClickHouse via `DEFAULT now64(3, 'UTC')`.

## ScyllaDB Schema (Clustered Mode)

Clustered mode uses ScyllaDB for distributed deduplication. Like ClickHouse, the schema can be **auto-created at startup** by setting `BH_DEDUPE_AUTO_MIGRATE=true` (defaults to `false` in clustered mode). The auto-migration creates the keyspace with `SimpleStrategy` / RF=1 — production deployments should pre-create the keyspace with the appropriate replication strategy:

```sql
-- Auto-created with SimpleStrategy/RF=1, or create manually with your preferred strategy:
CREATE KEYSPACE IF NOT EXISTS beachhouse
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

CREATE TABLE IF NOT EXISTS beachhouse.dedupe (
    tenant_id  text,
    event_hash text,
    created_at timestamp,
    PRIMARY KEY (tenant_id, event_hash)
);
```

## Resetting ClickHouse in Development

When you change the ClickHouse schema (e.g., adding columns or changing the table engine), you need to drop and recreate the `events` table. ClickHouse doesn't support some `ALTER TABLE` operations on column types or engine changes.

### Option 1: Drop and Recreate (Fastest)

```bash
# Connect to ClickHouse and drop the table
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "DROP TABLE IF EXISTS events"

# Restart BeachHouse — auto_migrate will recreate it with the new schema
docker compose -f deployments/compose/standalone.yaml restart beachhouse
```

### Option 2: Full Reset (Clean Slate)

This deletes all data (ClickHouse, NATS, Pebble) and starts fresh:

```bash
# Stop everything and remove volumes
docker compose -f deployments/compose/standalone.yaml down -v

# Start fresh
docker compose -f deployments/compose/standalone.yaml up -d
```

### Option 3: Reset for Local Binary Development

If you're running BeachHouse as a local binary (not Docker), you also need to clear local data directories:

```bash
# Stop any running BeachHouse process, then:
rm -rf data/         # Removes embedded NATS + Pebble data
make clean           # Removes bin/, tmp/, data/

# Drop the ClickHouse table
docker compose -f deployments/compose/dependencies.yaml exec clickhouse \
  clickhouse-client --query "DROP TABLE IF EXISTS events"

# Rebuild and run — auto_migrate recreates the table
make build && ./bin/beachhouse
```

### Clustered Mode Reset

```bash
# Drop ClickHouse table
docker compose -f deployments/compose/cluster.yaml exec clickhouse \
  clickhouse-client --query "DROP TABLE IF EXISTS events"

# Optionally clear ScyllaDB dedup data
docker compose -f deployments/compose/cluster.yaml exec scylladb \
  cqlsh -e "TRUNCATE beachhouse.dedupe;"

# Restart services (or recreate the table manually — see ClickHouse Schema section above)
docker compose -f deployments/compose/cluster.yaml restart
```

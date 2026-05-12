---
title: "Deployment"
description: "Running WaveHouse in production: Docker images, releases, environment variables, health checks, and schema setup."
sidebar:
  order: 8
---

How to run WaveHouse in production — single binary, Docker images, releases, health checks, and the required ClickHouse schema.

## Single binary

WaveHouse runs as one process with embedded NATS and optional Pebble dedup. The only external dependency is ClickHouse.

### Quick Start with Docker Compose

```bash
# Start ClickHouse + WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d

# Create your tables in ClickHouse (WaveHouse discovers schemas automatically)
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
- **WaveHouse** on port 8080

### Binary

```bash
# Build
make build

# Run standalone (uses config.yaml in current directory by default)
./bin/wavehouse
```

Or override any config with environment variables:

```bash
WH_CH_ADDR=clickhouse.example.com:9000 \
WH_SCHEMA_REFRESH_INTERVAL=30 \
./bin/wavehouse
```

## Docker Images

### Building

```bash
make docker
```

This builds one image:

- `wavehouse:latest`

All images use multi-stage builds (Go Alpine builder → distroless runtime) for minimal attack surface.

### Registry

Production images are published to GitHub Container Registry via GoReleaser:

```text
ghcr.io/wave-rf/wavehouse:<tag>
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
WH_CH_ADDR=clickhouse:9000
WH_CH_HTTP_PORT=8123                # Port for Bento HTTP inserts (default: 8123)
WH_CH_HTTP_SCHEME=http              # Scheme for Bento HTTP inserts (http/https)

# Schema discovery
WH_SCHEMA_REFRESH_INTERVAL=60      # Seconds between schema refreshes

# CORS
WH_SERVER_CORS_ALLOWED_ORIGINS=https://app.example.com,https://admin.example.com

# Optional auth
WH_AUTH_ENABLED=true
WH_AUTH_JWT_SECRET=<strong-random-secret>
WH_AUTH_JWKS_URL=https://auth.example.com/.well-known/jwks.json
WH_AUTH_ROLE_CLAIM=app_metadata.role
# WH_AUTH_DEV_MODE=true            # Dev only — skips JWT validation

# Access control & pipes
WH_POLICY_FILE_PATH=/etc/wavehouse/policy.yaml
WH_PIPES_DIR=/etc/wavehouse/pipes

# Cache tuning
WH_CACHE_TIMESTAMP_BUCKET_SECONDS=60

# Optional dedup
WH_DEDUPE_ENABLED=true
WH_DEDUPE_ID_FIELD=event_id

# Standalone tuning
WH_MQ_GAP_WINDOW_MINUTES=15       # Minutes of NATS history for SSE/WS gap-fill
WH_MQ_MAX_BYTES_GB=50              # Max NATS JetStream disk usage (triggers backpressure)

# DLQ
WH_DLQ_ENABLED=true                # Dead Letter Queue for failed inserts
```

## Persistent Storage (REQUIRED for containers)

WaveHouse keeps all embedded state under a single configurable root, `WH_DATA_DIR` (yaml: `data_dir`). Subdirectories are convention, not config:

- `<data_dir>/nats` — embedded NATS JetStream. Holds in-flight events between an ingest POST and the Bento → ClickHouse flush, plus the `mq.gap_window_minutes` window of history that powers SSE/WS gap-fill across restarts.
- `<data_dir>/pebble` — Pebble dedup KV. Only used when `WH_DEDUPE_ENABLED=true`.

In a Docker / Podman / Kubernetes deployment, **`data_dir` must resolve to a host-backed volume**. The reference compose files in `deployments/compose/standalone.yaml`, `tests/e2e/compose.yaml`, and `clients/ts/playground/compose.yaml` set `WH_DATA_DIR=/app/data` and bind a `wavehouse-data:/app/data` volume — copy that pattern. The bundled Dockerfiles pre-create `/app/data/nats`, `/app/data/pebble`, and `/app/pipes` owned by the nonroot user (UID 65532).

If `data_dir` resolves into the container's writable overlay layer instead, **JetStream state is wiped on every restart**: in-flight events are lost, gap-fill stops bridging restarts, and disk usage accumulates inside `/var/lib/docker` instead of the volume the operator chose.

WaveHouse runs a simple existence check on startup and logs a `WARN` if `<data_dir>/nats` (or `<data_dir>/pebble` when dedupe is on) is missing or empty:

```
WARN  data directory does not exist — starting with no prior state. If this is a redeploy, your persistent volume is not actually persisting; verify your mount.
```

On a first-ever run this is expected. On every subsequent run it should be silent — so when this warning *does* fire after a redeploy, that's the most direct signal that the persistent volume isn't actually persisting.

### Distroless Permission Traps (named volume vs bind mount)

WaveHouse images run as the distroless `nonroot` user (UID 65532). Bind mounts and named volumes interact with this differently, and the distroless image has no shell to `chown` things at runtime — so getting the host side wrong produces a hard-to-read permission error from NATS or Pebble at startup.

**Named volumes** (the recommended pattern):

```yaml
volumes:
  - wavehouse-data:/app/data
```

On first attach to an empty named volume, Docker performs a "copy-up": the contents and ownership of `/app/data` *from the image* are copied into the volume. The bundled `Dockerfile` and `Dockerfile.goreleaser` both pre-create `/app/data` and `/app/pipes` with `chown -R 65532:65532`, so the volume inherits the right ownership automatically. **No host-side `chown` needed.** Subsequent restarts reuse whatever's in the volume.

**Bind mounts** (host directory):

```yaml
volumes:
  - /srv/wavehouse:/app/data
```

Bind mounts do **not** copy-up — Docker exposes the host directory as-is, and the image's pre-created dir is masked entirely. If `/srv/wavehouse` is owned by `root:root` on the host (the default for a freshly `mkdir`'d directory), the binary fails at startup with a permission error from NATS:

```
ERROR  mq init failed  error="..."  path=/app/data/nats  hint="if running in a container with a host bind mount, the host directory must be owned by UID 65532..."
```

The fix is one host-side command before first start:

```bash
sudo mkdir -p /srv/wavehouse
sudo chown -R 65532:65532 /srv/wavehouse
```

UID 65532 is the canonical distroless `nonroot` user; the same number works regardless of whether your host has a matching name in `/etc/passwd`. The error log includes this remediation hint, so if you see "permission denied" at startup, copy the suggested `chown` command and re-run.

**Pipes bind mount** follows the same rule — but mount it **read-only** since pipes is a seed, not state:

```yaml
volumes:
  - ./my-pipes:/app/pipes:ro    # :ro is intentional, see below
```

Read-only mounts don't need write permission for the container user, so `chown` isn't strictly required — but matching ownership keeps everything consistent.

## Pipes Bootstrap (optional, read-only)

Named query pipes live in NATS KV (`WAVEHOUSE_PIPES`). On first run, you can seed them from `.sql` files by setting `WH_PIPES_DIR` and bind-mounting the directory **read-only**:

```yaml
services:
  wavehouse:
    environment:
      WH_PIPES_DIR: /app/pipes
    volumes:
      - wavehouse-data:/app/data
      - ./my-pipes:/app/pipes:ro     # ← read-only seed
```

The directory is a *seed*, not authoritative storage: after bootstrap, the API + KV are the source of truth. Runtime pipe edits go through `POST /v1/pipes`, not by editing the files. The `:ro` mount makes that contract explicit and prevents accidental writes from confusing future readers. Empty default (`WH_PIPES_DIR=""`) skips bootstrap entirely — most users will create pipes via the API.

## Health Checks

API servers in standalone mode expose two health endpoints:

- `GET /health` — Liveness probe. Returns 200 if the process is running. No external dependencies.
- `GET /ready` — Readiness probe. Returns 200 if ClickHouse is reachable, 503 otherwise.

Configure your load balancer or orchestrator to use these endpoints.

### Docker `HEALTHCHECK`

Both bundled Dockerfiles (`deployments/Dockerfile` and `deployments/Dockerfile.goreleaser`) ship a built-in `HEALTHCHECK` that probes `/health` every 10 seconds. Because the runtime image is distroless (no shell, no `curl`/`wget`), the check uses the binary's own `health` subcommand:

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD ["/app/wavehouse", "health"]
```

The `health` subcommand is a thin client that does an HTTP `GET http://127.0.0.1:$WH_SERVER_PORT/health` and exits 0 (200 OK) or 1 (anything else). It honours `WH_SERVER_PORT` so it tracks whatever port the server is actually listening on.

You can run it manually for debugging:

```bash
docker exec my-wavehouse /app/wavehouse health
echo $?   # 0 = healthy, 1 = unhealthy
```

`docker ps` will show `(healthy)` / `(unhealthy)` in the STATUS column once the start-period elapses.

### Compose `depends_on: service_healthy`

The Dockerfile `HEALTHCHECK` lets dependent services wait for WaveHouse to be ready before starting:

```yaml
services:
  wavehouse:
    image: ghcr.io/wave-rf/wavehouse:latest
    # HEALTHCHECK is inherited from the image — no override needed.

  my-frontend:
    image: my-frontend:latest
    depends_on:
      wavehouse:
        condition: service_healthy
```

If you need different intervals (e.g. faster probes for E2E tests), override per-service via the compose `healthcheck:` block — that replaces the image's HEALTHCHECK for that container.

### Kubernetes / orchestrator note

K8s `livenessProbe` and `readinessProbe` use kubelet HTTP probes from outside the container — they don't go through the Dockerfile `HEALTHCHECK` at all. Configure them directly against `/health` and `/ready` in the PodSpec:

```yaml
livenessProbe:
  httpGet: { path: /health, port: 8080 }
readinessProbe:
  httpGet: { path: /ready,  port: 8080 }
```

## ClickHouse Schema

WaveHouse uses a **Bring Your Own Schema** model. You create your tables in ClickHouse with whatever columns and engines you need. WaveHouse discovers the schemas automatically via `system.columns` and validates ingest data against them.

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

WaveHouse discovers this schema on startup and refreshes it every `schema.refresh_interval` seconds (default: 60). You can also trigger an immediate refresh via `POST /v1/schema/refresh`.

## Dead Letter Queue (DLQ)

When `dlq.enabled` is `true` (default), failed batch inserts are published to the `WAVEHOUSE_DLQ` NATS stream under subjects `dlq.{table}`. This prevents infinite retry loops. Monitor DLQ depth via `GET /v1/dlq/stats`.

## Observability (SigNoz)

Set `observability.enabled: true` (or `WH_OBSERVABILITY_ENABLED=true`) and point `observability.otel_addr` at the OTLP gRPC endpoint to export traces, metrics, and logs. Each signal can be toggled independently — see `docs/configuration.md` for the full table of knobs.

WaveHouse **pushes** to an OTel collector; scraping-style pipelines (Promtail/Grafana Alloy → Loki, Vector, Fluent Bit) read stdout directly and own their own sample rates. The `observability.{traces,logs}.sample_rate` knobs apply only to the OTLP push path. Stdout always emits 100%. The logger fans out to both stdout and OTLP, so stdout output never disappears regardless of collector state. gRPC exporters are lazy, so an unreachable collector does not block startup — transient export errors are surfaced via the OTel SDK's error handler instead.

`deployments/signoz/` is a self-contained Docker Compose setup for running SigNoz locally (ClickHouse + query service + OTel collector at `:4317`). Bring it up:

```bash
docker compose -f deployments/signoz/docker-compose.yaml up -d
```

ClickHouse credentials inside the SigNoz stack default to `default` / `password`. To override, copy `deployments/signoz/.env.example` to `deployments/signoz/.env` and set `SIGNOZ_CH_USER` / `SIGNOZ_CH_PASSWORD`. The `.env` file is gitignored.

The SigNoz UI is exposed on `http://localhost:3301`. Point WaveHouse at the collector with `WH_OTEL_ADDR=127.0.0.1:4317` (the default).

## Resetting Data in Development

### Option 1: Drop and Recreate Tables

```bash
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "DROP TABLE IF EXISTS clicks"

# Recreate the table, then restart WaveHouse to re-discover schemas
docker compose -f deployments/compose/standalone.yaml restart wavehouse
```

### Option 2: Full Reset (Clean Slate)

```bash
docker compose -f deployments/compose/standalone.yaml down -v
docker compose -f deployments/compose/standalone.yaml up -d
```

### Option 3: Reset for Local Binary Development

```bash
rm -rf data/         # Removes embedded NATS + Pebble data
make clean           # Removes bin/, tmp/, data/, dist/
make build && ./bin/wavehouse
```

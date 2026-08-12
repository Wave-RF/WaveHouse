---
title: "Deployment"
description: "Running WaveHouse in production: Docker images, releases, environment variables, health checks, and schema setup."
sidebar:
  order: 10
---

How to run WaveHouse in production — single binary, Docker images, releases, health checks, and the required ClickHouse schema.

## Single binary

WaveHouse runs as one process with embedded NATS and optional Pebble dedup; ClickHouse is the only external dependency.

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

# Ingest data (the standalone stack ships a permissive trial policy;
# WaveHouse is fail-closed otherwise — see Getting Started)
# A 404 "unknown table" right after creating the table means schema
# discovery hasn't picked it up yet — retry (worst case 60s)
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
```

This starts ClickHouse (ports 8123 HTTP, 9000 native) and WaveHouse (port 8080).

### Binary

```bash
# Build
make build

# Run standalone (uses config.yaml in current directory by default)
./bin/wavehouse
```

Override config via environment variables:

```bash
WH_CH_ADDR=clickhouse.example.com:9000 \
WH_SCHEMA_REFRESH_INTERVAL=30 \
./bin/wavehouse
```

## Docker Images

### Building

```bash
docker build -f deployments/Dockerfile -t wavehouse:latest .
```

Builds runtime image `wavehouse:latest`. Published `ghcr.io` images use GoReleaser and `deployments/Dockerfile.goreleaser`. All images use multi-stage builds (Go Alpine builder → distroless runtime) for minimal attack surface.

### Registry

Production images are published to GitHub Container Registry via GoReleaser:

```text
ghcr.io/wave-rf/wavehouse:<tag>
```

Images have signed [Sigstore](https://www.sigstore.dev/) build-provenance attestations. Verify before deploying:

```bash
gh attestation verify oci://ghcr.io/wave-rf/wavehouse:<tag> --repo Wave-RF/WaveHouse
```

## Releases

Releases use [GoReleaser](https://goreleaser.com/) via `.goreleaser.yaml`. GitHub Release archives include signed [Sigstore](https://www.sigstore.dev/) build-provenance attestations; verify with `gh attestation verify <file> --repo Wave-RF/WaveHouse`. This applies to prebuilt archives, not source-compiled `go install`.

### Supported Platforms

| OS | Architecture |
| -- | ----------- |
| Linux | amd64, arm64 |
| macOS | amd64, arm64 |
| Windows | amd64, arm64 |
| FreeBSD | amd64, arm64 |

### Creating a Release

Tag and push to trigger the workflow:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Environment Variables

Configuration via environment variables is recommended for container deployments. See [Configuration Reference](/configuration) for the full list.

Key production variables:

```bash
# Required
WH_CH_ADDR=clickhouse:9000
# Port for HTTP inserts + /v1/admin/query proxy (default: 8123)
WH_CH_HTTP_PORT=8123
WH_CH_HTTP_SCHEME=http              # Scheme for the same (http/https)

# Schema discovery
WH_SCHEMA_REFRESH_INTERVAL=60      # Seconds between schema refreshes

# CORS — comma-separated allowlist (or "*" for any origin).
# WaveHouse is a Bearer-token API; no cookies are used and the middleware
# deliberately omits Access-Control-Allow-Credentials, so this allowlist only
# controls *which origins can read responses*, not cookie scope.
WH_SERVER_CORS_ALLOWED_ORIGINS=https://app.example.com,https://admin.example.com

# Auth (the JWT middleware always runs — set a secret/JWKS to validate tokens;
# without one, every request resolves to the policy default_role)
WH_AUTH_JWT_SECRET=<strong-random-secret>
WH_AUTH_JWKS_URL=https://auth.example.com/.well-known/jwks.json
WH_AUTH_ROLE_CLAIM=app_metadata.role
# Optional non-JWT operator credential (Authorization: Operator <key>, or the
# X-Operator-Key alias): full-access
# admin for bootstrap/break-glass, honored even if the policy is wiped. Treat it
# as an admin secret — inject from your secret store, serve only over TLS.
WH_AUTH_OPERATOR_KEY=<strong-random-operator-key>

# Access control & pipes — both bootstrap paths are opt-in (no default). When
# WH_POLICY_FILE_PATH is set, the file MUST exist and parse or the process
# refuses to boot (silent fail-closed is the alternative). Leave unset to skip
# bootstrap and seed via PUT /v1/admin/policy.
WH_POLICY_FILE_PATH=/etc/wavehouse/policy.yaml
WH_PIPES_DIR=/etc/wavehouse/pipes

# Cache tuning
WH_CACHE_TIMESTAMP_BUCKET_SECONDS=60

# Optional dedup
WH_DEDUPE_ENABLED=true
WH_DEDUPE_ID_FIELD=event_id
# Reject rows missing the id field instead of publishing them un-deduped
# (default false → such rows are logged + counted, not rejected).
WH_DEDUPE_REQUIRE_ID=false

# Standalone tuning
WH_MQ_GAP_WINDOW_MINUTES=15       # Minutes of NATS history for SSE gap-fill
# Max NATS JetStream disk usage (triggers backpressure)
WH_MQ_MAX_BYTES_GB=50

# DLQ
WH_DLQ_ENABLED=true                # Dead Letter Queue for failed inserts
```

## Persistent Storage (REQUIRED for containers)

WaveHouse stores embedded state under a configurable root, `WH_DATA_DIR` (yaml: `data_dir`). Subdirectories are convention-based:

- `<data_dir>/nats`: Embedded NATS JetStream. Holds in-flight events between ingest POST and ClickHouse flush, plus the `mq.gap_window_minutes` history for SSE gap-fill across restarts.
- `<data_dir>/pebble`: Pebble dedup KV. Used only when `WH_DEDUPE_ENABLED=true`.

In Docker, Podman, or Kubernetes, **`data_dir` must resolve to a host-backed volume**. Follow the pattern in `deployments/compose/standalone.yaml`, which sets `WH_DATA_DIR=/app/data` and binds a `wavehouse-data:/app/data` volume. Dockerfiles pre-create `/app/data` and `/app/pipes` owned by the nonroot user (UID 65532); the binary creates `nats/` and `pebble/` subdirectories on first run.

If `data_dir` resides in the container's writable overlay layer, **JetStream state is wiped on every restart**: in-flight events are lost, gap-fill fails, and disk usage accumulates in `/var/lib/docker`.

Volume speed is critical: JetStream `fsync`s every event to `<data_dir>/nats` before the ingest endpoint returns `200`. Volume `fsync` latency defines your ingest latency floor. Managed cloud block storage is typically sufficient; however, commodity or virtualized substrates (e.g., ZFS without SLOG, qcow2-on-`ext4`, spinning disks) may cause multi-second `fsync` stalls. See [Durability & Storage](/durability) to measure performance.

WaveHouse logs a `WARN` if `<data_dir>/nats` (or `<data_dir>/pebble` when dedupe is on) is missing or empty at startup:

```text wrap=false
WARN  data directory does not exist — starting with no prior state.
      If this is a redeploy, your persistent volume is not actually
      persisting; verify your mount.
```

This is expected on the first run but signals a persistence failure if it occurs after a redeploy.

### Distroless Permission Traps (named volume vs bind mount)

Images run as the distroless `nonroot` user (UID 65532). Because distroless images lack a shell to `chown` at runtime, incorrect host permissions cause NATS or Pebble startup errors.

**Named volumes** (recommended):

```yaml
volumes:
  - wavehouse-data:/app/data
```

On first attach, Docker performs a "copy-up" of the image's `/app/data` contents and ownership. Since `Dockerfile` and `Dockerfile.goreleaser` pre-create these with `chown -R 65532:65532`, the volume inherits correct ownership automatically. **No host-side `chown` is needed.**

**Bind mounts** (host directory):

```yaml
volumes:
  - /srv/wavehouse:/app/data
```

Bind mounts do not copy-up; they expose the host directory as-is. If `/srv/wavehouse` is owned by `root:root`, the binary fails with a permission error:

```text wrap=false
ERROR  mq init failed  error="..."  path=/app/data/nats
       hint="if running in a container with a host bind mount, the host
       directory must be owned by UID 65532..."
```

UID 65532 is the canonical distroless `nonroot` user; the number works whether or not your host has a matching name in `/etc/passwd`. The error log carries this hint, so copy the suggested `chown` and re-run. Fix before starting:

```bash
sudo mkdir -p /srv/wavehouse
sudo chown -R 65532:65532 /srv/wavehouse
```

UID 65532 is the canonical distroless `nonroot` user. If you see "permission denied," use the suggested `chown` command from the logs.

**Pipes bind mounts** follow the same rules but should be **read-only**, as pipes are seeds, not state:

```yaml
volumes:
  - ./my-pipes:/app/pipes:ro    # :ro is intentional, see below
```

Read-only mounts do not strictly require `chown` for the container user, though matching ownership maintains consistency.

## Pipes Bootstrap (optional, read-only)

Named query pipes reside in NATS KV (`WAVEHOUSE_PIPES`). Seed them from `.sql` files by setting `WH_PIPES_DIR` and bind-mounting the directory **read-only**:

```yaml
services:
  wavehouse:
    environment:
      WH_PIPES_DIR: /app/pipes
    volumes:
      - wavehouse-data:/app/data
      - ./my-pipes:/app/pipes:ro     # ← read-only seed
```

This directory is a seed; thereafter, the API and KV are authoritative. Edit runtime pipes via `PUT /v1/admin/pipes/{name}`, not files. The `:ro` mount prevents accidental writes. Setting `WH_PIPES_DIR=""` skips bootstrap.

## Health Checks

Standalone API servers expose Kubernetes-convention endpoints:

- `GET /livez` — Liveness probe. Returns 200 after the gateway discovers ClickHouse table schemas once. Returns 503 with diagnostics during boot-time schema discovery retries (e.g., ClickHouse unreachable). After boot, it remains 200; runtime ClickHouse blips affect `/readyz` only.
- `GET /readyz` — Readiness probe. Returns 200 if the gateway is booted and ClickHouse is reachable; otherwise 503.

`/healthz` is a **permanent alias** of `/livez`. `/health` and `/ready` are **deprecated aliases** for v0.1.x and will be removed in v0.2.0. Use `/livez` and `/readyz` for new deployments.

**Exposure.** Probes use the API server port (`:8080`). Metrics optionally use `prometheus.port`. If `:8080` is public, probe paths are reachable. **Recommended:** keep `/livez`, `/readyz`, and `/healthz` internal; expose only **`/v1/health`** publicly (a content-free ping that doesn't touch ClickHouse). `/readyz` issues a ClickHouse `Ping` per call, so a public `/readyz` turns an unauthenticated flood into per-request backend pings, and bare probes leak boot state. Internal routing is a [reverse-proxy/ingress concern](/reverse-proxy#health-probes); orchestrators reach probes internally via kubelet or LB.

### Boot-time degraded mode

If ClickHouse is unreachable at start, the gateway binds `:8080` and serves `/livez` 503 with the latest schema-discovery error instead of exiting. Discovery retries in the background with exponential backoff (2s $\to$ 60s cap). Once a Refresh succeeds, `/livez` flips to 200 and serving begins.

Consequently:

- The binary does not crash-loop under a supervisor; process state is preserved across outages.
- Operators can `curl /livez` for failure modes instead of grepping logs.
- Schema-aware endpoints (e.g., `/v1/ingest?table={table}`) return 4xx until discovery succeeds.

**Orchestrator restart semantics.** A Kubernetes `livenessProbe` on `/livez` will restart the pod after `failureThreshold × periodSeconds` (default ~30s), recreating a restart loop. Use a `startupProbe` to gate liveness/readiness until first schema discovery. Docker `HEALTHCHECK` marks containers `(unhealthy)` without restarting them, so `docker-compose` deployments only need `--start-period=15s` and `service_healthy` dependencies.

### Docker `HEALTHCHECK`

Bundled Dockerfiles (`deployments/Dockerfile` and `deployments/Dockerfile.goreleaser`) include a `HEALTHCHECK` probing `/livez` every 10 seconds. Since the runtime image is distroless (no shell, no `curl`/`wget`), it uses the binary's `health` subcommand:

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD ["/app/wavehouse", "health"]
```

The `health` subcommand performs an HTTP `GET http://127.0.0.1:$WH_SERVER_PORT/livez`, exiting 0 (200 OK) or 1. It honors `WH_SERVER_PORT`.

Debug manually:

```bash
docker exec my-wavehouse /app/wavehouse health
echo $?   # 0 = healthy, 1 = unhealthy
```

`docker ps` shows status in the STATUS column after the start-period.

### Compose `depends_on: service_healthy`

The Dockerfile `HEALTHCHECK` allows dependent services to wait for WaveHouse:

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

Override via a compose `healthcheck:` block for different intervals (e.g., E2E tests).

### Kubernetes / orchestrator note

K8s probes use kubelet HTTP calls, bypassing the Dockerfile `HEALTHCHECK`. Configure them in the PodSpec with a `startupProbe` to prevent boot-time restarts:

```yaml
startupProbe:
  httpGet: { path: /livez, port: 8080 }
  # allow up to 5 min for first schema discovery (30 × periodSeconds)
  failureThreshold: 30
  periodSeconds: 10
livenessProbe:
  httpGet: { path: /livez, port: 8080 }
readinessProbe:
  httpGet: { path: /readyz, port: 8080 }
```

`livenessProbe` and `readinessProbe` only run after `startupProbe` succeeds. Set `failureThreshold` to your worst-case CH boot time; 5min (30 $\times$ 10s) is generally sufficient.

## Behind a reverse proxy

WaveHouse serves plain HTTP on `:8080` without TLS termination, certificate management, or rate-limiting; use a reverse proxy, CDN, or tunnel (nginx, Caddy, Cloudflare Tunnel) for internet deployments. Proxy considerations include TLS termination, request-body size limits, header/auth forwarding, health paths, and SSE buffering (keepalive comments now prevent idle timeouts, [#226](https://github.com/Wave-RF/WaveHouse/issues/226)). See **[Behind a reverse proxy](/reverse-proxy)** for guides and configs.

## ClickHouse Schema

WaveHouse uses **Bring Your Own Schema**. Create tables in ClickHouse with any columns or engines; WaveHouse automatically discovers schemas via `system.columns` and validates ingest data.

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

Schemas refresh every `schema.refresh_interval` seconds (default: 60) or via admin-only `POST /v1/schema/refresh`.

## Dead Letter Queue (DLQ)

If `dlq.enabled` is `true` (default), failed batch inserts retry row by row; persistent failures publish to `WAVEHOUSE_DLQ` NATS stream under `dlq.{table}` to prevent infinite loops. Monitor via `GET /v1/dlq/stats`.

## Observability

Set `otel.enabled: true` (or `WH_OTEL_ENABLED=true`) to export traces, metrics, and logs. Use `OTEL_EXPORTER_OTLP_ENDPOINT` for the collector/gateway; include a scheme (`https://` for TLS, `http://` for plaintext). If unset, the SDK defaults to **TLS** at `localhost:4317`. Use `OTEL_EXPORTER_OTLP_HEADERS` for cloud auth and `OTEL_EXPORTER_OTLP_CERTIFICATE` for private CAs. See [Configuration → OTel](/configuration#otel) for all toggles.

WaveHouse **pushes** to OTel collectors. Scraping pipelines (Promtail/Grafana Alloy → Loki, Vector, Fluent Bit) read stdout directly; `otel.{traces,logs}.sample_rate` only affects the OTLP push path. Stdout always emits 100% and remains active regardless of collector state. gRPC exporters are lazy; unreachable collectors won't block startup, and errors surface via the SDK handler.

### Pattern: Local collector (SigNoz, OTel Collector, Alloy)

Local collectors usually use **plaintext** gRPC. Since the SDK default is TLS, you must explicitly set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` (or `OTEL_EXPORTER_OTLP_INSECURE=true`). All signals push through one connection.

```yaml
otel:
  enabled: true   # plaintext local collector: also set OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 (the unset SDK default is TLS)
```

### Pattern: Direct-to-cloud OTLP (Honeycomb, Grafana Cloud)

Use an `https://` URL for TLS and `OTEL_EXPORTER_OTLP_HEADERS` for auth. For private gateways, use `OTEL_EXPORTER_OTLP_CERTIFICATE`; for mutual TLS, add `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` and `OTEL_EXPORTER_OTLP_CLIENT_KEY`. Note: the gRPC logs exporter ignores these TLS-cert vars (upstream bug [open-telemetry/opentelemetry-go#6661](https://github.com/open-telemetry/opentelemetry-go/issues/6661)); route logs through a local collector if using a private CA.

**Honeycomb**:

```bash
export WH_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=https://api.honeycomb.io:443
export OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=YOUR_API_KEY
```

**Grafana Cloud OTLP gateway**:

```bash
export WH_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp-gateway-prod-us-east-0.grafana.net:443
# instanceID:token, base64-encoded (tr -d '\n' strips base64's line wrap)
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Basic $(printf '%s' "$INSTANCE_ID:$TOKEN" | base64 | tr -d '\n')"
```

### Pattern: Datadog (via local DDOT Collector)

Datadog requires a local OTLP receiver (e.g., [DDOT Collector](https://docs.datadoghq.com/opentelemetry/setup/ddot_collector/) in the Datadog Agent). Point WaveHouse at the local plaintext receiver; auth is handled by the Agent:

```bash
export WH_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317   # plaintext gRPC; DD_API_KEY is on the Agent
```

### Pattern: Grafana Cloud / Mimir / Loki / Tempo via Grafana Alloy

- **Logs**: Alloy scrapes stdout (Docker socket/file tail/k8s API). No WaveHouse config needed.
- **Traces**: Set `OTEL_EXPORTER_OTLP_ENDPOINT` to Alloy's `otelcol.receiver.otlp` listener (`http://alloy:4317`); Alloy forwards to Tempo.
- **Metrics**: Set `prometheus.enabled: true`. Alloy's `prometheus.scrape` reads `http://wavehouse:8080/metrics`. The `prometheus` block is independent of `otel.*`: leave `otel.enabled: false` for scrape-only, or combine both if traces still go via OTLP.

WaveHouse uses the OTel SDK Prometheus exporter, automatically translating OTel metric names to Prometheus conventions (e.g., dots to underscores; counters get `_total`).

### Separating the `/metrics` listener

By default, `prometheus.port: 0` mounts `/metrics` on the main API port (usually `8080`). For production, set `port` to a non-zero value (e.g., `9091`) to create a dedicated HTTP listener for metrics. Firewall this port to internal networks.

### Local Observability Stack

We use lightweight, ephemeral tools via scripts in `scripts/otel/`:

```bash
make obs-aspire   # Simplest, in-memory, no login
make obs-grafana  # Full Grafana LGTM stack, auto-login enabled
# Simple OTeL Frontend like aspire, with more control over dashboards
make obs-front
```

These use **plaintext** receivers on ports `4317` (gRPC) and `4318` (HTTP).

- Host-run WaveHouse (`make dev`): Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`.
- Containerized WaveHouse: Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4317`.

### Dashboards

We do not maintain version-controlled JSON dashboards due to the ephemeral nature of local tools.

- `make obs-aspire`: Pre-built UI, zero config.
- `make obs-grafana`: Auto-provisioned data sources and bypassed login; use "Explore" for logs/traces.
- `make obs-front`: Supports custom dashboards with simpler configuration than Grafana.

For production, build vendor-specific dashboards based on standard OTel emissions.

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
                     # (run `make clean-all` to also drop docker volumes)
make clean           # Removes build artifacts:
                     # bin/, dist/, clients/ts/dist/, docs/dist/, docs/.dev-dist/
make build && ./bin/wavehouse
```

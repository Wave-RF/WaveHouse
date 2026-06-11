---
title: "Deployment"
description: "Running WaveHouse in production: Docker images, releases, environment variables, health checks, and schema setup."
sidebar:
  order: 10
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

# Ingest data (the standalone stack ships a permissive trial policy;
# WaveHouse is fail-closed otherwise — see Getting Started)
# A 404 "unknown table" right after creating the table means schema
# discovery hasn't picked it up yet — retry (worst case 60s)
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
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
docker build -f deployments/Dockerfile -t wavehouse:latest .
```

This builds the runtime image `wavehouse:latest`. (The published `ghcr.io` images are built by GoReleaser from `deployments/Dockerfile.goreleaser`, not this command — see Registry below.)

All images use multi-stage builds (Go Alpine builder → distroless runtime) for minimal attack surface.

### Registry

Production images are published to GitHub Container Registry via GoReleaser:

```text
ghcr.io/wave-rf/wavehouse:<tag>
```

Published images carry a signed [Sigstore](https://www.sigstore.dev/) build-provenance attestation (stored in the registry). Verify one before deploying:

```bash
gh attestation verify oci://ghcr.io/wave-rf/wavehouse:<tag> --repo Wave-RF/WaveHouse
```

## Releases

Releases are built with [GoReleaser](https://goreleaser.com/). The configuration is in `.goreleaser.yaml`. The release archives attached to each GitHub Release carry a signed [Sigstore](https://www.sigstore.dev/) build-provenance attestation — verify a downloaded archive with `gh attestation verify <file> --repo Wave-RF/WaveHouse`. (This covers the prebuilt archives, not `go install`, which compiles from source.)

### Supported Platforms

| OS | Architecture |
| -- | ----------- |
| Linux | amd64, arm64 |
| macOS | amd64, arm64 |
| Windows | amd64, arm64 |
| FreeBSD | amd64, arm64 |

### Creating a Release

Tag and push to trigger the release workflow:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Environment Variables

All configuration can be set via environment variables. This is the recommended approach for container deployments. See [Configuration Reference](/configuration) for the full list.

Key variables for production:

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

# Standalone tuning
WH_MQ_GAP_WINDOW_MINUTES=15       # Minutes of NATS history for SSE gap-fill
# Max NATS JetStream disk usage (triggers backpressure)
WH_MQ_MAX_BYTES_GB=50

# DLQ
WH_DLQ_ENABLED=true                # Dead Letter Queue for failed inserts
```

## Persistent Storage (REQUIRED for containers)

WaveHouse keeps all embedded state under a single configurable root, `WH_DATA_DIR` (yaml: `data_dir`). Subdirectories are convention, not config:

- `<data_dir>/nats` — embedded NATS JetStream. Holds in-flight events between an ingest POST and the ingest worker → ClickHouse flush, plus the `mq.gap_window_minutes` window of history that powers SSE gap-fill across restarts.
- `<data_dir>/pebble` — Pebble dedup KV. Only used when `WH_DEDUPE_ENABLED=true`.

In a Docker / Podman / Kubernetes deployment, **`data_dir` must resolve to a host-backed volume**. The reference compose file `deployments/compose/standalone.yaml` sets `WH_DATA_DIR=/app/data` and binds a `wavehouse-data:/app/data` volume — copy that pattern. The bundled Dockerfiles pre-create `/app/data` and `/app/pipes` owned by the nonroot user (UID 65532); the binary creates the `nats/` and `pebble/` subdirectories under `/app/data` itself on first run.

If `data_dir` resolves into the container's writable overlay layer instead, **JetStream state is wiped on every restart**: in-flight events are lost, gap-fill stops bridging restarts, and disk usage accumulates inside `/var/lib/docker` instead of the volume the operator chose.

WaveHouse runs a simple existence check on startup and logs a `WARN` if `<data_dir>/nats` (or `<data_dir>/pebble` when dedupe is on) is missing or empty:

```text wrap=false
WARN  data directory does not exist — starting with no prior state.
      If this is a redeploy, your persistent volume is not actually
      persisting; verify your mount.
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

```text wrap=false
ERROR  mq init failed  error="..."  path=/app/data/nats
       hint="if running in a container with a host bind mount, the host
       directory must be owned by UID 65532..."
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

The directory is a *seed*, not authoritative storage: after bootstrap, the API + KV are the source of truth. Runtime pipe edits go through `PUT /v1/admin/pipes/{name}`, not by editing the files. The `:ro` mount makes that contract explicit and prevents accidental writes from confusing future readers. Empty default (`WH_PIPES_DIR=""`) skips bootstrap entirely — most users will create pipes via the API.

## Health Checks

API servers in standalone mode expose liveness and readiness endpoints under the Kubernetes-convention names `/livez` and `/readyz`:

- `GET /livez` — Liveness probe. Returns 200 once the gateway has discovered ClickHouse table schemas at least once. Returns 503 with a diagnostic body while the boot-time schema discovery retry loop is still running (e.g. ClickHouse unreachable, target database missing). After successful boot, `/livez` stays 200 — transient ClickHouse blips at runtime are reflected in `/readyz`, not `/livez`.
- `GET /readyz` — Readiness probe. Returns 200 if the gateway is fully booted and ClickHouse is currently reachable, 503 otherwise.

`/healthz` remains registered as a **permanent alias** of `/livez` (it's the most widely-recognized name); `/health` and `/ready` are **deprecated aliases** for the v0.1.x line and will be removed in v0.2.0. Point new deployments at the `/livez` / `/readyz` names.

Configure your load balancer or orchestrator to use these endpoints.

**Exposure.** Probes share the API server's port (`:8080`) — kubelet probes the container internally, so there's no separate-port convention for them (metrics are the signal that optionally gets its own `prometheus.port`). If you forward `:8080` to the public internet the probe paths become reachable. The **recommended** posture is to keep `/livez`/`/readyz`/`/healthz` to internal callers and expose only **`/v1/health`** publicly (the SDK's content-free liveness ping, which never touches ClickHouse). `/readyz` issues a ClickHouse `Ping` on every call, so a public `/readyz` lets an unauthenticated flood become per-request backend pings, and the bare probes leak boot/readiness state — keeping them internal is a [reverse-proxy/ingress concern](/reverse-proxy#health-probes), and your orchestrator reaches them the internal way (kubelet on the container, LB on the backend) regardless.

### Boot-time degraded mode

If ClickHouse is unreachable when WaveHouse starts (connection refused, missing database, DNS failure, etc.), the gateway no longer exits — it binds `:8080` and serves `/livez` 503 with the latest schema-discovery error as the diagnostic. Schema discovery retries in the background with exponential backoff (2s → 60s cap). Once a Refresh succeeds, `/livez` flips to 200 and normal serving begins automatically.

This means:

- The binary itself no longer exits and crash-loops every ~10s under a supervisor. Process state is preserved across CH outages.
- An operator can `curl /livez` and read the exact failure mode instead of grepping a restart-loop log.
- `/v1/ingest?table={table}` and other schema-aware endpoints will reject requests with a 4xx until discovery succeeds, since the schema registry is empty.

**Important — orchestrator restart semantics.** `/livez` returning 503 during the retry window is what most LB / `depends_on` setups want (route around the unready instance, hold dependents), but a Kubernetes `livenessProbe` pointed at `/livez` will still mark the pod unhealthy and restart it after `failureThreshold × periodSeconds` elapses (default ~30s) — effectively re-creating the restart loop at a slower cadence. Use a `startupProbe` to gate liveness/readiness until the first successful schema discovery (see the K8s example below). Docker `HEALTHCHECK` marks the container `(unhealthy)` but does not restart it by default, so docker-compose deployments don't need a separate startupProbe-equivalent — the `HEALTHCHECK`'s `--start-period=15s` plus `service_healthy` dependency wait covers the same idea at a smaller scale.

### Docker `HEALTHCHECK`

Both bundled Dockerfiles (`deployments/Dockerfile` and `deployments/Dockerfile.goreleaser`) ship a built-in `HEALTHCHECK` that probes `/livez` every 10 seconds. Because the runtime image is distroless (no shell, no `curl`/`wget`), the check uses the binary's own `health` subcommand:

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD ["/app/wavehouse", "health"]
```

The `health` subcommand is a thin client that does an HTTP `GET http://127.0.0.1:$WH_SERVER_PORT/livez` and exits 0 (200 OK) or 1 (anything else). It honors `WH_SERVER_PORT` so it tracks whatever port the server is actually listening on.

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

K8s `livenessProbe` and `readinessProbe` use kubelet HTTP probes from outside the container — they don't go through the Dockerfile `HEALTHCHECK` at all. Configure them directly against `/livez` and `/readyz` in the PodSpec, and add a `startupProbe` so the boot-time schema-discovery retry window doesn't trip liveness and restart the pod:

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

Until `startupProbe` succeeds, kubelet doesn't run `livenessProbe` or `readinessProbe` against the pod — so a slow or temporarily-unreachable ClickHouse can't restart-loop the pod via the liveness path. Size `failureThreshold` to your expected worst-case CH boot time; the default 30 × 10s = 5min is generous and works for compose-on-NAS-style deployments where CH and WaveHouse can race during a host reboot.

## Behind a reverse proxy

WaveHouse serves plain HTTP on `:8080` and does **not** terminate TLS, manage certificates, or rate-limit — put a reverse proxy, CDN, or tunnel (nginx, Caddy, Cloudflare Tunnel) in front for any internet-facing deployment. A few behaviors only matter behind a proxy: TLS termination, the request-body size limits, Server-Sent Events buffering and idle timeouts (WaveHouse has no SSE heartbeat yet, [#226](https://github.com/Wave-RF/WaveHouse/issues/226)), header/auth forwarding, and which health paths to expose. See **[Behind a reverse proxy](/reverse-proxy)** for the full guide and example nginx/Caddy/Cloudflare configs.

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

WaveHouse discovers this schema on startup and refreshes it every `schema.refresh_interval` seconds (default: 60). You can also trigger an immediate refresh via `POST /v1/schema/refresh` (admin-only).

## Dead Letter Queue (DLQ)

When `dlq.enabled` is `true` (default), failed batch inserts are published to the `WAVEHOUSE_DLQ` NATS stream under subjects `dlq.{table}`. This prevents infinite retry loops. Monitor DLQ depth via `GET /v1/dlq/stats`.

## Observability

Set `otel.enabled: true` (or `WH_OTEL_ENABLED=true`) to export traces, metrics, and logs, then point the OpenTelemetry SDK at your collector or gateway with the standard `OTEL_EXPORTER_OTLP_ENDPOINT` env var (always include a scheme — `https://` selects TLS, `http://` selects plaintext; with the endpoint unset the SDK defaults to **TLS** at `localhost:4317`, so a plaintext local collector needs `http://localhost:4317` set explicitly). `OTEL_EXPORTER_OTLP_HEADERS` carries cloud auth and `OTEL_EXPORTER_OTLP_CERTIFICATE` trusts a private CA, so telemetry can go to a local collector or straight to a TLS-protected cloud gateway with no sidecar. Each signal can be toggled independently — see [Configuration → OTel](/configuration#otel) for the full table of knobs.

WaveHouse **pushes** to an OTel collector; scraping-style pipelines (Promtail/Grafana Alloy → Loki, Vector, Fluent Bit) read stdout directly and own their own sample rates. The `otel.{traces,logs}.sample_rate` knobs apply only to the OTLP push path. Stdout always emits 100%. The logger fans out to both stdout and OTLP, so stdout output never disappears regardless of collector state. gRPC exporters are lazy, so an unreachable collector does not block startup — transient export errors are surfaced via the OTel SDK's error handler instead.

### Pattern: Local collector (SigNoz, OTel Collector, Alloy)

A local collector almost always speaks **plaintext** gRPC, but the SDK's unset default endpoint is **TLS** at `localhost:4317` — so enabling OTel alone is not enough. Point it at the collector with an explicit `http://` scheme: `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` (or set `OTEL_EXPORTER_OTLP_INSECURE=true`). All three signals (traces, metrics, logs) push through the same connection. This is the simplest setup.

```yaml
otel:
  enabled: true   # plaintext local collector: also set OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 (the unset SDK default is TLS)
```

### Pattern: Direct-to-cloud OTLP (Honeycomb, Grafana Cloud)

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to an `https://` URL to select TLS (system root CAs), and `OTEL_EXPORTER_OTLP_HEADERS` for the per-RPC auth every cloud OTLP gateway expects — no sidecar required to terminate TLS or inject auth. For a private or self-signed gateway, point `OTEL_EXPORTER_OTLP_CERTIFICATE` at the CA certificate; for mutual TLS, add `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` and `OTEL_EXPORTER_OTLP_CLIENT_KEY`. These apply to the **trace and metric** signals only — the pinned gRPC logs exporter ignores the env TLS-cert vars (upstream bug [open-telemetry/opentelemetry-go#6661](https://github.com/open-telemetry/opentelemetry-go/issues/6661)), so against a private-CA gateway the logs signal falls back to system roots and won't connect; route logs through a local collector (which terminates TLS itself) until the fix lands upstream.

**Honeycomb** (single endpoint, per-RPC auth):

```bash
export WH_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=https://api.honeycomb.io:443
export OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=YOUR_API_KEY
```

**Grafana Cloud OTLP gateway** (Basic auth):

```bash
export WH_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp-gateway-prod-us-east-0.grafana.net:443
# instanceID:token, base64-encoded (tr -d '\n' strips base64's line wrap)
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Basic $(printf '%s' "$INSTANCE_ID:$TOKEN" | base64 | tr -d '\n')"
```

### Pattern: Datadog (via local DDOT Collector)

Datadog has no public direct-to-cloud OTLP endpoint — telemetry must transit a local OTLP receiver that re-exports over Datadog's own protocol. The supported receiver is the [DDOT Collector](https://docs.datadoghq.com/opentelemetry/setup/ddot_collector/) embedded in the Datadog Agent, which exposes a standard OTLP receiver on `4317`. Point WaveHouse at the local receiver as plaintext — the API-key auth lives on the Agent, so no `OTEL_EXPORTER_OTLP_HEADERS` is needed:

```bash
export WH_OTEL_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317   # plaintext gRPC; DD_API_KEY is on the Agent
```

### Pattern: Grafana Cloud / Mimir / Loki / Tempo via Grafana Alloy

The Grafana stack typically wants Prometheus-style scraping for metrics, stdout scraping for logs, and OTLP push for traces. Wire it like this:

- **Logs**: Alloy scrapes stdout via the Docker socket / file tail / k8s logs API. No WaveHouse config needed — stdout always emits 100%.
- **Traces**: Set `OTEL_EXPORTER_OTLP_ENDPOINT` to Alloy's `otelcol.receiver.otlp` listener (`http://alloy:4317`). Alloy forwards to Tempo.
- **Metrics**: Set `prometheus.enabled: true`. Alloy's `prometheus.scrape` reads `http://wavehouse:8080/metrics` (or whatever port you configured). The `prometheus` block is independent of `otel.*` — you can leave `otel.enabled: false` if Alloy is only scraping (no OTLP push at all), or combine the two if traces still go via OTLP.

For the metrics path specifically: WaveHouse uses the OTel SDK's Prometheus exporter under the hood, which translates OTel metric names to Prometheus conventions automatically (dots and dashes become underscores; counters get a `_total` suffix). Existing OTel instruments don't need renaming.

### Separating the `/metrics` listener

By default, `prometheus.port` is `0`, which mounts `/metrics` on the main API server port (typically `8080`). This is the friendliest setup for compose / quick-start use.

For production posture where metrics should not be exposed on the public API listener, set `port` to a separate non-zero value (e.g. `9091`). WaveHouse spins up a dedicated HTTP listener bound to that port serving only `/metrics`. Firewall the port to internal networks only; the main API listener stays where it was. Both listeners participate in graceful shutdown.

### Local Observability Stack

We intentionally do not maintain a heavy, multi-node observability cluster (like SigNoz or an ELK stack) for local development. Instead, we use lightweight, ephemeral, single-container tools that boot instantly and clean themselves up.

The underlying Docker run scripts live in `scripts/otel/` and are invoked via Make:

```bash
make obs-aspire   # Simplest, in-memory, no login
make obs-grafana  # Full Grafana LGTM stack, auto-login enabled
# Simple OTeL Frontend like aspire, with more control over dashboards
make obs-front
```

All options automatically listen on standard OTLP ports (`4317` gRPC / `4318` HTTP) as **plaintext** receivers. If you are running WaveHouse directly on your host (e.g. `make dev`), set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` to reach them — the SDK's unset default dials `localhost:4317` over **TLS**, which a plaintext receiver rejects.

If you are running a containerized WaveHouse (e.g., via `deployments/compose/standalone.yaml`), you must override its environment to reach the host-bound collector: `OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4317`.

### Dashboards

Because we use ephemeral, single-container observability tools for local development, we no longer maintain strict, version-controlled JSON dashboards in this repository.

- If you use `make obs-aspire`, the UI is pre-built and requires zero configuration.
- If you use `make obs-grafana`, it is pre-configured to automatically provision the internal data sources and bypass the login screen. You can use Grafana's "Explore" tab to quickly jump between logs and traces.
- If you use `make obs-front`, it allows custom and comparison dashboards like grafana, but is simpler and easier to configure like aspire.

For production deployments, you should construct dashboards specific to your telemetry vendor (Datadog, Honeycomb, New Relic, etc.) based on the standard OpenTelemetry metrics and traces WaveHouse emits.

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

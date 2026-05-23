---
title: "Configuration"
description: "Full configuration reference — YAML settings and environment variables."
sidebar:
  order: 7
---

WaveHouse is configured via a YAML file with environment variable overrides. All environment variables use the `WH_` prefix.

## Loading Order

1. If a config file exists at the specified path (default: `config.yaml`), it is loaded first.
2. Environment variables override any values from the YAML file.
3. If no config file exists, all values are read from environment variables (with defaults).

Set `WH_CONFIG` to change the config file path:

```bash
export WH_CONFIG=/etc/wavehouse/config.yaml
```

## Full Reference

### Top-Level

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |

### State

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `data_dir` | `WH_DATA_DIR` | `./data` | Root directory for embedded state. NATS JetStream lives at `<data_dir>/nats`; Pebble (when dedupe is enabled) at `<data_dir>/pebble`. Subdirectory names are conventions, not config — one knob, one mount. **In a container this MUST resolve to a host-backed volume**; the relative default is for local binary use. WaveHouse logs a startup `WARN` when the directory is missing or empty (no prior state). See [Persistent Storage](/deployment#persistent-storage-required-for-containers). |

### Server

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `server.port` | `WH_SERVER_PORT` | `8080` | HTTP server listen port. |
| `server.shutdown_timeout` | `WH_SERVER_SHUTDOWN_TIMEOUT` | `10` | Graceful shutdown timeout in seconds. |
| `server.cors_allowed_origins` | `WH_SERVER_CORS_ALLOWED_ORIGINS` | `*` | Comma-separated list of allowed CORS origins. `*` allows any browser origin. WaveHouse is a Bearer-token API — `Access-Control-Allow-Credentials` is intentionally never sent, so this allowlist controls *which origins can read responses*, not cookie scope. Tighten to your frontend's exact origin(s) in production (e.g. `https://dashboard.example.com,http://localhost:3000`). |

### ClickHouse

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `clickhouse.addr` | `WH_CH_ADDR` | `localhost:9000` | ClickHouse native protocol address. |
| `clickhouse.http_port` | `WH_CH_HTTP_PORT` | `8123` | ClickHouse HTTP interface port. Used by the Bento ingest worker (`internal/ingest`) for bulk INSERT and by the raw-SQL proxy (`POST /v1/admin/query`, `internal/api/query.go`) to forward SQL to ClickHouse. Schema discovery uses the native protocol on `addr` instead. |
| `clickhouse.http_scheme` | `WH_CH_HTTP_SCHEME` | `http` | HTTP scheme for the ClickHouse HTTP interface (`http` or `https`). Set to `https` for TLS-encrypted ClickHouse connections. |
| `clickhouse.database` | `WH_CH_DATABASE` | `default` | Database name. Tables are discovered from this database. |
| `clickhouse.username` | `WH_CH_USERNAME` | `default` | Authentication username. |
| `clickhouse.password` | `WH_CH_PASSWORD` | *(empty)* | Authentication password. |
| `clickhouse.query_timeout` | `WH_CH_QUERY_TIMEOUT` | `30s` | Maximum allowed execution time for ClickHouse queries |

### Schema Discovery

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `schema.refresh_interval` | `WH_SCHEMA_REFRESH_INTERVAL` | `60` | How often (in seconds) to re-discover ClickHouse table schemas. Also refreshable on-demand via `POST /v1/schema/refresh`. |

### Message Queue (NATS)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `mq.gap_window_minutes` | `WH_MQ_GAP_WINDOW_MINUTES` | `15` | How many minutes of messages to retain in NATS for SSE/WS gap-fill. The Active Sweeper will not purge messages newer than this window. |
| `mq.max_bytes_gb` | `WH_MQ_MAX_BYTES_GB` | `50` | Maximum NATS JetStream stream size in GB. When full, new publishes are rejected with `DiscardNew` policy, triggering 503 backpressure on the ingest endpoint. |

### Deduplication

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `dedupe.enabled` | `WH_DEDUPE_ENABLED` | `false` | Enable event deduplication. When enabled, the ingest handler checks for duplicates using the configured ID field. |
| `dedupe.id_field` | `WH_DEDUPE_ID_FIELD` | `event_id` | JSON field name in the ingest body used as the dedup key. |

### Cache

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `cache.l1_max_cost` | `WH_CACHE_L1_MAX_COST` | `67108864` | Maximum L1 cache size in bytes (~64 MB). |
| `cache.timestamp_bucket_seconds` | `WH_CACHE_TIMESTAMP_BUCKET_SECONDS` | `60` | Bucket size (seconds) for time-range truncation in structured queries. Improves cache hit rate by normalizing timestamps. |

### Authentication

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `auth.enabled` | `WH_AUTH_ENABLED` | `false` | Enable JWT authentication on `/v1/*` routes. When disabled, all endpoints are open. |
| `auth.jwt_secret` | `WH_AUTH_JWT_SECRET` | *(empty)* | HMAC secret for JWT validation. **Must be set when auth is enabled** (unless using JWKS). |
| `auth.jwks_url` | `WH_AUTH_JWKS_URL` | *(empty)* | JWKS endpoint URL for public key validation (e.g., `https://auth.example.com/.well-known/jwks.json`). When set, JWKS is tried first, falling back to HMAC secret. |
| `auth.role_claim` | `WH_AUTH_ROLE_CLAIM` | `role` | Dot-separated JWT claim path for role extraction (e.g., `app_metadata.role`). |
| `auth.dev_mode` | `WH_AUTH_DEV_MODE` | `false` | When `true`, skips JWT validation and treats all requests as admin. **For development only.** |

### Dead Letter Queue (DLQ)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `dlq.enabled` | `WH_DLQ_ENABLED` | `true` | Enable the Dead Letter Queue. Failed batch inserts are published to the `WAVEHOUSE_DLQ` NATS stream instead of blocking retries. |

### Access Control Policy

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `policy.file_path` | `WH_POLICY_FILE_PATH` | `policy.yaml` | Path to a YAML/JSON policy file. Used to bootstrap the policy store on first startup if no policy exists in NATS KV. |

### Named Pipes

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `pipes.dir` | `WH_PIPES_DIR` | *(empty)* | Optional bootstrap source for named query pipes. When set, `.sql` files in this directory are loaded into the NATS KV pipe store on startup. The directory is **read-only at runtime** — it's a seed, not authoritative storage. After bootstrap, the API and KV are the source of truth (runtime pipe edits go through the API, not the files). Empty default skips bootstrap entirely. Mount read-only in containers (e.g. `./my-pipes:/app/pipes:ro`). |

### OTel

The master switch is `otel.enabled`. When `true`, each signal (traces/metrics/logs) is then individually gated by its own `enabled` flag — you can run traces-only, logs-only, etc. Prometheus exposition is configured in its own top-level [`prometheus`](#prometheus) block; it operates independently and works in any combination with OTel (OTLP push only, Prometheus only, both, or neither). Stdout is always active for logs (the logger fans out to both stdout and the OTLP exporter), so logs never disappear regardless of collector state. gRPC exporters are lazy, so an unreachable collector does not block startup; transient export errors are surfaced via the OTel SDK's error handler. The `if err != nil` fallback in `main.go` only fires for genuine init errors (malformed options, resource construction failure).

**Sampling rates apply only to the OTLP push path.** Stdout always emits 100% of records — operators using a scraping-style pipeline (Promtail/Grafana Alloy → Loki, Vector, Fluent Bit, etc.) set the collection rate at the scraper, not the application. WaveHouse pushes telemetry to an OTel collector; the scraper world owns its own ingest policy. If you want to throttle OTLP volume for cost, lower the rates below. If you want to throttle Loki/Datadog Logs/etc., do it at that pipeline.

**TLS / direct-to-cloud limitation.** The OTLP exporters currently use `WithInsecure()` — plaintext gRPC only. WaveHouse cannot ship directly to TLS-protected OTLP endpoints (Grafana Cloud's OTLP gateway, Honeycomb, Datadog OTLP, etc.). The standard workaround is a sidecar collector (the OTel collector or Grafana Alloy) on `127.0.0.1:4317` that receives our plaintext OTLP and re-exports to the cloud endpoint with TLS + auth headers configured locally. Tracked in #97.

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `otel.enabled` | `WH_OTEL_ENABLED` | `false` | Master switch. When `false`, no signals are initialized regardless of the sub-toggles below. |
| `otel.addr` | `WH_OTEL_ADDR` | `127.0.0.1:4317` | OTLP gRPC endpoint used by every enabled signal. Plain `host:port` — no scheme, plaintext gRPC only (see TLS note above). See `deployments/signoz/` for a local collector setup. |
| `otel.traces.enabled` | `WH_OTEL_TRACES_ENABLED` | `true` | Export traces via OTLP gRPC. |
| `otel.traces.sample_rate` | `WH_OTEL_TRACES_SAMPLE_RATE` | `1.0` | Head-based trace sampling rate in `[0.0, 1.0]`. `1.0` exports every trace; `0.0` exports none. Defaults to 100% (matches the OpenTelemetry SDK default); lower it for high-QPS production services where collector or backend cost is a concern. Best practice is "100% at the source, downsample at the collector" via tail-based sampling. Validated at config load. |
| `otel.metrics.enabled` | `WH_OTEL_METRICS_ENABLED` | `true` | Export metrics + Go runtime metrics via OTLP gRPC. Periodic reader interval is fixed at 15s. Metrics are pre-aggregated so there is no sampling knob. |
| `otel.logs.enabled` | `WH_OTEL_LOGS_ENABLED` | `true` | Export logs via OTLP gRPC. Disabling this leaves stdout logging untouched — the OTel logger provider is simply not registered. |
| `otel.logs.sample_rate` | `WH_OTEL_LOGS_SAMPLE_RATE` | `1.0` | OTLP export rate for `DEBUG`/`INFO` records, in `[0.0, 1.0]`. Validated at config load. `WARN` and `ERROR` records always export at 100% — dropping them silently during incidents is too dangerous to expose as a knob. **Stdout always receives 100% of records regardless of this rate** (see the scraper note above). |

### Prometheus

Prometheus exposition is its own top-level config block, independent of `otel.*`. Operators using a scrape-based pipeline (Grafana Alloy, Mimir, the standalone Prometheus server) can leave the entire `[otel]` block at its `enabled: false` default and turn on only `prometheus.enabled` — no OTLP collector required. Conversely, OTLP push and Prometheus can both be on at once (the underlying OTel MeterProvider drives both readers, same `Meter()` API). Disabled by default since enabling adds an unauthenticated endpoint, so opt-in is explicit.

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `prometheus.enabled` | `WH_PROMETHEUS_ENABLED` | `false` | Expose a Prometheus-format `/metrics` endpoint. Works standalone (no OTel push) or alongside `otel.metrics.enabled`. |
| `prometheus.path` | `WH_PROMETHEUS_PATH` | `/metrics` | URL path. Must start with `/`. |
| `prometheus.port` | `WH_PROMETHEUS_PORT` | `0` | Listener port. `0` mounts the endpoint on the main API server (`server.port`) — simplest, no extra port to expose. Non-zero spins up a dedicated HTTP listener, which lets you firewall metrics off the public API surface (common production posture). Must not equal `server.port` when non-zero. |

### Logging

| Env Var | Default | Description |
| ------- | ------- | ----------- |
| `WH_LOG_LEVEL` | `INFO` | Minimum log level. One of `DEBUG`, `INFO`, `WARN`, `ERROR` (case-insensitive). Applies to both stdout and (when OTel is enabled) the OTLP log exporter. See `otel.logs.sample_rate` above for the OTLP export rate. |

## Example Config File

```yaml
data_dir: ./data         # nats → ./data/nats, pebble → ./data/pebble

server:
  port: 8080
  shutdown_timeout: 10

clickhouse:
  addr: localhost:9000
  http_port: 8123
  database: default
  username: default
  password: ""

mq:
  gap_window_minutes: 15
  max_bytes_gb: 50

dedupe:
  enabled: false
  id_field: event_id

cache:
  l1_max_cost: 67108864
  default_ttl: 300
  timestamp_bucket_seconds: 60

auth:
  enabled: false
  jwt_secret: change-me-in-production
  jwks_url: ""
  role_claim: role
  dev_mode: false

schema:
  refresh_interval: 60

dlq:
  enabled: true

policy:
  file_path: policy.yaml

pipes:
  dir: ""                # empty = skip bootstrap; set + read-only mount to seed pipes

otel:
  enabled: false         # master switch — set true to export via OTLP gRPC
  addr: 127.0.0.1:4317
  traces:
    enabled: true
    sample_rate: 1.0     # head-based, [0.0, 1.0]; tune down for high QPS
  metrics:
    enabled: true        # OTLP push for metrics
  logs:
    enabled: true
    sample_rate: 1.0     # DEBUG/INFO OTLP rate; WARN+ always 100%, stdout always 100%

prometheus:
  enabled: false         # independent of otel — works standalone for scrape
  path: /metrics
  port: 0                # 0 = mount on server.port; non-zero = sidecar listener
```

---
title: "Configuration"
description: "Full configuration reference — YAML settings and environment variables."
sidebar:
  order: 9
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
| `clickhouse.http_port` | `WH_CH_HTTP_PORT` | `8123` | ClickHouse HTTP interface port. Used by the ingest worker (`internal/ingest`) for bulk INSERT and by the raw-SQL proxy (`POST /v1/admin/query`, `internal/api/query.go`) to forward SQL to ClickHouse. Schema discovery uses the native protocol on `addr` instead. |
| `clickhouse.http_scheme` | `WH_CH_HTTP_SCHEME` | `http` | HTTP scheme for the ClickHouse HTTP interface (`http` or `https`). Set to `https` for TLS-encrypted ClickHouse connections. |
| `clickhouse.database` | `WH_CH_DATABASE` | `default` | Database name. Tables are discovered from this database. |
| `clickhouse.username` | `WH_CH_USERNAME` | `default` | Authentication username. |
| `clickhouse.password` | `WH_CH_PASSWORD` | *(empty)* | Authentication password. |
| `clickhouse.query_timeout` | `WH_CH_QUERY_TIMEOUT` | `30s` | Maximum allowed execution time for ClickHouse queries |

### Schema Discovery

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `schema.refresh_interval` | `WH_SCHEMA_REFRESH_INTERVAL` | `60` | How often (in seconds) to re-discover ClickHouse table schemas. Also refreshable on-demand via `POST /v1/schema/refresh` (admin-only). |

### Message Queue (NATS)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `mq.gap_window_minutes` | `WH_MQ_GAP_WINDOW_MINUTES` | `15` | How many minutes of messages to retain in NATS for SSE gap-fill. The Active Sweeper will not purge messages newer than this window. |
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
| `auth.jwt_secret` | `WH_AUTH_JWT_SECRET` | *(empty)* | HMAC secret for JWT validation. With neither this nor `jwks_url` set, no token can validate, so every request is the policy `default_role`. |
| `auth.jwks_url` | `WH_AUTH_JWKS_URL` | *(empty)* | JWKS endpoint URL for public key validation (e.g., `https://auth.example.com/.well-known/jwks.json`). When set, JWKS is the **sole** verifier and `jwt_secret` is ignored (not a per-token fallback); the endpoint must be reachable at startup or the server fails to boot. |
| `auth.role_claim` | `WH_AUTH_ROLE_CLAIM` | `role` | Dot-separated JWT claim path for role extraction (e.g., `app_metadata.role`). |

WaveHouse accepts only the signing algorithms matching the active verifier — `HS256`/`HS384`/`HS512` for the HMAC secret, or the asymmetric family (`RS*`/`ES*`/`PS*`/`EdDSA`) for JWKS — and validates the token's `alg` before any key is used, so your IdP must sign with one of these and `alg: none` is always rejected.

**There is no auth on/off switch.** The JWT middleware always runs. A request with no token, or an invalid/expired one, falls back to the policy `default_role`; elevated access needs a valid token whose role is granted (or equals the policy `admin_role`). The privileged role and public access are **policy** settings, not config flags:

- **`admin_role`** (policy field, `"admin"` by default, exact case-sensitive match): the role granted full access and the `/v1/admin/*` gate. There is no separate `service` role.
- **`default_role`** (policy field): set it to open public (no-token) access — roleless requests are evaluated as that role; remove it to close public access. Setting it equal to `admin_role` is allowed and makes every roleless request admin (including `/v1/admin/*`) — handy for local/dev, logged loudly on every node that loads such a policy, and not for production use. `/v1/admin/*` and the schema/DLQ endpoints are admin-only, and a pipe with no `allowed_roles` authorizes nobody but the admin role.

See [API — Authentication](/api#authentication).

### Dead Letter Queue (DLQ)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `dlq.enabled` | `WH_DLQ_ENABLED` | `true` | Enable the Dead Letter Queue. Failed batch inserts are published to the `WAVEHOUSE_DLQ` NATS stream instead of blocking retries. |

### Access Control Policy

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `policy.file_path` | `WH_POLICY_FILE_PATH` | *(empty)* | Optional path to a YAML/JSON policy file used to seed the policy store on first startup (when NATS KV is empty). **When set, the file MUST exist and parse — WaveHouse refuses to boot otherwise**, so a typo or missing mount surfaces immediately instead of silently denying every request (`Evaluate` fails closed on a `nil` policy, including the admin role). Empty default — no implicit `policy.yaml` lookup — so operators opt into the bootstrap file explicitly; without one, seed the policy via `PUT /v1/admin/policy`. Once KV is populated, the file is ignored on subsequent boots (KV is the source of truth; runtime updates flow through the API and KV Watch). |

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
| `otel.addr` | `WH_OTEL_ADDR` | `127.0.0.1:4317` | OTLP gRPC endpoint used by every enabled signal. Plain `host:port` — no scheme, plaintext gRPC only (see TLS note above). See `make help` for a local collector setup. |
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
  http_scheme: http
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
  timestamp_bucket_seconds: 60

auth:
  jwt_secret: change-me-in-production
  jwks_url: ""
  role_claim: role

schema:
  refresh_interval: 60

dlq:
  enabled: true

policy:
  file_path: ""          # empty = skip bootstrap (seed via PUT /v1/admin/policy); set to a path and the file MUST exist or boot fails

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

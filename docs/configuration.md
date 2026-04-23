# Configuration Reference

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
| `mode` | `WH_MODE` | `standalone` | Deployment mode: `standalone` or `clustered`. |

### Server

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `server.port` | `WH_SERVER_PORT` | `8080` | HTTP server listen port. |
| `server.shutdown_timeout` | `WH_SERVER_SHUTDOWN_TIMEOUT` | `10` | Graceful shutdown timeout in seconds. |
| `server.cors_allowed_origins` | `WH_SERVER_CORS_ALLOWED_ORIGINS` | `*` | Comma-separated list of allowed CORS origins. `*` allows all origins. |

### ClickHouse

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `clickhouse.addr` | `WH_CH_ADDR` | `localhost:9000` | ClickHouse native protocol address. |
| `clickhouse.http_port` | `WH_CH_HTTP_PORT` | `8123` | ClickHouse HTTP interface port. Used by schema discovery to query `system.columns`. |
| `clickhouse.database` | `WH_CH_DATABASE` | `default` | Database name. Tables are discovered from this database. |
| `clickhouse.username` | `WH_CH_USERNAME` | `default` | Authentication username. |
| `clickhouse.password` | `WH_CH_PASSWORD` | *(empty)* | Authentication password. |

### Schema Discovery

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `schema.refresh_interval` | `WH_SCHEMA_REFRESH_INTERVAL` | `60` | How often (in seconds) to re-discover ClickHouse table schemas. Also refreshable on-demand via `POST /v1/schema/refresh`. |

### Message Queue (NATS)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `mq.stream_name` | `WH_MQ_STREAM_NAME` | `WAVEHOUSE` | The JetStream namespace to isolate environments (e.g., Staging vs Prod). |
| `mq.embedded_dir` | `WH_MQ_EMBEDDED_DIR` | `./data/nats` | Data directory for the embedded NATS server (standalone mode). |
| `mq.url` | `WH_MQ_URL` | `nats://localhost:4222` | NATS server URL (clustered mode). |
| `mq.gap_window_minutes` | `WH_MQ_GAP_WINDOW_MINUTES` | `15` | How many minutes of messages to retain in NATS for SSE/WS gap-fill. The Active Sweeper will not purge messages newer than this window. |
| `mq.max_bytes_gb` | `WH_MQ_MAX_BYTES_GB` | `50` | Maximum NATS JetStream stream size in GB. When full, new publishes are rejected with `DiscardNew` policy, triggering 503 backpressure on the ingest endpoint. |

### Deduplication

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `dedupe.enabled` | `WH_DEDUPE_ENABLED` | `false` | Enable event deduplication. When enabled, the ingest handler checks for duplicates using the configured ID field. |
| `dedupe.id_field` | `WH_DEDUPE_ID_FIELD` | `event_id` | JSON field name in the ingest body used as the dedup key. |
| `dedupe.embedded_dir` | `WH_DEDUPE_EMBEDDED_DIR` | `./data/pebble` | Data directory for Pebble KV store (standalone mode). |
| `dedupe.scylla_hosts` | `WH_DEDUPE_SCYLLA_HOSTS` | `localhost:9042` | ScyllaDB contact points (clustered mode). Comma-separated. |
| `dedupe.scylla_keyspace` | `WH_DEDUPE_SCYLLA_KEYSPACE` | `wavehouse` | ScyllaDB keyspace name. |

### Cache

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `cache.l1_max_cost` | `WH_CACHE_L1_MAX_COST` | `67108864` | Maximum L1 cache size in bytes (~64 MB). |
| `cache.redis_url` | `WH_CACHE_REDIS_URL` | `redis://localhost:6379` | Redis URL for L2 cache (clustered mode). |
| `cache.default_ttl` | `WH_CACHE_DEFAULT_TTL` | `300` | Default cache TTL in seconds (5 minutes). |
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
| `pipes.directory` | `WH_PIPES_DIRECTORY` | `./pipes` | Directory containing `.sql` files for named query pipes. Files are loaded on startup to bootstrap the NATS KV pipe store. |

## Example Config File

```yaml
mode: standalone

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
  embedded_dir: ./data/nats
  url: nats://localhost:4222
  gap_window_minutes: 15
  max_bytes_gb: 50

dedupe:
  enabled: false
  id_field: event_id
  embedded_dir: ./data/pebble
  scylla_hosts:
    - localhost:9042
  scylla_keyspace: wavehouse

cache:
  l1_max_cost: 67108864
  redis_url: redis://localhost:6379
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
  directory: ./pipes
```

## Mode-Specific Settings

### Standalone Mode (`mode: standalone`)

Uses embedded components. Relevant settings:

- `mq.embedded_dir` — Where embedded NATS stores its data.
- `mq.gap_window_minutes` — Gap-fill window for SSE/WS replay via NATS history.
- `mq.max_bytes_gb` — Maximum disk usage for the embedded NATS JetStream store.
- `schema.refresh_interval` — How often to re-discover ClickHouse schemas.
- `dedupe.enabled` / `dedupe.id_field` / `dedupe.embedded_dir` — Optional dedup with Pebble.
- `cache.l1_max_cost` — L1 cache size (no L2 in standalone).
- `dlq.enabled` — DLQ for failed inserts.
- ClickHouse settings are always required.

Settings ignored: `mq.url`, `dedupe.scylla_*`, `cache.redis_url`.

### Clustered Mode (`mode: clustered`)

Uses distributed components. Relevant settings:

- `mq.url` — External NATS cluster URL.
- `schema.refresh_interval` — How often to re-discover ClickHouse schemas.
- `dedupe.enabled` / `dedupe.id_field` / `dedupe.scylla_*` — Optional dedup with ScyllaDB.
- `cache.redis_url` — Redis for L2 shared cache.
- `dlq.enabled` — DLQ for failed inserts.
- All ClickHouse and cache L1 settings still apply.

Settings ignored: `mq.embedded_dir`, `dedupe.embedded_dir`.

# Configuration Reference

BeachHouse is configured via a YAML file with environment variable overrides. All environment variables use the `BH_` prefix.

## Loading Order

1. If a config file exists at the specified path (default: `config.yaml`), it is loaded first.
2. Environment variables override any values from the YAML file.
3. If no config file exists, all values are read from environment variables (with defaults).

Set `BH_CONFIG` to change the config file path:

```bash
export BH_CONFIG=/etc/beachhouse/config.yaml
```

## Full Reference

### Top-Level

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `mode` | `BH_MODE` | `standalone` | Deployment mode: `standalone` or `clustered`. |

### Server

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `server.port` | `BH_SERVER_PORT` | `8080` | HTTP server listen port. |
| `server.shutdown_timeout` | `BH_SERVER_SHUTDOWN_TIMEOUT` | `10` | Graceful shutdown timeout in seconds. |

### ClickHouse

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `clickhouse.addr` | `BH_CH_ADDR` | `localhost:9000` | ClickHouse native protocol address. |
| `clickhouse.database` | `BH_CH_DATABASE` | `default` | Database name. Tables are discovered from this database. |
| `clickhouse.username` | `BH_CH_USERNAME` | `default` | Authentication username. |
| `clickhouse.password` | `BH_CH_PASSWORD` | *(empty)* | Authentication password. |

### Schema Discovery

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `schema.refresh_interval` | `BH_SCHEMA_REFRESH_INTERVAL` | `60` | How often (in seconds) to re-discover ClickHouse table schemas. Also refreshable on-demand via `POST /v1/schema/refresh`. |

### Message Queue (NATS)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `mq.embedded_dir` | `BH_MQ_EMBEDDED_DIR` | `./data/nats` | Data directory for the embedded NATS server (standalone mode). |
| `mq.url` | `BH_MQ_URL` | `nats://localhost:4222` | NATS server URL (clustered mode). |
| `mq.gap_window_minutes` | `BH_MQ_GAP_WINDOW_MINUTES` | `15` | How many minutes of messages to retain in NATS for SSE/WS gap-fill. The Active Sweeper will not purge messages newer than this window. |
| `mq.max_bytes_gb` | `BH_MQ_MAX_BYTES_GB` | `50` | Maximum NATS JetStream stream size in GB. When full, new publishes are rejected with `DiscardNew` policy, triggering 503 backpressure on the ingest endpoint. |

### Deduplication

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `dedupe.enabled` | `BH_DEDUPE_ENABLED` | `false` | Enable event deduplication. When enabled, the ingest handler checks for duplicates using the configured ID field. |
| `dedupe.id_field` | `BH_DEDUPE_ID_FIELD` | `event_id` | JSON field name in the ingest body used as the dedup key. |
| `dedupe.embedded_dir` | `BH_DEDUPE_EMBEDDED_DIR` | `./data/pebble` | Data directory for Pebble KV store (standalone mode). |
| `dedupe.scylla_hosts` | `BH_DEDUPE_SCYLLA_HOSTS` | `localhost:9042` | ScyllaDB contact points (clustered mode). Comma-separated. |
| `dedupe.scylla_keyspace` | `BH_DEDUPE_SCYLLA_KEYSPACE` | `beachhouse` | ScyllaDB keyspace name. |

### Cache

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `cache.l1_max_cost` | `BH_CACHE_L1_MAX_COST` | `67108864` | Maximum L1 cache size in bytes (~64 MB). |
| `cache.redis_url` | `BH_CACHE_REDIS_URL` | `redis://localhost:6379` | Redis URL for L2 cache (clustered mode). |
| `cache.default_ttl` | `BH_CACHE_DEFAULT_TTL` | `300` | Default cache TTL in seconds (5 minutes). |

### Authentication

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `auth.enabled` | `BH_AUTH_ENABLED` | `false` | Enable JWT authentication on `/v1/*` routes. When disabled, all endpoints are open. |
| `auth.jwt_secret` | `BH_AUTH_JWT_SECRET` | *(empty)* | HMAC secret for JWT validation. **Must be set when auth is enabled.** |

### Dead Letter Queue (DLQ)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `dlq.enabled` | `BH_DLQ_ENABLED` | `true` | Enable the Dead Letter Queue. Failed batch inserts are published to the `BEACHHOUSE_DLQ` NATS stream instead of blocking retries. |

## Example Config File

```yaml
mode: standalone

server:
  port: 8080
  shutdown_timeout: 10

clickhouse:
  addr: localhost:9000
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
  scylla_keyspace: beachhouse

cache:
  l1_max_cost: 67108864
  redis_url: redis://localhost:6379
  default_ttl: 300

auth:
  enabled: false
  jwt_secret: change-me-in-production

schema:
  refresh_interval: 60

dlq:
  enabled: true
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

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
| `clickhouse.database` | `BH_CH_DATABASE` | `default` | Database name. |
| `clickhouse.username` | `BH_CH_USERNAME` | `default` | Authentication username. |
| `clickhouse.password` | `BH_CH_PASSWORD` | *(empty)* | Authentication password. |

### Message Queue (NATS)

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
| `mq.embedded_dir` | `BH_MQ_EMBEDDED_DIR` | `./data/nats` | Data directory for the embedded NATS server (standalone mode). |
| `mq.url` | `BH_MQ_URL` | `nats://localhost:4222` | NATS server URL (clustered mode). |

### Deduplication

| YAML Key | Env Var | Default | Description |
| -------- | ------- | ------- | ----------- |
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
| `auth.jwt_secret` | `BH_AUTH_JWT_SECRET` | *(empty)* | HMAC secret for JWT validation. **Must be set in production.** |

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

dedupe:
  embedded_dir: ./data/pebble
  scylla_hosts:
    - localhost:9042
  scylla_keyspace: beachhouse

cache:
  l1_max_cost: 67108864
  redis_url: redis://localhost:6379
  default_ttl: 300

auth:
  jwt_secret: change-me-in-production
```

## Mode-Specific Settings

### Standalone Mode (`mode: standalone`)

Uses embedded components. Relevant settings:

- `mq.embedded_dir` — Where embedded NATS stores its data.
- `dedupe.embedded_dir` — Where Pebble stores deduplication state.
- `cache.l1_max_cost` — L1 cache size (no L2 in standalone).
- ClickHouse settings are always required.

Settings ignored: `mq.url`, `dedupe.scylla_*`, `cache.redis_url`.

### Clustered Mode (`mode: clustered`)

Uses distributed components. Relevant settings:

- `mq.url` — External NATS cluster URL.
- `dedupe.scylla_hosts`, `dedupe.scylla_keyspace` — ScyllaDB for distributed dedup.
- `cache.redis_url` — Redis for L2 shared cache.
- All ClickHouse and cache L1 settings still apply.

Settings ignored: `mq.embedded_dir`, `dedupe.embedded_dir`.

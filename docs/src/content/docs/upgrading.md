---
title: "Upgrading"
description: "Upgrading a WaveHouse deployment between releases: what to export, convert and drain, and what changes for clients."
sidebar:
  order: 11
---

WaveHouse is pre-1.0, so a minor release can change configuration, wire formats and on-disk state without an automatic migration. This page lists what to do for each upgrade, in order. The [changelog](https://github.com/Wave-RF/WaveHouse/blob/main/CHANGELOG.md) has the full list of changes behind each step.

## From 0.2.0 to 0.2.1

0.2.1 is a binary swap: no setting moves, nothing is migrated, and the queue, the settings directory and the dedupe store carry over as they are. `@wavehouse/sdk` stays at 0.2.0, since the wire format did not change. Check these before you roll it out:

1. **List your proxies.** [`server.trusted_proxies`](/configuration#server), empty by default, now decides two things. A request's server span records the client's address as `client.address` only from a listed peer: with the list empty it records the proxy's own address, where 0.2.0 recorded the leftmost `X-Forwarded-For` entry, whoever wrote it. And an incoming `traceparent` is joined only from a listed peer: from any other, the request starts a new trace linked to the one it named. List every proxy that sends `X-Forwarded-For` or `traceparent`, a tracing one (nginx's `ngx_otel_module`, Caddy's `tracing`, Envoy) included ([Reverse proxy](/reverse-proxy#client-address-and-trace-context)).
2. **Pick an install method on a non-Linux host.** The GitHub Release now carries Linux binaries only, for amd64 and arm64. On macOS, Windows or FreeBSD, run the container image, which is unchanged, or build with `go install`.
3. **Check your endpoints' certificates.** 0.2.1 is built with Go 1.26.10, which drops every certificate chain that passes through one of eight compromised eMudhra intermediate CA certificates ([GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809)). Go has no setting to allow them again, so a ClickHouse, NATS, Redis or other endpoint whose certificate chains through one needs a certificate from another issuer.
4. **Adjust queries on the SSE drop metric.** `wavehouse_sse_dropped_frames_total` now also counts the frames a stream left unwritten when it ended, and carries a `reason` label (`buffer_full`, `client_gone`, `stream_closed`) and a `table` label. A query that sums the counter now sees the two new reasons too, so filter on `reason="buffer_full"` to keep the old meaning.
5. **Check your OpenTelemetry environment.** 0.2.0 ignored `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`; 0.2.1 applies them, so one already set in the process's environment now renames `service.name`, and with it the `job` label a backend that ingests OTLP derives from it, or adds attributes. Spans, OTLP metric series and log records also carry `service.instance.id`, which such a backend turns into an `instance` label. It is a fresh `<hostname>-<8 hex>` at every boot unless you set [`instance_id`](/configuration#process-roles) (`WH_INSTANCE_ID`), for example to the pod name, so set it to keep a process's series continuous across restarts ([Telling processes apart](/deployment#telling-processes-apart)).

A DynamoDB dedupe table keyed by `pk` alone, the only layout 0.2.0 accepts, keeps working unchanged. Create new dedupe tables keyed by `pk` and `sk` instead, so that an ingest table's ids can be found without a scan; [A shared dedupe table on DynamoDB](/deployment#a-shared-dedupe-table-on-dynamodb) describes both layouts and how to move to the new one.

### Versions

- **ClickHouse:** 0.2.1 is still tested against ClickHouse 26.6, and 26.8's change to how it reads a bare number in a `DateTime64` column is still open ([#536](https://github.com/Wave-RF/WaveHouse/issues/536)), so stay on 26.6.
- **NATS:** an external NATS (`mq.backend: nats`) still needs 2.14 or later, and 2.15 is now the recommended line: a 2.14 server boots with an advisory naming it.
- **Go:** building from source or with `go install` needs Go 1.26.10 or later.

## From 0.1.0 to 0.2.0

0.2.0 moves every behavioral setting out of `config.yaml` and the environment into a [settings directory](/settings-directory), replaces the policy and pipes stored in NATS KV with files in that directory, and changes the streaming wire format. So the upgrade is a conversion and a drain, not a binary swap. Most of the work happens while 0.1.0 is still serving; ingest pauses only for the switch-over at the end. Upgrade `@wavehouse/sdk` in the same window as the server.

### Export and audit, while 0.1.0 runs

1. **Pin the SDK now if it floats.** Some clients float to the newest release: a page that loads `@wavehouse/sdk` from a bare CDN URL or an `@0` range, or an app on an npm `^0` or `latest` range. Such a client picks up 0.2.x as soon as it is published, whatever the server runs. Its streams then deliver nothing against 0.1.0. Pin `@0.1` until the switch-over, then move it to `@0.2` with the server.
2. **Export the live policy and pipes.** 0.1.0 kept both in NATS KV: the policy was seeded from `policy.file_path` only while KV was empty, and pipes from `pipes.dir` only for names KV did not already hold, so KV holds the current versions and the files may be stale. 0.2.0 never reads KV and has no endpoint to read them back, so export them now. Authenticate with the operator key if `auth.operator_key` is set (it is off by default in 0.1.0), or with a bearer token carrying the admin role. If neither is configured, as in the 0.1.0 reference compose file, set `WH_AUTH_OPERATOR_KEY` (or `auth.operator_key`) and restart 0.1.0 first; the policy and pipes survive the restart, because what is in KV wins over `policy.file_path` and `pipes.dir`:

   ```bash
   AUTH="Authorization: Operator $WH_AUTH_OPERATOR_KEY"   # or: AUTH="Authorization: Bearer $ADMIN_TOKEN"
   curl -fsS -H "$AUTH" http://localhost:8080/v1/ops/policy > policy-0.1.0.json
   curl -fsS -H "$AUTH" http://localhost:8080/v1/ops/pipes > pipes-0.1.0.json
   jq . policy-0.1.0.json pipes-0.1.0.json > /dev/null && echo exported
   ```

3. **Audit your policy and tables against the stricter ingest checks.** A policy insert `check` that names a column the table does not have, or a `MATERIALIZED`, `ALIAS` or `EPHEMERAL` column, now refuses every insert by that role: `403` for a single record, or, in a batch, `200` with the error in each record's `results` entry. A value sent for a `MATERIALIZED` or `ALIAS` column is refused the same way, with `400` for a single record. An omitted key for a `Nullable(T) DEFAULT …` column now stores `NULL` rather than the default. [Upgrading across the v2 ingest envelope](/deployment#upgrading-across-the-v2-ingest-envelope) lists what to check, and [Insert checks](/access-control#insert-checks) explains the rule.

### Build the settings directory

1. **Write a starter directory** with `wavehouse bootstrap <dir>`. It writes `config.json`, `roles.json`, `policies.json` and `pipes.json` with every key at its default. Its `policies.json` is `{}`, which denies every request until you replace it; only the operator key gets through.
2. **Port your settings into `config.json`.** Every key except `dedupe.retention` and the per-table overrides (`dedupe.tables`, `dlq.tables`) is required. Copy each value from your 0.1.0 `config.yaml` or environment variable:

   | 0.1.0 key (variable) | 0.2.0 `config.json` key | Change |
   | --- | --- | --- |
   | `clickhouse.addr` (`WH_CH_ADDR`), `clickhouse.http_scheme` (`WH_CH_HTTP_SCHEME`), `clickhouse.database` (`WH_CH_DATABASE`), `clickhouse.username` (`WH_CH_USERNAME`) | same names under `clickhouse` | |
   | `clickhouse.http_port` (`WH_CH_HTTP_PORT`) | `clickhouse.http_port` | a number, not a string |
   | `clickhouse.password` (`WH_CH_PASSWORD`) | `clickhouse.password` | a per-tenant secret; restrict the file (below) |
   | `clickhouse.query_timeout` (`WH_CH_QUERY_TIMEOUT`) | `clickhouse.query_timeout` | whole seconds (`30`), not a duration (`30s`) |
   | none | `clickhouse.tls`, `headers`, `max_open_conns`, `max_idle_conns` | new; keep the bootstrap values unless you need them |
   | `auth.jwt_secret` (`WH_AUTH_JWT_SECRET`) | `auth.jwt_secret` | a per-tenant secret; restrict the file (below) |
   | `auth.jwks_url` (`WH_AUTH_JWKS_URL`), `auth.role_claim` (`WH_AUTH_ROLE_CLAIM`) | same names under `auth` | |
   | `dedupe.enabled` (`WH_DEDUPE_ENABLED`), `dedupe.id_field` (`WH_DEDUPE_ID_FIELD`), `dedupe.require_id` (`WH_DEDUPE_REQUIRE_ID`) | same names under `dedupe` | |
   | `dlq.enabled` (`WH_DLQ_ENABLED`) | `dlq.enabled` | |
   | `query.default_max_rows` (`WH_QUERY_DEFAULT_MAX_ROWS`) | `query.default_max_rows` | |
   | `cache.timestamp_bucket_seconds` (`WH_CACHE_TIMESTAMP_BUCKET_SECONDS`) | `query.timestamp_bucket_seconds` | moves to `query` |
   | `schema.refresh_interval` (`WH_SCHEMA_REFRESH_INTERVAL`) | `schema.refresh_interval` | |
   | `stream.keepalive_interval` (`WH_STREAM_KEEPALIVE_INTERVAL`) | `stream.keepalive_interval` | whole seconds, not a duration |
   | `stream.keepalive_buckets` (`WH_STREAM_KEEPALIVE_BUCKETS`) | `stream.keepalive_buckets` | |
   | `mq.gap_window_minutes` (`WH_MQ_GAP_WINDOW_MINUTES`) | `stream.gap_window_minutes` | moves to `stream` |
   | `mq.max_bytes_gb` (`WH_MQ_MAX_BYTES_GB`) | `mq.max_bytes_gb` | |
   | `server.cors_allowed_origins` (`WH_SERVER_CORS_ALLOWED_ORIGINS`) | `cors.allowed_origins` | `[]` now allows no origin; `["*"]` allows every origin |

   Several keys that accepted `0` in 0.1.0 now require a positive value. `wavehouse validate` names any value out of range. [`config.json` keys](/settings-directory#configjson-keys) documents each one.
3. **Restrict the directory once the secrets are in.** `config.json` now carries `clickhouse.password` and `auth.jwt_secret`, and `bootstrap` writes it world-readable with both empty. Make it readable only by the server's user or group; validation warns while it is world-readable. [Distroless permission traps](/deployment#distroless-permission-traps-named-volume-vs-bind-mount) covers bind mounts and Kubernetes `Secret` volumes.
4. **Convert the exported policy.** `policies.json` is JSON in the role-first v2 layout: `tables.<table>.<role>.select` rather than 0.1.0's `tables.<table>.select.<role>`. Nothing converts it for you. [Migrating from the operation-first layout](/access-control#migrating-from-the-operation-first-layout) shows the conversion and the fields that move or are now refused.
5. **Declare every role in `roles.json`**: each role a grant names, your `default_role`, your `admin_role` if you set one, and every role in a pipe's `allowed_roles`. See [`roles.json`](/settings-directory#rolesjson).
6. **Write `pipes.json`** as `{"pipes": …}`, with the array from `pipes-0.1.0.json` as its value. Each pipe's fields are unchanged. The `.sql` pipes directory is no longer read. See [`pipes.json`](/settings-directory#pipesjson).
7. **Validate the directory** with `wavehouse validate <dir>`. It reports every finding at once and exits non-zero when the server would refuse to start.

### Prepare the boot config

1. **Point the server at the directory.** Set `settings.dir` or `WH_SETTINGS_DIR`; without it the server refuses to start. The container images set `WH_SETTINGS_DIR=/app/settings` and ship it empty, so bind-mount your directory there. It is not a volume, and `/app/pipes` is gone from the images.
2. **Remove every key that moved.** Boot config is now strict: it refuses to start on a YAML key it does not declare or an unbound `WH_*` variable, naming each one. That covers every key and variable in the table above, plus `policy.file_path` (`WH_POLICY_FILE_PATH`) and `pipes.dir` (`WH_PIPES_DIR`). The stock 0.1.0 `config.yaml` is itself refused, so start from the [example `config.yaml`](https://github.com/Wave-RF/WaveHouse/blob/v0.2.0/config.yaml) in the repository. What stays in boot config is `data_dir` (`WH_DATA_DIR`), `settings.dir` (`WH_SETTINGS_DIR`), `server.port` (`WH_SERVER_PORT`), `server.shutdown_timeout` (`WH_SERVER_SHUTDOWN_TIMEOUT`), `cache.l1_max_cost` (`WH_CACHE_L1_MAX_COST`), `auth.operator_key` (`WH_AUTH_OPERATOR_KEY`), `otel.*` and `prometheus.*`, plus the new backend blocks. Under Kubernetes, a Service named `wh` or `wh-*` injects `WH_*` variables of its own; the boot error names them, and `enableServiceLinks: false` stops them.
3. **Check `data_dir` and explicit zero values.** `data_dir` must be usable before boot: not empty, not a file or a dangling symlink, and writable, or creatable under a writable parent. An explicit `0`, `false` or `""` in `config.yaml` is now honored rather than replaced by the default, so `otel.traces.sample_rate: 0` exports nothing, and `server.port: 0` or `cache.l1_max_cost: 0` refuses to start. See [Loading Order](/configuration#loading-order).
4. **Remove extra arguments to the binary.** An unknown subcommand now exits with status 2 instead of starting the server, so check any `command:` or `args:` override.
5. **Strip `X-Tenant-ID` at your proxy** if it sends one. See [Upgrading behind a proxy that already sends `X-Tenant-ID`](/deployment#upgrading-behind-a-proxy-that-already-sends-x-tenant-id).

### Switch over

Ingest pauses for these steps.

1. **Let in-flight retries finish, then stop producers.** Dedupe keys now carry the tenant and the table as well as the id, so on a table with dedupe on, a retry sent to 0.1.0 and repeated to 0.2.0 is not recognized as a duplicate. See [Upgrading across the dedupe key change](/deployment#upgrading-across-the-dedupe-key-change).
2. **Drain the queue.** The first 0.2.0 boot deletes 0.1.0's queue streams, including any dead-lettered rows and the SSE replay history, and logs how many messages it dropped. Let the queue empty and confirm nothing is still redelivering, as [Upgrading across the v2 ingest envelope](/deployment#upgrading-across-the-v2-ingest-envelope) describes.
3. **Stop 0.1.0 and start 0.2.0** with the settings directory and the new boot config, and wait for `/readyz` to answer `200`.
4. **Move the clients** (below), then resume producers.

### What changes for clients

- **`@wavehouse/sdk` moves to 0.2.x with the server.** The SSE stream now sends an `event: schema` frame followed by positional rows. A 0.1.x SDK against a 0.2.0 server, or the reverse, delivers no usable rows and raises no error, and no version speaks both, so expect a gap in streaming during the switch. A `^0.1` range does not reach 0.2.x. A page that loads it from a bare CDN URL or an `@0` range should already be pinned, from the first step. See [Transport Behavior](/sdk/streaming#transport-behavior).
- **Raw `/v1/stream` readers** that parse the SSE themselves must read the new frames: an `event: schema` frame naming the columns, then rows as positional arrays instead of objects. See [Client-Facing Format (SSE)](/api#client-facing-format-sse).
- **The policy API and the pipe write APIs are gone.** `GET`/`PUT /v1/ops/policy`, `POST /v1/ops/policy/validate`, and `PUT`/`DELETE /v1/ops/pipes/{name}` are removed (`GET /v1/ops/pipes` and `GET /v1/ops/pipes/{name}` remain), and so are the SDK's `wh.policy`, `wh.pipes.set()` and `wh.pipes.delete()`. Edit the files instead; the server picks up a change to the directory by itself, or reload it with `wh.settings.reload()`, `POST /v1/ops/settings/reload` or `SIGHUP`. See [Managing the policy](/access-control#managing-the-policy).
- **Every ingest request needs a `Content-Type`** if you call the HTTP API directly. A request without a supported one now gets `415`. The SDK already sends one.
- **Error responses are more specific.** A ClickHouse failure on a query or pipe now maps to `400`, `403`, `502` or `503`, or with no verdict to `500` (`502` on `/v1/ops/query`) as `clickhouse.unknown`, with a `code` and a `retryable` flag. In 0.1.0 it was a flat `500` on `/v1/query` and pipes, and `400` or `502` on `/v1/ops/query`, chosen by ClickHouse's own HTTP status, so a raw-SQL syntax error there moves from `502` to `400`. See [ClickHouse errors on the query paths](/api#clickhouse-errors-on-the-query-paths).

### Versions

- **ClickHouse:** 0.2.0 is tested against ClickHouse 26.6. ClickHouse 26.8 changed how it reads a bare number in a `DateTime64` column, and 0.2.0 does not yet account for it ([#536](https://github.com/Wave-RF/WaveHouse/issues/536)), so stay on 26.6 for now. The 0.1.0 compose file ran `clickhouse-server:latest`, so a deployment built from it may already be on 26.8 or later; 0.2.0 behaves there as 0.1.0 did. Send `DateTime64` values as strings rather than bare numbers, and pin the compose file's ClickHouse image to the version you already run: the 0.2.0 compose file pins 26.6, and this guide does not cover moving an existing ClickHouse data volume back to an older version.
- **NATS:** only an external NATS, which is new in 0.2.0 (`mq.backend: nats`), needs a version: 2.14 or later. See [External NATS](/deployment#external-nats).
- **Go:** building from source or with `go install` needs Go 1.26.9 or later.

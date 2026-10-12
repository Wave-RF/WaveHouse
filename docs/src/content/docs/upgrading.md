---
title: "Upgrading"
description: "Upgrading a WaveHouse deployment between releases: what to export, drain and convert before stopping the old version, and what changes for clients."
sidebar:
  order: 11
---

WaveHouse is pre-1.0, so a minor release can change configuration, wire formats and on-disk state without an automatic migration. This page lists, in order, what to do for each upgrade. The [changelog](https://github.com/Wave-RF/WaveHouse/blob/main/CHANGELOG.md) has the full list of changes behind each step.

## From 0.1.0 to 0.2.0

0.2.0 moves every behavioral setting out of `config.yaml` and the environment into a [settings directory](/settings-directory), replaces the policy and pipes stored in NATS KV with files in that directory, and changes the streaming wire format. So the upgrade is a conversion and a drain, not a binary swap. Plan for a pause in ingest, and upgrade `@wavehouse/sdk` in the same window as the server.

### Before you stop 0.1.0

1. **Export the live policy and pipes.** 0.1.0 kept both in NATS KV, seeded from `policy.file_path` and `pipes.dir` on first boot only, so the copy in KV is the current one and the files may be stale. 0.2.0 never reads KV and has no endpoint to read them back, so export them while 0.1.0 is still running:

   ```bash
   curl -H "Authorization: Operator $WH_AUTH_OPERATOR_KEY" http://localhost:8080/v1/ops/policy > policy-0.1.0.json
   curl -H "Authorization: Operator $WH_AUTH_OPERATOR_KEY" http://localhost:8080/v1/ops/pipes > pipes-0.1.0.json
   ```

2. **Audit your policy and tables against the stricter ingest checks.** A policy insert `check` that names a column the table does not have, or a `MATERIALIZED`, `ALIAS` or `EPHEMERAL` column, now refuses every insert by that role with `403`. A value sent for a `MATERIALIZED` or `ALIAS` column is now refused with `400`. An omitted key for a `Nullable(T) DEFAULT …` column now stores `NULL` rather than the default. [Upgrading across the v2 ingest envelope](/deployment#upgrading-across-the-v2-ingest-envelope) lists what to check, and [Insert checks](/access-control#insert-checks) explains the rule.
3. **Drain the queue.** The first 0.2.0 boot deletes 0.1.0's queue streams, including any dead-lettered rows and the SSE replay history, and logs how many messages it dropped. Stop producers, let the queue empty, and confirm nothing is still redelivering, as [Upgrading across the v2 ingest envelope](/deployment#upgrading-across-the-v2-ingest-envelope) describes.
4. **Let retrying producers finish.** Dedupe keys now carry the tenant, so a retry sent to 0.1.0 and repeated to 0.2.0 is not recognized as a duplicate. See [Upgrading across the dedupe key change](/deployment#upgrading-across-the-dedupe-key-change).

### Build the settings directory

1. **Write a starter directory** with `wavehouse bootstrap <dir>`. It writes `config.json`, `roles.json`, `policies.json` and `pipes.json` with every key at its default. Its `policies.json` is `{}`, which denies every token-based request until you replace it.
2. **Port your settings into `config.json`.** Every key except `dedupe.retention` is required. Copy each value from your 0.1.0 `config.yaml` or `WH_*` variable:

   | 0.1.0 key (variable) | 0.2.0 `config.json` key | Change |
   | --- | --- | --- |
   | `clickhouse.addr`, `http_scheme`, `database`, `username` (`WH_CH_*`) | same names under `clickhouse` | |
   | `clickhouse.http_port` (`WH_CH_HTTP_PORT`) | `clickhouse.http_port` | a number, not a string |
   | `clickhouse.password` (`WH_CH_PASSWORD`) | `clickhouse.password` | a per-tenant secret; see step 7 |
   | `clickhouse.query_timeout` (`WH_CH_QUERY_TIMEOUT`) | `clickhouse.query_timeout` | whole seconds (`30`), not a duration (`30s`) |
   | none | `clickhouse.tls`, `headers`, `max_open_conns`, `max_idle_conns` | new; keep the bootstrap values unless you need them |
   | `auth.jwt_secret` (`WH_AUTH_JWT_SECRET`) | `auth.jwt_secret` | a per-tenant secret; see step 7 |
   | `auth.jwks_url`, `auth.role_claim` (`WH_AUTH_*`) | same names under `auth` | |
   | `dedupe.enabled`, `id_field`, `require_id` (`WH_DEDUPE_*`) | same names under `dedupe` | |
   | `dlq.enabled` (`WH_DLQ_ENABLED`) | `dlq.enabled` | |
   | `query.default_max_rows` | `query.default_max_rows` | |
   | `cache.timestamp_bucket_seconds` | `query.timestamp_bucket_seconds` | moves to `query` |
   | `schema.refresh_interval` | `schema.refresh_interval` | |
   | `stream.keepalive_interval` | `stream.keepalive_interval` | whole seconds, not a duration |
   | `stream.keepalive_buckets` | `stream.keepalive_buckets` | |
   | `mq.gap_window_minutes` | `stream.gap_window_minutes` | moves to `stream` |
   | `mq.max_bytes_gb` | `mq.max_bytes_gb` | |
   | `server.cors_allowed_origins` | `cors.allowed_origins` | `[]` now allows no origin; `["*"]` allows every origin |

   Several keys that accepted `0` in 0.1.0 now require a positive value. `wavehouse validate` names any value out of range. [`config.json` keys](/settings-directory#configjson-keys) documents each one.
3. **Restrict the directory once the secrets are in.** `config.json` now carries `clickhouse.password` and `auth.jwt_secret`, and `bootstrap` writes it world-readable with both empty. Make it readable only by the server's user or group; validation warns while it is world-readable. [Distroless permission traps](/deployment#distroless-permission-traps-named-volume-vs-bind-mount) covers bind mounts and Kubernetes `Secret` volumes.
4. **Convert the exported policy.** `policies.json` is JSON in the role-first v2 layout: `tables.<table>.<role>.select` rather than 0.1.0's `tables.<table>.select.<role>`. Nothing converts it for you. [Migrating from the operation-first layout](/access-control#migrating-from-the-operation-first-layout) shows the conversion and the fields that move or are now refused.
5. **Declare every role in `roles.json`**: each role a grant names, your `default_role`, your `admin_role` if you set one, and every role in a pipe's `allowed_roles`. See [`roles.json`](/settings-directory#rolesjson).
6. **Write `pipes.json`** as `{"pipes": …}`, with the array from `pipes-0.1.0.json` as its value. Each pipe's fields are unchanged. The `.sql` pipes directory is no longer read. See [`pipes.json`](/settings-directory#pipesjson).
7. **Validate the directory** with `wavehouse validate <dir>`. It reports every finding at once and exits non-zero when the server would refuse to start.

### Trim the boot config

 1. **Point the server at the directory.** Set `settings.dir` or `WH_SETTINGS_DIR`; without it the server refuses to start. The container images set `WH_SETTINGS_DIR=/app/settings` and ship it empty, so bind-mount your directory there. It is not a volume, and `/app/pipes` is gone from the images.
 2. **Remove every key that moved.** Boot config is now strict, and it refuses to start on a YAML key it does not declare or an unbound `WH_*` variable, naming each one. That includes every key in the table above, `policy.file_path` (`WH_POLICY_FILE_PATH`) and `pipes.dir` (`WH_PIPES_DIR`). The stock 0.1.0 `config.yaml` is itself refused, so start from the 0.2.0 one. What stays in boot config is `data_dir`, `settings.dir`, `server.port`, `server.shutdown_timeout`, `cache.l1_max_cost`, `auth.operator_key`, `otel.*` and `prometheus.*`, plus the new backend blocks. Under Kubernetes, a Service named `wh` or `wh-*` injects `WH_*` variables of its own; the boot error names them, and `enableServiceLinks: false` stops them.
 3. **Check `data_dir` and explicit zero values.** `data_dir` must now be an existing writable directory, and an explicit `0`, `false` or `""` in `config.yaml` is now honored rather than replaced by the default. So `otel.traces.sample_rate: 0` exports nothing, and `server.port: 0` or `cache.l1_max_cost: 0` refuses to start. See [Loading Order](/configuration#loading-order).
 4. **Remove extra arguments to the binary.** An unknown subcommand now exits with status 2 instead of starting the server, so check any `command:` or `args:` override.
 5. **Strip `X-Tenant-ID` at your proxy** if it sends one. See [Upgrading behind a proxy that already sends `X-Tenant-ID`](/deployment#upgrading-behind-a-proxy-that-already-sends-x-tenant-id).

### Start 0.2.0 and move the clients with it

 1. **Upgrade `@wavehouse/sdk` to 0.2.x at the same time.** The SSE stream now sends an `event: schema` frame followed by positional rows. A 0.1.x SDK against a 0.2.0 server, or the reverse, delivers no usable rows and raises no error, and there is no version that speaks both, so expect a gap in streaming during the switch. A `^0.1` range does not reach 0.2.x; a bare `@0` CDN URL does. See [Transport Behavior](/sdk/streaming#transport-behavior).
 2. **Update code that used the removed write APIs.** `GET`/`PUT /v1/ops/policy`, `POST /v1/ops/policy/validate`, and `PUT`/`DELETE /v1/ops/pipes/{name}` are gone, and so are the SDK's `wh.policy`, `wh.pipes.set()` and `wh.pipes.delete()`. Edit the files instead, then reload with `SIGHUP` or `POST /v1/ops/settings/reload`; the server also picks up a change to the directory by itself. See [Managing the policy](/access-control#managing-the-policy).
 3. **Send a `Content-Type` on every ingest request** if you call the HTTP API directly. A request without a supported one now gets `415`. The SDK already sends one.
 4. **Expect more specific error responses.** A ClickHouse failure on a query or pipe now maps to `400`, `403`, `502` or `503` with a `code` and a `retryable` flag, instead of a flat `500`. See [ClickHouse errors on the query paths](/api#clickhouse-errors-on-the-query-paths).

### Versions

- **ClickHouse:** 0.2.0 is tested against ClickHouse 26.6. ClickHouse 26.8 changed how it reads a bare number in a `DateTime64` column, and 0.2.0 does not yet account for it ([#536](https://github.com/Wave-RF/WaveHouse/issues/536)), so stay on 26.6 for now.
- **NATS:** only an external NATS, which is new in 0.2.0 (`mq.backend: nats`), needs a version: 2.14 or later. See [External NATS](/deployment#external-nats).
- **Go:** building from source or with `go install` needs Go 1.26.9 or later.

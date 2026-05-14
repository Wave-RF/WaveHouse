# WaveHouse — Known Issues

Running log of issues found while deploying/operating WaveHouse. Newest first.

---

## 1. SigNoz compose stack (`deployments/signoz/compose.yaml`) fails to start — RESOLVED

**Found:** 2026-05-12, bringing the bundled SigNoz stack up locally on OrbStack.
**Fixed:** 2026-05-12 — `deployments/signoz/` rebuilt from the upstream SigNoz
`deploy/docker/docker-compose.yaml` (SigNoz `v0.122.0` / `signoz-otel-collector
v0.144.3`), trimmed to a single node.

**Symptom (before):** `docker compose -f deployments/signoz/compose.yaml up -d`
→ `signoz-migrator` exits 1; consequently `signoz` (query-service) and
`signoz-otel-collector` never start (both `depends_on: migrator:
service_completed_successfully`). Only `signoz-clickhouse` stays up.

Migrator log:

```text
migrate/bootstrap.go:119  Creating databases
Error: failed to bootstrap store for migrations: code: 62, message: Syntax error: failed at position 42 (end of query): . Expected one of: identifier, string literal
```

**Cause:** the old compose file pinned `:latest` on every SigNoz image. The current
`signoz/signoz-otel-collector:latest` migrate CLI defaults `--clickhouse-cluster`
to `cluster` and `--clickhouse-replication` to `true`; the compose passed
`--clickhouse-cluster=""` (empty). An empty cluster name made the bootstrap step
emit a `... ON CLUSTER ` clause with nothing after it → ClickHouse `code: 62`
syntax error. Secondary problem: `signoz/query-service:latest` was a ~14-month-old
build (2025-03) while `signoz/signoz-otel-collector:latest` is from 2026-05 — those
are no longer a compatible pair (`signoz/query-service` has been superseded by the
consolidated `signoz/signoz` image).

**Fix applied:** rebuilt `deployments/signoz/` to mirror upstream's current single-node
layout, with everything version-pinned:
- `signoz/signoz:v0.122.0` (consolidated query + UI; replaces `signoz/query-service`),
  published on host `3301` (→ container `8080`)
- `signoz/signoz-otel-collector:v0.144.3` (OTLP gRPC `4317`, HTTP `4318`) — runs
  `migrate sync check` then the collector, with `--manager-config` pointing at the
  bundled `otel-collector-opamp-config.yaml`
- `signoz-telemetrystore-migrator` (same collector image) — `migrate bootstrap && sync
  up && async up`, driven by `SIGNOZ_OTEL_COLLECTOR_CLICKHOUSE_CLUSTER=cluster` /
  `…_REPLICATION=true` env vars (the cluster `ON CLUSTER cluster` DDL now resolves)
- `clickhouse/clickhouse-server:25.5.6` with `clickhouse/{config,users,cluster,custom-function}.xml`
  vendored under `deployments/signoz/clickhouse/` — `cluster.xml` defines a 1-shard /
  1-replica cluster named `cluster` and points at `zookeeper-1`
- `signoz/zookeeper:3.7.1` — needed for the `ON CLUSTER` / replicated tables
- `signoz-init-clickhouse` (one-shot) — downloads the `histogramQuantile` UDF binary
  into a shared `signoz-clickhouse-user-scripts` volume
- removed the old `deployments/signoz/.env.example` (the `SIGNOZ_CH_USER` /
  `SIGNOZ_CH_PASSWORD` knobs no longer exist; ClickHouse uses `CLICKHOUSE_SKIP_USER_SETUP=1`
  + the vendored `users.xml`, which has the `default` user with an empty password)

WaveHouse is wired in via `deployments/compose/standalone.signoz.yaml`
(`WH_OTEL_ENABLED=true`, `WH_OTEL_ADDR=host.docker.internal:4317`); verified that
spans from `service.name=wavehouse` land in `signoz_traces.distributed_signoz_index_v3`.

**Note:** on first start the collector logs an OpAMP error
(`cannot create agent without orgId` / `Server returned an error response`) and the
WaveHouse OTLP exporter logs a few `error reading server preface: EOF` lines while the
collector finishes restarting — both are transient. The OpAMP error clears once you
create the first user/org in the SigNoz UI; the EOF lines stop once the collector is
steady (data flows regardless).

**Status:** resolved.

---

## 2. `deployments/compose/standalone.yaml` quick-start fails on a default-sized Docker/OrbStack VM

**Found:** 2026-05-12.

**Symptom:** `docker compose -f deployments/compose/standalone.yaml up -d --build` →
the `wavehouse` container exits 1 on boot:

```text
mq init failed  error="create stream: nats: API error: code=500 err_code=10047 description=insufficient storage resources available"  path=/app/data/nats
```

**Cause:** `standalone.yaml` does not set `WH_MQ_MAX_BYTES_GB`, so the embedded
JetStream stream uses the 50 GB default (`config.go`: `MaxBytesGB env-default:"50"`).
A stock OrbStack VM disk is ~28 GB; JetStream caps usable file storage well below
that (~16 GB observed), so creating a 50 GB stream fails. WaveHouse treats the MQ
init failure as fatal.

**Workaround / fix:** set `WH_MQ_MAX_BYTES_GB` to something that fits in
`standalone.yaml` (the e2e compose uses `1`), or document the disk requirement in
`docs/deployment.md`. Locally this is handled by
`deployments/compose/standalone.signoz.yaml` (sets it to `2`).

---

## 3. `standalone.yaml` doesn't gate WaveHouse on ClickHouse readiness

**Found:** 2026-05-12.

**Symptom:** on a cold `up`, the `wavehouse` container exits 1:

```text
schema discovery failed on boot  error="query system.columns: dial tcp <ch-ip>:9000: connect: connection refused"
```

**Cause:** `standalone.yaml` uses `depends_on: [clickhouse]`, which only waits for
the ClickHouse container to *start*, not to accept connections. WaveHouse runs
schema discovery immediately and treats a failure on boot as fatal (`main.go`
`return 1`), so it races a still-initializing ClickHouse and loses.
`deployments/compose/dependencies.yaml` and `tests/e2e/compose.yaml` both add a
ClickHouse healthcheck and `depends_on: { clickhouse: { condition:
service_healthy } }`; `standalone.yaml` should do the same.

**Workaround / fix:** add the healthcheck + `service_healthy` condition to
`standalone.yaml` (done in `standalone.signoz.yaml` for the local stack), or restart
the `wavehouse` container after ClickHouse is up.

---

## 4. Gotcha: attaching WaveHouse to the SigNoz network breaks ClickHouse name resolution

**Found:** 2026-05-12, while wiring WaveHouse's OTLP export into the SigNoz stack.

**Symptom:** if you put the WaveHouse container on the SigNoz compose network so it
can reach `signoz-otel-collector:4317` by name, schema discovery fails with:

```text
query system.columns: code: 516, message: default: Authentication failed: password is incorrect, or there is no user with such name.
```

**Cause:** `deployments/compose/standalone.yaml` and
`deployments/signoz/compose.yaml` both define a service named `clickhouse`.
On a container attached to both networks, the unqualified hostname `clickhouse` is
ambiguous, and WaveHouse can end up dialing SigNoz's ClickHouse — which is
configured with a non-empty password (`SIGNOZ_CH_PASSWORD`, default `password`),
unlike WaveHouse's own ClickHouse (empty password) — so auth fails.

**Workaround / fix:** don't dual-home the WaveHouse container. Reach the collector
via `host.docker.internal:4317` (the SigNoz collector publishes `4317` on the host) —
this is what `standalone.signoz.yaml` does. Alternatively, rename one of the
`clickhouse` services so the names don't collide.

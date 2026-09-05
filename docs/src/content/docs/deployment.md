---
title: "Deployment"
description: "Running WaveHouse in production: Docker images, releases, environment variables, health checks, and schema setup."
cloudCta:
  body: "Everything on this page — pinned images, health probes, rollout, secret handling, and the ClickHouse cluster underneath all of it — is what WaveHouse Cloud operates for you. Same binary, same config surface, none of the pager duty."
sidebar:
  order: 10
---

How to run WaveHouse in production — single binary, Docker images, releases, health checks, and the required ClickHouse schema.

## Single binary

WaveHouse runs as one process with embedded NATS and optional Pebble dedup. The only external dependency is ClickHouse.

### Quick Start with Docker Compose

```bash
# Start ClickHouse + WaveHouse (the stack bind-mounts deployments/compose/settings/ as the settings directory)
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
# Seed the settings directory config.yaml points at (gitignored ./settings;
# no-op if it already exists): the bootstrap seed plus the compose stack's
# permissive "public" trial policy — replace policies.json before production
make settings/config.json

# Build
make build

# Run standalone (uses config.yaml in current directory by default)
./bin/wavehouse
```

Or override any boot-config key with environment variables (the ClickHouse address and the rest of the wiring are settings-directory keys, edited in `config.json`):

```bash
WH_CH_PASSWORD=s3cret \
WH_SERVER_PORT=9090 \
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

`:vX.Y.Z` is the immutable per-release tag. A **stable** release also moves `:latest`; a **prerelease** moves `:alpha` / `:beta` / `:rc` / `:next` instead — chosen from the *first* prerelease identifier matched exactly, so `v0.2.0-rc.1` gives `:rc` while `-alpha1` or `-preview.1` give `:next` — one rule (`scripts/ci/release-channel.sh`), shared with the npm dist-tags — so a release candidate never displaces the `:latest` a shipped stable release owns. `:dev` is the rolling `main`-branch build, and `:dev-<full-commit-sha>` (immutable, pruned after 30 days) captures a single commit. To pin (see the [alpha-stage caution](https://github.com/Wave-RF/WaveHouse#-project-status) in the README), use a `:dev-<full-commit-sha>` tag — the full 40-character commit SHA, not the short form — or an image digest.

Published images carry a signed [Sigstore](https://www.sigstore.dev/) build-provenance attestation (stored in the registry). Verify one before deploying, pinning the signer to the workflow that publishes the tag — `--repo` alone accepts an attestation from any workflow in the repo:

```bash
# :dev and :dev-<sha> images are published by publish-dev.yml
gh attestation verify oci://ghcr.io/wave-rf/wavehouse:dev \
  --repo Wave-RF/WaveHouse \
  --signer-workflow Wave-RF/WaveHouse/.github/workflows/publish-dev.yml

# :vX.Y.Z and :latest release images are published by release.yml
gh attestation verify oci://ghcr.io/wave-rf/wavehouse:vX.Y.Z \
  --repo Wave-RF/WaveHouse \
  --signer-workflow Wave-RF/WaveHouse/.github/workflows/release.yml
```

## Releases

Releases are built with [GoReleaser](https://goreleaser.com/). The configuration is in `.goreleaser.yaml`. The release archives attached to each GitHub Release carry a signed [Sigstore](https://www.sigstore.dev/) build-provenance attestation — verify a downloaded archive with `gh attestation verify <file> --repo Wave-RF/WaveHouse --signer-workflow Wave-RF/WaveHouse/.github/workflows/release.yml`. (This covers the prebuilt archives, not `go install`, which compiles from source.)

### Supported Platforms

| OS | Architecture |
| -- | ----------- |
| Linux | amd64, arm64 |
| macOS | amd64, arm64 |
| Windows | amd64, arm64 |
| FreeBSD | amd64, arm64 |

### Creating a Release

```bash
make release-server VERSION=0.1.0
```

That runs the preflight checks (on `main`, clean tree, in sync with `origin`, tag free, CI green on this commit), shows what will be published, and prompts before creating and pushing the annotated `v0.1.0` tag — which is what triggers the release workflow. Tag creation is restricted to repo admins by the `release tag protection` ruleset.

The TypeScript SDK releases separately under its own `clients/ts/v*` tag; a `v*` tag publishes only the binaries and the container image. Full walkthrough, including what each tag publishes and how to verify provenance: [Development → Cutting a release](/development#cutting-a-release).

## Environment Variables

All configuration can be set via environment variables. This is the recommended approach for container deployments. See [Configuration Reference](/configuration) for the full list.

Key variables for production:

```bash
# ClickHouse: only the password is env. The address, HTTP port/scheme,
# database, and user are clickhouse.* in the settings directory's config.json.
WH_CH_PASSWORD=<clickhouse-password>

# Auth secrets (the JWT middleware always runs — set a secret, or auth.jwks_url
# in the settings directory, to validate tokens; without one, every request
# resolves to the policy default_role). role_claim is a settings key too.
WH_AUTH_JWT_SECRET=<strong-random-secret>
# Optional non-JWT operator credential (Authorization: Operator <key>, or the
# X-Operator-Key alias): full-access
# admin for break-glass, honored even when no policy is adopted. Treat it
# as an admin secret — inject from your secret store, serve only over TLS.
WH_AUTH_OPERATOR_KEY=<strong-random-operator-key>

# Settings directory (required): roles.json, policies.json, pipes.json,
# config.json — the hot-reloadable configuration: the access-control policy
# and its roles, the named pipes, and the tunables including the ClickHouse
# wiring (see the Settings Directory page). Seed it with
# `wavehouse bootstrap [dir]`, set clickhouse.addr in its config.json, write
# your policy to policies.json (the seed ships none — every request is
# denied until you do), and point this variable at it. The directory must
# exist and validate or the process refuses to boot. Container images already preset
# WH_SETTINGS_DIR=/app/settings and ship no directory: omit this line and
# mount (or seed) your directory at /app/settings.
WH_SETTINGS_DIR=/etc/wavehouse/settings
```

## Persistent Storage (REQUIRED for containers)

WaveHouse keeps all embedded state under a single configurable root, `WH_DATA_DIR` (yaml: `data_dir`). Subdirectories are convention, not config:

- `<data_dir>/nats` — embedded NATS JetStream. Holds in-flight events between an ingest POST and the ingest worker → ClickHouse flush, plus the `stream.gap_window_minutes` window (settings directory) of history that powers SSE gap-fill across restarts.
- `<data_dir>/pebble` — Pebble dedup KV. Only used while `dedupe.enabled` is `true` in the settings directory's `config.json` (opened and closed on reload).

In a Docker / Podman / Kubernetes deployment, **`data_dir` must resolve to a host-backed volume**. The reference compose file `deployments/compose/standalone.yaml` sets `WH_DATA_DIR=/app/data` and binds a `wavehouse-data:/app/data` volume — copy that pattern. The bundled Dockerfiles pre-create `/app/data` and `/app/settings` owned by the nonroot user (UID 65532); the binary creates the `nats/` and `pebble/` subdirectories under `/app/data` itself on first run.

If `data_dir` resolves into the container's writable overlay layer instead, **JetStream state is wiped on every restart**: in-flight events are lost, gap-fill stops bridging restarts, and disk usage accumulates inside `/var/lib/docker` instead of the volume the operator chose.

Beyond persistence, the *speed* of that volume matters: JetStream `fsync`s every event to `<data_dir>/nats` before the ingest endpoint returns `200`, so the volume's `fsync` latency is your ingest latency floor. Managed cloud block storage handles this without thinking; commodity or virtualized substrates (ZFS without a SLOG, qcow2-on-`ext4`, spinning disks) can stall ingest with multi-second `fsync` tails. See [Durability & Storage](/durability) to measure yours before going live.

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

On first attach to an empty named volume, Docker performs a "copy-up": the contents and ownership of `/app/data` *from the image* are copied into the volume. The bundled `Dockerfile` and `Dockerfile.goreleaser` both pre-create `/app/data` and `/app/settings` with `chown -R 65532:65532`, so the volume inherits the right ownership automatically. **No host-side `chown` needed.** Subsequent restarts reuse whatever's in the volume.

**Settings directory.** The [settings directory](/settings-directory) is *required* — the image ships none, so an unmounted `/app/settings` refuses to boot. It's config, not state, so unlike `/app/data` it is deliberately *not* a `VOLUME` (an anonymous volume would hide the missing mount), and the mount is always a **bind mount** of a directory you edit on the host: the reference compose file mounts the checked-in `deployments/compose/settings/` (the `bootstrap` seed with `clickhouse.addr` pointed at the `clickhouse` service). The server only reads it, so the files just need to be world-readable (which `bootstrap` writes), and it re-reads them on change with no restart. Don't reach for a named volume here: the image is distroless, so there is no shell to edit the files inside the volume — and editing them is the point.

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

**Settings-directory bind mount** follows the same rule when you seed it with `wavehouse bootstrap` from inside the container (the seed is written as UID 65532); a directory you bootstrap on the host only needs to be readable by that user, since WaveHouse only ever reads it. Named pipes and the access-control policy live in that directory (`pipes.json`, `policies.json`, `roles.json`) and hot-reload on edit — see [Settings Directory](/settings-directory).

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

WaveHouse serves plain HTTP on `:8080` and does **not** terminate TLS, manage certificates, or rate-limit — put a reverse proxy, CDN, or tunnel (nginx, Caddy, Cloudflare Tunnel) in front for any internet-facing deployment. A few behaviors only matter behind a proxy: TLS termination, the request-body size limits, Server-Sent Events buffering (WaveHouse now sends keepalive comments so quiet streams survive proxy idle timeouts, [#226](https://github.com/Wave-RF/WaveHouse/issues/226)), header/auth forwarding, and which health paths to expose. See **[Behind a reverse proxy](/reverse-proxy)** for the full guide and example nginx/Caddy/Cloudflare configs.

## ClickHouse Schema

WaveHouse uses a **Bring Your Own Schema** model. You create your tables in ClickHouse with whatever columns and engines you need. WaveHouse discovers the schemas automatically via `system.columns` and validates ingest data against them — see [Schema Validation](/api#post-v1ingesttabletable--ingest-data) for the rules a record must satisfy.

Two schema-design consequences are worth knowing before you write the DDL. A `MATERIALIZED` or `ALIAS` column is computed by ClickHouse and cannot be inserted: omit it from your records, and a record that names one is rejected. And a `Nullable(T) DEFAULT …` column never takes its default through ingest at all: an omitted key stores `NULL`, because an absent field travels as an explicit null in its slot and ClickHouse stores an explicit null on a nullable column as `NULL` — only an *absent* key ever took the default, and a positional row has no way to express absence. A **non-nullable** column with a default is unaffected: the insert turns that null back into the default.

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

WaveHouse discovers this schema on startup and refreshes it every `schema.refresh_interval` seconds (settings directory; seed default 60). You can also trigger an immediate refresh via `POST /v1/ops/schema/refresh` (admin-only).

## Upgrading across the v2 ingest envelope

The NATS envelope changed shape in this release: the row now travels positionally, with `format`, `columns` and `row` replacing `data`. **The new worker cannot read a message published by an older version** — it carries no `format`, so there is no way to say which value belongs to which column.

This affects the streaming surface too, and more quietly. SSE gap-fill (`?since=` / `Last-Event-ID`) reads the same stream, and a pre-v2 envelope cannot be paired into a positional row there either — it is withheld from every role with **no error, no frame and no DLQ entry**. Because the stream keeps ACKed messages until the sweeper purges past `stream.gap_window_minutes` (15 by default), this outlives a *correct* drain: for that window, any replay spanning the upgrade silently omits the pre-upgrade events. Clients that need them should backfill over REST.

On the worker side the outcome depends on the DLQ. **With the DLQ enabled for the table**, the message is parked on `dlq.{table}` with `X-DLQ-*` headers and is recoverable by hand — but re-ingest each parked envelope's inner `data` object as a fresh `POST /v1/ingest`; republishing the envelope as-is onto `ingest.{table}` fails the same `format` check and simply re-parks it. **With the DLQ switched off for the table, it is permanently lost**: acked and dropped with an `ERROR` log and a `wavehouse_ingest_poison_dropped_total` increment, unrecoverable from either the ingest stream or the DLQ, because a message that can never insert must not redeliver forever. Draining first is cheaper than a manual replay, and it is the only option at all where the DLQ is off.

Two audits belong **before** the drain, because neither announces itself afterwards:

- **`Nullable(T) DEFAULT …` columns now store `NULL` where they took their default.** A positional row has one slot per insertable column and no way to say *absent*, so a key the record omits rides as an explicit `null`. `input_format_null_as_default=1` turns that back into the default for a **non-nullable** column, but ClickHouse stores `NULL` on a nullable one whatever the setting says — only an absent key ever took the default. Following this runbook exactly still changes what lands in those columns, silently. See [the ingest note](/ingest-pipeline#the-journey-of-one-event).
- **Policy `check` blocks are now validated against the table.** A `check` naming a column the table lacks, one it computes (`MATERIALIZED`/`ALIAS`), or an `EPHEMERAL` one is a per-record `403` on *every* insert by that role. `wavehouse validate` cannot catch it — it never sees the ClickHouse schema — so audit them against their tables first. See [Access control → Insert checks](/access-control#insert-checks).

To drain before upgrading:

1. **Stop the producers**, or cut `/v1/ingest` at the reverse proxy. Nothing new should enter the stream.
2. **Wait for the in-flight batches to flush.** A table's batch closes on size or after `maxWait` (5s by default), so a few seconds after the last write is enough; give it longer if ClickHouse is slow or retrying.
3. **Confirm nothing is left unconsumed** before swapping binaries. Not that the stream is empty: it is dual-use, and deliberately retains ACKed messages for the SSE replay window, so a non-zero depth right after a clean drain is expected. Rows landing in ClickHouse is a success signal, **not proof the queue is drained** — when the DLQ is off for a table, a row that fails its retry is skipped without being acked, so NATS keeps redelivering it while its neighbors land. Check that nothing is still failing or redelivering, and note which signal covers which case: [`GET /v1/ops/dlq/stats`](/api#get-v1opsdlqstats--dlq-statistics) is non-zero only where the DLQ is **on**; `wavehouse_ingest_poison_dropped_total` counts unreadable envelopes dropped where it is **off**; and for a twice-failed row with the DLQ off — the case just described — the **only** signal is the `ERROR` log (`isolated bad row, DLQ disabled for table`). A clean `dlq/stats` with the DLQ off proves nothing. There is no queue-depth gauge today ([#544](https://github.com/Wave-RF/WaveHouse/issues/544) tracks the related in-flight accounting), and `wavehouse_nats_in_msgs_total` going flat is a supporting signal rather than a guarantee. Enabling the DLQ is not itself a drain — replay from `dlq.{table}` is manual.
4. **Upgrade**, then re-enable ingest.

If you skipped the drain, check `dlq.{table}` for parked envelopes (and `wavehouse_ingest_poison_dropped_total` for any dropped where the DLQ was off) — see [Dead Letter Queue](#dead-letter-queue-dlq) below.

## Dead Letter Queue (DLQ)

A failed batch insert is retried row by row; while `dlq.enabled` is `true` for the table (the seed default — a hot-reloadable [settings directory](/settings-directory#dead-letter-queue) key, overridable per table), the rows that fail again are published to the `WAVEHOUSE_DLQ` NATS stream under subjects `dlq.{table}` instead of retrying forever. Monitor DLQ depth via `GET /v1/ops/dlq/stats`.

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

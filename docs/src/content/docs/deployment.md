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

WaveHouse images run as the distroless `nonroot` user (UID 65532). Bind mounts and named volumes interact with this differently, and the distroless image has no shell to `chown` things at runtime — so getting the host side of `/app/data` wrong refuses boot in the first lines of the log with a named `data_dir` error and the `chown` remediation attached. `/app/settings` fails differently: the server only reads it, so a mount it cannot read, or that does not validate, is a settings-validation error rather than a `data_dir` one.

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

Bind mounts do **not** copy-up — Docker exposes the host directory as-is, and the image's pre-created dir is masked entirely. The condition boot enforces is that UID 65532 can **write** to the directory — a freshly `mkdir`'d `root:root` directory at the default mode cannot be, which is the common case, though a root-owned directory with permissive mode bits or an ACL passes. If `/srv/wavehouse` is not writable, the binary refuses to start before it touches the settings directory or ClickHouse — `data_dir` is probed for writability right after the config loads:

```text wrap=false
ERROR  check data_dir  error="data_dir /app/data is not writable; if running in a
       container with a host bind mount, the host directory must be writable by
       UID 65532 (the `nonroot` user in the distroless image); the usual fix is
       `sudo chown -R 65532:65532 /your/host/path`. ...: permission denied"
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

Don't name WaveHouse's own Service `wh` or `wh-*`. kubelet injects `WH_SERVICE_HOST` and friends into every pod in the namespace started after such a Service exists, WaveHouse's own pods included, and the strict environment check refuses them — not at deploy time, but on the next restart. Any Service in the namespace named that way has the same effect; `enableServiceLinks: false` on the pod spec turns the injection off. See [Configuration → Loading Order](/configuration#loading-order).

Until `startupProbe` succeeds, kubelet doesn't run `livenessProbe` or `readinessProbe` against the pod — so a slow or temporarily-unreachable ClickHouse can't restart-loop the pod via the liveness path. Size `failureThreshold` to your expected worst-case CH boot time; the default 30 × 10s = 5min is generous and works for compose-on-NAS-style deployments where CH and WaveHouse can race during a host reboot.

## Stopping

`SIGTERM` or `SIGINT` begins a graceful stop in three bounded phases whose budgets add up:

1. **Drain**, within [`server.shutdown_timeout`](/configuration#server) (default 10s). The listener stops accepting, every open [SSE stream](/api#get-v1stream--server-sent-events-stream) is ended at once, gap-fill in progress included (clients reconnect and resume from `Last-Event-ID`), and in-flight requests and the ingest worker's in-hand batches finish. A settings reload caught mid-hook gives up too. Whatever is still open at the deadline is force-closed.
2. **Release**, within a fixed 5s. The stores (embedded NATS, Pebble, the cache, ClickHouse) close; one still closing at the deadline is abandoned, the ones after it are left to the exit, and both are named in the log.
3. **Flush**, within a fixed 3s. Telemetry is flushed last, on its own budget, so the lines the release logged reach the collector even when a close was slow.

Only the drain scales with the deployment's workload, so it is the one operators tune; the other two are constants.

A second `SIGTERM`/`SIGINT` while the stop is running abandons it and exits non-zero immediately. `SIGHUP` reloads the [settings directory](/settings-directory) during normal operation and is ignored once a stop has begun.

Size the orchestrator's kill grace at `server.shutdown_timeout` plus 8s: at the default a stop needs up to 18s before it should be `SIGKILL`ed, and raising the timeout raises that total by the same amount. Docker's default `stop_grace_period` is 10s, so the [compose file](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/compose/standalone.yaml) sets `stop_grace_period: 25s`, that bound plus headroom; on Kubernetes the equivalent is `terminationGracePeriodSeconds`, whose 30s default already covers it — raise it if you raise `server.shutdown_timeout`. A stop with nothing in flight takes well under a second either way, unless OTLP export is on and the collector is unreachable: the flush then waits out its 3s.

## Behind a reverse proxy

WaveHouse serves plain HTTP on `:8080` and does **not** terminate TLS, manage certificates, or rate-limit — put a reverse proxy, CDN, or tunnel (nginx, Caddy, Cloudflare Tunnel) in front for any internet-facing deployment. A few behaviors only matter behind a proxy: TLS termination, the request-body size limits, Server-Sent Events buffering (WaveHouse now sends keepalive comments so quiet streams survive proxy idle timeouts, [#226](https://github.com/Wave-RF/WaveHouse/issues/226)), header/auth forwarding, and which health paths to expose. See **[Behind a reverse proxy](/reverse-proxy)** for the full guide and example nginx/Caddy/Cloudflare configs.

## Multi-tenant deployments

Most deployments serve one tenant and can skip this section: send no `X-Tenant-ID` header and none of it applies, with one exception — [a proxy that already sends the header](#upgrading-behind-a-proxy-that-already-sends-x-tenant-id).

A *tenant* here is one set of the four [settings files](/settings-directory): the `X-Tenant-ID` request header selects whose `roles.json`, `policies.json`, `pipes.json`, and `config.json` serve the request. The header is client-supplied and resolved before authentication, so it is **not** a row-isolation boundary — it picks which `policies.json` applies, and scoping a caller to their own rows stays that policy's job, from a value in the signed token ([row-level security](/access-control#row-level-security)). Tenant selection and row scoping are different axes.

Every `/v1` route outside `/v1/ops/*` resolves the tenant before it authenticates the request:

```text
X-Tenant-ID: 0
```

A request without the header, or with an empty one, resolves to tenant `0`, the default tenant. A settings directory that holds the four files itself defines that one tenant, so any other id is unknown; [a nested settings directory](#the-nested-settings-directory) defines one tenant per folder. Setting the header on every request is the client's or the fronting proxy's job; WaveHouse never derives it from the token.

A tenant id is 1–64 characters of ASCII letters, digits, `_`, and `-`. It is a string, not a number, so a long numeric id keeps every digit.

| Status | Body | When |
| ------ | ---- | ---- |
| `400` | `{"error": "invalid X-Tenant-ID: …"}` | The id breaks the grammar above, or the header was sent more than once |
| `404` | `{"error": "unknown tenant: <id>"}` | The id is well formed but no such tenant exists |
| `503` | `{"error": "tenant settings are invalid"}` | The tenant exists but its settings folder was rejected ([nested directories](#the-nested-settings-directory) only) |

All three are decided before authentication, so they are returned whatever token the request carries — which is why the `503` says nothing about what was wrong with the settings. Every response that passes through tenant resolution — a route's own answer and these three alike — carries `Vary: X-Tenant-ID`, so a shared cache that stores one keys it on the header. A router-level `405` and the CORS preflight `204` are answered before tenant resolution and carry no such `Vary`; neither depends on the tenant. `Vary` covers the tenant and nothing else: a response also depends on who is asking, which is why [a shared cache must not store the authenticated reads](/reverse-proxy#header-and-auth-forwarding).

The probes (`/livez`, `/readyz`, `/healthz`, and the deprecated `/health` and `/ready`), `/version`, the Prometheus metrics path, and `/v1/ops/*` are tenant-exempt: they ignore the header entirely.

`X-Tenant-ID` is in the CORS `Access-Control-Allow-Headers` list, so a browser client can send it cross-origin. The SDK sends it through [`options.headers`](/sdk#custom-headers).

### The nested settings directory

Serving more than one tenant from one process takes a settings directory that holds one folder per tenant instead of the four files:

```text
settings/
├── acme/
│   ├── config.json
│   ├── pipes.json
│   ├── policies.json
│   └── roles.json
└── globex/
    ├── config.json
    ├── pipes.json
    ├── policies.json
    └── roles.json
```

That is the layout a control plane writes, and it does not answer queries on its own: tenant `0`'s folder supplies the wiring the whole process shares — the ClickHouse address included — so a directory with no `0` folder boots with none. "What a tenant's folder decides", below, lists what comes from where.

The folder name is the tenant id, and each folder is a complete settings directory: everything on the [Settings Directory](/settings-directory) page applies to it as written, except where the rules below say otherwise. The two shapes don't mix — a folder beside the four files, or a loose file beside the folders, is a validation error — and a running server keeps the shape it booted with, so switching is stop, restructure, start. Dot-prefixed entries are ignored in either shape. `wavehouse validate` checks either shape with the same exit codes; a finding in a nested directory names its folder (`acme/policies.json`), and a folder whose name is not a tenant id is a finding of its own — that folder is skipped, and the rest of the directory still loads.

**A rejected folder fails closed, for that tenant alone.** A folder that fails validation stops its tenant being served — its requests answer `503` — while every other tenant carries on, at boot and on a reload alike. There is no fall back to the tenant's previous settings, unlike [the single-tenant directory](/settings-directory#loading-and-hot-reload): the recovery is fixing the folder and reloading it. A request already in flight finishes on the settings it started with. The findings go to the log and to the reload response, never into the `503`. A finding about the directory itself — a loose file, a directory that can't be read, a changed shape — is another matter: it refuses boot, and on a reload it rejects the reload whole and leaves every tenant as it was.

**Reloading is the writer's call.** A nested directory is not watched, because a watcher would validate a folder halfway through being written and drop its tenant. Whoever writes a tenant's folder reloads it once it is complete: `POST /v1/ops/settings/reload?tenant=acme` re-validates that folder and reads nothing else. It must name a tenant the server already holds (`404` otherwise). Without the parameter — and on `SIGHUP` — the whole directory is reloaded and mirrors its folders: a new folder becomes a tenant, and a removed one becomes unknown. The response is the [single-tenant one](/api#post-v1opssettingsreload--reload-settings-directory). After a whole-directory reload, `adopted: false` with a `422` can mean adopted in part: the folders named in `findings` were rejected and the rest were adopted.

**The admin routes take the operator key only.** `/v1/ops/*` reaches every tenant, so over a nested directory no tenant's admin role opens it: the [operator key](/api#authentication) alone does, and a token carrying an admin role gets `403`. Boot a nested directory without `auth.operator_key` and no caller can reach these routes at all, which leaves `SIGHUP` as the only reload; the server warns about it at boot. `GET /v1/ops/pipes` and `GET /v1/ops/pipes/{name}` take the same `?tenant=`, and read tenant `0` without it. On all three routes the parameter is parsed strictly — a query string that does not parse, an empty or repeated `tenant`, or a malformed id is a `400`, never a silent read of the default tenant or, on the reload route, a reload of every tenant. The SDK sends it as the [`tenant` option](/sdk/admin#settings--whsettings).

**What a tenant's folder decides, and what tenant `0`'s does.** A request is evaluated against its own tenant's `policies.json` and `pipes.json` (ingest, structured queries, pipes), its `query.*` keys, and its `dedupe` block — by which id, and whether its records are deduplicated at all, provided tenant `0`'s `dedupe.enabled` has the store open: with the store closed they are published un-deduped and counted by `wavehouse_ingest_dedupe_disabled_total`, which then climbs steadily rather than only across a reload, as it does for [a directory that holds the four files](/settings-directory#deduplication). The process still has one ClickHouse connection, one message queue, one dedupe store, one token verifier, and one event hub, and those follow tenant `0`'s folder: `clickhouse.*`, `auth.*`, `mq.max_bytes_gb`, `cors.allowed_origins`, `schema.refresh_interval`, `stream.gap_window_minutes`, `dlq.*`, and whether the dedupe store is open at all (tenant `0`'s `dedupe.enabled`). They follow the settings tenant `0` last adopted: a `0` folder that a reload rejects or removes stops tenant `0` being served like any other, and leaves all of them as they were. So every tenant reads and writes the same ClickHouse, a token that verifies is accepted under any tenant's header, and a tenant's own `policies.json` does not govern its event stream: every `GET /v1/stream` connection is authorized by tenant `0`'s policy. A nested directory without a `0` folder has no ClickHouse address: it boots, reports degraded on `/livez`, and answers no query. The one shared setting that weighs every tenant is the SSE keepalive: the wheel runs at the shortest `stream.keepalive_interval` among the tenants being served, with that tenant's `stream.keepalive_buckets`.

### Upgrading behind a proxy that already sends `X-Tenant-ID`

`X-Tenant-ID` is a generic name, and some gateways and service meshes stamp one on every request. WaveHouse used to ignore it; now, over a settings directory that holds the four files, any value other than `0` names an unknown tenant, so **every `/v1` route outside `/v1/ops/*` answers `404 unknown tenant: <id>`** — the SDK's `/v1/health` reachability ping included, while the bare probes and the admin surface stay green. Strip the inbound header at the edge ([header forwarding](/reverse-proxy#header-and-auth-forwarding)) unless you are using it deliberately.

## ClickHouse Schema

WaveHouse uses a **Bring Your Own Schema** model. You create your tables in ClickHouse with whatever columns and engines you need. WaveHouse discovers the schemas automatically via `system.columns` and validates ingest data against them — see [Schema Validation](/api#post-v1ingesttabletable--ingest-data) for the rules a record must satisfy.

Three schema-design consequences are worth knowing before you write the DDL. A `MATERIALIZED` or `ALIAS` column is computed by ClickHouse and cannot be inserted: omit it from your records, and a record that names one is rejected. An `EPHEMERAL` column is the awkward one — it *is* insertable, but it is never stored and no query can read it back, so it is only useful as an input to another column's `DEFAULT` expression, and a policy `check` naming one is refused outright. And a `Nullable(T) DEFAULT …` column never takes its default through ingest: an omitted key stores `NULL`, not the default — see [the journey of one event](/ingest-pipeline#the-journey-of-one-event) for why. A **non-nullable** column with a default is unaffected.

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

This affects the streaming surface too, and more quietly. SSE gap-fill (`?since=` / `Last-Event-ID`) reads the same stream, and the hub refuses a pre-v2 envelope on the same missing `format` the worker does — it is withheld from every role with **no error and no frame**, and the stream side files no DLQ entry of its own — the worker's copy of the same message is what lands in `dlq.{table}` (next paragraph). Because the stream keeps ACKed messages until the sweeper purges past `stream.gap_window_minutes` (15 by default), this outlives a *correct* drain: for that window, any replay spanning the upgrade silently omits the pre-upgrade events. Clients that need them should backfill over REST.

On the worker side the outcome depends on the DLQ. **With the DLQ enabled for the table**, the message is parked on `dlq.{table}` with `X-DLQ-*` headers and is recoverable by hand — but re-ingest each parked envelope's inner `data` object as a fresh `POST /v1/ingest`; republishing the envelope as-is onto `ingest.{table}` fails the same `format` check and simply re-parks it. **With the DLQ switched off for the table, it is permanently lost**: acked and dropped with an `ERROR` log and a `wavehouse_ingest_poison_total` increment carrying `disposition="dropped"`, unrecoverable from either the ingest stream or the DLQ, because a message that can never insert must not redeliver forever. Draining first is cheaper than a manual replay, and it is the only option at all where the DLQ is off.

Three audits belong **before** the drain, because none of them announces itself afterwards:

- **`Nullable(T) DEFAULT …` columns now store `NULL` where they took their default.** A positional row has one slot per insertable column and no way to say *absent*, so a key the record omits rides as an explicit `null`. `input_format_null_as_default=1` turns that back into the default for a **non-nullable** column, but ClickHouse stores `NULL` on a nullable one whatever the setting says — only an absent key ever took the default. Following this runbook exactly still changes what lands in those columns, silently. See [the ingest note](/ingest-pipeline#the-journey-of-one-event).
- **Policy `check` blocks are now validated against the table.** A `check` naming a column the table lacks, one it computes (`MATERIALIZED`/`ALIAS`), or an `EPHEMERAL` one is a per-record `403` on *every* insert by that role. `wavehouse validate` cannot catch it — it never sees the ClickHouse schema — so audit them against their tables first. See [Access control → Insert checks](/access-control#insert-checks).
- **Every `WH_*` variable the binary does not bind refuses boot.** The old binary ignored a variable it did not read; the new one names every unbound one and exits before it opens the queue, so a pod spec or compose file that still carries one comes back from the upgrade as a container that will not start. Diff the environment against the [Configuration Reference](/configuration) first: a `WH_*` variable that is not in its tables is unbound, and whatever it used to configure now lives in the [settings directory](/settings-directory) or is gone. A Kubernetes Service in the pod's namespace named `wh` or `wh-*` counts too: it injects link variables under the `WH_` prefix (`WH_SERVICE_HOST` and `WH_PORT` for `wh`, `WH_FOO_SERVICE_HOST` and `WH_FOO_PORT` for `wh-foo`), so set `enableServiceLinks: false` on the pod spec.

To drain before upgrading:

1. **Stop the producers**, or cut `/v1/ingest` at the reverse proxy. Nothing new should enter the stream.
2. **Wait for the in-flight batches to flush.** A table's batch closes on size or after `maxWait` (5s by default), so a few seconds after the last write is enough; give it longer if ClickHouse is slow or retrying.
3. **Confirm nothing is left unconsumed** before swapping binaries. Not that the stream is empty: it is dual-use, and deliberately retains ACKed messages for the SSE replay window, so a non-zero depth right after a clean drain is expected. Rows landing in ClickHouse is a success signal, **not proof the queue is drained** — when the DLQ is off for a table, a row that fails its retry is skipped without being acked, so NATS keeps redelivering it while its neighbors land. Check that nothing is still failing or redelivering, and note which signal covers which case: [`GET /v1/ops/dlq/stats`](/api#get-v1opsdlqstats--dlq-statistics) is non-zero only where the DLQ is **on**; `wavehouse_ingest_poison_total` counts unreadable envelopes on **either** setting, told apart by its `disposition` label (`parked` / `dropped`); and for a twice-failed row with the DLQ off — the case just described — the **only** signal is the `ERROR` log (`isolated bad row, DLQ disabled for table`). A clean `dlq/stats` with the DLQ off proves nothing. There is no queue-depth gauge today ([#544](https://github.com/Wave-RF/WaveHouse/issues/544) tracks the related in-flight accounting), and `wavehouse_nats_in_msgs_total` going flat is a supporting signal rather than a guarantee. Enabling the DLQ is not itself a drain — replay from `dlq.{table}` is manual.
4. **Upgrade**, then re-enable ingest.

If you skipped the drain, check `wavehouse_ingest_poison_total`, which counts both — `disposition="parked"` is recoverable from `dlq.{table}`, `disposition="dropped"` is gone — see [Dead Letter Queue](#dead-letter-queue-dlq) below.

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

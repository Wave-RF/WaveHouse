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
# ClickHouse: only the password and the connection ceiling are env. The
# address, HTTP port/scheme, database, user, TLS, headers and pool sizes are
# clickhouse.* in the settings directory's config.json.
WH_CH_PASSWORD=<clickhouse-password>
# Ceiling on open native ClickHouse connections; 0 = none
# WH_CH_MAX_TOTAL_CONNS=0

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
- `<data_dir>/pebble` — the Pebble dedup KV: one instance shared by every tenant, each key led by its tenant. Only used while some tenant's `dedupe.enabled` is `true` in its `config.json` (opened and closed on reload).

In a Docker / Podman / Kubernetes deployment, **`data_dir` must resolve to a host-backed volume**. The reference compose file `deployments/compose/standalone.yaml` sets `WH_DATA_DIR=/app/data` and binds a `wavehouse-data:/app/data` volume — copy that pattern. The bundled Dockerfiles pre-create `/app/data` and `/app/settings` owned by the nonroot user (UID 65532); the binary creates the `nats/` and `pebble/` subdirectories under `/app/data` itself on first run.

If `data_dir` resolves into the container's writable overlay layer instead, **JetStream state is wiped on every restart**: in-flight events are lost, gap-fill stops bridging restarts, and disk usage accumulates inside `/var/lib/docker` instead of the volume the operator chose.

Beyond persistence, the *speed* of that volume matters: JetStream `fsync`s every event to `<data_dir>/nats` before the ingest endpoint returns `200`, so the volume's `fsync` latency is your ingest latency floor. Managed cloud block storage handles this without thinking; commodity or virtualized substrates (ZFS without a SLOG, qcow2-on-`ext4`, spinning disks) can stall ingest with multi-second `fsync` tails. See [Durability & Storage](/durability) to measure yours before going live.

WaveHouse runs a simple existence check on startup and logs a `WARN` if `<data_dir>/nats` (or `<data_dir>/pebble`, when dedupe is on) is missing or empty:

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

- `GET /livez` — Liveness probe. Returns 200 once the gateway has discovered ClickHouse table schemas at least once. Returns 503 with a diagnostic body while the boot-time schema discovery retry loop is still running (e.g. ClickHouse unreachable, target database missing). After successful boot, `/livez` stays 200 — transient ClickHouse blips at runtime are reflected in `/readyz`, not `/livez`. Over a [nested settings directory](#the-nested-settings-directory) it is 503 while no tenant has completed a first discovery, and 200 from the first tenant's success on.
- `GET /readyz` — Readiness probe. Returns 200 if the gateway is fully booted and ClickHouse is currently reachable, 503 otherwise. Over a nested directory every open ClickHouse pool is pinged at once and one that answers is enough; the 503 names every pool that did not.

`/healthz` remains registered as a **permanent alias** of `/livez` (it's the most widely-recognized name); `/health` and `/ready` are **deprecated aliases** for the v0.1.x line and will be removed in v0.2.0. Point new deployments at the `/livez` / `/readyz` names.

Configure your load balancer or orchestrator to use these endpoints.

**Exposure.** Probes share the API server's port (`:8080`) — kubelet probes the container internally, so there's no separate-port convention for them (metrics are the signal that optionally gets its own `prometheus.port`). If you forward `:8080` to the public internet the probe paths become reachable. The **recommended** posture is to keep `/livez`/`/readyz`/`/healthz` to internal callers and expose only **`/v1/health`** publicly (the SDK's content-free liveness ping, which never touches ClickHouse). `/readyz` pings every open ClickHouse pool on every call, so a public `/readyz` lets an unauthenticated flood become per-request backend pings, and the bare probes disclose boot and readiness state — each unanswering pool's ClickHouse address, database and user, and over a nested directory the failing tenant's id — keeping them internal is a [reverse-proxy/ingress concern](/reverse-proxy#health-probes), and your orchestrator reaches them the internal way (kubelet on the container, LB on the backend) regardless.

### Boot-time degraded mode

If ClickHouse is unreachable when WaveHouse starts (connection refused, missing database, DNS failure, etc.), the gateway no longer exits — it binds `:8080` and serves `/livez` 503 with the latest schema-discovery error as the diagnostic. Schema discovery retries in the background with exponential backoff (2s → 60s cap). Once a Refresh succeeds, `/livez` flips to 200 and normal serving begins automatically.

This means:

- The binary itself no longer exits and crash-loops every ~10s under a supervisor. Process state is preserved across CH outages.
- An operator can `curl /livez` and read the exact failure mode instead of grepping a restart-loop log.
- `/v1/ingest?table={table}` and the other schema-aware endpoints answer `503` with `Retry-After: 5` (`schema not loaded yet`) until discovery succeeds — not a `404`, since the table may well exist.

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
2. **Release**, within a fixed 5s. The stores (embedded NATS, the Pebble dedupe store, the cache, ClickHouse) close; one still closing at the deadline is abandoned, the ones after it are left to the exit, and both are named in the log.
3. **Flush**, within a fixed 3s. Telemetry is flushed last, on its own budget, so the lines the release logged reach the collector even when a close was slow.

Only the drain scales with the deployment's workload, so it is the one operators tune; the other two are constants.

A second `SIGTERM`/`SIGINT` while the stop is running abandons it and exits non-zero immediately. `SIGHUP` reloads the [settings directory](/settings-directory) during normal operation and is ignored once a stop has begun.

Size the orchestrator's kill grace at `server.shutdown_timeout` plus 8s: at the default a stop needs up to 18s before it should be `SIGKILL`ed, and raising the timeout raises that total by the same amount. Docker's default `stop_grace_period` is 10s, so the [compose file](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/compose/standalone.yaml) sets `stop_grace_period: 25s`, that bound plus headroom; on Kubernetes the equivalent is `terminationGracePeriodSeconds`, whose 30s default already covers it — raise it if you raise `server.shutdown_timeout`. A stop with nothing in flight takes well under a second either way, unless OTLP export is on and the collector is unreachable: the flush then waits out its 3s.

## Behind a reverse proxy

WaveHouse serves plain HTTP on `:8080` and does **not** terminate TLS, manage a server certificate, or rate-limit — put a reverse proxy, CDN, or tunnel (nginx, Caddy, Cloudflare Tunnel) in front for any internet-facing deployment. A few behaviors only matter behind a proxy: TLS termination, the request-body size limits, Server-Sent Events buffering (WaveHouse now sends keepalive comments so quiet streams survive proxy idle timeouts, [#226](https://github.com/Wave-RF/WaveHouse/issues/226)), header/auth forwarding, and which health paths to expose. See **[Behind a reverse proxy](/reverse-proxy)** for the full guide and example nginx/Caddy/Cloudflare configs.

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

All three are decided before authentication, so they are returned whatever token the request carries — which is why the `503` says nothing about what was wrong with the settings. Every response that passes through tenant resolution — a route's own answer and these three alike — carries `Vary: X-Tenant-ID`, so a shared cache that stores one keys it on the header. A router-level `405` is answered before tenant resolution and carries no such `Vary`, and neither does the CORS preflight `204`: its answer does follow the header (below), but a response to `OPTIONS` is never stored by an HTTP cache, so there is nothing to key. `Vary` covers the tenant and nothing else: a response also depends on who is asking, which is why [a shared cache must not store the authenticated reads](/reverse-proxy#header-and-auth-forwarding).

The probes (`/livez`, `/readyz`, `/healthz`, and the deprecated `/health` and `/ready`), `/version`, the Prometheus metrics path, and `/v1/ops/*` are tenant-exempt: they ignore the header entirely.

`X-Tenant-ID` is in the CORS `Access-Control-Allow-Headers` list, so a browser client can send it cross-origin. The SDK sends it through [`options.headers`](/sdk#custom-headers); behind the fronting proxy of a nested deployment the value is overwritten (below).

The CORS answer follows the header too. On a `/v1` route outside `/v1/ops/*` the response is decorated from the [`cors.allowed_origins`](/settings-directory) of the tenant the request names, absent meaning tenant `0`, so each tenant's list governs its own responses. The preflight is decided the same way, and a browser sends no custom header on an `OPTIONS`: a preflight carries `X-Tenant-ID` only when the fronting proxy sets it there as it does on every other request, which is what a multi-tenant deployment needs, and a preflight naming no tenant is tenant `0`'s. That [proxy](/reverse-proxy#header-and-auth-forwarding) must fix the tenant from the request's host or path and overwrite whatever the client sent, on every request: a browser caches a successful preflight by URL and reuses it whatever the header's value, so a tenant a page could pick by header would let one tenant's cached preflight release writes at another. With the tenant tied to the URL, no tenant's list widens another's. The tenant-exempt routes, and a request naming a tenant that is not served (the `400`, `404` and `503` above), are answered from tenant `0`'s list; with no tenant `0` being served they carry no CORS headers at all.

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

That is the layout a control plane writes. Each folder's `clickhouse` block is its tenant's own ClickHouse, so a tenant answers queries once its first schema discovery against that ClickHouse succeeds (until then its schema-aware routes answer `503`, `schema not loaded yet`); what tenant `0`'s folder still supplies for the whole process — the token verifier of the routes that name no tenant, their CORS list — is listed under "What a lost tenant `0` costs", below.

The folder name is the tenant id, and each folder is a complete settings directory: everything on the [Settings Directory](/settings-directory) page applies to it as written, except where the rules below say otherwise. The two shapes don't mix — a folder beside the four files, or a loose file beside the folders, is a validation error — and a running server keeps the shape it booted with, so switching is stop, restructure, start. The dedupe store needs no restructuring: it keys every tenant's seen ids by tenant, and the four files are tenant `0`, as a `0` folder is. Dot-prefixed entries are ignored in either shape. `wavehouse validate` checks either shape with the same exit codes; a finding in a nested directory names its folder (`acme/policies.json`), and a folder whose name is not a tenant id is a finding of its own — that folder is skipped, and the rest of the directory still loads.

**A rejected folder fails closed, for that tenant alone — tenant `0`'s excepted.** A folder that fails validation stops its tenant being served — its requests answer `503` — while every other tenant carries on, at boot and on a reload alike. Tenant `0` is the exception: the process still draws some shared wiring from that folder, so rejecting it costs every tenant something ("What a lost tenant `0` costs", below, says what). There is no fall back to the tenant's previous settings, unlike [the single-tenant directory](/settings-directory#loading-and-hot-reload): the recovery is fixing the folder and reloading it. A request already in flight finishes on the settings it started with, except an open `GET /v1/stream`, which is ended at once: its reconnect gets the `503` until the folder is fixed — the SDK keeps retrying and then resumes from `Last-Event-ID`, while a browser `EventSource` gives up on the `503` and has to be reopened. The rows the tenant had already accepted but not yet inserted, those of an ingest request in flight included, which still answers `200`, are parked on the DLQ under the tenant's own subject rather than held for the fix, as a removed tenant's are (see [Dead Letter Queue](#dead-letter-queue-dlq)). Its message queue is kept, at the budget it last had, and so is the history that gap-fill replays, for the `stream.gap_window_minutes` its folder last had (all of it, for a folder rejected since the server started, whose window the server never read): a stream resumed after a fix within that window picks up where it left off. The findings go to the log and to the reload response, never into the `503`. A finding about the directory itself — a loose file, an entry or a directory that can't be read, a changed shape — is another matter: it refuses boot, and on a reload it rejects the reload whole and leaves every tenant as it was.

**Reloading is the writer's call.** A nested directory is not watched, because a watcher would validate a folder halfway through being written and drop its tenant. Whoever writes a tenant's folder reloads it once it is complete: `POST /v1/ops/settings/reload?tenant=acme` re-validates that folder and reads nothing else. It must name a tenant the server already holds (`404` otherwise), so a folder the server does not hold yet — one added since the last whole-directory reload — is picked up by a whole-directory reload, not by naming it; a tenant it holds but rejected is reloaded by name like any other. Without the parameter — and on `SIGHUP` — the whole directory is reloaded and mirrors its folders: a new folder becomes a tenant, and a removed one becomes unknown. That is how a tenant is removed: delete its folder, then reload the whole directory. Its open streams end, its routes answer `404`, and its queued rows are parked on the DLQ under its own subject; nothing it stored is deleted — its message queue is kept at the budget it last had, and only the history that gap-fill replays goes from it, at the next sweep — so restoring the folder restores the tenant, seen ids and parked rows included. Reloading a deleted folder by name instead leaves its tenant rejected, answering `503`. The last folder can be removed the same way, with two catches, since `wavehouse validate` and boot both read an emptied directory as the four files missing: `validate` exits `1`, so a writer that gates each reload on it has to skip the check for that one reload, and a server restarted before a folder is written back refuses to boot. A whole-directory reload re-validates every folder, so it carries the exposure the watcher would: a folder caught halfway through being written can fail validation, and its tenant then stops being served until a later reload adopts it. The response is the [single-tenant one](/api#post-v1opssettingsreload--reload-settings-directory). After a whole-directory reload, `adopted: false` with a `422` can mean adopted in part: the folders with an error among their `findings` were rejected and the rest were adopted — warnings included, since `findings` carries every folder's.

**The admin routes take the operator key only.** `/v1/ops/*` reaches every tenant, so over a nested directory no tenant's admin role opens it: the [operator key](/api#authentication) alone does, and a token carrying an admin role gets `403`. Boot a nested directory without `auth.operator_key` and no caller can reach these routes at all, which leaves `SIGHUP` as the only reload; the server warns about it at boot. `GET /v1/ops/pipes`, `GET /v1/ops/pipes/{name}`, `GET /v1/ops/schema`, `POST /v1/ops/schema/refresh` and `POST /v1/ops/query` take the same `?tenant=`, and address tenant `0` without it; `GET /v1/ops/dlq/stats` takes it too, and reads a rejected or removed tenant's dead-letter queue like a served one's, since the queue is kept; a tenant that has none is a `404`. On the routes that take it the parameter is parsed strictly — a query string that does not parse, an empty or repeated `tenant`, or a malformed id is a `400`, never a silent read of the default tenant or, on the reload route, a reload of every tenant. The SDK sends it as the [`tenant` option](/sdk/admin#settings--whsettings).

**What a tenant's folder decides.** A request is evaluated against its own tenant's `policies.json` and `pipes.json` (ingest, structured queries, pipes), its `query.*` keys, its `cors.allowed_origins`, and its `dedupe` block: whether its records are deduplicated, by which id, against the tenant's own store, which that folder's `dedupe.enabled` opens and closes on reload exactly as [the single-tenant one](/settings-directory#deduplication) does (every tenant's store is a share of the one Pebble instance at `<data_dir>/pebble`, each key led by its tenant), so `wavehouse_ingest_dedupe_disabled_total` ticks only across a tenant's own reload, whatever the other tenants' switches say. A tenant's seen ids are its own: the same event id is first seen under each tenant that sends it. Its `auth` block is its own too: each tenant's folder wires that tenant's token verifier (`jwks_url`, `role_claim`), built when the folder is adopted and rebuilt when its wiring changes, so a JWKS-issued token verifies only under the tenants whose `jwks_url` names its provider's key set. Under another tenant's header a token is treated as invalid, and the request falls back to that tenant's `default_role` like any other unverifiable token, possibly after a rate-limited key refetch (see [Authentication](/settings-directory#authentication)). Keep `X-Tenant-ID` pinned at the proxy so a token is never presented under the wrong tenant. Tenants can still accept each other's tokens: those that leave `jwks_url` empty share the boot HMAC secret when `auth.jwt_secret` is set, so a token verifies under any of them (with no secret they validate no token at all), and those whose `jwks_url` names the same key set accept each other's tokens; isolate them by provider, or scope rows by a signed claim ([row-level security](/access-control#row-level-security)). A tenant whose `jwks_url` has not been fetched yet answers `503` with `Retry-After` to its token-bearing requests alone. A tenant that stops being served — its folder rejected or removed — loses its verifier and the JWKS refresh with it, and gets a fresh one when its folder is adopted again. The HMAC secret and the operator key stay boot config, shared by every tenant; the operator key is stamped with the request tenant's `admin_role`. A tenant's `clickhouse` and `schema` blocks are its own as well: each tenant reads and writes its own ClickHouse — one native pool per distinct address, database, user, password and `tls` tuple, shared by the tenants naming it, under the process-wide [connection ceiling](/settings-directory#clickhouse) — and discovers its own tables from its own database on its own `schema.refresh_interval`. Its message queue is its own as well: its events are queued on a stream of their own, capped at its own `mq.max_bytes_gb` — at that budget its ingest answers `503` while every other tenant's keeps publishing — beside a dead-letter stream of its own at a tenth of it, and the history that gap-fill replays from it is kept for its own `stream.gap_window_minutes`. Nothing checks what the tenants' budgets add up to against the disk, so size them together ([Message Queue](/settings-directory#message-queue)). An event is published on its tenant's subject (`ingest.{tenant}.{table}`), so a `GET /v1/stream` connection is authorized by its own tenant's `policies.json` and receives its own tenant's rows alone, the ingest worker inserts a row into its own tenant's ClickHouse, a failed row is parked under its own tenant's `dlq.enabled` and subject (`dlq.{tenant}.{table}`), and two tenants' tables of one name never share a batch. The query cache is one pool, but its entries are keyed by tenant: identical `POST /v1/query` and pipe requests from two tenants are two entries and two queries to ClickHouse, and a tenant is never served another's cached rows. An insert invalidates the table's cached results under every tenant on the same ClickHouse address and database as the tenant it was ingested for, whatever their user or `tls` block, since they read the same tables; a tenant on no pool — its folder rejected or removed, or no pool could be opened for it, such as by the ceiling — is out of that fan-out while it is, and has its cached `POST /v1/query` results dropped the moment it is back on one, so a repaired or restored folder never serves query rows cached before the inserts it missed, and so does a tenant whose folder moves it to another address or database, whose cached rows came from other tables; a cached pipe result is left alone by all of this — no insert invalidates one, since it names no table — and stays until its TTL expires. One setting weighs every tenant: the SSE keepalive, where the wheel runs at the shortest `stream.keepalive_interval` among the tenants being served, with that tenant's `stream.keepalive_buckets`.

**What a lost tenant `0` costs.** A `0` folder that a reload rejects or removes stops tenant `0` being served like any other, and what becomes of the shared settings depends on how they are read. Tenant `0` leaves its ClickHouse pool (closed only once no served tenant names its tuple), and its schema registry and verifier are released with the folder, like any other tenant's; the `/v1/ops/*` routes, which resolve no tenant, verify against it, so a token there reads as invalid (`401`) rather than merely non-admin (`403`) until tenant `0` is served again — the operator key, which never consults a verifier, is unaffected. CORS does not stay either: the responses that read tenant `0`'s list — the tenant-exempt routes, the refusals, a preflight naming no tenant — carry no CORS headers until the folder is served again, while every other tenant's routes keep their own list. Tenant `0`'s own dedupe store closes, as any rejected or removed tenant's does, its seen ids kept for the folder that restores it. What is read per event follows the event's tenant, so tenant `0`'s events are the ones affected: with no ClickHouse to insert into, its rows fail and are parked on the DLQ whatever its switch said, and its open `GET /v1/stream` connections are ended, as any tenant's are when it stops being served — the other tenants' events are untouched. A nested directory that has never served a tenant `0` — no `0` folder, or one rejected at boot — serves every other tenant from its own ClickHouse. Outside `/v1/ops/*`, a `/v1` request that sends no `X-Tenant-ID` resolves to tenant `0`, so with no `0` folder it answers `404 unknown tenant: 0` (`503` with a rejected one) — the SDK's `/v1/health` reachability ping included.

### Upgrading behind a proxy that already sends `X-Tenant-ID`

`X-Tenant-ID` is a generic name, and some gateways and service meshes stamp one on every request. WaveHouse used to ignore it; now, over a settings directory that holds the four files, any value other than `0` names an unknown tenant, so **every `/v1` route outside `/v1/ops/*` answers `404 unknown tenant: <id>`** (a `400` when the value is not a tenant id at all, a dotted hostname, say) — the SDK's `/v1/health` reachability ping included, while the bare probes and the admin surface stay green. Strip the inbound header at the edge ([header forwarding](/reverse-proxy#header-and-auth-forwarding)) unless you are using it deliberately.

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

The NATS envelope changed shape in this release: the row now travels positionally, with `format`, `columns` and `row` replacing `data` — and the queue changed layout with it: boot deletes the earlier build's queue (below), so nothing an older version published reaches the new worker, which could not read it anyway (it carries no `format`, so there is no way to say which value belongs to which column). **Drain first** to keep what the old build had not yet inserted.

The streaming surface loses something too, more quietly. SSE gap-fill (`?since=` / `Last-Event-ID`) replays from the queue, so the deletion takes the replay history with it, even after a *correct* drain: any replay spanning the upgrade silently omits the pre-upgrade events, with **no error and no frame**. Clients that need them should backfill over REST.

**The upgrade does not carry the old queue over at all.** Boot deletes the earlier build's queue and dead-letter queue (`WAVEHOUSE`, `WAVEHOUSE_DLQ`) and everything in them, logging a `WARN` with each one's message count: an event the old build had not yet inserted, and a row it had already parked, do not survive the upgrade. Draining first keeps the events not yet inserted; a row already parked is lost with the queue, since the earlier build offers no way to read one back (`GET /v1/ops/dlq/stats` returns counts only).

Three audits belong **before** the drain, because none of them announces itself afterwards:

- **`Nullable(T) DEFAULT …` columns now store `NULL` where they took their default.** A positional row has one slot per insertable column and no way to say *absent*, so a key the record omits rides as an explicit `null`. `input_format_null_as_default=1` turns that back into the default for a **non-nullable** column, but ClickHouse stores `NULL` on a nullable one whatever the setting says — only an absent key ever took the default. Following this runbook exactly still changes what lands in those columns, silently. See [the ingest note](/ingest-pipeline#the-journey-of-one-event).
- **Policy `check` blocks are now validated against the table.** A `check` naming a column the table lacks, one it computes (`MATERIALIZED`/`ALIAS`), or an `EPHEMERAL` one is a per-record `403` on *every* insert by that role. `wavehouse validate` cannot catch it — it never sees the ClickHouse schema — so audit them against their tables first. See [Access control → Insert checks](/access-control#insert-checks).
- **Every `WH_*` variable the binary does not bind refuses boot.** The old binary ignored a variable it did not read; the new one names every unbound one and exits before it opens the queue, so a pod spec or compose file that still carries one comes back from the upgrade as a container that will not start. Diff the environment against the [Configuration Reference](/configuration) first: a `WH_*` variable that is not in its tables is unbound, and whatever it used to configure now lives in the [settings directory](/settings-directory) or is gone. A Kubernetes Service in the pod's namespace named `wh` or `wh-*` counts too: it injects link variables under the `WH_` prefix (`WH_SERVICE_HOST` and `WH_PORT` for `wh`, `WH_FOO_SERVICE_HOST` and `WH_FOO_PORT` for `wh-foo`), so set `enableServiceLinks: false` on the pod spec.

To drain before upgrading:

1. **Stop the producers**, or cut `/v1/ingest` at the reverse proxy. Nothing new should enter the stream.
2. **Wait for the in-flight batches to flush.** A table's batch closes on size or after `maxWait` (5s by default), so a few seconds after the last write is enough; give it longer if ClickHouse is slow or retrying.
3. **Confirm nothing is left unconsumed** before swapping binaries. Not that the stream is empty: it is dual-use, and deliberately retains ACKed messages for the SSE replay window, so a non-zero depth right after a clean drain is expected. Rows landing in ClickHouse is a success signal, **not proof the queue is drained** — when the DLQ is off for a table, a row ClickHouse rejects again is skipped without being acked, so NATS keeps redelivering it while its neighbors land. Check that nothing is still failing or redelivering, and note which signal covers which case: [`GET /v1/ops/dlq/stats`](/api#get-v1opsdlqstats--dlq-statistics) is non-zero only where the DLQ is **on**; `wavehouse_ingest_poison_total` counts unreadable envelopes on **either** setting, told apart by its `disposition` label (`parked` / `dropped`); and for a twice-failed row with the DLQ off — the case just described — the **only** signal is the `ERROR` log (`isolated bad row, DLQ disabled for table`); and for rows handed back because ClickHouse cannot take them at all, `wavehouse_ingest_retries_total` (any recent increase) and the `WARN` log (`cannot take inserts`, which matches the first and the repeated line, for the whole ClickHouse or one table). A clean `dlq/stats` with the DLQ off proves nothing. There is no queue-depth gauge today ([#544](https://github.com/Wave-RF/WaveHouse/issues/544) tracks the related in-flight accounting), and `wavehouse_nats_in_msgs_total` going flat is a supporting signal rather than a guarantee. Enabling the DLQ is not itself a drain: a parked row is not inserted, and the upgrade deletes it.
4. **Upgrade**, then re-enable ingest.

If you skipped the drain, the boot's `WARN` line for each deleted stream (`deleted the stream an earlier build kept for every tenant together`) says how many messages went with it: for `WAVEHOUSE_DLQ`, the parked rows lost; for `WAVEHOUSE`, a count that includes the acknowledged history kept for replay, already in ClickHouse — so it bounds the events lost rather than counting them, and is non-zero even after a clean drain.

## Dead Letter Queue (DLQ)

A batch insert ClickHouse **rejects** is retried row by row; while the tenant's `dlq.enabled` is `true` for the table (the seed default — a hot-reloadable [settings directory](/settings-directory#dead-letter-queue) key, overridable per table), the rows that fail again are published to the tenant's own dead-letter stream (`DLQ_{tenant}`) under subjects `dlq.{tenant}.{table}` (`0` for a directory that holds the four files) instead of retrying forever. A batch whose tenant has no ClickHouse connection — one no longer served, or one no pool could be opened for, such as by the connection ceiling — skips the row-by-row retry, which no row of it could pass: its tenant's switch is read once for the whole batch, and a tenant no longer served has no switch to read, so its batch is always parked. A ClickHouse that cannot take inserts at all — down, unreachable, overloaded, read-only, or refusing the configured credentials — parks nothing: its rows stay in the tenant's ingest queue and are retried with backoff, counted by `wavehouse_ingest_retries_total`, so a long outage shows up as a growing ingest stream (and, at the tenant's `mq.max_bytes_gb`, as ingest `503`s), not as a full DLQ — see [Ingest Pipeline](/ingest-pipeline#when-clickhouse-cannot-take-an-insert). Monitor DLQ depth via `GET /v1/ops/dlq/stats`, per tenant (`?tenant=`; tenant `0` without it).

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

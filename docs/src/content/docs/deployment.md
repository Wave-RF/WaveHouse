---
title: "Deployment"
description: "Running WaveHouse in production: Docker images, releases, environment variables, health checks, and schema setup."
cloudCta:
  body: "Everything on this page — pinned images, health probes, rollout, secret handling, and the ClickHouse cluster underneath all of it — is what WaveHouse Cloud operates for you. Same binary, same config surface, none of the pager duty."
sidebar:
  order: 10
---

How to run WaveHouse in production — one binary, Docker images, releases, health checks, and the required ClickHouse schema.

## One binary, plus a per-version artifact

WaveHouse runs as one process with embedded NATS and optional Pebble dedup. An API-role process also needs a second artifact — a per-ClickHouse-version shared library that runs ClickHouse's own parser in-process for ingest validation and row-level security (via [chtypes](#chtypes-artifacts)), fetched into a cache and opened when the first tenant on its ClickHouse line is bound; a process that runs only the ingest worker or the sweeper needs none. The only external network dependencies are ClickHouse and, until each line is cached, the chtypes artifact registry or your mirror of it, unless you select a shared backend: [`mq.backend: nats`](#external-nats) (and `coord.backend: nats`), [`cache.backend: redis`](#multiple-instances-and-the-shared-cache) or [`dedupe.backend: dynamodb`](#a-shared-dedupe-table-on-dynamodb).

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
- **WaveHouse** on port 8080, which fetches the chtypes artifact for ClickHouse 26.8 (about 45 MB) when it first binds the tenant, into the `chtypes-cache` volume, so the first start takes a few seconds longer and later starts none

### Binary

```bash
# Seed the settings directory config.yaml points at (gitignored ./settings;
# no-op if it already exists): the bootstrap seed plus the compose stack's
# permissive "public" trial policy — replace policies.json before production
make settings/config.json

# Optional: prefetch the chtypes artifact for ClickHouse 26.8 into
# ~/.cache/chtypes/v1; otherwise the first tenant bind fetches it
scripts/fetch-chtypes.sh

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

This builds the runtime image `wavehouse:latest`. (The published `ghcr.io` images are built by the release workflows with `docker buildx` from `deployments/Dockerfile.goreleaser`, not this command — see Registry below.)

All images use multi-stage builds (`golang:1.27-bookworm` glibc builder → `gcr.io/distroless/cc-debian12` runtime — cgo needs glibc, so the previous Alpine/musl builder and `distroless/static` runtime no longer work) for minimal attack surface. The image carries no chtypes artifact: it carries the `chtypes` CLI at `/app/chtypes` and an empty cache at `/var/cache/chtypes/v1`, and fetches each ClickHouse line's artifact on first use (see [chtypes artifacts](#chtypes-artifacts)).

### Registry

Production images are published to GitHub Container Registry by the release workflows:

```text
ghcr.io/wave-rf/wavehouse:<tag>
```

`:vX.Y.Z` is the immutable per-release tag. A **stable** release also moves `:latest`; a **prerelease** moves `:alpha` / `:beta` / `:rc` / `:next` instead — chosen from the *first* prerelease identifier matched exactly, so `v0.2.0-rc.1` gives `:rc` while `-alpha1` or `-preview.1` give `:next` — one rule (`scripts/ci/release-channel.sh`), shared with the npm dist-tags — so a release candidate never displaces the `:latest` a shipped stable release owns. `:dev` is the rolling `main`-branch build, and `:dev-<full-commit-sha>` (immutable, pruned after 30 days) captures a single commit. To pin (see the [alpha-stage caution](https://github.com/Wave-RF/WaveHouse#project-status) in the README), use a `:dev-<full-commit-sha>` tag — the full 40-character commit SHA, not the short form — or an image digest.

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

## chtypes artifacts

**What it is.** WaveHouse validates ingest data, coerces types, substitutes `DEFAULT`s, and evaluates row-level security by running ClickHouse's own parser in-process, through [chtypes](https://github.com/wave-rf/chtypes) (`github.com/wave-rf/chtypes/go` v1.0.4, chtypes 1.0). The parser itself ships as a shared library (`libchtypes.so` / `.dylib`) built per **ClickHouse minor line** and per platform, and published as signed OCI artifacts that every fetch verifies. chtypes 1.0 publishes four lines: **26.3, 26.7, 26.8 and 26.9**. A tenant on any other line (24.x, 25.x, 26.5, 26.6, …) has no artifact and is unavailable. There is no nearest-version fallback: each tenant's table set is compiled from its own schema refresh against the artifact matching that tenant's ClickHouse server, and a library is opened lazily, the first time a tenant on that line is bound.

**The line this build is tested on.** The repository pins one ClickHouse, `clickhouse/clickhouse-server:26.8.15.10` (`internal/chversion`), which the compose files and every test suite run; CI and `scripts/fetch-chtypes.sh` fetch the artifact for its line, **26.8**, so bumping that pin moves the artifact line with it. The other published lines are not exercised by this repository's suites. The type layer inherits ClickHouse's own parsing rules, so they are the connected server's: for instance, 26.8 reads a bare JSON number in a `DateTime64` column as epoch **seconds**, so an epoch-millisecond number clamps to `9999-12-31` — WaveHouse stores what the server would.

**Which processes load it.** Only processes with the `api` role — the ones that serve ingest and the stream. A process that runs only the ingest worker or the sweeper loads no artifact and boots without one. An API process boots with none installed and fetches each line when its first tenant binds; with [autofetch](#autofetch-the-cache-and-mirrors) off, it refuses to start when no artifact for its platform is installed at all.

**When a tenant or table is unavailable.** The type layer refuses a whole tenant for three causes: the tenant is not bound yet (its schema discovery has not completed a refresh); its ClickHouse line has no loadable artifact (a line chtypes 1.0 does not publish, one whose fetch failed or, with autofetch off, that was not installed, or one the SDK refuses to load, such as a truncated library, or one built for another line than the tenant's, which chtypes refuses with `CHTYPES_ARTIFACT_CORRUPT`); its server reports a time zone name WaveHouse does not recognize, or, when that tenant makes the first open, one chtypes cannot load (the open fails with ClickHouse code 36, `Cannot load time zone`), so no row of that tenant can be read in it (once another tenant has set the image zone, a tenant in such a zone is bound and refused per call instead: see [time zones](#time-zones)). A fourth cause refuses one table and leaves the tenant's other tables working: a table whose schema the parser could not compile, or, for a tenant whose zone differs from the process's [image zone](#time-zones), a table whose zone-less `DateTime`/`DateTime64` column sits beside any `DEFAULT`, `MATERIALIZED`, `ALIAS` or `EPHEMERAL` expression. In every case a stream whose role has a row `filter` withholds that tenant's (or that table's) rows with reason `unavailable`, while roles with no row filter are unaffected, and every other tenant keeps working. Ingest into that tenant (or that table) answers `503` with `Retry-After: 5` and the generic body `ingest validation is unavailable`, decided before the body is read — except for a tenant not bound yet, whose schema is not loaded yet either: the type layer is bound in the same refresh that marks the schema loaded, so its ingest answers `503` `schema not loaded yet`, and only a request racing a reload that stops serving the tenant meets the type layer's `503`.

**How it recovers, and what to watch.** Every cause is checked again at each of the tenant's schema refreshes (`schema.refresh_interval`), and clears at the first refresh after the cause is gone. A tenant not bound yet clears at its first completed refresh. A failed fetch is retried at every refresh, so it clears once the registry or mirror answers; with autofetch off, a missing artifact clears once a loadable one is installed on the search path, with no restart. A zone name WaveHouse does not recognize clears once the server reports one it recognizes. A zone chtypes cannot load holds up no other tenant, and clears once the server reports a zone chtypes can load, or once another tenant has set the image zone; in the second case the tenant is bound, but chtypes refuses each of its calls ([time zones](#time-zones)). A table refused over its zone clears once the tenant's zone matches the image zone. The image zone is fixed for the life of the process, so changing it, or the server `timezone` that set it, takes a restart; the recovery is to restart, or to serve such tenants from a process of their own (see [time zones](#time-zones)). A table that does not compile is compiled again at every refresh, and clears once its schema, or the artifact answering for it, changes so that it compiles. `/livez` and `/readyz` do not read the type layer, so they stay green through every cause but the first; a tenant not bound yet has not completed a schema discovery, which they report as `503` only while no tenant has completed one ([Boot-time degraded mode](#boot-time-degraded-mode)). So watch for the ingest `503`s, for `wavehouse_sse_rows_withheld_total{reason="unavailable"}`, for the `INFO` lines `chtypes type layer opened` (the cache, whether autofetch is on, and the versions installed for this platform at boot) and `chtypes image zone committed` (the zone this process chose), and for three `ERROR` log lines, each carrying the cause: `chtypes cannot serve this tenant` (a missing, unfetchable or unloadable artifact, an unrecognized zone name, a zone chtypes cannot load on the first open, logged at every refresh), `chtypes could not compile table schema` (one table, logged at every refresh) and `ingest type layer unavailable` (any of the four, logged for the ingest requests it refuses, at most once a minute per tenant and table). A tenant whose zone chtypes cannot load, bound after the image zone is committed ([time zones](#time-zones)), fires none of them: its signal is every record declined (`422`) with chtypes' generic reason `batch outcome is 'rejected': only a fully accepted batch exports bytes`, and `wavehouse_sse_rows_withheld_total{reason="error"}`. Compare that tenant's `server_tz` on the `schema registry refreshed` log line with the zone on `chtypes image zone committed`. See [API → Ingest error responses](/api#post-v1ingesttabletable--ingest-data) and [Access Control → Where each rule is enforced](/access-control#where-each-rule-is-enforced).

**Where it lives.** chtypes 1.0 reads a v1 cache layout (`oci-layout`, `index.json`, `blobs/`, and `unpacked/sha256/<manifest>/` holding `libchtypes.so` or `.dylib`, `manifest.json` and `verified.json`, the signed verification record the loader requires), several lines and platforms side by side. WaveHouse reads, in order: the cache — `clickhouse.chtypes_cache` (`WH_CHTYPES_CACHE`) when set, which names the layout directory itself, else `$CHTYPES_CACHE`, else `~/.cache/chtypes/v1`; the Docker images set `CHTYPES_CACHE=/var/cache/chtypes/v1` — then the read-only system layouts `/usr/local/share/chtypes/v1` and `/opt/chtypes/v1`. A fetch writes into the cache only. Boot is refused when a `clickhouse.chtypes_cache` that is set does not exist, when the cache exists and cannot be read (chtypes would otherwise skip a record this user cannot read without a word, until a strict cache mode lands upstream: Wave-RF/chtypes#486), or, with autofetch off, when no layout holds an artifact for this platform. With autofetch on, a cache the process cannot write is a boot warning, `chtypes cache is not writable`, since an installed line still binds; a line that needs fetching then fails at its first bind.

<a id="time-zones"></a>

**Time zones.** One chtypes process has one **image zone**, which is what a zone-less `DateTime` column and the functions over it read in. WaveHouse takes it from the first tenant bound that can be served (its line's artifact is installed or fetches, its server's zone name is one WaveHouse recognizes, and its first open succeeds): that tenant's server time zone. A zone name Go resolves but chtypes cannot load (for example, one added to the time zone database after the artifact was built) is caught only at the first open: that tenant is unavailable on its own and commits no image zone, so the next tenant that can be served sets it. Once an image zone is committed, chtypes is not asked about a tenant's zone again, so a tenant in such a zone is bound, and chtypes refuses the zone on each call (ClickHouse code 36, `Invalid time zone`): a JSON, NDJSON or positional CSV/TSV body has every record declined (`422`, logged as `validation engine declined a record`, with chtypes' generic reason `batch outcome is 'rejected'`, which does not name the zone), a `header=present` CSV/TSV body is refused whole (`400`, error `Invalid time zone: <zone>`, code `clickhouse.rejected`, exception code 36, logged as `ingest body refused by the parser`), and a stream whose role has a row `filter` withholds its rows with reason `error`, not `unavailable`. No record of that tenant is accepted. The zone itself never makes the tenant unavailable, but a table the zone rule below refuses still answers `503`. A zone name WaveHouse recognizes is a zone name Go's time package resolves (from the host's zone files, or, for a name they lack, the copy built into the binary), so the host needs no tzdata. Every other tenant's calls carry its own server zone, whenever it differs from the image zone, so literal comparisons in a row `filter` and the parsing of zone-less input match that tenant's server. What a per-call zone does not reach is a column function: an expression over a zone-less `DateTime`/`DateTime64` column runs in the image zone, so a table with such a column and a `DEFAULT`, `MATERIALIZED`, `ALIAS` or `EPHEMERAL` expression is unavailable for a tenant whose zone differs, whether or not the expression reads that column (upstream: Wave-RF/chtypes#419). Every other table of that tenant works, and a deployment whose tenants share one zone never meets this. WaveHouse's compiled schema declares no `CHECK` constraint, so none is part of the rule.

Two more consequences follow when a tenant's zone differs from the image zone. A role's `_eq` check on a zone-less `DateTime`/`DateTime64` column is not auto-injected (the literal would be read in the image zone), so a record must supply that column and one that omits it fails the check (`403`); a record that supplies it is judged correctly. And the image zone is committed once and fixed for the life of the process: changing it takes a restart. A process serving tenants of several zones does not choose which one fixes it, since it is whichever eligible tenant's first schema discovery completes first, so a restart can pick a different tenant's zone and flip which tenants' tables are refused. Keep each zone's tenants in a process of their own when any of their tables is affected; making the choice deterministic is a [follow-up](https://github.com/Wave-RF/WaveHouse/issues/726).

**When an artifact fails to load or fetch on the first open.** A first open whose artifact fails to load (for example with `CHTYPES_ARTIFACT_INCOMPATIBLE`) or whose fetch fails leaves chtypes unlocked since 1.0.3: that tenant alone is unavailable, and the next tenant that can be served sets the image zone, whatever its zone. The failed tenant's own next refresh retries the open or fetch.

<a id="a-cache-that-holds-several-lines"></a>

**A cache that holds several lines.** chtypes answers a request only with a build within the requested line, and an exact request only with that exact build; it also refuses to load a library whose own version is not within the request (`CHTYPES_ARTIFACT_CORRUPT`, refusal reason `build_info_mismatch:clickhouse_version`). A cache holding 26.9 only never serves a tenant on 26.8: with autofetch on, the 26.8 build is fetched at that tenant's first bind; with autofetch off, that tenant is unavailable with the missing-artifact cause naming the line. Fetch order does not matter.

**Known limitations.** A `DEFAULT` that draws a value per row works: `generateUUIDv4()`, `generateUUIDv7()`, `rand()` and `now()` are evaluated before the row is published. A body in which any record omits a `generateSnowflakeID()`, `rowNumberInAllBlocks()` or `timezone()` column is declined whole (every record `422`, nothing published); records that supply the column are unaffected. `timezone()` is declined by design, because the answer is a property of the server. A `UUID` shorter than 36 characters makes ClickHouse's reader take the bytes after it; when that costs a following record its verdict, WaveHouse declines the whole body ([Batch ingest](/api#batch-ingest)). A `Float` NaN or infinity is published and streamed as `"nan"`, `"inf"`, `"-inf"` and stored as the value, and `/v1/query` and pipes render it the same way.

**Size.** Each line's artifact is a 40–50 MB download (the compressed layer) that unpacks to roughly 300–340 MB on disk, per platform. Measured on darwin-arm64: 303 MiB, a 262 MiB unpacked library plus the 41 MiB layer; the CI setup records about 340 MiB on linux-amd64 (a 289 MiB library plus a 49 MiB layer). A newer build of a line installs beside the old one, which stays until removed. A running process holding several loaded versions (e.g. across a rolling ClickHouse upgrade) costs roughly 120 MB of resident memory per loaded version (the chtypes multi-version guide's figure; a library is opened on first use of its line, not at registry construction). On top of the library, every table of every tenant an API process serves holds **one** compiled parser handle, shared by every request on it with no pool, plus up to 256 role shapes per table (one handle per distinct set of writable columns and claim-stamped defaults among the roles that insert). Ingest scaling across concurrent requests improved with chtypes Go SDK 1.0.2: upstream measured the Go `Rows` path scaling like `ParseBlock` on Linux runners, each goroutine on its own handle, and closed Wave-RF/chtypes#456. How one shared handle, which is what WaveHouse uses per table or role shape, scales on 1.0.2 is not yet measured. Each handle also caches up to 4,096 compiled filters, one per distinct expression and claim values, row filters and insert checks alike: about 10 KiB for a one-clause filter and 27 KiB for two clauses with a three-value `in` (measured on the 26.8 artifact, darwin-arm64, with chtypes 0.5), so a full cache of one-clause filters is about 43 MiB per handle. A cache fills only once that many distinct claim sets have been evaluated on one handle — a row-filtered table streamed to thousands of subscribers whose claims differ — and then stays full, evicting the least recently used. All of it multiplies by tables and by tenants.

### Autofetch, the cache and mirrors

**What a fetch does.** With `clickhouse.chtypes_autofetch` on (the default), the first bind of a tenant whose line no layout holds asks the registry for that line's floating tag (`26.8`), which names its newest build, downloads the build for this platform (about 45 MB), verifies it, and installs it into the cache. The bind waits for the download, so on an empty cache the first tenant on each line binds a few seconds late (a flat settings directory binds its tenant during boot, before the port opens); every tenant already bound keeps answering meanwhile. A line already in the cache is never fetched again and never updated: a bind of a cached line makes no request at all, and the newest build the cache holds for the line answers it. To move a deployment to a newer build of its line, fetch it into the cache with the [bundled CLI](#the-bundled-chtypes-cli) (`chtypes fetch 26.8`, which makes two small requests when the build is current and downloads only when it is not) — by hand, from a scheduled job, or from an init container or entrypoint step into a cache of the pod's own — and restart. A fetch that fails is that tenant's cause (`chtypes could not fetch the artifact for ClickHouse <line> …`, with the SDK's `CHTYPES_ARTIFACT_*` or `CHTYPES_SOURCE_*` code) and is retried at its next schema refresh. The SDK retries a temporary failure up to five times over about a minute before giving up, so an unreachable registry delays that tenant's bind by about that much per base. Turn autofetch off (`WH_CHTYPES_AUTOFETCH=false`) for an air-gapped deployment; the chtypes SDK's own `CHTYPES_AUTOFETCH` is not read.

**Integrity without a lock.** Nothing pins an exact build: integrity comes from chtypes' own verification, on every fetch. The registry's index only names a candidate; the fetch requires a signed statement (an in-toto statement in a DSSE envelope, found as an OCI referrer of the build's manifest) that verifies under chtypes' release key, which the SDK embeds, and whose subject is the digest of the layer it downloads; it checks the layer against that digest, and the unpacked library's sha256 and size against the signed predicate, before it renames the install into place. The loader then cross-checks the library's own build info against that signed record at every open. A mirror or proxy can therefore withhold or delay a build, but cannot substitute one; a stale mirror does keep its clients on an older build of a line. `CHTYPES_TRUSTED_KEYS` replaces the embedded key, and `CHTYPES_ALLOW_UNSIGNED=1` skips the signature, loudly — never set it in production.

**Mirrors and proxies.** `clickhouse.chtypes_artifacts_url` (`WH_CHTYPES_ARTIFACTS_URL`) lists the bases a fetch tries in order, comma-separated; empty is `$CHTYPES_ARTIFACTS_URL`, else `https://registry.wavehouse.dev/chtypes/v1`. A base is an OCI registry repository, read through the distribution API (`/v2/<repository>/manifests/<tag or digest>`, `/v2/<repository>/blobs/<digest>`, and the build's signature as a referrer, `/v2/<repository>/referrers/<digest>`, or the `sha256-<hex>` fallback tag), so a registry mirror or pull-through cache in front of `registry.wavehouse.dev` works when it passes referrers or that tag through. A base that cannot supply a build (unreachable, a 5xx after retries, a 401, 403 or 404) moves the fetch to the next; a verification failure never does. `CHTYPES_DOWNLOAD_TOKEN` sends a bearer token to the configured bases only.

**Sharing a cache.** Any number of processes can read, and install into, one cache directory at once, on one host or on a shared volume. Installs are rename-first, keep an existing good install, and never delete another process's finished install. There is still no lock, so replicas that miss the same line at once each download it; prefetching the lines with a single process, such as the [bundled CLI](#the-bundled-chtypes-cli) run as a Kubernetes Job, only saves that bandwidth on a fresh cache and is not needed for correctness. chtypes writes its install records readable by their owner only, so every process sharing a cache must run as the same user.

### Cache mounts

The images keep their cache at `/var/cache/chtypes/v1`, created empty and owned by the image's `nonroot` user. Mount a volume or host directory there that outlives the container, so a restart or an image pull does not fetch again; without one the process still fetches, into the container's own layer, and every new container fetches again (under `--read-only` it cannot, and warns at boot). Never bake a filled cache into an image layer of your own: installing over an overlay filesystem's lower layer fails its rename (`EXDEV`). The images declare no `VOLUME` for it: an anonymous volume per container would fetch again on every run and be left behind when the container is removed.

A **named volume** is the simplest: on its first mount Docker copies the empty, `nonroot`-owned directory into it, so it is writable with no `chown`.

```bash
docker run -d --name wavehouse -p 8080:8080 \
  -v wavehouse-data:/app/data \
  -v "$PWD/settings:/app/settings:ro" \
  -v chtypes-cache:/var/cache/chtypes/v1 \
  ghcr.io/wave-rf/wavehouse:latest
```

The [compose file](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/compose/standalone.yaml) mounts a `chtypes-cache` volume the same way. A **host directory** works too, for example the cache a host-side WaveHouse or `scripts/fetch-chtypes.sh` already fills: `-v "$HOME/.cache/chtypes/v1:/var/cache/chtypes/v1"`. Linux and macOS builds sit side by side in one layout. On a Linux host the container must run as the directory's owner, because chtypes writes its records owner-readable only: add `--user "$(id -u):$(id -g)"` and give that user a writable data directory too, or `chown -R 65532:65532` a directory dedicated to the container.

**Read-only root filesystem.** The images run with `--read-only` (Kubernetes `readOnlyRootFilesystem: true`) as long as the cache and `/app/data` are mounted writable; nothing else is written.

On **Kubernetes**, mount a volume at the cache path; `fsGroup: 65532` makes it writable by the image's user. Give each pod its own cache (an `emptyDir`, refilled when the pod is replaced, or a StatefulSet's per-pod claim, which survives it); an init container can prefetch into it with the bundled CLI, so the first bind does not wait. A `ReadWriteMany` claim shared by every replica also works ([Sharing a cache](#autofetch-the-cache-and-mirrors)); a Job that prefetches into it before the Deployment scales up saves each replica downloading the same build on a cold cache:

```yaml
spec:
  securityContext:
    fsGroup: 65532
  initContainers:
    - name: chtypes-prefetch       # optional, into this pod's own cache
      image: ghcr.io/wave-rf/wavehouse:latest
      command: ["/app/chtypes", "fetch", "26.8"]
      volumeMounts:
        - { name: chtypes-cache, mountPath: /var/cache/chtypes/v1 }
  containers:
    - name: wavehouse
      image: ghcr.io/wave-rf/wavehouse:latest
      securityContext: { readOnlyRootFilesystem: true }
      volumeMounts:
        - { name: chtypes-cache, mountPath: /var/cache/chtypes/v1 }
        - { name: data, mountPath: /app/data }
  volumes:
    - name: chtypes-cache
      emptyDir: {}                 # this pod's own; see above for a shared claim
```

### The bundled chtypes CLI

Both images carry the `chtypes` command at `/app/chtypes`, the same SDK version the binary links, and it reads the same `CHTYPES_CACHE`. Run it with `--entrypoint` against the cache volume to prefetch a line, list what is installed and published, or re-hash the installed libraries (`verify`, which exits 0 on a cache it cannot read or a chtypes 0.x one, so it is no readability check: Wave-RF/chtypes#486):

```bash
docker run --rm --entrypoint /app/chtypes -v chtypes-cache:/var/cache/chtypes/v1 \
  ghcr.io/wave-rf/wavehouse:latest fetch 26.9 --cache /var/cache/chtypes/v1
docker run --rm --entrypoint /app/chtypes -v chtypes-cache:/var/cache/chtypes/v1 \
  ghcr.io/wave-rf/wavehouse:latest list
docker run --rm --entrypoint /app/chtypes -v chtypes-cache:/var/cache/chtypes/v1 \
  ghcr.io/wave-rf/wavehouse:latest verify
```

`fetch` takes several lines, `--platform` (another platform's build, e.g. `linux-amd64` from an arm64 host), and `--offline`; it prints the installed directory of each line. It reads `CHTYPES_ARTIFACTS_URL` for a mirror, not WaveHouse's `WH_CHTYPES_ARTIFACTS_URL`.

### Air-gapped deployments

Fetch the lines you need on a machine that can reach the registry, for the deployment's platform, into a directory of their own — `go run github.com/wave-rf/chtypes/go/cmd/chtypes@v1.0.4 fetch --platform linux-amd64 --cache ./chtypes-v1 26.3 26.8`, or the bundled CLI as above — copy that directory over, mount it at the cache path (writable or read-only) or read-only at `/opt/chtypes/v1`, and set `WH_CHTYPES_AUTOFETCH=false`. A tenant on a line it lacks is then unavailable with a cause naming the line, and an API process with no artifact for its platform refuses to boot. Run the container as the directory's owner, or make it readable to the image's user (`chmod -R a+rX`, never writable by others), since chtypes writes its records owner-readable only; a cache this user cannot read refuses boot rather than being skipped. Fetching as the user that reads avoids this: the bundled CLI runs as the image's own user.

### Without Docker

**Release archives and `go install` / building from source** carry no artifact either; with autofetch on, the first bind of each line fetches it into `~/.cache/chtypes/v1` (or `$CHTYPES_CACHE`, or `clickhouse.chtypes_cache`). To fetch ahead, from a checkout:

```bash
scripts/fetch-chtypes.sh        # the test ClickHouse's line (26.8); or name lines: scripts/fetch-chtypes.sh 26.3 26.9
```

which runs `go run github.com/wave-rf/chtypes/go/cmd/chtypes fetch <line>` at the SDK version `go.mod` requires, or, anywhere with Go 1.27, `go run github.com/wave-rf/chtypes/go/cmd/chtypes@v1.0.4 fetch <line>`. See the [README's `go install` caveat](https://github.com/Wave-RF/WaveHouse#c-go-install-binary-no-docker).

### Which build CI and the tests use

There is no lock file. CI, the test helpers and `scripts/fetch-chtypes.sh` use the newest build of the line of the pinned test ClickHouse (`internal/chversion`), so a new upstream build of that line is picked up by the next run, still verified as above. CI keeps `~/.cache/chtypes/v1` in a cache keyed on the platform, the line and that build's digest, so a run on a warm cache costs three small requests (resolving the key, and the fetch confirming the build is current) and a new build costs one download; see `.github/workflows/README.md`. The test helpers (`typelayertest`) and the integration and E2E suites fetch into the same per-user cache when it lacks the line, so a fresh checkout needs no manual step.

## Releases

Releases are built with [GoReleaser](https://goreleaser.com/). The configuration is in `.goreleaser.yaml`. The release archives attached to each GitHub Release carry a signed [Sigstore](https://www.sigstore.dev/) build-provenance attestation — verify a downloaded archive with `gh attestation verify <file> --repo Wave-RF/WaveHouse --signer-workflow Wave-RF/WaveHouse/.github/workflows/release.yml`. (This covers the prebuilt archives, not `go install`, which compiles from source.)

### Supported Platforms

The binary requires cgo (dlopen only — no static link to the chtypes artifact) and, on Linux, glibc 2.34 or later, which sets the supported platform matrix:

| OS | Architecture |
| -- | ----------- |
| Linux | amd64, arm64 |
| macOS | arm64 only |

Windows, FreeBSD, and darwin/amd64 are no longer built — there is no chtypes artifact for them, and the binary cannot run without one. If you need one of these, [open an issue](https://github.com/Wave-RF/WaveHouse/issues) describing your use case.

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
# ClickHouse: only the password, the connection ceiling and the chtypes
# artifact keys are env. The address, HTTP port/scheme, database, user, TLS,
# headers and pool sizes are clickhouse.* in the settings directory's
# config.json.
WH_CH_PASSWORD=<clickhouse-password>
# Ceiling on open native ClickHouse connections; 0 = none
# WH_CH_MAX_TOTAL_CONNS=0
# chtypes artifacts (see chtypes artifacts): the cache (the images set
# CHTYPES_CACHE=/var/cache/chtypes/v1; mount a volume there), autofetch, and
# the registries/mirrors a fetch tries
# WH_CHTYPES_CACHE=
# WH_CHTYPES_AUTOFETCH=true
# WH_CHTYPES_ARTIFACTS_URL=https://mirror.example/chtypes/v1

# Auth secrets (the JWT middleware always runs — set a secret, or auth.jwks_url
# in the settings directory, to validate tokens; without one, every request
# resolves to the policy default_role). role_claim is a settings key too.
WH_AUTH_JWT_SECRET=<strong-random-secret>
# Optional non-JWT operator credential (Authorization: Operator <key>, or the
# X-Operator-Key alias) for break-glass: /v1/ops/* always, and the whole data
# plane while a policy is adopted (with none, data-plane requests are denied
# like anyone else's). Treat it as an admin secret — inject from your secret store, serve only over TLS.
WH_AUTH_OPERATOR_KEY=<strong-random-operator-key>

# Optional shared query cache for several instances (see Multiple instances
# and the shared cache below); the password is a secret like the ones above.
# WH_CACHE_BACKEND=redis
# WH_CACHE_REDIS_ADDRS=redis:6379
# WH_CACHE_REDIS_PASSWORD=<redis-password>

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
- `<data_dir>/pebble` — the Pebble dedup KV (with `dedupe.backend: pebble`, the default): one instance shared by every tenant, each key led by its tenant and table. Only used while some tenant's `dedupe.enabled` is `true` in its `config.json` (opened and closed on reload). It grows with every id kept: with `dedupe.retention` at `"0"` (forever) nothing is ever removed, so size the volume for it or set a [retention](/settings-directory#deduplication), whose expired ids an hourly sweep deletes.

In a Docker / Podman / Kubernetes deployment, **`data_dir` must resolve to a host-backed volume**. The reference compose file `deployments/compose/standalone.yaml` sets `WH_DATA_DIR=/app/data` and binds a `wavehouse-data:/app/data` volume — copy that pattern. The bundled Dockerfiles pre-create `/app/data` and `/app/settings` owned by the nonroot user (UID 65532); the binary creates the `nats/` and `pebble/` subdirectories under `/app/data` itself on first run.

If `data_dir` resolves into the container's writable overlay layer instead, **JetStream state is wiped on every restart**: in-flight events are lost, gap-fill stops bridging restarts, and disk usage accumulates inside `/var/lib/docker` instead of the volume the operator chose.

Beyond persistence, the *speed* of that volume matters: the embedded broker (`mq.backend: embedded`, the default) `fsync`s every event to `<data_dir>/nats` before the ingest endpoint returns `200`, so the volume's `fsync` latency is your ingest latency floor. Managed cloud block storage handles this without thinking; commodity or virtualized substrates (ZFS without a SLOG, qcow2-on-`ext4`, spinning disks) can stall ingest with multi-second `fsync` tails. See [Durability & Storage](/durability) to measure yours before going live. Under [`mq.backend: nats`](#external-nats) the events are not under `data_dir`, and WaveHouse does not require an `fsync` per event: see [Durability](#durability) there.

WaveHouse runs a simple existence check on startup and logs a `WARN` if `<data_dir>/nats` (or `<data_dir>/pebble`, when dedupe is on with the `pebble` backend) is missing or empty:

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

If ClickHouse is unreachable when WaveHouse starts (connection refused, missing database, DNS failure, etc.), the gateway no longer exits — it binds `:8080` and serves `/livez` 503 with the latest schema-discovery error as the diagnostic. Schema discovery retries in the background with jittered exponential backoff (each wait a random time below a bound that doubles from 2s to a 60s cap). Once a Refresh succeeds, `/livez` flips to 200 and normal serving begins automatically.

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
2. **Release**, within a fixed 5s. The stores (embedded NATS, the Pebble dedupe store, the cache, ClickHouse) and the type layer's compiled handles close; one still closing at the deadline is abandoned, the ones after it are left to the exit, and both are named in the log.
3. **Flush**, within a fixed 3s. Telemetry is flushed last, on its own budget, so the lines the release logged reach the collector even when a close was slow.

Only the drain scales with the deployment's workload, so it is the one operators tune; the other two are constants.

A second `SIGTERM`/`SIGINT` while the stop is running abandons it and exits non-zero immediately. `SIGHUP` reloads the [settings directory](/settings-directory) during normal operation and is ignored once a stop has begun.

Size the orchestrator's kill grace at `server.shutdown_timeout` plus 8s: at the default a stop needs up to 18s before it should be `SIGKILL`ed, and raising the timeout raises that total by the same amount. Docker's default `stop_grace_period` is 10s, so the [compose file](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/compose/standalone.yaml) sets `stop_grace_period: 25s`, that bound plus headroom; on Kubernetes the equivalent is `terminationGracePeriodSeconds`, whose 30s default already covers it — raise it if you raise `server.shutdown_timeout`. A stop with nothing in flight takes well under a second either way, unless OTLP export is on and the collector is unreachable: the flush then waits out its 3s.

## External NATS

With `mq.backend: nats`, WaveHouse's message queue is a NATS JetStream cluster you run, shared by every WaveHouse process that points at it. This is what makes more than one replica, or a [split by role](#one-deployment-per-role), possible. **WaveHouse never creates, changes, purges or deletes a stream, a durable consumer or a KV bucket there.** You create them, WaveHouse checks them at boot, and it refuses to start until they are right. The only objects WaveHouse creates are short-lived consumers on the history stream, one per API process for its live SSE events and one per SSE replay, which the server removes on its own when they are idle.

### What WaveHouse needs

- **N ingest partition streams.** Partition `p` holds `<prefix>.ingest.<p>.>`, and each row's subject is `<prefix>.ingest.<p>.<s>.<tenant>.<table>`, where `s` is one of its V shards. It has work-queue retention: a row stays until the ingest worker has written it and acked it, even while no consumer covers it, and is deleted then, so one tenant whose ClickHouse is down keeps only its own rows on disk. Each partition also republishes every row it stores to `<prefix>.hist.<tenant>.<table>`, the same subject without the partition and shard, where the history stream captures it. A work queue takes no consumer beside the ingest durables whose subjects overlap theirs, so a debugging consumer on a partition is refused; read the history stream, or get a row directly, instead. A table's events always go to the same partition and shard, chosen by a consistent (jump) hash of the tenant and table: the tenant's other tables spread over the partitions, and raising N to N′ moves (N′−N)/N′ of the tables, each into one of the new partitions (about 1/N′ for one more), and lowering it moves only the removed partitions' tables; V the same. Each partition has no age limit (an age limit would drop rows not yet written), and `discard: new` with a byte limit: a full partition refuses new events with `503` and `Retry-After: 30`, for every tenant in it.
- **A durable per shard on every partition,** `wh-ingest-<s>` for shard `s` (unpadded, so a name never changes when V grows), filtering `<prefix>.ingest.<p>.<s>.>`, with `priority_policy: pinned_client` in the priority group `wavehouse` and a pinned TTL (`priority_timeout`) of at least 10 seconds, twice the 5 seconds between the pulls that keep the pin of a shard at its share of held rows, or stopping. The ingest processes share the shards out (see [Scaling ingest processes](#scaling-ingest-processes)), and the server delivers a shard to one puller at a time, the one it pinned; the pin lapses once its holder stops pulling for the TTL, and a holder that stops cleanly releases it once it has written what it holds, rather than letting it lapse.
- **The history stream,** which holds `<prefix>.hist.>` and has no sources: the partitions' republish is what writes it. SSE replay (`Last-Event-ID`) and every API process's live events read from it. It is best effort: a row republished while the history is unavailable (deleted, or electing a leader) reaches ClickHouse but never SSE, and a full or missing history never holds up ingest. Its `max_age` is how far back a replay can reach, so set it to at least the longest [`stream.gap_window_minutes`](/settings-directory#streaming) among the tenants you serve. The generated manifests use `15m`, the seed's default window; the API processes check at boot and after each reload, and warn once per tenant and window (again only if that tenant's window changes) for each tenant whose window is longer.
- **One dead-letter stream** holding `<prefix>.dlq.>`, shared by every tenant.
- **The lease bucket,** a KV bucket named `<prefix>_coord` (`wh_coord`; [`coord.nats.bucket`](/configuration#nats-leases-coordnats) names another), where [`coord.backend: nats`](/configuration#backends) holds the ingest processes' leases. Every process running the `ingest` role needs it, because `mq.backend: nats` refuses `coord.backend: local` there. Keep one value per key (`history: 1`), allow direct gets (`allow_direct`, which nack and `nats kv add` always set, because the `wavehouse` user reads leases only that way), and set no `ttl`: a lease expires on its candidates' clocks, and a key the server expires would end a live holder's lease. Boot checks it only in a process with `coord.backend: nats`, and refuses while it is missing.

### Create the topology

1. **Run NATS 2.14 or later** with JetStream on file storage. The shards need pinned-client priority groups and unpinning, which arrived in 2.11, and a consumer reset to its ack floor, which arrived in 2.14; boot refuses an older server. 2.14.x, the line WaveHouse embeds, is recommended, and boot warns on a newer line. [`deployments/nats/values.yaml`](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/nats/values.yaml) is a values file for the [NATS Helm chart](https://github.com/nats-io/k8s): a three-node cluster with one account and two users, `nack` for the JetStream controller and `wavehouse` for WaveHouse, whose passwords come from a `nats-users` Secret.
2. **Generate the streams, consumers and lease bucket** as [nack](https://github.com/nats-io/nack) resources (nack's `KeyValue` needs its control-loop mode):

   ```bash
   wavehouse mq manifests --partitions 4 --shards 32 --prefix wh --replicas 3 > jetstream.yaml
   ```

   [`deployments/nats/jetstream.yaml`](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/nats/jetstream.yaml) is its output for four partitions of eight shards (`--partitions 4 --shards 8`). Pass the names and timeout you configure: `--ingest-consumer` for `mq.nats.ingest_consumer`, `--history-stream` for `mq.nats.history_stream`, `--publish-timeout` for `mq.nats.publish_timeout` (the partitions' duplicate window follows it), and `--coord-bucket` for `coord.nats.bucket`. The streams' byte limits are sized for the servers' file store, `--file-store` (see [Sizing the file store](#sizing-the-file-store)); the other sizes (the history's `maxAge`, `maxMsgsPerSubject`) are starting points: tune them before you apply.
3. **Generate the `wavehouse` user's permissions for the same V**, and replace the user's `permissions` block in `values.yaml` with the output (see [Permissions](#permissions)). The shipped values cover eight shards. Boot probes every shard durable as the `wavehouse` user and refuses to start while the user may not pull from one, so permissions generated for a smaller V stop boot rather than leave shards unpulled:

   ```bash
   wavehouse mq permissions --shards 32 --prefix wh
   ```

4. **Apply the values and the manifests,** in any order: WaveHouse publishes nothing until all of them pass its boot check. A partition keeps every row until the ingest worker acks it, so a missing durable only delays rows. The history stream should exist before events flow if you want SSE to see them: rows republished before it exists reach ClickHouse but not SSE.
5. **Start WaveHouse** with `mq.backend: nats`, `coord.backend: nats` and the [`mq.nats`](/configuration#external-nats-mqnats) block: the server URLs, the `wavehouse` user and a mounted password file, and `partitions` and `shards` equal to the N and V you generated. Boot waits up to `mq.nats.topology_wait` (60s) for the cluster and your resources, because on Kubernetes they may roll out together, then refuses to start and logs every finding at once. A finding marked `recommended` is logged and does not stop boot.

The generated manifests satisfy every required finding (pass `--dedupe-lease` when your `dedupe.lease` is not the default). Some you may meet when you write your own:

- A partition must use `retention: workqueue` and republish to the history: `republish: {source: <prefix>.ingest.<p>.*.>, destination: <prefix>.hist.>}`, which drops the partition and shard tokens. The history must hold exactly `<prefix>.hist.>` and have no sources. `discard: old` is recommended for it, so a full history keeps the newest rows for SSE; either way it never holds up ingest.
- A partition's `duplicate_window` must cover every attempt of one publish: three times `mq.nats.publish_timeout`, plus half a second. A publish that got no answer is retried with the same message id, so the partition stores it once. The generated `2m` covers the default `5s`; `wavehouse mq manifests --publish-timeout <timeout>` widens it for a longer timeout.
- It must also cover [`dedupe.lease`](/configuration#dedupe) twice over, plus a second: the lease, the lease rounded up to whole seconds, and one more second (61s for the default 30s). With dedupe on, a publish whose outcome was unknown keeps its id claimed until the lease lapses, and a client obeying `Retry-After` republishes it as late as that under the same idempotency key; the partition drops the copy only while it still remembers the first. The shipped `2m` covers any lease up to `59s`; for a longer one, `wavehouse mq manifests --dedupe-lease <lease>` widens it.
- A tenant with dedupe on whose finite [`dedupe.retention`](/settings-directory#deduplication), for the tenant or one of its tables, is shorter than the partitions' `duplicate_window` is logged at `WARN`, at boot and after every reload. An id re-sent after its retention but inside the window would be claimed again and then dropped by the partition, while the client is told it was accepted. The settings directory refuses a retention under `2m`, the embedded queue's window, but cannot see yours: keep retention at least as long as the window, or `"0"`.
- Each shard durable needs `max_expires` unset or at least 5 seconds. WaveHouse's pulls wait at most a second today; the floor leaves room for longer ones, since the server refuses a pull that asks for more than `max_expires`.
- Each shard durable needs `max_deliver: -1`. With a limit, a row that failed that many times would stay on its partition and never be delivered again.
- Each shard durable must filter exactly its shard's subjects and use `pinned_client` in the group `wavehouse`, with a pinned TTL of at least 10 seconds. The server renews a pin only when its holder sends a new pull, so a shorter TTL lets a live holder that is at its share of held rows, or stopping, lose its pin between its 5-second renewals. A TTL of 15 seconds or more is recommended against: a dead holder's shard is received by another process only once its pin lapses.

WaveHouse checks the topology again every five minutes and never repairs it. If you delete a partition, its publishes answer `503` with `Retry-After: 5`. If you delete one of the N×V configured shard durables (`wh-ingest-0` to `wh-ingest-<V−1>` on each of the N partitions), or the connection is closed for good (for example, its credentials are revoked), the ingest worker ends and the process exits, so that the orchestrator restarts it and the next boot names what is missing. An ingest worker that stayed up without its queue would leave the API accepting events that nothing writes.

### Sizing the file store

Each server reserves the `maxBytes` of every stream replica it holds against its JetStream file store, `max_file_store`, and refuses a stream that does not fit (JetStream error `10047`, insufficient storage resources). With three replicas on three servers every server holds a replica of every stream, so the streams' `maxBytes` together must fit in each server's store. The NATS Helm chart sets `max_file_store` to `config.jetstream.fileStore.maxSize`, else to the JetStream PVC's size: `100Gi` in the shipped [`values.yaml`](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/nats/values.yaml).

`wavehouse mq manifests --file-store <size>` (default `100Gi`) sizes the streams for that store: each partition gets 15% of it, the history 10% and the dead-letter stream 5%. The shipped four partitions reserve `75Gi` of the `100Gi`, leaving a quarter for what else the store holds (the Raft logs of a replicated stream, the lease bucket, the store's own overhead). A partition's size does not follow N, so lowering N never needs more store while the removed partitions drain. The generator refuses streams that together reserve more than `--file-store`: from six partitions at the defaults, set `--partition-max-bytes` or a larger store. If you change the PVC, pass its size as `--file-store` and apply the regenerated manifests.

### Durability

The two backends make a `200` from `POST /v1/ingest` durable in different ways:

- **Embedded** (`mq.backend: embedded`): the one JetStream server `fsync`s every event to `<data_dir>/nats` before the `200`. There is one copy, so only the disk stands behind it.
- **External NATS** (`mq.backend: nats`): the `200` comes after the partition stream acks the publish, and a stream with 3 or more replicas acks only once a Raft quorum of its servers has stored the event. WaveHouse does not require `sync_always` on your servers, and [`values.yaml`](https://github.com/Wave-RF/WaveHouse/blob/main/deployments/nats/values.yaml) does not set it: an acked event survives the loss of any server short of a quorum, so its durability comes from placing the replicas in separate failure domains (zones, racks or hosts), not from each server's disk. Losing a quorum's servers at once, before they sync, can lose events they acked.
- **External NATS at one replica:** no second copy exists, so the server's `sync_interval` (2 minutes unless you set it) governs. Events stored since the last sync can be lost if that server crashes. Boot logs a `recommended` finding for every partition, history or dead-letter stream with fewer than 3 replicas, and still starts, so a one-server development cluster works. For production, generate the manifests with `--replicas 3` (the default) on a cluster whose servers do not share a failure domain, or set `sync_always` on a one-server cluster and accept the per-event `fsync` cost that [Durability & Storage](/durability) describes. `sync_always` does not reach a stream with `persist_mode: async`, which flushes in the background, so boot refuses that mode on an ingest partition and recommends against it on the dead-letter stream.

### Permissions

The `wavehouse` user in `values.yaml` has exactly what WaveHouse needs: it can publish to its subjects, read stream and consumer info and list consumer names, pull from, unpin and reset each shard durable and ack the rows it delivers, create, pull from and delete consumers on the history stream, and read and write the `lease.` keys in the lease bucket (a KV write is a publish to the key's subject, and a read is a direct get). It cannot create, change, purge or delete a stream or a bucket, nor create a durable on a partition, nor ack for another consumer, nor touch another key, nor publish to the history: only the partitions' republish writes `wh.hist.>`. The permissions are written for the default prefix `wh`, history stream `WH_HISTORY`, durables `wh-ingest-<s>` with eight shards, and bucket `wh_coord`. They name every shard's durable one by one, because a NATS wildcard is a whole token (`wh-ingest-*` names no durable), so print them for your settings with `wavehouse mq permissions --shards <V>` (and `--prefix`, `--ingest-consumer`, `--history-stream`, `--coord-bucket` and `--js-domain` as you set them), and paste the block into the user's entry. For two shards:

```yaml
publish:
  allow: [wh.ingest.>, wh.dlq.>, $JS.API.INFO, $JS.API.STREAM.NAMES, $JS.API.STREAM.INFO.*,
          $JS.API.CONSUMER.INFO.*.*, $JS.API.CONSUMER.NAMES.*,
          $JS.API.CONSUMER.MSG.NEXT.*.wh-ingest-0, $JS.API.CONSUMER.MSG.NEXT.*.wh-ingest-1,
          $JS.API.CONSUMER.UNPIN.*.wh-ingest-0, $JS.API.CONSUMER.UNPIN.*.wh-ingest-1,
          $JS.API.CONSUMER.RESET.*.wh-ingest-0, $JS.API.CONSUMER.RESET.*.wh-ingest-1,
          $JS.ACK.*.wh-ingest-0.>, $JS.ACK.*.wh-ingest-1.>,
          $JS.ACK.*.*.*.wh-ingest-0.>, $JS.ACK.*.*.*.wh-ingest-1.>,
          $JS.API.CONSUMER.CREATE.WH_HISTORY.>, $JS.API.CONSUMER.MSG.NEXT.WH_HISTORY.>,
          $JS.API.CONSUMER.DELETE.WH_HISTORY.>, $KV.wh_coord.lease.>,
          $JS.API.DIRECT.GET.KV_wh_coord.$KV.wh_coord.lease.>]
  deny:  [wh.hist.>, $JS.API.STREAM.CREATE.>, $JS.API.STREAM.UPDATE.>, $JS.API.STREAM.DELETE.>,
          $JS.API.STREAM.PURGE.>, $JS.API.CONSUMER.DURABLE.CREATE.>]
subscribe:
  allow: [_INBOX_wh.>]
```

An ack goes to the subject the server delivered the row with, which names the stream and the consumer: `$JS.ACK.<stream>.<consumer>.…` by default, and `$JS.ACK.<domain>.<account hash>.<stream>.<consumer>.…` on a server with the `js_ack_fc_v2` feature flag. The two `$JS.ACK` entries per shard cover both layouts. WaveHouse's history consumers ack nothing.

Boot checks the permissions against the topology: it sends each shard durable a pull request and an unpin request that the server rejects on their merits (a pull whose heartbeat is more than half its expiry, an unpin naming no priority group), so neither delivers a row nor moves a pin, and a `required` finding names every durable whose request the server refused, with the `wavehouse mq permissions` command to regenerate them. The check runs again every five minutes. A consumer request the server refuses at any other time is logged as an error and sets `wavehouse_mq_topology_ok` to `0` at once, until a check passes again. The reset permission is not probed, since a valid reset request moves the durable back to its ack floor; `wavehouse mq permissions` always grants it with the other two.

**Under a JetStream domain** (`mq.nats.js_domain`), WaveHouse sends every JetStream API request and every lease write under `$JS.<domain>.API.`, including the pulls that keep a busy or stopping shard's pin. A server in that domain maps those subjects to the plain `$JS.API.…` before it checks permissions, while a server outside it, such as a leafnode WaveHouse connects through, checks them as sent. `wavehouse mq permissions --js-domain <domain>` allows and denies every such subject in both forms, so the same block works either way.

WaveHouse's replies arrive under `_INBOX_<prefix>.>`, which is why the subscribe permission can be that narrow.

**Publishing to `<prefix>.ingest.>` or `<prefix>.dlq.>` is trusted as WaveHouse itself.** The ingest worker takes the tenant from the subject and writes the event as published, so a principal with that right writes to any tenant without passing authentication, policy or schema validation. Grant it to the `wavehouse` user alone. Publishing to `<prefix>.hist.>` feeds every tenant's live SSE events and replays directly; grant it to no one, since only the partitions' republish writes there. (`nack` has full access to the account; keep its credentials to the controller.)

### Limits that differ from the embedded queue

- **Per-tenant budgets are not enforced.** A tenant's [`mq.max_bytes_gb`](/settings-directory#message-queue) is not applied; a partition's byte limit is shared by the tenants in it. `maxMsgsPerSubject` with `discardPerSubject: true`, which the generated manifests set, refuses one tenant's table once it holds that many unwritten rows, before it fills the partition.
- **A shard's delivery can stall on one tenant.** If one tenant's ClickHouse hangs, its unwritten rows can fill its shard's share of the rows the ingest process may hold (see [Scaling ingest processes](#scaling-ingest-processes)), and then delivery pauses for every table in that shard, about 1/(N×V) of the tables. The other shards of the same process keep their own shares and keep flowing. More shards shrink that share. (A ClickHouse that refuses inserts outright does not stall a shard: its rows are handed back to the partition for a delayed retry.)
- **Dead-lettered rows share one stream.** Its `discard: old` evicts the oldest rows when it is full; with `maxMsgsPerSubject` set, it evicts per table, so one tenant's flood evicts only its own rows. `GET /v1/ops/dlq/stats?tenant=` answers `200` with zeros for a tenant that has never parked a row, where the embedded queue answers `404`.
- **SSE replay and live events are best effort.** A row republished while the history stream is unavailable, for example full with `discard: new`, deleted, or electing a leader, is written to ClickHouse but never reaches SSE. `wavehouse_mq_history_behind_seconds` shows a history that stops taking rows, and a deleted one fails the topology check (`wavehouse_mq_topology_ok`).

### Choosing and changing N

A table lives in one partition, so one table's ingest rate is bounded by what one stream can take, and a tenant's tables spread over the partitions. N must match `mq.nats.partitions` in every process. Raising it to N′ moves (N′−N)/N′ of the tables, each into one of the new partitions; lowering it moves only the tables of the partitions removed. A moved table's events are not in order across the move. WaveHouse publishes only to partitions `0` to `N−1`, and its ingest worker also drains the shard durables left on any stream holding ingest subjects outside them (the generated `preventDelete` keeps them), so lowering N loses no rows; a stream with no shard durable is only reported, since nothing can drain it.

Every generated partition records its index and N in its metadata (`wavehouse.dev/partition`, `wavehouse.dev/partitions`), and a process configured for another N refuses them. So change N by regenerating: `wavehouse mq manifests --partitions <new N>`, and apply the whole output, which updates every partition's metadata. From then until every process runs the new N, the processes still on the old N report `wavehouse_mq_topology_ok` `0` at their next check and cannot restart, so roll out promptly.

- **To raise N,** apply the regenerated manifests, then roll WaveHouse out with the new N, promptly. Until every ingest process runs the new N, processes on the old and new N share the shards out differently, so some shards (the new ones among them) may have no owner: their rows wait on the partitions, and `wavehouse_ingest_shards_unowned` reads above `0` until the rollout ends.
- **To lower N,** apply the regenerated manifests, then roll WaveHouse out with the smaller N. Ingest does not need to stop. The regenerated manifests leave the removed partitions out, and the generated resources set `preventDelete`, so each removed partition's stream and its shard durables stay, with their rows. The ingest processes on the new N share each such stream's shard durables out with the rest, and drain them, and processes still on the old N keep publishing to it until they are replaced. Boot warns about each one with the rows it still holds. Once a removed partition holds no rows and no process runs the old N, delete its nack `Stream` and `Consumer` resources if they are still applied, then the stream itself with the operator's credentials (`nats stream rm <name>`): `preventDelete` keeps the stream when only its resources go, and the `wavehouse` user cannot delete it. That ends delivery from that stream only, not the worker. Deleting it while it still holds rows loses them, as deleting any partition does. A removed partition keeps its republish, so rows the old processes publish to it still reach live SSE and replay; the history needs no change when N does.

### Scaling ingest processes

Scale the ingest processes as you like; no manifest changes. Each one holds a membership lease in the lease bucket (`lease.ingest.m<j>`, for the lowest free `j`; there are at most 64 such slots, so at most 64 ingest processes get work), and every process computes the same owner for every shard from the live members: rendezvous hashing, capped so that no process owns more than an even share, rounded up. A process that joins or leaves moves roughly one to two times its share of the shards while the even share, rounded up, stays the same (a few times it at worst); past about seven processes, where that share steps down (over 32 shards in all, partitions × shards: at 8, 11, 16 and 32 processes), it moves several times its share. It never moves more than half of them. The cap bounds the most a process owns, not the least: above about half as many processes as shards, some hold none, and a process beyond partitions × shards, or beyond 64, always holds none. Shard durables left over after N or V is lowered are shared out apart from the configured ones, so deleting a drained one moves no configured shard.

- **A shard changing owner** stops fetching at the old owner, which keeps the shard's pin while it waits for the rows it delivered to be acked, or handed back to the partition, then releases the pin; the new owner receives at once, and only rows newer than the ones the old owner wrote. Fetching stops within a second, the length of one pull; the rows already fetched must then still reach the worker, which takes as long as the worker is behind. The wait for them to be acked is bounded by one minute from when they have reached the worker, the worker's ack wait and the shortest `ack_wait` a shard durable may have, and the old owner keeps the pin that long and a few seconds more; if it runs out, the new owner may receive before the old one has finished, so that one shard's rows are briefly written by two processes, out of order.
- **A clean stop** stops fetching every shard first, keeping their pins, so the worker writes what it holds and whatever those shards still deliver; then it releases every shard at once and resigns the membership lease, and the others take over within about a tick (2 seconds). A row the worker could not take while stopping is handed back to the partition before the release, so it comes back to the next owner ahead of anything newer. Rows the partition redelivers (handed back, or past `ack_wait`) come back ahead of newer rows, but not always in order among themselves across a handover or a stop: while the next owner's pull waits for the pin, the server requeues a due redelivery behind the others.
- **A process that dies** keeps its membership lease until the others have seen it unrenewed for 15 seconds, and its shards' pins until the pinned TTL (10 seconds in the generated manifests) after its last pull. Then each new owner resets the shard to its ack floor before pulling it, so the rows the dead process received and never acked come back at once rather than after `ack_wait` (at least 1 minute). The reset needs more than the membership view: the shard must also hold no pin and have delivered nothing and had no ack for the pinned TTL, so a process the others wrongly think dead, but which still works the shard, is never reset under it. A lone ingest process restarting after a clean stop does the same on its first pass, for every shard nobody holds. One restarting after a crash still sees its predecessor's lease as live for 15 seconds: the shards the new process is assigned beside it come back after `ack_wait`, the rest at once when that lease lapses. Rows it had written but not acked are written again: delivery is at least once, so use a deduplicating table engine, or [`dedupe`](/configuration#dedupe).
- **A live process that stops making progress** (a hung ClickHouse insert) keeps its shards, and their pins, however long its inserts take: liveness is not progress. The worker never holds up a shard's pulls, so its pins never lapse, and no other process takes its shards or resets them. Each shard may hold rows delivered and not yet acked up to its share of 10,000: an even share over the shards the process holds, but never under 1,000 (two batches), so one busy table still fills whole batches. At its share a shard fetches one row every 5 seconds, which keeps its pin (a row the partition redelivers comes back in order that way), up to one `ack_wait` of such rows past its share (12 at the generated 1-minute `ack_wait`); past that it keeps its pin with pulls that deliver nothing, and a stuck shard's redelivered rows may then come back out of order among themselves; the rest stay on the partition, and one stuck shard never takes another shard's share. A process so holds about 10,000 rows at most, or 1,000 per shard when it holds more than ten, plus those 12 per stuck shard. A row stops counting at its first ack or nak, confirmed or not, or once `ack_wait` has passed, when the partition redelivers it anyway.

### Choosing and changing V

A shard's rows reach ClickHouse through one ingest process at a time, so V×N caps how many ingest processes get work, and one table's insert rate is bounded by what one process writes. A process splits its prefetch (500 rows) between the shards it pulls, so one shard's pull asks for fewer rows the more shards the process holds. The default V of 32 leaves room to scale out; a larger V costs a consumer per shard on every partition (a Raft group each at 3 replicas), a permission entry per shard, and one pull a second per shard that has nothing to deliver. V must match `mq.nats.shards` in every process, and each partition records it in its metadata (`wavehouse.dev/shards`).

- **To raise V,** regenerate with `--shards <new V>` and apply the whole output: the new shards' durables are added, and the existing ones keep their names and their rows. Regenerate the permissions with the new V too, then roll WaveHouse out with it promptly: the regenerated metadata says the new V, so processes still on the old V report `wavehouse_mq_topology_ok` `0` at their next check and cannot restart. Raising V to V′ moves (V′−V)/V′ of the tables, each into one of the new shards; a moved table's older rows drain from its old shard while its new rows go to the new one, so for that table alone two processes may insert at once for a while, and its events are not in order across the move. A process still on the old V publishes to the old shards. Until every ingest process runs the new V, the old and new processes share the shards out differently, so some shards (the new ones among them) may have no owner: their rows wait, and `wavehouse_ingest_shards_unowned` reads above `0` until the rollout ends.
- **To lower V,** apply the regenerated manifests (the generated durables set `preventDelete`, so the removed shards' durables stay with their rows) and roll WaveHouse out with the smaller V, promptly, for the same reason. Keep the permissions for the larger V until they drain. The ingest processes share out each durable past the new V and drain it, and boot warns about each with the rows it holds; once one holds none and no process runs the old V, delete its nack `Consumer` resource if it is still applied (nack's control loop would recreate the durable otherwise), then the durable itself with the operator's credentials (`nats consumer rm <stream> <durable>`): `preventDelete` keeps the durable when only its resource goes. That ends delivery from that durable only, not the worker. Then regenerate the permissions for the smaller V.

### Before production: manual checks

Two things the automated tests do not cover. Check them once on your own cluster before production:

- **nack applies what the manifests say.** Apply the generated manifests with your nack release, then read back one partition and one shard durable (`nats stream info WH_INGEST_0`, `nats consumer info WH_INGEST_0 wh-ingest-0`): the partition needs `retention: workqueue` and its `republish`, the durable `pinned_client`, the group `wavehouse` and its pinned TTL. WaveHouse's boot check reports any field that did not arrive.
- **Three replicas behave as the design assumes.** On a three-server cluster at `--replicas 3`: kill the leader of a shard durable while a worker holds its pin, and check that the worker re-pins with its next pull and no row is lost; kill a partition's stream leader while events flow, and check how many of them the history misses (the rows themselves still reach ClickHouse); and watch the servers' CPU and memory with the full N×V consumers under load.

### Monitoring

These gauges are exported through [OpenTelemetry or Prometheus](#observability) under `mq.backend: nats`:

| Gauge | Meaning |
| --- | --- |
| `wavehouse_mq_connected` | `1` while this process is connected to the cluster, else `0`. |
| `wavehouse_mq_topology_ok` | `1` while the last check found every required stream, consumer and (under `coord.backend: nats`) the lease bucket, and found that the `wavehouse` user may pull from and unpin every shard durable, else `0`. It drops at once when a publish finds a partition deleted, or when the server refuses a consumer request (see [Permissions](#permissions)). |
| `wavehouse_mq_history_behind_seconds` | How far the history's newest row trails the newest row any partition stored, read every 30 seconds. A value that stays up or keeps growing means the history is not taking the rows the partitions republish, so SSE replay and live events miss them; it returns to about `0` with the next row the history takes, and rows missed before that are not counted. A missing history reads as the last value while `wavehouse_mq_topology_ok` goes to `0`. It never affects ingest. |

The ingest processes export these for their shards:

| Metric | Meaning |
| --- | --- |
| `wavehouse_ingest_shards_owned` | Shards this process holds and consumes. The sum over processes is partitions × shards, plus any extras still draining, once membership has settled. |
| `wavehouse_ingest_shard_members` | Ingest processes this process last counted. |
| `wavehouse_ingest_shards_unowned` | Shards with rows waiting that no process holds, and that have delivered nothing and had no ack for the pinned TTL, read every 15 seconds by the lowest-ranked member only (the others report nothing, so take the `max` across processes). Alert when it stays above `0` for a minute or more: rows nobody is writing. A shard whose process is stuck on a hung insert keeps its pin, so it does not count. |
| `wavehouse_ingest_rows_held` | Rows delivered to this process that it has not yet acked or nacked (confirmed or not), and that are younger than `ack_wait`, across its shards; each shard holds about its share at most (see [Scaling ingest processes](#scaling-ingest-processes)). |
| `wavehouse_ingest_shard_events_total{event}` | `taken`, `released`, `reset` (a dead owner's rows redelivered at takeover), `handover_timeout` (a shard released before its rows were acked or handed back), `lost` (the membership lease ended under the process), `bind_failed` (a shard could not be bound; tried again next tick, and logged as an error once it has failed for a minute). A configured shard durable (one of the N×V) that is gone, with its stream or alone, or that no longer fits the worker (its `ack_wait` shorter than a minute, or no `max_ack_pending`), ends the worker instead, as a durable deleted while it is pulled does; a leftover durable past a lower N or V is only logged. |
| `wavehouse_ingest_shard_handover_seconds` | From giving a shard up to releasing it. |

`wavehouse_nats_connections` and `wavehouse_nats_in_msgs_total` describe this process's client connection under `nats` (`1` or `0`, and the messages it has received), where under `embedded` they describe the embedded server.

## One Deployment per role

By default one process runs all of WaveHouse. [`roles`](/configuration#process-roles) (`WH_ROLES`) lets the API and the background workers run as separate processes, so that each scales on its own. On Kubernetes that is one Deployment per role, from the same image, differing only in `WH_ROLES`:

| Deployment | `WH_ROLES` | Replicas | Serves on `:8080` |
| --- | --- | --- | --- |
| API | `api` | as many as your request load needs | the full API |
| Ingest | `ingest` | as many as your write load needs | the ops listener |

- **API.** Each API pod runs its own schema discovery, token verifiers, dedupe handle and SSE hub, and receives every event so that it can serve its own SSE clients. Put your Service and ingress in front of these pods only.
- **Ingest.** The ingest pods share the shards out, none owning more than an even share rounded up, and each shard is written by one pod at a time, so a table's rows are written by one pod. Scaling the Deployment moves only whole shards, never rows; at most partitions × shards pods get work. See [Scaling ingest processes](#scaling-ingest-processes).

A split needs backends that every process can reach: a shared `mq.backend`, so that every process reaches the same queue; a shared `coord.backend`, so that the ingest processes' leases span pods; and, when `api` and `ingest` run in separate processes, a shared `cache.backend`, so that the ingest pods' invalidations reach the API pods' cache. This build has four shared backends: [`mq.backend: nats`](#external-nats) and `coord.backend: nats` on the same cluster, [`cache.backend: redis`](#multiple-instances-and-the-shared-cache), and [`dedupe.backend: dynamodb`](#a-shared-dedupe-table-on-dynamodb). Boot refuses a split the selected backends cannot serve, naming the backend to change:

- **Separate `api` and `ingest` Deployments need `cache.backend: redis`.** With a local cache, boot refuses a process that runs one of them without the other; run them together (`WH_ROLES=api,ingest`) instead, and each replica's cache serves reads that may be stale until an entry expires (boot warns).
- **Dedupe across replicas needs `dedupe.backend: dynamodb`.** With `pebble` each replica dedupes only the event ids it has seen itself, so a retry that lands on another replica is written twice (boot warns).
- **There is no sweeper process.** Under `mq.backend: nats` the streams' own retention replaces the sweeper, so it is not wired, and boot refuses a process whose only role is `sweeper`. The API processes warn, once per tenant and window, about a tenant whose gap window is longer than the history stream keeps.
- **Ingest processes need `coord.backend: nats`:** boot refuses `coord.backend: local` in a process running `ingest` on a shared queue.

Run every role in one process, the default, until you need more than one.

A pod without the `api` role serves an ops listener on `:8080`: `/livez`, `/readyz` and their aliases, `/version`, the metrics path when `prometheus.port` is `0`, and `POST /v1/ops/settings/reload`. Every other route answers 404 (under `/v1/ops`, 403 without the operator key, and 401 for a bearer token). Point the same probes at it as at an API pod. `/livez` does not wait for schema discovery there, because only the API runs it. `/readyz` checks ClickHouse in an ingest pod. Every pod reads the settings directory, so mount it in every Deployment. The reload route on the ops listener accepts only the operator key, so whatever reloads your API pods over HTTP must send the operator key to the worker pods too, or rely on `SIGHUP` (or, over a flat directory, the directory watcher) instead.

Give each pod a stable `WH_INSTANCE_ID` only if you need one in the logs or in the lease's `holder`. The default, the pod's hostname with a random suffix, already names each pod uniquely.

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

**A rejected folder fails closed, for that tenant alone — tenant `0`'s excepted.** A folder that fails validation stops its tenant being served — its requests answer `503` — while every other tenant carries on, at boot and on a reload alike. Tenant `0` is the exception: the process still draws some shared wiring from that folder, so rejecting it costs every tenant something ("What a lost tenant `0` costs", below, says what). There is no fall back to the tenant's previous settings, unlike [the single-tenant directory](/settings-directory#loading-and-hot-reload): the recovery is fixing the folder and reloading it. A request already in flight finishes on the settings it started with, except an open `GET /v1/stream`, which is ended at once: its reconnect gets the `503` until the folder is fixed — the SDK keeps retrying and then resumes from `Last-Event-ID`, while a browser `EventSource` gives up on the `503` and has to be reopened. The rows the tenant had already accepted but not yet inserted, those of an ingest request in flight included, which still answers `200`, are parked on the DLQ under the tenant's own subject rather than held for the fix, as a removed tenant's are (see [Dead Letter Queue](#dead-letter-queue-dlq)). Under the embedded broker its message queue is kept, at the budget it last had, and so is the history that gap-fill replays, for the `stream.gap_window_minutes` its folder last had (all of it, for a folder rejected since the server started, whose window the server never read); under `mq.backend: nats` the shared history ages out by its `max_age` whatever happens to the folder: a stream resumed after a fix within that window picks up where it left off. The findings go to the log and to the reload response, never into the `503`. A finding about the directory itself — a loose file, an entry or a directory that can't be read, a changed shape — is another matter: it refuses boot, and on a reload it rejects the reload whole and leaves every tenant as it was. A directory that would serve no tenant — every folder rejected, or none named by a tenant id, as a fresh volume whose only entry is `lost+found` is — refuses boot too, with the findings; on a reload the same directory drops every tenant and keeps running.

**Reloading is the writer's call.** A nested directory is not watched, because a watcher would validate a folder halfway through being written and drop its tenant. Whoever writes a tenant's folder reloads it once it is complete: `POST /v1/ops/settings/reload?tenant=acme` re-validates that folder and reads nothing else. It must name a tenant the server already holds (`404` otherwise), so a folder the server does not hold yet — one added since the last whole-directory reload — is picked up by a whole-directory reload, not by naming it; a tenant it holds but rejected is reloaded by name like any other. Without the parameter — and on `SIGHUP` — the whole directory is reloaded and mirrors its folders: a new folder becomes a tenant, and a removed one becomes unknown. That is how a tenant is removed: delete its folder, then reload the whole directory. Its open streams end, its routes answer `404`, and its queued rows are parked on the DLQ under its own subject; nothing it stored is deleted — under the embedded broker its message queue is kept at the budget it last had, and only the history that gap-fill replays goes from it, at the next sweep (under `mq.backend: nats` the history ages out by its `max_age`) — so restoring the folder restores the tenant, seen ids and parked rows included. Reloading a deleted folder by name instead leaves its tenant rejected, answering `503`. The last folder can be removed the same way, with two catches, since `wavehouse validate` and boot both read an emptied directory as the four files missing: `validate` exits `1`, so a writer that gates each reload on it has to skip the check for that one reload, and a server restarted before a folder is written back refuses to boot. A whole-directory reload re-validates every folder, so it carries the exposure the watcher would: a folder caught halfway through being written can fail validation, and its tenant then stops being served until a later reload adopts it. The response is the [single-tenant one](/api#post-v1opssettingsreload--reload-settings-directory). After a whole-directory reload, `adopted: false` with a `422` can mean adopted in part: the folders with an error among their `findings` were rejected and the rest were adopted — warnings included, since `findings` carries every folder's.

**The admin routes take the operator key only.** `/v1/ops/*` reaches every tenant, so over a nested directory no tenant's admin role opens it: the [operator key](/api#authentication) alone does, and a token carrying an admin role gets `403`. Boot a nested directory without `auth.operator_key` and no caller can reach these routes at all, which leaves `SIGHUP` as the only reload; the server warns about it at boot. `GET /v1/ops/pipes`, `GET /v1/ops/pipes/{name}`, `GET /v1/ops/schema`, `POST /v1/ops/schema/refresh` and `POST /v1/ops/query` take the same `?tenant=`, and address tenant `0` without it; `GET /v1/ops/dlq/stats` takes it too, and reads a rejected or removed tenant's dead-letter queue like a served one's, since the queue is kept; under the embedded broker a tenant that has none is a `404`. On the routes that take it the parameter is parsed strictly — a query string that does not parse, an empty or repeated `tenant`, or a malformed id is a `400`, never a silent read of the default tenant or, on the reload route, a reload of every tenant. The SDK sends it as the [`tenant` option](/sdk/admin#settings--whsettings).

**What a tenant's folder decides.** A request is evaluated against its own tenant's `policies.json` and `pipes.json` (ingest, structured queries, pipes), its `query.*` keys, its `cors.allowed_origins`, and its `dedupe` block: whether its records are deduplicated, by which id, against the tenant's own store, which that folder's `dedupe.enabled` opens and closes on reload exactly as [the single-tenant one](/settings-directory#deduplication) does (every tenant's store is a share of the one Pebble instance at `<data_dir>/pebble`, each key led by its tenant and table), so `wavehouse_ingest_dedupe_disabled_total` ticks only across a tenant's own reload, whatever the other tenants' switches say. A tenant's seen ids are its own: the same event id is first seen under each tenant that sends it, and in each table. Its `auth` block is its own too: each tenant's folder wires that tenant's token verifier (`jwks_url`, `role_claim`), built when the folder is adopted and rebuilt when its wiring changes, so a JWKS-issued token verifies only under the tenants whose `jwks_url` names its provider's key set. Under another tenant's header a token is treated as invalid, and the request falls back to that tenant's `default_role` like any other unverifiable token, possibly after a rate-limited key refetch (see [Authentication](/settings-directory#authentication)). Keep `X-Tenant-ID` pinned at the proxy so a token is never presented under the wrong tenant. Tenants can still accept each other's tokens: those that leave `jwks_url` empty share the boot HMAC secret when `auth.jwt_secret` is set, so a token verifies under any of them (with no secret they validate no token at all), and those whose `jwks_url` names the same key set accept each other's tokens; isolate them by provider, or scope rows by a signed claim ([row-level security](/access-control#row-level-security)). A tenant whose `jwks_url` has not been fetched yet answers `503` with `Retry-After` to its token-bearing requests alone. A tenant that stops being served — its folder rejected or removed — loses its verifier and the JWKS refresh with it, and gets a fresh one when its folder is adopted again. The HMAC secret and the operator key stay boot config, shared by every tenant; the operator key is stamped with the request tenant's `admin_role`. A tenant's `clickhouse` and `schema` blocks are its own as well: each tenant reads and writes its own ClickHouse — one native pool per distinct address, database, user, password and `tls` tuple, shared by the tenants naming it, under the process-wide [connection ceiling](/settings-directory#clickhouse) — and discovers its own tables from its own database on its own `schema.refresh_interval`. Tenants may have different server time zones: the first tenant bound that can be served fixes the process's image zone, and every other tenant's own zone is passed on each call, so literals and input parsing match its server. Only a table with a zone-less `DateTime` column beside any `DEFAULT`, `MATERIALIZED`, `ALIAS` or `EPHEMERAL` expression is refused (`503`, its row-filtered streams withheld) for a tenant whose zone differs, and a tenant whose zone cannot be served is unavailable whole: an unrecognized zone name, or a zone chtypes cannot load when that tenant makes the first open (once another tenant has set the image zone, such a tenant is bound and has each call refused instead; see [time zones](#time-zones)). Under the embedded broker its message queue is its own as well (under [`mq.backend: nats`](#limits-that-differ-from-the-embedded-queue) tenants share the partitions): its events are queued on a stream of their own, capped at its own `mq.max_bytes_gb` — at that budget its ingest answers `503` while every other tenant's keeps publishing — beside a dead-letter stream of its own at a tenth of it, and the history that gap-fill replays from it is kept for its own `stream.gap_window_minutes`. Nothing checks what the tenants' budgets add up to against the disk, so size them together ([Message Queue](/settings-directory#message-queue)). An event is published on its tenant's subject (`ingest.{tenant}.{table}`), so a `GET /v1/stream` connection is authorized by its own tenant's `policies.json` and receives its own tenant's rows alone, the ingest worker inserts a row into its own tenant's ClickHouse, a rejected row is parked under its own tenant's `dlq.enabled` and subject (`dlq.{tenant}.{table}`), and two tenants' tables of one name never share a batch. The query cache is one pool, but its entries are keyed by tenant: identical `POST /v1/query` and pipe requests from two tenants are two entries and two queries to ClickHouse, and a tenant is never served another's cached rows. An insert invalidates the table's cached results under every tenant on the same ClickHouse address and database as the tenant it was ingested for, whatever their user or `tls` block, since they read the same tables; a tenant on no pool — its folder rejected or removed, or no pool could be opened for it, such as by the ceiling — is out of that fan-out while it is, and has its cached results (`POST /v1/query` and pipe alike) dropped the moment it is back on one, so a repaired or restored folder never serves rows cached before the inserts it missed. A tenant whose folder moves it to another address or database has them dropped too, since they came from other tables. Apart from those two drops, a cached pipe result stays until its TTL expires, since a pipe names no table and no insert invalidates it. One setting weighs every tenant: the SSE keepalive, where the wheel runs at the shortest `stream.keepalive_interval` among the tenants being served, with that tenant's `stream.keepalive_buckets`.

**What a lost tenant `0` costs.** A `0` folder that a reload rejects or removes stops tenant `0` being served like any other, and what becomes of the shared settings depends on how they are read. Tenant `0` leaves its ClickHouse pool (closed only once no served tenant names its tuple), and its schema registry and verifier are released with the folder, like any other tenant's; the `/v1/ops/*` routes, which resolve no tenant, verify against it, so a token there reads as invalid (`401`) rather than merely non-admin (`403`) until tenant `0` is served again — the operator key, which never consults a verifier, is unaffected. CORS does not stay either: the responses that read tenant `0`'s list — the tenant-exempt routes, the refusals, a preflight naming no tenant — carry no CORS headers until the folder is served again, while every other tenant's routes keep their own list. Tenant `0`'s own dedupe store closes, as any rejected or removed tenant's does, its seen ids kept for the folder that restores it. What is read per event follows the event's tenant, so tenant `0`'s events are the ones affected: with no ClickHouse to insert into, its rows fail and are parked on the DLQ whatever its switch said, and its open `GET /v1/stream` connections are ended, as any tenant's are when it stops being served — the other tenants' events are untouched. A nested directory that has never served a tenant `0` — no `0` folder, or one rejected at boot — serves every other tenant from its own ClickHouse. Outside `/v1/ops/*`, a `/v1` request that sends no `X-Tenant-ID` resolves to tenant `0`, so with no `0` folder it answers `404 unknown tenant: 0` (`503` with a rejected one) — the SDK's `/v1/health` reachability ping included.

### Upgrading behind a proxy that already sends `X-Tenant-ID`

`X-Tenant-ID` is a generic name, and some gateways and service meshes stamp one on every request. WaveHouse used to ignore it; now, over a settings directory that holds the four files, any value other than `0` names an unknown tenant, so **every `/v1` route outside `/v1/ops/*` answers `404 unknown tenant: <id>`** (a `400` when the value is not a tenant id at all, a dotted hostname, say) — the SDK's `/v1/health` reachability ping included, while the bare probes and the admin surface stay green. Strip the inbound header at the edge ([header forwarding](/reverse-proxy#header-and-auth-forwarding)) unless you are using it deliberately.

## Multiple instances and the shared cache

Several WaveHouse instances can serve one ClickHouse behind a load balancer. What they share is decided per layer. With the defaults, most of what each one holds is its own: the embedded message queue means an event is inserted by the instance that took its `POST /v1/ingest` and reaches only that instance's SSE subscribers, and the Pebble dedupe store means an id one instance has seen is new to another. [`mq.backend: nats`](#external-nats) gives every instance one queue, so each one's SSE subscribers see every event, and [`dedupe.backend: dynamodb`](#a-shared-dedupe-table-on-dynamodb) one set of seen ids.

The query-result cache is shared the same way. With the default `cache.backend: local`, each instance caches in its own memory, and an insert invalidates only the cache of the instance that made it. Every other instance keeps serving its cached results for the rows before the insert until each entry's TTL runs out, between 10 s and 1 h depending on how long the query took. With [`cache.backend: redis`](/configuration#cache), every instance reads and fills one Redis-compatible server, and an insert on any instance invalidates the cached results of every instance. The server is a standalone one or a Redis Cluster; Sentinel (`mode: sentinel`) refuses boot until [#656](https://github.com/Wave-RF/WaveHouse/issues/656), since the cache does not yet authenticate to the sentinels or refresh their topology.

**What another instance can see.** Ingest is already asynchronous: `/v1/ingest` answers before the batch is inserted. Once the inserting instance's worker has written the batch to ClickHouse, it replaces the table's version token in Redis, and from then on a lookup on any instance misses and reads the new rows. The cache adds no delay of its own beyond that single write. The exceptions:

- **The server is unreachable from the inserting instance.** The invalidation is kept and retried until it lands (`wavehouse_cache_invalidations_pending` counts what is owed). Meanwhile other instances that can still reach the server keep serving the older results, for as long as the outage lasts and at most until each entry's TTL. An instance that stops while invalidations are still owed loses them, with the same bound. The same thing happens today when a process stops between an insert and its invalidation.
- **A failover to a replica that had not yet received the latest token writes** can bring back entries filed under the older tokens, bounded by the replication lag at the moment of failover and those entries' TTL. WaveHouse never reads from replicas. Behind a stable address (a managed primary endpoint), an instance still connected to the demoted node has its writes refused, which bypasses its cache; connections are replaced every minute, so it reaches the new primary and delivers the invalidations it owes within about that long. The breaker's own probe write gets a longer budget for a reconnect — twice `dial_timeout` (the client bounds the dial and the handshake by it in turn) plus `timeout` for the write itself — but only closes the breaker when a write actually lands within `timeout`: one slower than that is repeated under `timeout` alone, and the repeat decides, so a server that merely answers slowly stays bypassed instead of flapping open and shut. Every other connection redials under `timeout` alone: size it above how long a reconnect actually takes, or operations that land on one of those keep failing after the probe has already succeeded.
- **The server is full and `maxmemory-policy` is `noeviction`.** It refuses the token writes. The inserting instance keeps its invalidations and retries them, bypassing its cache meanwhile, but every other instance serves the results from before the insert until one lands, up to their TTL.
- **A pipe that writes** (an `INSERT` in `pipes.json`) is neither cached nor coalesced: it runs on every call, on whichever instance takes it ([Pipes that write](/pipes#pipes-that-write)). It does not invalidate cached reads of the tables it writes ([#394](https://github.com/Wave-RF/WaveHouse/issues/394)), so with a shared cache every instance serves those results from before the write until their TTL.
- **Admin writes through `POST /v1/ops/query`** do not invalidate the cache ([#394](https://github.com/Wave-RF/WaveHouse/issues/394)). With a shared cache, the stale results they leave are served by every instance, not only one.

**Sizing the server.** Every key WaveHouse writes has a TTL, and a version token lost to eviction, expiry or `FLUSHALL` can only cause misses, never bring back an entry it had invalidated. So set `maxmemory` and let the server evict: `maxmemory-policy allkeys-lru` (or `allkeys-lfu`, `volatile-lru`, `volatile-lfu`). Under `noeviction`, a full server refuses the writes: each refusal (a fill's is counted by `wavehouse_cache_set_failures_total{reason="oom"}`) bypasses the cache of the instance that got it, and invalidations are kept and retried, so the pre-insert results above stay served by the others: avoid `noeviction`. A stored result is capped at `cache.redis.max_value_bytes` (1 MiB compressed). A tenant's version tokens share one hash tag, so each lookup reads them in one `MGET` in cluster mode as well. The results themselves carry no hash tag and spread across shards. **Run it without persistence** (`save ""` and `appendonly no`): stock Redis and Valkey persist by default (periodic RDB save points), so a crash that is followed by a restart reloads the last save on its own — the same rollback as restoring a snapshot by hand, not a loss. Without persistence, a restart can only cause misses, the same as any other token loss. With it on (the default), a restart reloads whatever snapshot or AOF it last wrote, tokens and values it had already invalidated included, so a fresh instance can serve the pre-write rows filed under them as hits until their TTL (up to 1 h) expires. Treat restoring a snapshot, or a crash-restart on a server that still has its defaults, as a rollback, not a resume.

**The server is inside the trust boundary.** A cached result is served after the access policy has filtered it, so whoever can write to the server can change what any caller reads. Keep it on a private network, require a password or ACL user (`WH_CACHE_REDIS_PASSWORD`), use TLS across links you do not trust, and share it only with deployments you trust as much as this one.

**Coalescing stays per instance.** `singleflight` collapses identical concurrent queries within each instance, so a cold hot query costs at most one ClickHouse query per instance, not one per request.

**Metrics** (meter `wavehouse-cache`, every series labeled `backend="redis"`, no tenant label): `wavehouse_cache_lookups_total{result}` (`hit`, `miss`, `stale`, `bypass`, `error`), `wavehouse_cache_op_duration_seconds{op}` (`lookup`, `set`, `invalidate`), `wavehouse_cache_breaker_open` (1 while the cache is bypassed), `wavehouse_cache_invalidations_total{result}` (`ok` counts every bump that lands, retried ones included, and `deferred` each bump an invalidation could not deliver when made, a repeat of one already owed included; a failed retry is not counted again — the two overlap, not a split), `wavehouse_cache_invalidations_pending`, `wavehouse_cache_value_bytes`, `wavehouse_cache_oversize_total` and `wavehouse_cache_set_failures_total{reason}` (`oom`, `timeout`, `other`). Two signals are worth alerting on: `wavehouse_cache_breaker_open` at 1, or `wavehouse_cache_invalidations_pending` above 0, for more than a few minutes.

For local development, `docker compose -f deployments/compose/dependencies.yaml --profile redis up -d` starts a Redis on `localhost:6379` with no persistence.

## ClickHouse Schema

WaveHouse uses a **Bring Your Own Schema** model. You create your tables in ClickHouse with whatever columns and engines you need. WaveHouse discovers the schemas automatically via `system.columns` and validates ingest data against them — see [Schema Validation](/api#post-v1ingesttabletable--ingest-data) for the rules a record must satisfy.

Four schema-design consequences are worth knowing before you write the DDL. A `MATERIALIZED`, `ALIAS`, or `EPHEMERAL` column is never part of a published row: WaveHouse's ingest validation runs ClickHouse's own parser in-process (via [chtypes](#chtypes-artifacts)), and a record that names a `MATERIALIZED` or `ALIAS` one is rejected with ClickHouse's own code (117) rather than published, while an `EPHEMERAL` value is accepted only where the format names columns (the JSON family, `…WithNames`), the role may write it and a `DEFAULT` reads it (and no `MATERIALIZED`, `ALIAS` or other `EPHEMERAL` column does), and then feeds that `DEFAULT` without being stored or published (anywhere else it is code 117); a policy `check` naming any of the three is refused outright. An omitted column — on any table — takes its `DEFAULT` expression, or the type's default where none is declared (`NULL` on a `Nullable` column), evaluated by that same parser before the row is published; there is no longer a positional-encoding quirk that stores `NULL` on a `Nullable(T) DEFAULT …` column instead — see [Ingest Pipeline → High-level shape](/ingest-pipeline#high-level-shape) for detail. Rows retried after a ClickHouse outage reach ClickHouse out of ingest order, so a table whose engine picks a winner by insert order — a `ReplacingMergeTree` without a version column, a `CollapsingMergeTree` — needs a version column the producer sets in the record (`ReplacingMergeTree(ver)`, `VersionedCollapsingMergeTree`), not an insert-time `DEFAULT now64()` like the example's `received_timestamp`. And a retry after an insert whose outcome WaveHouse could not see (a timeout, a dropped connection) can land its rows twice on any engine — the example's plain `MergeTree` included, and a `VersionedCollapsingMergeTree` then keeps a state row its one cancel cannot remove — so a table that must not count a row twice needs a `ReplacingMergeTree` keyed on an id the producer sets, read with `FINAL` (it removes a duplicate only when parts merge; a [pipe](/pipes) can say `FINAL`, a structured query never adds it), or reads that tolerate duplicates, such as `uniqExact(id)`. `dedupe.enabled` does not prevent this: it drops a repeated publish at the HTTP edge, and this duplicate is made after the queue. See [When ClickHouse cannot take an insert](/ingest-pipeline#when-clickhouse-cannot-take-an-insert).

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

## Upgrading across the dedupe key change

The dedupe key now carries the table as well as the tenant ([#222](https://github.com/Wave-RF/WaveHouse/issues/222)), so **an id deduped before the upgrade is not recognized after it**: a record carrying it is accepted once more. Nothing is migrated. The old keys never count as seen, and the dedupe sweep deletes them: its first pass runs about a minute after the instance opens, and `wavehouse_dedupe_swept_keys_total{reason="version_0"}` counts them ([#220](https://github.com/Wave-RF/WaveHouse/issues/220)). Pebble returns their disk space as it compacts, not at once. Only a tenant with `dedupe.enabled` on is affected, and only by a record sent both before and after the upgrade — typically a producer retrying across the restart. To avoid duplicate rows, let retrying producers finish, or pause them, before upgrading.

The same release adds an optional **`dedupe.retention`** key. No upgrade step is needed: a `config.json` without it keeps every id forever, as before. See [Deduplication](/settings-directory#deduplication) for a finite one.

## A shared dedupe table on DynamoDB

Pebble is per process, so two pods on it do not share seen ids. The DynamoDB backend keeps every tenant's ids in **one shared table**, and a conditional write makes a claim atomic across every pod that uses the table. WaveHouse **never creates this table in production**: the table belongs to your infrastructure code. The backend refuses to create a table unless it is pointed at a custom endpoint, so table creation only works against [dynamodb-local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html).

What the backend requires of the table:

| Attribute | Type | Role |
|---|---|---|
| `pk` | String | Partition key, and the only key: tenant, table and id as readable text, for example `acme/clicks/evt-123` (the table and id escaped the way NATS subject tokens are: letters, digits, `_` and `-` kept, every other byte written as `%XX`). No sort key. |
| `st` | Number | `1` = pending claim, `2` = committed. |
| `ex` | Number | Epoch seconds: the lease end while pending, the retention end once committed; absent = never expires. |
| `tk` | Binary | The claim token that `Release` matches. |

Only `pk` is declared in the table definition. Turn TTL on for `ex`. Correctness never depends on TTL, because a claim whose `ex` has passed counts as absent whether or not DynamoDB has deleted it yet; TTL only reclaims the storage. TTL removes lapsed claims, and a committed id once its [`dedupe.retention`](/settings-directory#deduplication) ends. With the default retention `"0"` (forever) a committed item carries no `ex` and is kept, so the table grows by one item (about 200 bytes) per distinct id. Boot checks the table and logs a warning if TTL is off; a key schema that does not match is a misconfigured table, handled as described below.

An example in Terraform. Replace the tags with your own conventions:

```hcl
resource "aws_dynamodb_table" "wavehouse_dedupe" {
  name                        = "wavehouse-dedupe-${var.environment}"
  billing_mode                = "PAY_PER_REQUEST" # provisioned + auto scaling once traffic is steady
  hash_key                    = "pk"
  deletion_protection_enabled = true

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "ex"
    enabled        = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = {
    Name        = "wavehouse-dedupe-${var.environment}"
    Project     = "wavehouse"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# The pods' role (EKS Pod Identity or IRSA). No Scan, no CreateTable.
data "aws_iam_policy_document" "wavehouse_dedupe" {
  statement {
    actions = [
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:DescribeTable",
      "dynamodb:DescribeTimeToLive",
    ]
    resources = [aws_dynamodb_table.wavehouse_dedupe.arn]
  }
}
```

Select it in the boot config, on every pod that should share seen ids (all the keys are in the [Configuration Reference](/configuration#dynamodb-dedupe)):

```yaml
dedupe:
  backend: dynamodb
  dynamodb:
    table: wavehouse-dedupe-prod
    region: us-east-1 # or leave empty for AWS_REGION
```

or `WH_DEDUPE_BACKEND=dynamodb`, `WH_DEDUPE_DYNAMODB_TABLE=wavehouse-dedupe-prod`. A table that is missing, has the wrong key schema, or refuses the pod's credentials refuses boot over a flat settings directory whose tenant has dedupe on, and is logged at `ERROR` otherwise. In every other case — a throttle or network failure, a nested directory, or no tenant with dedupe on — the pod boots, every tenant with dedupe on (now or after a reload) fails its ingest closed, and the check is retried in the background (backing off from one second to thirty, and at once after every reload). A reload makes no table call itself, and does not wait on a tenant whose dedupe setting is unchanged; it waits only for a tenant whose store it closes — dedupe switched off, or the tenant removed or rejected — and then only for that tenant's in-flight calls, before its store closes. No region at all (neither `region` nor one from the SDK chain: `AWS_REGION`, `AWS_DEFAULT_REGION` or a profile) refuses boot in both shapes. The check runs in every pod running the `api` [role](/configuration#process-roles), whether or not any tenant has `dedupe.enabled` on; a pod without it opens no dedupe store. The per-tenant switch stays in each tenant's `config.json`.

For development against dynamodb-local, set `dedupe.dynamodb.endpoint` (for example `http://localhost:8000`) and `create_table: true`, and give the SDK any static credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) and a region. `create_table` without an `endpoint` refuses boot.

- **Credentials** come from the AWS SDK's default chain (EKS Pod Identity or IRSA in a pod; the environment or a profile locally), never from WaveHouse configuration.
- **Point-in-time recovery** is not needed. The table records which ids have been seen, so losing it produces duplicate rows, not lost events.
- **Cost:** every new event is two writes (the claim, then the commit), and a duplicate is one. On-demand, that is about $1.25 per million new events in us-east-1. Provisioned capacity with auto scaling is cheaper once traffic is steady. Storage is the other line: every distinct id stays in the table until its retention ends, forever at the default (see TTL above), at DynamoDB's per-GB-month rate.
- **One table serves every tenant,** so one tenant's burst can throttle the rest. A throttled or unreachable table fails the ingest request closed rather than publishing un-deduped. After five throttled or unreachable claims in a row within one second, the backend stops calling the table for a second and fails every tenant's dedupe requests immediately (`wavehouse_dedupe_dynamodb_short_circuits_total`). A duplicate or in-flight answer is not a failure and resets the count.
- **Metrics:** `wavehouse_dedupe_dynamodb_requests_total{op,outcome}`, `wavehouse_dedupe_dynamodb_request_duration_seconds{op}`, `wavehouse_dedupe_dynamodb_unprocessed_items_total`, `wavehouse_dedupe_dynamodb_short_circuits_total`. The table's own CloudWatch metrics `ThrottledRequests`, `SystemErrors` and `ConsumedWriteCapacityUnits` are worth alerting on too.

## Upgrading across the v2 ingest envelope

The NATS envelope changed shape in this release: the row now travels positionally, with `format`, `columns` and `row` replacing `data` — and the queue changed layout with it: boot deletes the earlier build's queue (below), so nothing an older version published reaches the new worker, which could not read it anyway (it carries no `format`, so there is no way to say which value belongs to which column). **Drain first** to keep what the old build had not yet inserted.

The streaming surface loses something too, more quietly. SSE gap-fill (`?since=` / `Last-Event-ID`) replays from the queue, so the deletion takes the replay history with it, even after a *correct* drain: any replay spanning the upgrade silently omits the pre-upgrade events, with **no error and no frame**. Clients that need them should backfill over REST.

**The upgrade does not carry the old queue over at all.** Boot deletes the earlier build's queue and dead-letter queue (`WAVEHOUSE`, `WAVEHOUSE_DLQ`) and everything in them, logging a `WARN` with each one's message count: an event the old build had not yet inserted, and a row it had already parked, do not survive the upgrade. Draining first keeps the events not yet inserted; a row already parked is lost with the queue, since the earlier build offers no way to read one back (`GET /v1/ops/dlq/stats` returns counts only).

Three audits belong **before** the drain, because none of them is cheap to discover afterwards:

- **API processes now need a chtypes artifact for their ClickHouse line.** By default each line's is fetched when its first tenant binds (chtypes 1.0 publishes 26.3, 26.7, 26.8 and 26.9; a server on any other line has none, and its tenant answers every ingest `503`), so the API processes need to reach the artifact registry or a mirror, and a container needs a cache volume to keep what it fetched ([chtypes artifacts](#chtypes-artifacts)). An air-gapped deployment turns autofetch off and installs the artifacts first; it then refuses to boot with none installed. Windows, FreeBSD and darwin/amd64 builds are no longer published.
- **Policy `check` blocks are now validated against the table.** A `check` naming a column the table lacks, one it computes (`MATERIALIZED`/`ALIAS`), or an `EPHEMERAL` one is a per-record `403` on *every* insert by that role. `wavehouse validate` cannot catch it — it never sees the ClickHouse schema — so audit them against their tables first. See [Access control → Insert checks](/access-control#insert-checks).
- **Every `WH_*` variable the binary does not bind refuses boot.** The old binary ignored a variable it did not read; the new one names every unbound one and exits before it opens the queue, so a pod spec or compose file that still carries one comes back from the upgrade as a container that will not start. Diff the environment against the [Configuration Reference](/configuration) first: a `WH_*` variable that is not in its tables is unbound, and whatever it used to configure now lives in the [settings directory](/settings-directory) or is gone. A Kubernetes Service in the pod's namespace named `wh` or `wh-*` counts too: it injects link variables under the `WH_` prefix (`WH_SERVICE_HOST` and `WH_PORT` for `wh`, `WH_FOO_SERVICE_HOST` and `WH_FOO_PORT` for `wh-foo`), so set `enableServiceLinks: false` on the pod spec.

**Keep ClickHouse healthy for the whole drain.** The build being drained does not retry an outage: every row of a failed insert is isolated and, failing again, parked on `WAVEHOUSE_DLQ` (or, with the DLQ off, left for redelivery), and the upgrade deletes both queues.

To drain before upgrading:

1. **Stop the producers**, or cut `/v1/ingest` at the reverse proxy. Nothing new should enter the stream.
2. **Wait for the in-flight batches to flush.** A table's batch closes on size or after `maxWait` (5s by default), so a few seconds after the last write is enough; give it longer if ClickHouse is slow.
3. **Confirm nothing is left unconsumed** before swapping binaries. Not that the stream is empty: it is dual-use, and deliberately retains ACKed messages for the SSE replay window, so a non-zero depth right after a clean drain is expected. Rows landing in ClickHouse is a success signal, **not proof the queue is drained** — when the DLQ is off, a row that fails its retry is skipped without being acked, so NATS keeps redelivering it while its neighbors land. Check that nothing is still failing or redelivering, and note which signal covers which case: [`GET /v1/ops/dlq/stats`](/api#get-v1opsdlqstats--dlq-statistics) is non-zero only where the DLQ is **on**; on a build that exposes it (from the v2 envelope on), `wavehouse_ingest_poison_total` counts unreadable envelopes on **either** setting, told apart by its `disposition` label (`parked` / `dropped`); and for a twice-failed row with the DLQ off — the case just described — the **only** signal is the `ERROR` log: `isolated bad row, DLQ disabled for table` on a build with the per-table switch, or, on v0.1.0, `isolated bad row, sending to DLQ` followed by `NATS DLQ publish failed`. A clean `dlq/stats` with the DLQ off proves nothing. There is no queue-depth gauge today ([#544](https://github.com/Wave-RF/WaveHouse/issues/544) tracks the related in-flight accounting), and `wavehouse_nats_in_msgs_total` going flat is a supporting signal rather than a guarantee. Enabling the DLQ is not itself a drain: a parked row is not inserted, and the upgrade deletes it.
4. **Upgrade**, then re-enable ingest.

If you skipped the drain, the boot's `WARN` line for each deleted stream (`deleted the stream an earlier build kept for every tenant together`) says how many messages went with it: for `WAVEHOUSE_DLQ`, the parked rows lost; for `WAVEHOUSE`, a count that includes the acknowledged history kept for replay, already in ClickHouse — so it bounds the events lost rather than counting them, and is non-zero even after a clean drain.

## Dead Letter Queue (DLQ)

Under [`mq.backend: nats`](#external-nats) every tenant's parked rows go to the one shared dead-letter stream, under `<prefix>.dlq.{tenant}.{table}`, not `DLQ_{tenant}`, and a long outage fills the partitions its tables are in (or a table's `maxMsgsPerSubject`) rather than its `mq.max_bytes_gb`; the rest of this section holds. A batch insert ClickHouse **rejects** is retried row by row; while the tenant's `dlq.enabled` is `true` for the table (the seed default — a hot-reloadable [settings directory](/settings-directory#dead-letter-queue) key, overridable per table), the rows ClickHouse rejects again are published to the tenant's own dead-letter stream (`DLQ_{tenant}`) under subjects `dlq.{tenant}.{table}` (`0` for a directory that holds the four files) instead of retrying forever. A batch whose tenant has no ClickHouse connection — one no longer served, or one no pool could be opened for, such as by the connection ceiling — skips the row-by-row retry, which no row of it could pass: its tenant's switch is read once for the whole batch, and a tenant no longer served has no switch to read, so its batch is always parked. A ClickHouse that cannot take inserts at all — down, unreachable, overloaded, read-only, or refusing the configured credentials — parks nothing: its rows stay in the tenant's ingest queue and are retried with backoff, counted one per row each time they are handed back by `wavehouse_ingest_retries_total`, so a long outage shows up as a growing ingest stream (and, at the tenant's `mq.max_bytes_gb`, as ingest `503`s), not as a full DLQ — see [Ingest Pipeline](/ingest-pipeline#when-clickhouse-cannot-take-an-insert). Monitor DLQ depth via `GET /v1/ops/dlq/stats`, per tenant (`?tenant=`; tenant `0` without it).

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

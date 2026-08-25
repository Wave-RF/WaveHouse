<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/public/branding/lockup-dark.svg">
    <img src="docs/public/branding/lockup-light.svg" alt="" height="60">
  </picture>
</h1>

<p align="center">
  <strong>
    ClickHouse® is a great database with a subpar API. WaveHouse fixes that.
  </strong>
</p>

<p align="center">
  The open-source real-time API gateway for ClickHouse: schema-aware ingest, async batching, real-time SSE streaming, and tiered query caching.
  <strong>
  All in a single binary.
  </strong>
</p>

<p align="center">
  <a href="https://github.com/Wave-RF/WaveHouse/releases"><img alt="GitHub Release" src="https://img.shields.io/github/v/release/Wave-RF/WaveHouse?filter=!client&color=%2306B0BF"></a>
  <a href="LICENSE"><img alt="License: Apache 2.0" src="https://img.shields.io/badge/License-Apache_2.0-%23086D77"></a>
</p>

<p align="center">
  <a href="https://wavehouse.dev"><strong>Docs</strong></a> ·
  <a href="#-quick-start"><strong>Quick start</strong></a> ·
  <a href="https://wavehouse.dev/why-wavehouse"><strong>Why WaveHouse</strong></a> ·
  <a href="https://github.com/Wave-RF/WaveHouse/discussions"><strong>Discussions</strong></a>
</p>

---

## Quick Start Guide

```bash
git clone https://github.com/Wave-RF/WaveHouse.git && cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d
```

The in-repo compose spins up WaveHouse and ClickHouse. Then, create a table (or import your existing schema):

```bash
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "CREATE TABLE IF NOT EXISTS events (kind String, user String, received_timestamp DateTime64(3,'UTC') DEFAULT now64(3,'UTC')) ENGINE=MergeTree ORDER BY kind"
```

Then, stream live events in one terminal, ingest one in another, and watch the ingested events arrive from the stream:

```bash
curl -N "http://localhost:8080/v1/stream?table=events" & sleep 1
```

> [!IMPORTANT]
> No auth is needed to ingest with the default dev policy. The policy is VERY permissive, and not intended for production deployments.

```bash
curl -sX POST "http://localhost:8080/v1/ingest?table=events" \
  -H 'content-type: application/json' -d '{"kind":"click","user":"u_42"}'
```

> [!TIP]
> A 404 "unknown table" error here just means schema discovery hasn't seen the new table yet. Just retry; worst case 60s before discovery.

Full walkthrough at **[wavehouse.dev/getting-started](https://wavehouse.dev/getting-started)**.

## Why WaveHouse?

ClickHouse is a phenomenal OLAP database, but pointing a frontend right at it leaves a lot to be desired: one-row inserts trigger `Too many parts`, there's no backpressure or edge validation, no real-time push, and no row/column security. You end up building custom APIs, a Kafka queue, a batch consumer, a cache tier, and an auth service. **WaveHouse is that whole stack as one binary** — the only external dependency is ClickHouse.

If you're building user-facing analytics, WaveHouse is like **Supabase for ClickHouse**. Or an **open-source Tinybird** that pushes data to the frontend in real time over SSE, not just pull-based REST.

- **Ingest** — async durable WAL (embedded NATS JetStream), `200 OK` instantly, background batch-flush; schema-validated against `system.columns`; optional ID-based dedup (idempotent ingest); dead-letter queue for failed inserts.
- **Query** — in-process Ristretto cache + `singleflight` coalescing; type-safe structured query AST; Tinybird-style named pipes (parameterized SQL endpoints).
- **Real-time** — native SSE push, broadcast *before* the ClickHouse flush, with JetStream gap-fill for late/reconnecting clients.
- **Security** — Hasura-style per-table, per-role column + row policies with JWT claim templating, stored in NATS KV.
- **Client SDKs** — TypeScript (`@wavehouse/sdk`) and Go (`github.com/Wave-RF/WaveHouse/clients/go`): query builder, live queries, streaming, and schema codegen in both, over one shared wire format.

## How it compares

|                               | Direct ClickHouse | Kafka + CH (DIY) |   Tinybird    | **WaveHouse**  |
| ----------------------------- | :---------------: | :--------------: | :-----------: | :------------: |
| Self-hosted, single binary    |         —         |        —         |   ✗ (SaaS)    |       ✓        |
| Safe high-rate inserts        |         ✗         |  ✓ (via Kafka)   |       ✓       |       ✓        |
| Schema validation at the edge |         ✗         |      custom      |       ✓       |       ✓        |
| Real-time push (SSE)          |         ✗         |  custom service  |       ✗       |    ✓ native    |
| Thundering-herd coalescing    |         ✗         |      custom      |       ✓       |       ✓        |
| Row/column policies (JWT)     |         ✗         |      custom      |  tokens only  | ✓ Hasura-style |
| Cost model                    |       infra       | infra + eng time | per-vCPU SaaS |   infra only   |

Full breakdown, failure modes, and our engineering rationale at **[wavehouse.dev/why-wavehouse](https://wavehouse.dev/why-wavehouse)**.

## Getting Started

Pick between the in-repo Docker Compose, pulling a container image, or `go install`. The Compose option starts WaveHouse; the other options just download/install a container or binary. Run the selected artifact before using `http://localhost:8080`.

### A. Docker Compose (recommended)

```bash
git clone https://github.com/Wave-RF/WaveHouse.git && cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d
```

The stack has a permissive dev policy, so you can ingest without a token. Create a table in ClickHouse (or import existing schema), then ingest — see the [getting-started walkthrough](https://wavehouse.dev/getting-started) for the full ingest → query → stream tour.

### B. Prebuilt container image

```bash
docker pull ghcr.io/wave-rf/wavehouse:latest    # latest stable release 
docker pull ghcr.io/wave-rf/wavehouse:dev       # rolling main-branch build
```

Tags carry a signed [Sigstore](https://www.sigstore.dev/) build-provenance attestation — verify before you deploy:

```bash
gh attestation verify oci://ghcr.io/wave-rf/wavehouse:dev \
  --repo Wave-RF/WaveHouse \
  --signer-workflow Wave-RF/WaveHouse/.github/workflows/publish-dev.yml
```

Swap in `:vX.Y.Z` and `release.yml` for a release image. Pin the signer either way. `--repo` alone accepts an attestation from any workflow in the repo.

### C. `go install` (binary, no Docker)

```bash
go install github.com/Wave-RF/WaveHouse/cmd/wavehouse@latest
```

You'll still need ClickHouse reachable. Point WaveHouse at it by setting `WH_CH_ADDR` (defaults to `localhost:9000`). See [Configuration](https://wavehouse.dev/configuration) for more information.

## Project status

WaveHouse is in **alpha**. See [SUPPORT.md](SUPPORT.md) for where to ask what, the alpha-stage response cadence (best-effort, 1–2 business days), and what's in vs. out of scope right now.

Track what's shipped, in progress, and planned on the [**project board**](https://github.com/orgs/Wave-RF/projects/7).

> **Alpha = expect change.** WaveHouse is pre-1.0: APIs, configuration, wire formats, and on-disk state can change between releases without a migration path, and some capabilities are still hardening. Pin a version and don't rely on stability guarantees until a tagged GA release.

## Local Development

You'll need **Go 1.26+, GNU Make 4+, Docker (Compose v2), Node.js 22 LTS, and pnpm 11.21+**. See [development docs](https://wavehouse.dev/development) for the authoritative source of truth with the full list, version requirements, and gotchas.

```bash
make tools    # one-time bootstrap
docker compose -f deployments/compose/dependencies.yaml up -d clickhouse
make dev      # hot-reload on .go save
```

## 🤝 Contributing

Issues, pull requests, and feedback welcome! See our [CONTRIBUTING.md](CONTRIBUTING.md) guidelines on how to structure your code and run the integration test suites.

## Security

Found a vulnerability? **Do NOT open a public issue.** Email `security@wave-rf.com` per [SECURITY.md](SECURITY.md) — we acknowledge within 48 hours and aim for an initial assessment in 5 business days.

## AI-Assisted Development

The repo contains a minimal [Claude Code](https://claude.com/claude-code) configuration: safety guardrails, a couple of slash commands and subagents, auto-format hooks, and [worktrunk](https://worktrunk.dev) project hooks for parallel agent workflows. Personal preferences (status line, model, allow lists) stay user-level. See [Claude Code & AI agents](https://wavehouse.dev/claude-code) for setup + reference. `AGENTS.md` at the repo root is the canonical source of truth for project conventions.

## Responsible GenAI Usage Disclosure

> **AI-assisted, human-reviewed.** WaveHouse has been developed with AI assistance ([Claude Code](https://claude.com/claude-code)). Every change, whether AI- or human-authored, goes through the same review gates, tests, and CI before it gets merged. The docs are the source of truth, and please [open an issue](https://github.com/Wave-RF/WaveHouse/issues) if anything is out of date. (or, obviously, if you notice an issue that isn't security-related)

## License

WaveHouse is open source under the [Apache License 2.0](LICENSE).

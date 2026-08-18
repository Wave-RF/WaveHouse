<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/public/branding/lockup-dark.svg">
    <img src="docs/public/branding/lockup-light.svg" alt="" height="60">
  </picture>
</h1>

<p align="center">
  <strong>
    ClickHouse is a great database and a poor API. WaveHouse is the API.
  </strong>
</p>

<p align="center">
  The open-source real-time API gateway for ClickHouse — schema-aware ingest, async batching, real-time SSE streaming, and tiered query caching in a single binary.
</p>

<p align="center">
  <a href="https://github.com/Wave-RF/WaveHouse/actions/workflows/ci.yml"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Wave-RF/WaveHouse/badges/coverage-go.json" alt="Go Coverage"></a>
  <a href="https://goreportcard.com/report/github.com/Wave-RF/WaveHouse"><img src="https://goreportcard.com/badge/github.com/Wave-RF/WaveHouse" alt="Go Report Card"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License: Apache 2.0"></a>
</p>

<p align="center">
  <a href="https://wavehouse.dev"><strong>Docs</strong></a> ·
  <a href="#-quick-start"><strong>Quick start</strong></a> ·
  <a href="https://wavehouse.dev/why-wavehouse"><strong>Why WaveHouse</strong></a> ·
  <a href="https://github.com/Wave-RF/WaveHouse/discussions"><strong>Discussions</strong></a>
</p>

---

## ⚡ Try it in 30 seconds

```bash
git clone https://github.com/Wave-RF/WaveHouse.git && cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d   # ClickHouse + WaveHouse (ships a dev policy)

# create a table (Bring Your Own Schema)
docker compose -f deployments/compose/standalone.yaml exec clickhouse \
  clickhouse-client --query "CREATE TABLE IF NOT EXISTS events (kind String, user String, received_timestamp DateTime64(3,'UTC') DEFAULT now64(3,'UTC')) ENGINE=MergeTree ORDER BY kind"

# stream live events in one terminal, then ingest one in another and watch it arrive
curl -N "http://localhost:8080/v1/stream?table=events" & sleep 1
# in another terminal, ingest an event (no auth needed with the dev policy)
# (a 404 "unknown table" here just means schema discovery hasn't seen the new table yet — retry; worst case 60s)
curl -sX POST "http://localhost:8080/v1/ingest?table=events" \
  -H 'content-type: application/json' -d '{"kind":"click","user":"u_42"}'
```

Full walkthrough → **[wavehouse.dev/getting-started](https://wavehouse.dev/getting-started)**.

## ✨ Why WaveHouse

ClickHouse is a phenomenal OLAP database, but pointing a frontend straight at it has sharp edges: one-row inserts trigger `Too many parts`, there's no backpressure or edge validation, no real-time push, and no row/column security. You end up building custom APIs, a Kafka queue, a batch consumer, a cache tier, and an auth service. **WaveHouse is that whole stack as one binary** — the only external dependency is ClickHouse.

If you're building user-facing analytics, WaveHouse is like **Supabase for ClickHouse** — or an **open-source Tinybird** that pushes data to the frontend in real time over SSE, not just pull-based REST.

- **Ingest** — async durable WAL (embedded NATS JetStream), `200 OK` instantly, background batch-flush; schema-validated against `system.columns`; optional ID-based dedup (idempotent ingest); dead-letter queue for failed inserts.
- **Query** — in-process Ristretto cache + `singleflight` coalescing; type-safe structured query AST; Tinybird-style named pipes (parameterized SQL endpoints).
- **Real-time** — native SSE push, broadcast *before* the ClickHouse flush, with JetStream gap-fill for late/reconnecting clients.
- **Security** — Hasura-style per-table, per-role column + row policies with JWT claim templating, stored in NATS KV.
- **Client** — `@wavehouse/sdk`: TypeScript client with query builder, live queries, streaming, and schema codegen; one runtime dependency (an SSE frame parser, ~1.4 KB gzipped).

## 📊 How it compares

|                               | Direct ClickHouse | Kafka + CH (DIY) |   Tinybird    | **WaveHouse**  |
| ----------------------------- | :---------------: | :--------------: | :-----------: | :------------: |
| Self-hosted, single binary    |         —         |        —         |   ✗ (SaaS)    |       ✓        |
| Safe high-rate inserts        |         ✗         |  ✓ (via Kafka)   |       ✓       |       ✓        |
| Schema validation at the edge |         ✗         |      custom      |       ✓       |       ✓        |
| Real-time push (SSE)          |         ✗         |  custom service  |       ✗       |    ✓ native    |
| Thundering-herd coalescing    |         ✗         |      custom      |       ✓       |       ✓        |
| Row/column policies (JWT)     |         ✗         |      custom      |  tokens only  | ✓ Hasura-style |
| Cost model                    |       infra       | infra + eng time | per-vCPU SaaS |   infra only   |

Full breakdown, failure modes, and the engineering rationale → **[wavehouse.dev/why-wavehouse](https://wavehouse.dev/why-wavehouse)**.

## 🛠️ Quick Start

Pick whichever fits — each ends with WaveHouse listening on `http://localhost:8080`.

### A. Docker Compose (recommended first run)

```bash
git clone https://github.com/Wave-RF/WaveHouse.git && cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d
```

The stack ships a permissive dev policy, so you can ingest without a token. Create a table in ClickHouse (Bring Your Own Schema), then ingest — see the [getting-started walkthrough](https://wavehouse.dev/getting-started) for the full ingest → query → stream tour.

### B. Prebuilt container image

```bash
docker pull ghcr.io/wave-rf/wavehouse:latest    # latest stable release
docker pull ghcr.io/wave-rf/wavehouse:dev       # rolling main-branch build
```

`:latest` follows stable releases only — a prerelease moves `:alpha` / `:beta` / `:rc` / `:next` instead, so `:latest` starts existing with the first stable tag. Until then, use `:dev`.

Both tags carry a signed [Sigstore](https://www.sigstore.dev/) build-provenance attestation — verify before you deploy:

```bash
gh attestation verify oci://ghcr.io/wave-rf/wavehouse:dev \
  --repo Wave-RF/WaveHouse \
  --signer-workflow Wave-RF/WaveHouse/.github/workflows/publish-dev.yml
```

Swap in `:vX.Y.Z` and `release.yml` for a release image. Pin the signer either way — `--repo` alone accepts an attestation from any workflow in the repo.

### C. `go install` (binary, no Docker)

```bash
go install github.com/Wave-RF/WaveHouse/cmd/wavehouse@latest
```

You'll still need ClickHouse reachable — point WaveHouse at it via `WH_CH_ADDR` (defaults to `localhost:9000`).
See [Configuration](https://wavehouse.dev/configuration).

## 🚦 Project status

WaveHouse is in **alpha** — built in the open, Apache-2.0-licensed, no vendor lock-in. See [SUPPORT.md](SUPPORT.md) for where to ask what, the alpha-stage response cadence (best-effort, 1–2 business days), and what's in vs. out of scope right now.

Track what's shipped, in progress, and planned on the [**project board**](https://github.com/orgs/Wave-RF/projects/7).

> **Alpha — expect change.** WaveHouse is pre-1.0: APIs, configuration, wire formats, and on-disk state can change between releases without a migration path, and some capabilities are still hardening. Pin a version and don't rely on stability guarantees until a tagged GA release.

## 💻 Local Development

You'll need **Go 1.26+, GNU Make 4+, Docker (Compose v2), Node.js 22 LTS, and pnpm 11.21+**. See [development docs](https://wavehouse.dev/development) for the authoritative source of truth with the full list, version requirements, and gotchas.

```bash
make tools    # one-time bootstrap
docker compose -f deployments/compose/dependencies.yaml up -d clickhouse
make dev      # hot-reload on .go save
```

## 🤖 Working with Claude Code

> **AI-assisted, human-reviewed.** Much of WaveHouse — code and docs alike — is written with AI assistance ([Claude Code](https://claude.com/claude-code)). Every change, whether AI- or human-authored, goes through the same review gates, tests, and CI before it lands. We note it for transparency: treat the docs as the source of truth, and please [open an issue](https://github.com/Wave-RF/WaveHouse/issues) if anything reads as off or out of date.

The repo ships minimal team-wide [Claude Code](https://claude.com/claude-code) configuration — safety guardrails, a couple of slash commands / subagents, an auto-format hook, and [worktrunk](https://worktrunk.dev) project hooks for parallel agent workflows. Personal preferences (status line, model, allow lists) stay user-level. See [Claude Code & AI agents](docs/src/content/docs/claude-code.md) for setup + reference. `AGENTS.md` at the repo root is the canonical source of truth for project conventions.

## 🤝 Contributing

Issues, pull requests, and feedback welcome! See our [CONTRIBUTING.md](CONTRIBUTING.md) guidelines on how to structure your code and run the integration test suites.

## 🛡️ Security

Found a vulnerability? **Don't open a public issue.** Email `security@wave-rf.com` per [SECURITY.md](SECURITY.md) — we acknowledge within 48 hours and aim for an initial assessment in 5 business days.

## 📜 License

WaveHouse is open source under the [Apache License 2.0](LICENSE).

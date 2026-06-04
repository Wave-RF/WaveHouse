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
  <a href="https://github.com/Wave-RF/WaveHouse/actions/workflows/ci.yml">
    <img src="https://github.com/Wave-RF/WaveHouse/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
  <a href="https://go.dev/">
    <img src="https://img.shields.io/github/go-mod/go-version/Wave-RF/WaveHouse" alt="Go Version">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT">
  </a>
  <a href="https://github.com/Wave-RF/WaveHouse/stargazers">
    <img src="https://img.shields.io/github/stars/Wave-RF/WaveHouse?style=social" alt="Stars">
  </a>
  <a href="https://github.com/Wave-RF/WaveHouse/releases">
    <img src="https://img.shields.io/github/v/release/Wave-RF/WaveHouse" alt="Release">
  </a>
  <a href="https://goreportcard.com/report/github.com/Wave-RF/WaveHouse">
    <img src="https://goreportcard.com/badge/github.com/Wave-RF/WaveHouse" alt="Go Report Card">
  </a>
</p>

<p align="center">
  <a href="https://wavehouse.dev"><strong>Docs</strong></a> ·
  <a href="#-quick-start"><strong>Quick start</strong></a> ·
  <a href="https://wavehouse.dev/why-wavehouse"><strong>Why WaveHouse</strong></a> ·
  <a href="https://github.com/Wave-RF/WaveHouse/discussions"><strong>Discussions</strong></a>
</p>

---

## ⚡ Try it in 30 seconds

<!-- TODO: update this -->

```bash
git clone https://github.com/Wave-RF/WaveHouse.git && cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d   # ClickHouse + WaveHouse

# ingest an event, then watch it stream back live
curl -sX POST "http://localhost:8080/v1/ingest?table=events" \
  -H 'content-type: application/json' -d '{"kind":"click","user":"u_42"}'
curl -N "http://localhost:8080/v1/stream?table=events"
```

Full walkthrough → **[wavehouse.dev/getting-started](https://wavehouse.dev/getting-started)**.

## ✨ Why WaveHouse

ClickHouse is a phenomenal OLAP database, but pointing a frontend straight at it has sharp edges: one-row inserts trigger `Too many parts`, there's no backpressure or edge validation, no real-time push, and no row/column security. You end up building custom APIs, a Kafka queue, a batch consumer, a cache tier, and an auth service. **WaveHouse is that whole stack as one binary** — the only external dependency is ClickHouse.

If you're building user-facing analytics, WaveHouse is like **Supabase for ClickHouse** — or an **open-source Tinybird** that pushes data to the frontend in real time over SSE, not just pull-based REST.

- **Ingest** — async durable WAL (embedded NATS JetStream), `200 OK` instantly, background batch-flush; schema-validated against `system.columns`; optional exact-match dedup; dead-letter queue for failed inserts.
- **Query** — in-process Ristretto cache + `singleflight` coalescing; type-safe structured query AST; Tinybird-style named pipes (parameterized SQL endpoints).
- **Real-time** — native SSE push, broadcast *before* the ClickHouse flush, with JetStream gap-fill for late/reconnecting clients.
- **Security** — Hasura-style per-table, per-role column + row policies with JWT claim templating, stored in NATS KV.
- **Client** — `@wavehouse/sdk`: zero-dependency TypeScript client with query builder, live queries, streaming, and schema codegen.

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

<!-- TODO: update this -->

```bash
git clone https://github.com/Wave-RF/WaveHouse.git && cd WaveHouse
docker compose -f deployments/compose/standalone.yaml up -d
```

Then create a table in ClickHouse (Bring Your Own Schema) and ingest — see the [getting-started walkthrough](https://wavehouse.dev/getting-started) for the full ingest → query → stream tour.

### B. Prebuilt container image

```bash
docker pull ghcr.io/wave-rf/wavehouse:latest    # tagged release
docker pull ghcr.io/wave-rf/wavehouse:dev       # rolling main-branch build
```

### C. `go install` (binary, no Docker)

```bash
go install github.com/Wave-RF/WaveHouse/cmd/wavehouse@latest
```

You'll still need ClickHouse reachable — point WaveHouse at it via `WH_CH_ADDR`.
See [Configuration](https://wavehouse.dev/configuration).

## 🚦 Project status

WaveHouse is in **alpha** — built in the open, MIT-licensed, no vendor lock-in. See [SUPPORT.md](SUPPORT.md) for where to ask what, the alpha-stage response cadence (best-effort, 1–2 business days), and what's in vs. out of scope right now.

Here's the high-level status of the major capabilities:

| Capability                    | Status |
| ----------------------------- | ------ |
| Async ingest + batch flush    |   ✅   |
| Schema validation             |   ⚠️   |
| Real-time SSE + gap-fill      |   ✅   |
| Query cache + singleflight    |   ✅   |
| Access control (JWT policies) |   ⚠️   |
| Named pipes                   |   ⚠️   |
| Exact-match dedup             |   ⚠️   |
| TypeScript SDK                |   ⚠️   |

<!-- TODO: update the above list and/or switch to roadmap somewhere? Maybe link to project board? -->

## 💻 Local Development

You'll need **Go 1.26+, GNU Make 4+, Docker (Compose v2), Node.js 22 LTS, and pnpm 11+**. See [development docs](https://wavehouse.dev/development) for the authoratative source of truth with the full list, version requirements, and gotchas.

```bash
make tools    # one-time bootstrap
docker compose -f deployments/compose/dependencies.yaml up -d clickhouse
make dev      # hot-reload on .go save
```

## 🤖 Working with Claude Code

<!-- TODO: disclaimer about AI-generated content -->

The repo ships minimal team-wide [Claude Code](https://claude.com/claude-code) configuration — safety guardrails, a couple of slash commands / subagents, an auto-format hook, and [worktrunk](https://worktrunk.dev) project hooks for parallel agent workflows. Personal preferences (status line, model, allow lists) stay user-level. See [Claude Code & AI agents](docs/src/content/docs/claude-code.md) for setup + reference. `AGENTS.md` at the repo root is the canonical source of truth for project conventions.

## 🤝 Contributing

Issues, pull requests, and feedback welcome! See our [CONTRIBUTING.md](CONTRIBUTING.md) guidelines on how to structure your code and run the integration test suites.

## 🛡️ Security

Found a vulnerability? **Don't open a public issue.** Email `security@wave-rf.com` per [SECURITY.md](SECURITY.md) — we acknowledge within 48 hours and aim for an initial assessment in 5 business days.

## 📜 License

Open source under the [MIT License](LICENSE).

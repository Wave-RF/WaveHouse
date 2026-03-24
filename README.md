# 🏖️ BeachHouse

[![CI](https://github.com/Wave-RF/BeachHouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Wave-RF/BeachHouse/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Wave-RF/BeachHouse)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The Open-Source Real-Time API Gateway for ClickHouse.**

BeachHouse is a high-performance, Go-based gateway designed to sit entirely in front of ClickHouse. It acts as the exclusive entry and exit point for your analytics data, solving the hardest parts of high-scale data ingestion and real-time querying so ClickHouse can focus on what it does best: fast analytics.

If you are building user-facing analytics, **BeachHouse acts like Supabase for ClickHouse**—or an **open-source Tinybird** that pushes data to your frontend in real time over SSE and WebSockets, not just via pull-based REST queries.

## ✨ Why BeachHouse?

ClickHouse is a phenomenal OLAP database, but directly exposing it to frontend applications comes with sharp edges. You typically have to build custom APIs, manage Kafka queues to prevent "too many parts" errors during insertion, and write complex replacing logic for deduplication. BeachHouse abstracts all of this away into a single, deployable binary so you stop interacting with ClickHouse directly.

* **🛡️ Multi-Tenant Access Control:** Enforces strict Row-Level Security (RLS) via JWTs, ensuring tenants only ever read and write their own data.
* **📥 Asynchronous Buffered Ingestion:** Never drop a packet. BeachHouse writes incoming data to a highly durable Write-Ahead Log (WAL) and returns `200 OK` instantly, batching inserts to ClickHouse in the background.
* **👯 Exact-Once Deduplication:** Built-in exact-match deduplication ensures duplicate payloads are dropped *before* they ever reach ClickHouse, saving expensive merge operations.
* **⚡ Two-Tier Query Caching:** An ultra-fast local memory cache (L1) and a shared distributed cache (L2) coalesce identical queries, protecting ClickHouse from dashboard "thundering herds."
* **🌊 Zero-Latency Real-Time Push:** When data is pushed via the BeachHouse API, it is immediately broadcast to authorized SSE/WebSocket listeners—even before it gets flushed to ClickHouse. This ensures instant perceived ingestion for your users, with seamless gap-fill from an in-memory replay buffer for clients that connect late.
* **🗂️ Dynamic Schema Normalization:** Accept arbitrary, nested JSON payloads from clients. BeachHouse flattens them into an optimized Entity-Attribute-Value (EAV) Map schema, eliminating the need for constant ClickHouse `ALTER TABLE` migrations.

## 🚀 Deployment Modes

BeachHouse is designed using a clean architecture that allows it to run anywhere, from a laptop to a multi-region cloud.

1. **Standalone (OSS):** A single, statically compiled Go binary with zero external dependencies. It embeds its own Message Queue and Key-Value store. Perfect for local development or single-server deployments.
2. **Clustered (Managed/Enterprise):** Stateless API routers and independent Worker nodes backed by distributed Message Queues (NATS) and Caches (Redis). Designed for infinite horizontal scalability.

## 🛠️ Getting Started (Standalone)

The easiest way to see BeachHouse in action is using Docker Compose. This spins up a local ClickHouse instance alongside the BeachHouse standalone binary.

```bash
# Clone the repository
git clone [https://github.com/Wave-RF/BeachHouse.git](https://github.com/Wave-RF/BeachHouse.git)
cd BeachHouse

# Start ClickHouse and BeachHouse
docker-compose -f deployments/compose/standalone.yaml up -d
```

BeachHouse will now be accepting API requests on `http://localhost:8080`.

## 💻 Local Development

For building and testing BeachHouse locally, we use `air` for live-reloading.

1. Install [air](https://github.com/air-verse/air).
2. Start your local ClickHouse dependency: `docker-compose -f deployments/compose/dependencies.yaml up -d`
3. Run the development server:

   ```bash
    make dev
    ```

BeachHouse will automatically recompile and restart whenever you save a `.go` file.

## 🤝 Contributing

We welcome issues, pull requests, and feedback! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to structure your code and run the integration test suites.

## 📖 Documentation

* [Architecture](docs/architecture.md) — System design, data flows, and package overview
* [API Reference](docs/api.md) — All endpoints, authentication, request/response formats
* [Configuration](docs/configuration.md) — Full config reference (YAML + environment variables)
* [Deployment](docs/deployment.md) — Standalone, clustered, Docker, and release guide
* [Development](docs/development.md) — Building, testing, linting, and project structure

## 📜 License

BeachHouse is open source under the [MIT License](LICENSE).

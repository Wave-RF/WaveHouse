# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Multi-tenant JWT authentication with row-level security
- Asynchronous buffered ingestion via NATS JetStream
- Exact-once event deduplication (Pebble for standalone, ScyllaDB for clustered)
- Two-tier query caching (L1 Ristretto + L2 Redis) with singleflight
- Real-time streaming via SSE and WebSocket with replay buffer gap-fill
- Dynamic JSON schema flattening to ClickHouse Map(String, String)
- Standalone deployment mode (single binary, embedded components)
- Clustered deployment mode (separate API + worker binaries)
- Docker Compose configurations for standalone, clustered, and dependencies-only
- Multi-platform Docker images (distroless) via GoReleaser
- Liveness and readiness health check endpoints
- Comprehensive documentation (architecture, API, configuration, deployment, development)
- CI/CD workflows (lint, test, build, release)

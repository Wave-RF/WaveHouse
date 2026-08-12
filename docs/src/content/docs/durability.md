---
title: "Durability & Storage"
description: "What a WaveHouse ingest ack guarantees, why embedded JetStream fsyncs every publish, and how to tell whether your storage substrate can sustain it."
sidebar:
  order: 11
---

WaveHouse buffers ingested events in embedded NATS JetStream at `<data_dir>/nats` before the [ingest worker](/ingest-pipeline) drains them into ClickHouse. **WaveHouse runs JetStream in its strictest durability mode**: every publish is `fsync`'d to non-volatile storage before acknowledgment.

Ingest latency is a direct function of your storage's `fsync` latency. On managed cloud block storage, this is typically negligible; on some commodity or virtualized substrates, `fsync` tails can balloon into seconds, degrading ingest performance.

## The durability contract

The embedded server starts with `SyncAlways: true` (`internal/mq/embedded.go`).

> When a client receives `200` from `POST /v1/ingest`, the event has already been `fsync`'d to disk on the WaveHouse node.

Ingestion remains [asynchronous](/architecture): `200` means *durably buffered in JetStream*, not yet *written to ClickHouse*. The buffering step is hard-durable; an event receiving a `200` survives immediate, uncontrolled power loss. This is stronger than the default JetStream mode, where publishes are acked once they reach the OS page cache.

| Mode | Ack means | Crash exposure | Throughput |
| --- | --- | --- | --- |
| **`SyncAlways` (WaveHouse)** | data is `fsync`'d to disk | none for acked events | bounded by `fsync` latency |
| Periodic group commit (default) | data is in OS page cache | up to one sync interval | bounded by memory/CPU |

WaveHouse does not currently expose a knob to relax this; a configurable group-commit interval (`mq.sync_interval`) is tracked in [#139](https://github.com/Wave-RF/WaveHouse/issues/139).

## Why the fsync tail is your ingest floor

Because publishes block on `fsync`, typical and worst-case ingest latency mirror your storage's `fsync` performance. If the tail is unhealthy:

- Publishes block for the duration of the `fsync`.
- Slow substrates may cause JetStream client request timeouts, manifesting at boot as `create stream: ... context deadline exceeded`.
- If the worker cannot drain to ClickHouse faster than producers publish, the stream hits `mq.max_bytes_gb` and the API returns `503` ([backpressure by construction](/ingest-pipeline#backpressure-and-durability-knobs)).

## Where `SyncAlways` is cheap vs. expensive

**Healthy (effectively free):**

| Substrate | Why |
| --- | --- |
| Cloud block storage (gp3/io2 EBS, GCP pd-ssd, Azure Premium SSD) | Battery-backed cache acks sync writes from non-volatile DRAM. |
| Enterprise NVMe with PLP (Optane, Samsung PM, Solidigm D7) | PLP capacitor lets the controller ack a sync write from DRAM — `fsync` ≈ `memcpy`. |
| Local `ext4` on consumer NVMe | Single-device journal commit; 1–10 ms typical. |

**Problematic (measure before trusting):**

| Substrate | Failure mode |
| --- | --- |
| ZFS without SLOG, consumer NVMe | Sync writes hit the ZIL; gated by transaction-group commit cadence. **5–25 s under load**. |
| Loopback / qcow2 on `ext4` in VM | Double journaling layer; often 10× slower than direct `ext4`. |
| Spinning disks | Mechanical seek: multi-millisecond baseline, multi-second tail. |

Benchmark with multiple writers and check both guest and host if virtualized to surface commit-cadence problems.

## Check your storage before you trust it

Replicate JetStream's pattern—a 4 KiB write followed by a flush in a tight loop. Use [`fio`](https://fio.readthedocs.io/) on the volume backing `<data_dir>/nats` under representative load:

```bash
# 8 concurrent writers — the variant that surfaces commit-cadence problems
fio --name=jetstream-fsync --directory=/var/lib/wavehouse/nats \
    --rw=write --bs=4k --size=64M --fsync=1 --runtime=30 --time_based \
    --numjobs=8 --group_reporting
```

| p99 `fsync` | Verdict for `SyncAlways: true` |
| ---: | --- |
| < 1 ms | **Ideal** |
| 1–5 ms | **Good** |
| 5–50 ms | **Workable** — watch bursty load |
| 50 ms – 1 s | **Marginal** — relax durability via `mq.sync_interval` ([#139](https://github.com/Wave-RF/WaveHouse/issues/139)) or upgrade storage |
| > 1 s | **Broken** — `create stream` will time out; fix substrate |

:::caution[macOS `fsync` lies by default]
Plain `fsync()` on macOS returns once data is in volatile cache, not NAND; only `fcntl(fd, F_FULLFSYNC)` forces a flush. `fio` on macOS uses plain `fsync()`, making its numbers unreliable for tail-latency planning. Production WaveHouse runs on Linux, where `fio` is honest.
:::

A `wavehouse storage-check` preflight subcommand is tracked in [#84](https://github.com/Wave-RF/WaveHouse/issues/84).

## Symptoms of storage that can't keep up

Benchmark `<data_dir>/nats` if you observe:

- `create stream: ... context deadline exceeded` at startup.
- Ingest p99 latency or occasional `200` responses taking multiple seconds.
- Intermittent `503 Service Unavailable` from `/v1/ingest` while ClickHouse is healthy.
- Flaky CI/load tests that pass on fast storage but fail on shared/virtualized hosts.

## See also

- [Configuration → Message Queue (NATS)](/configuration#message-queue-nats) — `mq.*` knobs (`gap_window_minutes`, `max_bytes_gb`).
- [Deployment → Persistent Storage](/deployment#persistent-storage-required-for-containers) — `data_dir` requirements.
- [Ingest Pipeline → Backpressure and durability knobs](/ingest-pipeline#backpressure-and-durability-knobs) — worker-side ack costs.

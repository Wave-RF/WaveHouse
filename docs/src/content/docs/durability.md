---
title: "Durability & Storage"
description: "What a WaveHouse ingest ack guarantees, why embedded JetStream fsyncs every publish, and how to tell whether your storage substrate can sustain it."
cloudCta:
  body: "Finding out your disk cannot sustain an fsync per publish is the kind of lesson that arrives at peak traffic. WaveHouse Cloud runs on storage already benchmarked against this page, with the WAL sizing and retention handled for you."
sidebar:
  order: 11
---

WaveHouse buffers every ingested event in embedded NATS JetStream before the [ingest worker](/ingest-pipeline) drains it into ClickHouse. That buffer lives on disk at `<data_dir>/nats`, and **WaveHouse runs JetStream in its strictest durability mode**: every publish is `fsync`'d to non-volatile storage before the producer is acknowledged.

This is a deliberate, strong guarantee — but it makes your ingest latency a direct function of your storage's `fsync` latency. On managed cloud block storage that is effectively free; on some commodity or virtualized substrates the `fsync` tail balloons into seconds and ingest visibly suffers. This page explains the contract, where it is cheap versus expensive, and how to measure your storage before you trust it.

## The durability contract

The embedded server is started with `SyncAlways: true` (`internal/mq/embedded.go`). Concretely:

> When a client receives `200` from `POST /v1/ingest`, the event has already been `fsync`'d to disk on the WaveHouse node.

Ingestion is still [asynchronous](/architecture) end-to-end — the `200` means *durably buffered in JetStream*, not yet *written to ClickHouse* (the worker flushes to ClickHouse later, and the [worker's own ack](/ingest-pipeline#backpressure-and-durability-knobs) is what records "now in ClickHouse"). But the buffering step itself is hard-durable: an event that got a `200` survives an immediate, uncontrolled power loss on the node.

This is the strongest mode JetStream offers. It is stronger than the default, where a publish is acked once the write reaches the OS page cache and the data is flushed to disk later by a periodic background sync — fast, but a hard crash can lose the not-yet-flushed window.

| Mode | Ack means | Crash exposure | Throughput |
| --- | --- | --- | --- |
| **`SyncAlways` (WaveHouse today)** | data is `fsync`'d to disk | none for acked events | bounded by `fsync` latency |
| Periodic group commit (default JetStream) | data is in the OS page cache | up to one sync interval of acked-but-unflushed events | bounded by memory/CPU |

WaveHouse does not currently expose a knob to relax this — `SyncAlways` is always on. Exposing a configurable group-commit interval (`mq.sync_interval`) is tracked in [#139](https://github.com/Wave-RF/WaveHouse/issues/139).

## Why the fsync tail is your ingest floor

Because the publish blocks on `fsync`, **your typical ingest latency is your storage's typical `fsync` latency, and your worst-case publish is your storage's worst-case `fsync`.** When that tail is healthy (sub-millisecond to single-digit milliseconds) the guarantee is essentially free. When it is not, the same code path that handles every production message stalls:

- Publishes block for the duration of the `fsync`, so a multi-second `fsync` tail is a multi-second ingest tail.
- The embedded server's consumer setup at boot and every publish run under the JetStream client's request timeout, and opening or resizing a tenant's queue — and joining the consumers to one that opens while the server runs — under ten-second budgets of WaveHouse's own; a slow-enough substrate makes them exceed it. The symptom when a tenant's queue first opens — at the boot or reload that first serves the tenant — is `open dlq stream: ... context deadline exceeded`, or `open ingest stream: ...` (the two share the budget); a boot that finds every queue already at its budget writes nothing, so there the first publish is where it shows.
- If the worker cannot drain to ClickHouse faster than producers publish, a tenant's stream fills toward its [`mq.max_bytes_gb`](/settings-directory#message-queue) and the API returns `503` to that tenant ([backpressure by construction](/ingest-pipeline#backpressure-and-durability-knobs)).

## Where `SyncAlways` is cheap vs. expensive

The strict guarantee translates well to managed cloud infrastructure — the presumed production target — and to enterprise-grade local disks. It is the commodity and virtualized substrates that bite.

**Healthy — `SyncAlways` is effectively free:**

| Substrate | Why |
| --- | --- |
| Cloud block storage (gp3/io2 EBS, GCP pd-ssd, Azure Premium SSD) | Battery-backed cache acks sync writes from non-volatile DRAM, not NAND. Sub-millisecond at typical load. |
| Enterprise NVMe with power-loss protection (Optane, Samsung PM-series, Solidigm D7) | The PLP capacitor lets the controller ack a sync write from DRAM — the `fsync` ≈ `memcpy`. |
| Local `ext4` on consumer NVMe | Single-device journal commit, 1–10 ms typical. Tail spikes under heavy concurrent dirty data, but bounded. |

**Problematic — measure before you trust it:**

| Substrate | Failure mode |
| --- | --- |
| ZFS without a SLOG, consumer NVMe | Every sync write hits the ZIL, gated by transaction-group commit cadence that serializes across all pool consumers. Single-digit-ms idle, **5–25 s under concurrent load**. |
| Loopback / qcow2 on `ext4` inside a VM | Stacks a second journaling layer; often 10× slower than direct `ext4` and highly variable. |
| Spinning disks | Mechanical seek on the NAND-equivalent program path: multi-millisecond baseline, multi-second tail. |

The tell for a commit-cadence problem (ZFS-without-SLOG, noisy-neighbor VM host) is that a single-threaded benchmark looks fine while a concurrent one is far worse — so always benchmark with multiple writers, and benchmark the guest **and** the host if virtualized.

## Deduplication: one more fsync per window

With [deduplication](/settings-directory#deduplication) on, a `200` also means the records' ids were committed to the dedupe store, or, if that commit failed, that the failure was counted by `wavehouse_dedupe_commit_failed_total` and the ids lapse with their lease. On the embedded Pebble store that commit is an `fsync` of its own. It is taken once per window of up to 256 records of a request, after the window's publishes, rather than once per record: a 1,000-record batch costs four dedupe syncs, not a thousand. Measured with `BenchmarkIngest_DedupBatchOnPebble` on a developer laptop, with the queue stubbed out so only the dedupe store touched disk, the dedupe work for that batch took 24 ms windowed against 5.7 s one record at a time; the JetStream publishes' own fsyncs come on top. A single-record request still pays one sync for its publish and one for its commit.

A publish can also fail after JetStream stored the event (a timeout on the ack). The record's id is then left to lapse with its 30-second dedupe lease rather than given back, and every deduped record is published under an idempotency key derived from its tenant, table and id, which each tenant's ingest stream remembers for two minutes after the first publish. A retry after the lease but inside those two minutes is therefore dropped by the stream rather than stored twice; one later than that is stored again. The 30-second lease sits well inside those two minutes, so a prompt retry is covered.

## Check your storage before you trust it

Replicate JetStream's exact pattern — a 4 KiB write followed by a flush, in a tight loop — and report the percentiles. The numbers that matter are **p99** and **max**: those are your worst-case publish latency.

On Linux, [`fio`](https://fio.readthedocs.io/) (packaged on every distro) is the honest, standard tool. Point it at the volume that backs `<data_dir>/nats`, ideally before WaveHouse is running:

```bash
# 8 concurrent writers — the variant that surfaces commit-cadence problems
fio --name=jetstream-fsync --directory=/var/lib/wavehouse/nats \
    --rw=write --bs=4k --size=64M --fsync=1 --runtime=30 --time_based \
    --numjobs=8 --group_reporting
```

Run it under representative load, not on an idle box — idle benchmarks understate real-world tails.

Read the measured p99 against these bands, which track WaveHouse's `SyncAlways` default:

| p99 `fsync` | Verdict for `SyncAlways: true` |
| ---: | --- |
| < 1 ms | **Ideal** |
| 1–5 ms | **Good** |
| 5–50 ms | **Workable** — watch bursty load |
| 50 ms – 1 s | **Marginal** — relax durability once `mq.sync_interval` ([#139](https://github.com/Wave-RF/WaveHouse/issues/139)) lands, or move to faster storage |
| > 1 s | **Broken** — opening a tenant's queue (`open dlq stream` / `open ingest stream`) will time out under load; fix the storage substrate |

:::caution[macOS `fsync` lies by default]
A plain `fsync()` on macOS returns once data is in the drive's volatile cache — it does **not** force a flush to NAND; only `fcntl(fd, F_FULLFSYNC)` does (NATS, Postgres, and SQLite all use it). On a Mac, any per-flush number under ~1 ms is almost certainly not a real flush — the gap between plain `fsync()` and `F_FULLFSYNC` can be ~180× on the same consumer NVMe. `fio` on macOS calls plain `fsync()`, so don't trust Mac `fio` numbers for tail-latency planning. This mostly matters when benchmarking a dev machine; production WaveHouse runs on Linux, where `fio` is honest.
:::

A self-contained `wavehouse storage-check` preflight subcommand that bakes this measurement and verdict into the binary — including the per-platform honest flush — is tracked in [#84](https://github.com/Wave-RF/WaveHouse/issues/84).

## Symptoms of storage that can't keep up

If you see any of these, benchmark the `<data_dir>/nats` volume as above:

- `open dlq stream: ... context deadline exceeded`, or `open ingest stream: ...`, when a tenant's queue first opens, at the boot or reload that first serves the tenant.
- `ingest consumer delivery ended; ingestion has stopped` with `join its queue: ... context deadline exceeded`, and the process exiting, when a tenant's queue opens while the server runs and the ingest worker's consumer cannot join it in time; the stream hub's consumer failing the same way logs `a tenant's events do not reach this consumer until the next boot` instead.
- Ingest p99 latency in the seconds, or occasional `200`s that take multiple seconds to return.
- Intermittent `503 Service Unavailable` from `/v1/ingest` when ClickHouse is healthy (the worker can't drain fast enough because acking is `fsync`-bound).
- Flaky CI or load tests that pass on fast storage and fail on a shared/virtualized host.

## See also

- [Settings Directory → Message Queue](/settings-directory#message-queue) — `mq.max_bytes_gb`, each tenant's queue's disk budget (hot-reloadable); the SSE gap window inside it is [`stream.gap_window_minutes`](/settings-directory#streaming).
- [Deployment → Persistent Storage](/deployment#persistent-storage-required-for-containers) — `data_dir` must resolve to a host-backed volume.
- [Ingest Pipeline → Backpressure and durability knobs](/ingest-pipeline#backpressure-and-durability-knobs) — the worker-side ack cost and the in-flight backpressure layers.

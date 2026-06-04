<!-- UNTRACKED REVIEW DOC — not part of the site. Delete or convert to a GitHub issue once triaged.
     Drafted by Claude as a pre-launch checklist of every quantitative claim on the docs site,
     so the k6-vs-Tinybird benchmarking work can target each one. Nothing here is committed. -->

# Performance & Quantitative Claims — Pre-Launch Review

Every number the site states publicly, with its exact location and what would
make it defensible on Hacker News / Reddit. The skeptical-reader reflex on a
launch is *"benchmarked how?"* — so the goal is that each claim below is either
(a) backed by a reproducible benchmark, (b) clearly framed as illustrative, or
(c) softened. Grouped by priority.

Legend for **Action**: **BENCH** = needs a real measurement; **VERIFY** =
check against code/docs/vendor; **FRAME** = label as illustrative or soften
wording; **FIX** = internal inconsistency to reconcile.

---

## Priority 1 — Latency claims (the ones HN will attack first)

These are presented as performance characteristics and currently carry only
*"local bench"* / *"representative"* hedging. Either back them with a published,
reproducible benchmark (hardware + load profile + tool + raw p50/p99) or soften.

| Claim | Location | Asserts | Action |
| --- | --- | --- | --- |
| `< 100 ms` "warm-cache query path, local bench" | `docs/src/content/docs/index.mdx` (hero stat strip) | End-to-end warm query under 100ms | **BENCH** + **FIX** (see inconsistency note below) |
| `200 OK — ~2ms p50` | `why-wavehouse.md` ingest/broadcast diagram | Ingest ack p50 ~2ms | **BENCH** |
| `hit: ~0.5ms` (L1 cache) | `why-wavehouse.md` query-path diagram | Cache-hit serve ~0.5ms | **BENCH** |
| Latency budget table — 7 rows | `why-wavehouse.md` Part IV (`Typical p50` / `Typical p99`) | See per-row below | **BENCH** |

The Part IV table rows, spelled out (all labeled *"representative"*):

| Stage | Stated p50 | Stated p99 |
| --- | --- | --- |
| API auth + validation | `< 1 ms` | `~3 ms` |
| NATS JetStream publish | `~1 ms` | `~5 ms` |
| API `200 OK` to client | `~2 ms` | `~8 ms` |
| Hub broadcast to SSE subscriber | `~1 ms` | `~10 ms` |
| Batch flush to ClickHouse | `5 s` (configurable) | `5 s` + CH insert time |
| Query cache hit (L1) | `< 0.5 ms` | `~1 ms` |
| Query cache miss → ClickHouse | "depends" | "depends" |

**Recommendation:** publish a `BENCHMARKS.md` (or a `/benchmarks` docs page)
with the hardware, the exact load command, payload shape, concurrency, and raw
output. That turns the single biggest credibility liability into an asset — and
it's exactly what the k6-vs-Tinybird work will produce. Link the hero stat and
the Part IV table to it.

### Inconsistency to reconcile (FIX)

The hero flexes **`< 100 ms` for the "warm-cache query path"** while Part IV
says a cache **hit is `< 0.5 ms`**. A reader who sees both is confused, and
`< 100 ms` actively *under*-sells a sub-millisecond cache. Pick one: either lead
the hero with the real sub-ms number (with methodology), or drop the numeric
flex for a categorical claim. Don't ship both numbers for "the same" thing.

---

## Priority 2 — Throughput & scenario numbers

Mostly arithmetic or illustrative. They're fine to keep **if** the assumptions
are stated; HN will redo the math.

| Claim | Location | Notes | Action |
| --- | --- | --- | --- |
| `~10k tiny parts per second` (10k clients, 1 event each) | `why-wavehouse.md` diagram | Illustrative worst case | **FRAME** |
| `flush every 5s` | `why-wavehouse.md` diagram | Must match the real default flush interval | **VERIFY** |
| `10M events/day → ~115 events/sec → ~17k bulk inserts/day` | `why-wavehouse.md` "scenario in dollars" | Math checks (10M/86400 ≈ 115; 86400/5s ≈ 17,280 flushes) **if** ~1 batch/5s | **FRAME** (state batch size + flush interval) |
| `3,000 queries/min, ~95% return 0 rows` (100 viewers, 2s poll) | `why-wavehouse.md` polling diagram | 100 × 60/2 = 3,000 ✓; the 95% is an assumption | **FRAME** |
| `50 dashboards → up to 50 identical queries` | `why-wavehouse.md` thundering-herd diagram | Logical upper bound | OK |
| `two engineers ~30% of their time` / `one full-time engineer of drag` | `why-wavehouse.md` DIY scenario | Clearly a scenario already | **FRAME** (keep "scenario" framing) |

---

## Priority 3 — Factual ClickHouse claims (verify against current CH)

These are statements about ClickHouse itself; a ClickHouse-savvy commenter will
fact-check them. They're already hedged, but confirm the specifics the week of
launch.

| Claim | Location | Action |
| --- | --- | --- |
| "insert batches of 1,000–100,000 rows … no more often than once per second" | `why-wavehouse.md` (cited to CH docs) | **VERIFY** the citation + link still current |
| `parts_to_delay_insert` / `parts_to_throw_insert` "historical defaults around 1,000 and 3,000" | `why-wavehouse.md` | **VERIFY** against current MergeTree defaults (already hedged "version-dependent") |
| Error text: `Too many parts (N). Merges are processing significantly slower than inserts` | `why-wavehouse.md` | **VERIFY** exact string against a current CH version |

---

## Priority 4 — Config-default claims (must match the code)

These appear as concrete defaults; if they drift from the actual config, an
early user files a "docs wrong" issue on day one. Confirm each against the
config loader / constants.

| Claim | Location | Action |
| --- | --- | --- |
| Schema refresh "every 60 seconds by default" | `getting-started.md`, `architecture.md` (Active Sweeper "every 60s") | **VERIFY** |
| Batch flush "~5 seconds" | `getting-started.md`, `why-wavehouse.md` | **VERIFY** default matches |
| `cache.l1_max_cost` = `67108864` (~64 MB) | `configuration.md` | **VERIFY** |
| `clickhouse.query_timeout` = `30s` | `configuration.md` | **VERIFY** |
| SDK `DEFAULT_LIMIT` 1000; server `DefaultMaxRows` 10,000 | `sdk.md` | **VERIFY** |
| Access-control example "capped at 1000 rows / 5s" | `access-control.md` | **VERIFY** consistent with the limits above |
| Schema-discovery backoff `2s → 60s` | `api.md`, `deployment.md` | **VERIFY** |

---

## Priority 5 — Tinybird comparison (verify current + keep scrupulously fair)

Every "X vs Y" launch gets fact-checked by partisans of Y. One unfair row gets
the whole post dismissed. Re-verify the week of launch and add an "as of
<date>" stamp + a link to Tinybird's pricing.

| Claim | Location | Action |
| --- | --- | --- |
| "managed tiers: Developer **$49/mo** → Enterprise custom" | `why-wavehouse.md` | **VERIFY** current pricing |
| "egress fees for cross-region" | `why-wavehouse.md` | **VERIFY** still accurate |
| Real-time push: Tinybird = "Pipe endpoints (request/response)" / matrix `✗` | `why-wavehouse.md` | **VERIFY** — does Tinybird ship any streaming/SSE/push today? If so, the `✗` is a strawman |
| Every other `✗`/`✓`/"Partial"/"Custom" in both matrices | `why-wavehouse.md` Parts II & III | **VERIFY** each cell against current Tinybird + DIY reality |

This is the natural home for the **k6-vs-Tinybird** numbers: a like-for-like
ingest + query benchmark (same hardware class, same dataset) is the most
credible possible artifact and pre-empts "but did you actually compare?"

---

## Cross-reference — artifact existence (Tier-0 launch blockers)

Not perf, but the site/README make these promises and they must resolve the
moment you post (you confirmed these are on the launch checklist — listed here
only so the benchmark scripts target the real artifacts):

- `ghcr.io/wave-rf/wavehouse:latest` and `:dev` — published + public.
- `go install github.com/Wave-RF/WaveHouse/cmd/wavehouse@latest` / `@v0.1.0` — module public, `v0.1.0` tag exists.
- `@wavehouse/sdk` — published on npm.
- Repo public; `wavehouse.dev` live and matching the `site:` config.
- The hero shows `docker run … ghcr.io/wave-rf/wavehouse` while getting-started shows `git clone … && docker compose …` — pick one canonical "fastest path" and make sure that exact artifact works.

---

## Suggested benchmark shape (for the k6 issue)

A minimal, reproducible setup that backs the Priority 1 + Priority 5 claims:

```text
Hardware:    <pin a concrete instance type, e.g. a specific cloud VM / the NAS>
ClickHouse:  <version>, single node, same box vs. separate box (state which)
Dataset:     <row shape, e.g. the clicks table; N columns; payload size in bytes>
Ingest test: k6 ramp to <X> events/sec across <Y> VUs; record ack p50/p95/p99
Query test:  k6 constant-arrival on a warm cache vs cold; record p50/p99 + hit ratio
Stream test: <N> concurrent SSE subscribers; time-to-first-event after ingest
Compare:     same dataset + same load against Tinybird; publish both side by side
Repro:       commit the k6 scripts + a one-command `make bench`; link from BENCHMARKS.md
```

Output → `BENCHMARKS.md` (or `/benchmarks`), linked from the hero stat and the
Part IV latency table. Replace every `~` / `<` hedge that survives with a real
number or an explicit "indicative, not benchmarked" note.

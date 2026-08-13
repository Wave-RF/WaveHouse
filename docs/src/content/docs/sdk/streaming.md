---
title: "SDK Streaming & Live Queries"
description: "Real-time SSE streams, client-side filtering, and backfill-then-live queries in @wavehouse/sdk."
---

Real-time consumption with `@wavehouse/sdk`: SSE event streams from tables,
builders, and pipes, plus live queries that backfill history before going
live. Builders and table refs come from [Queries](/sdk/queries).
Examples import from `@wavehouse/sdk`; using the CDN instead, import from
`https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

## Streaming

Streams are Server-Sent Events over `fetch`. The `auth` token rides in an
`Authorization: Bearer` header — the same as every other request, on browsers
and servers alike — and is re-read on each connection attempt, so a stream that
outlives its token picks up a fresh one. Unauthenticated streams work the same
way, minus the header.

### `StreamController`

Returned by `.stream()` on `TableRef`, `QueryBuilder`, `PipeRef`, and `DLQNamespace` (the DLQ variant is not yet functional server-side — [#197](https://github.com/Wave-RF/WaveHouse/issues/197)). It is **NOT thenable**.

```ts
const stream = wh.from('clicks').stream({ since: '2026-01-01T00:00:00Z' });
```

### `.subscribe(subscriber)` → `unsubscribe()`

Callback-based consumption. Returns a cleanup function.

```ts
const unsub = stream.subscribe({
  next: (event) => {
    // event: { table: 'clicks', timestamp: '2026-...', data: { page: '/', ... } }
    console.log('New event:', event.data);
  },
  status: (state) => {
    // state: 'connecting' | 'live' | 'reconnecting' | 'closed'
    updateIndicator(state);
  },
  error: (err) => {
    console.error('Stream error:', err.message);
  },
});

// Cleanup — closes the connection if no other subscribers remain
unsub();
```

A handler that throws *during delivery* doesn't end the stream; the exception is
logged and the connection keeps running. But delivery of that event stops at the
handler that threw — your *later* subscribers, and any concurrent `for await`,
do not get it. The first `status` call, the synchronous one `.subscribe()` makes
before returning, isn't caught at all and throws back out at you. Wrap your
handler bodies in your own `try`/`catch`; see
[Error Handling](/sdk/reference#error-handling) for all four carve-outs.

### Async Iterator

```ts
const stream = wh.from('clicks').stream();

for await (const event of stream) {
  console.log(event.table, event.data);
  if (shouldStop) break; // breaking auto-closes the stream
}
```

### `.close()`

Explicitly close the stream and release all resources.

```ts
stream.close();
```

### `.status`

Current connection status: `'connecting' | 'live' | 'reconnecting' | 'closed'`.

### `.connected(timeoutMs?)` → `Promise<void>`

Resolves once the stream reaches `live`. Rejects if it is already `closed`, if
it closes before connecting, or after `timeoutMs` (default `5000`). Useful when
you need the subscription established before doing something that depends on it
— inserting a row you expect to see come back, for instance.

```ts
const stream = wh.from('clicks').stream();
const unsub = stream.subscribe({ next: (e) => console.log(e) });
await stream.connected();          // wait until the transport is live
await wh.from('clicks').insert({ page: '/home' });
```

A *timeout* rejection does **not** stop the transport — reconnection is
unbounded, so it means "not live yet", not "given up"; call `.close()` if you
want it to stop. A rejection because the stream closed is different: there the
transport has already stopped.

One way this rejects against a perfectly healthy stream: if a subscriber you
registered *before* calling `.connected()` has a `status` handler that throws,
the throw aborts the fan-out before `connected()`'s internal watcher sees `live`,
so it times out while `.status` already reads `live`. See
[Error Handling](/sdk/reference#error-handling).

### `StreamOptions`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `since` | `string` | RFC3339 timestamp for gap-fill replay |
| `signal` | `AbortSignal` | Cancel/close the stream. Wired via `attachSignal()` internally. |

### `StreamEvent<T>`

```ts
interface StreamEvent<T> {
  table: string;     // table name (e.g. 'clicks')
  timestamp: string; // received_timestamp (RFC3339Nano)
  data: T;           // row data
}
```

Row values of top-level `DateTime`/`DateTime64` columns inside `data` (not
timestamps nested in `Array`/`Map`/`Tuple` columns) arrive in canonical RFC 3339
UTC (`2026-06-21T04:00:00.123Z`), matching what `/v1/query` returns for the
same row — `new Date(value)` parses correctly with no zone fix-up.
Values WaveHouse couldn't canonicalize (ingest is fail-open) stream in the
producer's original spelling, and the `/v1/query` match doesn't hold for them:
a spelling ClickHouse accepted anyway still queries back in canonical UTC (one
it rejected never lands in the table at all), and a zone-less date-time is
what `new Date()` reads as *local* time — though a date-only `YYYY-MM-DD`
string is read as UTC, an ECMAScript quirk
(see [Timestamp canonicalization](/api#timestamp-canonicalization)).

### Transport Behavior

| Transport | Reconnect | Protocol |
| --------- | --------- | -------- |
| SSE over `fetch` | Automatic, jittered backoff, resumes via `Last-Event-ID` | HTTP/2 recommended |

:::note[SSE connection limit]
The SDK warns above 5 concurrent SSE connections — just under the browser's 6-per-domain limit for HTTP/1.1. HTTP/2 multiplexes over one connection, so the limit doesn't apply there.
:::

A dropped stream reconnects on a jittered exponential backoff, capped at 30s, and
resumes with `Last-Event-ID` so the server gap-fills what was missed. The
schedule resets only after a connection has *held* for a few seconds, so a
server accepting and immediately closing — slow-consumer eviction, a half-broken
upstream — can't pin the client at sub-second retries. Reconnection is
**unbounded**: a stream keeps re-dialing until it hits a terminal error or you
`close()` it. `options.maxRetries` bounds REST requests only and is never
consulted here.

:::caution[Resumption is at-least-once, and time-bounded]
Delivery across a reconnect is **at-least-once**. The `Last-Event-ID` the client
sends is the last event's `received_timestamp`, and the server replays from that
instant *inclusively* — so the last event you already saw, and anything sharing
its timestamp, arrives again. The SDK does not deduplicate live frames —
`liveQuery()` makes one pass at the backfill seam, and only under an ascending
order ([#449](https://github.com/Wave-RF/WaveHouse/issues/449)) — so key on
`timestamp` plus your
own row identity if duplicates matter.

Replay is also bounded by the server's
[`mq.gap_window_minutes`](/configuration#message-queue-nats) — 15 minutes by default.
A drop longer than that resumes with a hole and no signal, because the purged
messages are simply gone.
:::

A `4xx` is terminal and surfaces through `error` with the real status code
rather than an opaque connection failure. It won't be an *authentication*
rejection from WaveHouse, which leaves `/v1/stream` ungated and answers an
expired token with a filtered view rather than a `401`; the only 4xx it raises
itself is `400` for a missing or empty table name. Anything else means something
in front of it (an auth gateway, a proxy) turned the request away — and note the
exception to "retrying wouldn't help": a `429` or `408` from a rate limiter *is*
transient, but the stream still ends, so catch it and open a new one after a
delay ([#469](https://github.com/Wave-RF/WaveHouse/issues/469)). See
[Error Handling](/sdk/reference#error-handling) for every code a stream can
report and which ones re-dial.

Streams go through `options.fetch`, `options.headers`, and
`options.fetchOptions` like every other request — which is what lets a stream
reach a header-gated origin. A custom `fetch` is asked more of on this
path; see [Supplying your own fetch](/sdk#supplying-your-own-fetch).

### Client-Side Stream Filtering

When a `QueryBuilder` with `.where()` filters or `.select()` columns calls `.stream()`, the returned stream applies those filters client-side:

```ts
const stream = wh.from('clicks')
  .select('page', 'button')
  .where('page', '=', '/home')
  .stream();

// Only events where page === '/home' are emitted, with only page + button columns
```

Supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `like`, `not_like` — the same `FilterOp` set `.where()` takes everywhere (the SDK maps them to wire tokens such as `eq`/`neq` internally).

---

## Live Queries

Live queries combine a historical backfill (`.fetch()`) with a real-time stream, providing a seamless initial load + live updates experience.

```ts
const lq = wh.from('clicks')
  .selectAll()
  .where('page', '=', '/home')
  .orderBy('received_timestamp', 'desc')
  .limit(100)
  .liveQuery({
    initial: (result) => {
      // Called once with the historical data
      setRows(result.data ?? []);
    },
    next: (event) => {
      // Called for each live event after backfill
      addRow(event.data);
    },
    error: (err) => console.error(err),
  });

// Cleanup
lq.close();
```

### `StreamSubscriber<T>`

```ts
interface StreamSubscriber<T> {
  initial?: (result: Result<T[]>) => void; // Historical backfill result
  next: (event: StreamEvent<T>) => void;    // Live events
  status?: (state: StreamStatus) => void;   // Connection state changes
  error?: (err: WaveHouseError) => void;    // Errors
}
```

### How it works

1. Opens the stream **immediately** and buffers incoming events.
2. Runs the `.fetch()` query for historical data, calls `subscriber.initial()` with the result — unless the fetch itself throws, in which case neither happens (see below).
3. Deduplicates buffered events against the **last row** of the backfill result — which is the newest only when the query orders ascending. A `desc` query (like the example above) puts the oldest row last, so for live frames the boundary is the oldest timestamp and this step filters nothing. With a `since` gap-fill in flight it works the other way — replay lands in the same buffer, so replayed events at or older than that row are dropped before reaching `next()`. A projection that omits `received_timestamp` skips the pass entirely. Tracked in [#449](https://github.com/Wave-RF/WaveHouse/issues/449).
4. Flushes remaining buffered events and switches to live mode.

This "stream-first" approach is what closes the window between the fetch and the
stream starting — events arriving during the fetch are buffered rather than
missed.

It holds only when the backfill completes cleanly. The buffer is discarded, with
no log and no `error` callback, if the backfill query returns an error `Result`,
if the fetch itself throws, or if your `initial()` or `next()` throws during the
flush — so a handler that throws mid-flush costs you the remaining buffered
events. Check `result.error` inside `initial()`, and keep the flush handlers
total.

The fetch-throws case is the one you cannot catch that way, because `initial()`
is never called at all — the throw happens inside step 2, before the callback is
reached, so there is no `Result` to inspect. A rejecting `auth` callback is the
case to watch: the backfill vanishes while the stream, which treats the same
rejection as transient, keeps re-dialing — so you get live events with no
snapshot and no signal that the backfill was what failed. A relative `baseURL`
throws there too, but it also ends the stream with a terminal
`SSE_CONNECT_ERROR`, so that half at least announces itself. Tracked in
[#473](https://github.com/Wave-RF/WaveHouse/issues/473).

A `status` handler that throws *on the first, synchronous call* is a separate
and worse case — it escapes `liveQuery()` before you get a handle back. Throwing
on any later transition is isolated and logged like `next`. See
[Error Handling](/sdk/reference#error-handling).

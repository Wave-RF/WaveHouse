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
<!-- | TBD | Automatic (retries?) | HTTP/2 recommended | -->
<!-- TODO: Fill in above ^ for SSE fallback, likely polling? -->

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

A `4xx` is terminal and surfaces through `error` with the real status code
rather than an opaque connection failure. It won't be an *authentication*
rejection from WaveHouse, which leaves `/v1/stream` ungated and answers an
expired token with a filtered view rather than a `401`; the only 4xx it raises
itself is `400` for a missing or empty table name. Anything else means something
in front of it (an auth gateway, a proxy) turned the request away. See
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
2. Runs the `.fetch()` query for historical data, calls `subscriber.initial()` with the result.
3. Deduplicates buffered events by comparing timestamps against the latest historical timestamp.
4. Flushes remaining buffered events and switches to live mode.

This "stream-first" approach ensures no events are lost between the fetch and stream start.

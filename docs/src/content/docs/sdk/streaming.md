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

Streams use SSE (Server-Sent Events) for both unauthenticated connections and for authenticated ones.

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

### `.connected(timeoutMs?)`

Returns a promise that resolves once `.status` reaches `'live'` — the "safe to ingest now" barrier, so tests and scripts don't need sleep loops between opening a stream and inserting the rows they expect to see. Rejects if the stream is already `'closed'`, or after `timeoutMs` milliseconds (default `5_000`) if it never connects. Safe to call before `.subscribe()`.

```ts
const stream = wh.from('clicks').stream();
const unsub = stream.subscribe({ next: (e) => console.log(e) });
await stream.connected(); // transport is live
await wh.from('clicks').insert({ page: '/home', button: 'signup' }); // this row will arrive on the stream
```

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

### Transport Behavior

| Transport | Reconnect | Protocol |
| --------- | --------- | -------- |
| SSE | Automatic (native `EventSource` with `Last-Event-ID`) | HTTP/2 recommended |
<!-- | TBD | Automatic (retries?) | HTTP/2 recommended | -->
<!-- TODO: Fill in above ^ for SSE fallback, likely polling? -->

:::note[SSE connection limit]
The SDK warns when more than 5 concurrent SSE connections are open (browser limit per domain).
:::

### Server-Side Policy Filtering

Access-control policy applies on the server before anything reaches the client: tables the connection's role can't `select` are skipped, denied columns are stripped from each event, and the role's row-level `filter` is evaluated per subscriber against the connection's JWT claims. A stream on a row-policied table therefore delivers only the rows the policy admits for that connection — and, where the server's in-memory comparison can't prove a match, fewer; see [Access control — where each rule is enforced](/access-control#where-each-rule-is-enforced).

### Client-Side Stream Filtering

On top of that, when a `QueryBuilder` with `.where()` filters or `.select()` columns calls `.stream()`, the returned stream applies those filters client-side:

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
  status?: (state: string) => void;         // Connection state changes
  error?: (err: WaveHouseError) => void;    // Errors
}
```

### How it works

1. Opens the stream **immediately** and buffers incoming events.
2. Runs the `.fetch()` query for historical data, calls `subscriber.initial()` with the result.
3. Deduplicates buffered events by comparing timestamps against the latest historical timestamp.
4. Flushes remaining buffered events and switches to live mode.

This "stream-first" approach ensures no events are lost between the fetch and stream start.

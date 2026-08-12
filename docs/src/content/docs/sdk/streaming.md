---
title: "SDK Streaming & Live Queries"
description: "Real-time SSE streams, client-side filtering, and backfill-then-live queries in @wavehouse/sdk."
---

Real-time consumption with `@wavehouse/sdk`: SSE event streams from tables, builders, and pipes, plus live queries that backfill history before going live. Builders and table refs come from [Queries](/sdk/queries). Import from `@wavehouse/sdk` or the CDN `https://esm.sh/@wavehouse/sdk` ([Imports & Runtimes](/sdk#imports--runtimes)).

## Streaming

Streams use SSE for both authenticated and unauthenticated connections.

### `StreamController`

Returned by `.stream()` on `TableRef`, `QueryBuilder`, `PipeRef`, and `DLQNamespace` (DLQ is not yet functional server-side — [#197](https://github.com/Wave-RF/WaveHouse/issues/197)). It is **NOT thenable**.

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

Close the stream and release all resources.

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

Top-level `DateTime`/`DateTime64` columns in `data` (not timestamps nested in `Array`/`Map`/`Tuple`) arrive in canonical RFC 3339 UTC (`2026-06-21T04:00:00.123Z`), matching `/v1/query`. Non-canonicalized values keep the producer's spelling; `new Date()` reads zone-less date-times as local, `YYYY-MM-DD` as UTC ([Timestamp canonicalization](/api#timestamp-canonicalization)).

### Transport Behavior

| Transport | Reconnect | Protocol |
| --------- | --------- | -------- |
| SSE | Automatic (native `EventSource` with `Last-Event-ID`) | HTTP/2 recommended |

:::note[SSE connection limit]
The SDK warns when more than 5 concurrent SSE connections are open (browser limit per domain).
:::

### Client-Side Stream Filtering

`QueryBuilder` `.stream()` applies `.where()`/`.select()` filters client-side:

```ts
const stream = wh.from('clicks')
  .select('page', 'button')
  .where('page', '=', '/home')
  .stream();

// Only events where page === '/home' are emitted, with only page + button columns
```

Supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `like`, `not_like` — the `FilterOp` set `.where()` takes everywhere (mapped to wire tokens `eq`/`neq`).

---

## Live Queries

Live queries combine a historical backfill (`.fetch()`) with a live stream: initial load plus live updates.

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

1. Opens the stream immediately and buffers incoming events.
2. Runs `.fetch()` for historical data, calling `subscriber.initial()`.
3. Deduplicates buffered events against the latest historical timestamp.
4. Flushes remaining buffered events and switches to live mode.

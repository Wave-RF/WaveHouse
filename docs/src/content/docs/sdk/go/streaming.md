---
title: "Go SDK Streaming & Live Queries"
description: "Real-time SSE streams, client-side filtering, and backfill-then-live queries in the WaveHouse Go SDK."
---

Real-time consumption with `github.com/Wave-RF/WaveHouse/clients/go`: SSE
event streams from tables, builders, and pipes, plus live queries that
backfill history before going live. Builders and table refs come from
[Queries](/sdk/go/queries). Compare with the TypeScript SDK's
[Streaming & Live Queries](/sdk/streaming) page — the two implement the same
protocol and mostly the same client-side filtering, but connection lifecycle
differs: Go streams are goroutine-backed and closed explicitly, not tied to
a `context.Context` or a browser's `EventSource`.

## Streaming

Streams use SSE (Server-Sent Events) parsed via `net/http` with zero runtime dependencies.

### `*StreamController`

Returned by `.Stream(opts)` on `*TableRef`, `*QueryBuilder`, `*PipeRef`, and `*DLQNamespace` (DLQ is not yet functional server-side — [#197](https://github.com/Wave-RF/WaveHouse/issues/197)). Calling `.Stream` returns immediately; the connection opens in a background goroutine.

```go
stream := wh.From("clicks").Stream(&wavehouse.StreamOptions{
    Since: "2026-01-01T00:00:00Z",
})
defer stream.Close()
```

### `.Subscribe(sub) → func()`

Callback-based consumption. Returns an unsubscribe function. The `Status` callback fires immediately with the current status.

```go
unsub := stream.Subscribe(&wavehouse.StreamSubscriber{
    Next: func(e wavehouse.StreamEvent) {
        // e: {Table: "clicks", Timestamp: "2026-...", Data: map[string]any{"page": "/", ...}}
        fmt.Println("New event:", e.Data)
    },
    Status: func(s wavehouse.StreamStatus) {
        // s: StatusConnecting | StatusLive | StatusReconnecting | StatusClosed
        updateIndicator(s)
    },
    Error: func(err error) {
        fmt.Println("Stream error:", err)
    },
})

// Cleanup — removes this subscriber; the connection stays open for any
// others and must still be closed with stream.Close() when you're done
// with the stream itself.
defer unsub()
```

Cleanup via `unsub()` removes the subscriber; the connection remains open for others and must be closed with `stream.Close()`.

### Channel-based consumption — `.Events()`

A read-only channel, closed automatically when the stream shuts down.

```go
stream := wh.From("clicks").Stream(nil)
defer stream.Close()

for event := range stream.Events() {
    fmt.Println(event.Table, event.Data)
    if shouldStop {
        break
    }
}
```

:::caution[`break` does not close the stream]
Unlike the TypeScript SDK's async iterator, where breaking a `for await` loop closes the connection, breaking a Go `for range stream.Events()` loop only stops consumption — the background goroutine and HTTP connection persist. Always `defer stream.Close()`.
:::

The channel is buffered (256 events). A slow consumer makes the SDK **drop** new events for that channel rather than block the read loop. The first drop logs via `log`; later drops are silent (`.Subscribe` callbacks fire regardless).

### `.Close()`

Explicitly closes the stream and releases resources. Non-blocking and safe to call from inside a subscriber callback.

```go
stream.Close()
```

### `.Status()`

Returns the current `StreamStatus`.

```go
status := stream.Status()
```

### `.Connected(ctx)`

Blocks until the stream reaches `StatusLive` or `ctx` is canceled; returns an error if the stream closes before connecting. Useful for ensuring a stream is live (e.g., in tests).

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := stream.Connected(ctx); err != nil {
    log.Fatal(err)
}
```

### `StreamOptions`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `Since` | `string` | RFC3339 timestamp for gap-fill replay |

There's no `Signal`/context field: a stream isn't canceled by passing a `context.Context` into `.Stream()` — call `.Close()` instead.

### `StreamEvent`

```go
type StreamEvent struct {
    Table     string         // table name (e.g. "clicks")
    Timestamp string         // received_timestamp (RFC3339Nano)
    Data      map[string]any // row data
}
```

:::note[`Events()` carries events only]
`Error` and `Status` are delivered exclusively via `.Subscribe(...)`. The channel closes on terminal errors (401/403/404). Pair `Events()` with a subscriber to determine why a stream ended.
:::

:::note[The channel buffers from stream construction]
Events buffer (up to 256) starting at `.Stream()`; events arriving before the first `Events()` call are not lost.
:::

### Transport Behavior

| Transport | Reconnect | Protocol |
| --------- | --------- | -------- |
| SSE | Automatic, exponential backoff (max 30s), gap-fill replay via last event ID | HTTP/2 recommended |

Reconnect covers transport failures and retryable (5xx/429) responses. Non-retryable ones (401/403/404) are terminal: the `Error` callback fires, status goes `StatusClosed`, no reconnect. `Auth` provider errors during (re)connect are retryable (`SSE_ERROR`) and reconnects continue — `ClientOptions.MaxRetries` bounds request retries only, not stream reconnects — so call `.Close()` if the provider fails permanently.

Auth goes as an `Authorization: Bearer` header on every connection ([note in Getting Started](/sdk/go#creating-a-client)). Browser `EventSource` limits don't apply.

### Client-Side Stream Filtering

When a `*QueryBuilder` with `.Where()` or `.Select()` calls `.Stream()`, filters are applied client-side:

```go
stream := wh.From("clicks").
    Select("page", "button").
    Where("page", wavehouse.OpEq, "/home").
    Stream(nil)

// Only events where page == "/home" are emitted, with only page + button fields
```

Supported operators: `OpEq`, `OpNeq`, `OpGt`, `OpGte`, `OpLt`, `OpLte`, `OpIn`, `OpLike`, `OpNotLike` — the `FilterOp` set `.Where()` takes everywhere (mapped to wire tokens `eq`/`neq`). `OpLike`/`OpNotLike` use SQL LIKE semantics (`%`, `_`), case-insensitively. `OpIn` accepts any Go slice type (e.g., `[]string`, `[]int`).

## Live Queries

Live queries combine a historical backfill (`.FetchUntyped`) with a real-time stream for seamless initial loads and updates. They are available only on `*QueryBuilder` (no `TableRef.LiveQuery` shortcut), matching the TypeScript SDK.

```go
lq := wh.From("clicks").
    SelectAll().
    Where("page", wavehouse.OpEq, "/home").
    OrderBy("received_timestamp", "desc").
    Limit(100).
    LiveQuery(&wavehouse.StreamSubscriber{
        Initial: func(rows []map[string]any, err error) {
            // Called once with the historical backfill.
            setRows(rows)
        },
        Next: func(e wavehouse.StreamEvent) {
            // Called for each live event after backfill.
            addRow(e.Data)
        },
        Error: func(err error) {
            log.Println(err)
        },
    }, nil)

// Cleanup
defer lq.Close()
```

### `StreamSubscriber`

```go
type StreamSubscriber struct {
    // Initial is called once with historical backfill data (live queries only).
    Initial func(rows []map[string]any, err error)
    // Next is called for each live event.
    Next func(event StreamEvent)
    // Status is called when the connection status changes.
    Status func(status StreamStatus)
    // Error is called on stream errors.
    Error func(err error)
}
```

:::note[`Initial` is always untyped]
Unlike the TypeScript SDK's `initial: (result: Result<T[]>) => void`, Go's `LiveQuery` takes no type parameter: `Initial` always receives `[]map[string]any` plus a plain `error`, even if you'd use `wavehouse.FetchTyped[Row]` for the same query outside a live query. Decode inside the callback if needed.
:::

### How it works

1. Subscribes to the stream immediately and buffers events.
2. Runs `.FetchUntyped(ctx)` for historical data, then calls `sub.Initial(rows, err)`.
3. Deduplicates buffered events against the maximum `received_timestamp` in the backfill (not necessarily the last row).
4. Flushes remaining buffered events and switches to live mode.

This "stream-first" approach prevents event loss between fetch and stream start.

:::caution[Dedup needs `received_timestamp` in the projection]
Dedup relies on `received_timestamp`. `.SelectAll()` (or no projection) includes it; a `.Select(...)` omitting it disables dedup, causing events in the overlap window to be delivered twice (via `Initial` and `Next`).
:::

:::caution[`OpLike` matching differs between backfill and live]
Client-side `OpLike` is case-insensitive, but server-side backfills use ClickHouse `LIKE`, which is case-sensitive. Consequently, a live query filtering on `OpLike` may exclude rows in the backfill that it includes in the live stream ([#451](https://github.com/Wave-RF/WaveHouse/issues/451)).

`OpNotLike` is rejected by `/v1/query` with a `400`, causing `Initial` callbacks to fail. See [Queries](/sdk/go/queries#wherecolumn-op-value).
:::

### `.Close()`

Shuts down the live query and its underlying stream. Safe to call more than once (idempotent via `sync.Once`).

```go
lq.Close()
```

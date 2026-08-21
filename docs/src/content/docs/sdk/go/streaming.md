---
title: "Go SDK Streaming & Live Queries"
description: "Real-time SSE streams, client-side filtering, and backfill-then-live queries in the WaveHouse Go SDK."
---

Real-time consumption with `github.com/Wave-RF/WaveHouse/clients/go`: SSE event streams from tables, builders, and pipes, plus live queries that backfill history before going live. Builders and table refs come from [Queries](/sdk/go/queries). Compare with the TypeScript SDK's [Streaming & Live Queries](/sdk/streaming) page — the two implement the same protocol and mostly the same client-side filtering, but connection lifecycle differs: Go streams are goroutine-backed and closed explicitly, not tied to a `context.Context` or a browser's `EventSource`.

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

Top-level `DateTime`/`DateTime64` values inside `Data` arrive in canonical RFC 3339 UTC, byte-identical to what `/v1/query` renders for the same stored value — the ingest handler rewrites them before publishing, so a live frame and a later query can't disagree on the spelling of an instant. Two consequences worth knowing: a value you sent as `2026-06-21T06:00:00.123+02:00` comes back as `2026-06-21T04:00:00.123Z` (same instant, different spelling), and the canonicalization is deliberately fail-open — a value the server can't parse, or one whose zone it can't resolve, is published verbatim. See [Timestamp canonicalization](/api#timestamp-canonicalization).

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

Reconnect covers transport failures and retryable responses (5xx/429, plus `SSE_AUTH_ERROR`, `SSE_PARSE_ERROR`, and `SSE_READ_ERROR`). Terminal failures fire the `Error` callback, set status `StatusClosed`, and stop: non-retryable HTTP statuses, `SSE_CONNECT_ERROR` (bad `BaseURL`), `SSE_REDIRECT` (a credentialed request was redirected), and `SSE_BAD_CONTENT_TYPE` (a `200` that wasn't an event stream). Every error reaches the callback as a `*wavehouse.Error`, so `errors.As` and `wavehouse.IsRetryable` work on all of them — see the [error-code table](/sdk/go/reference#error-handling).

Note that `/v1/stream` is not admin-gated, so WaveHouse itself never answers a stream with `401`. A `401` on a stream came from something in front of it. `Auth` provider errors during (re)connect are retryable (`SSE_ERROR`) and reconnects continue — `ClientOptions.MaxRetries` bounds request retries only, not stream reconnects — so call `.Close()` if the provider fails permanently.

Auth goes as an `Authorization: Bearer` header on every connection, re-read from `Auth` per attempt ([note in Getting Started](/sdk/go#creating-a-client)). The TypeScript SDK streams over `fetch` and authenticates the same way, so this is shared behavior rather than a Go-only property — what Go avoids is the browser's per-domain connection ceiling, not a different auth mechanism.

Delivery across a reconnect is **at-least-once**: the server replays from the last event ID *inclusively*, so the first frame after a gap-fill is usually one you already saw. Replay reaches back only as far as the server's `mq.gap_window_minutes` (15 minutes by default); a longer outage resumes live with a hole.

### Server-Side Policy Filtering

Before anything reaches the client, the server applies the caller's policy to the stream: a table the role can't `select` never opens, denied columns are stripped from every frame, and a role carrying a row `filter` has non-matching rows withheld per subscriber — on live frames and on `Since` gap-fill replay alike. The claims are captured from the JWT at connect time.

Two things follow. **Event-id gaps are normal on a filtered stream** — a gap means a row was withheld, not that a frame was dropped. And **the row filter fails closed**: a comparison the server can't prove — an unresolvable claim, a type it can't compare — withholds the row rather than passing it. See [Access control](/access-control#row-level-security).

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

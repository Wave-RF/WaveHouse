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

Streams use SSE (Server-Sent Events), parsed by hand over `net/http` (no
third-party SSE library — the SDK has zero runtime dependencies).

### `*StreamController`

Returned by `.Stream(opts)` on `*TableRef`, `*QueryBuilder`, `*PipeRef`, and
`*DLQNamespace` (the DLQ variant is not yet functional server-side —
[#197](https://github.com/Wave-RF/WaveHouse/issues/197)). Calling `.Stream`
returns immediately; the connection opens in a background goroutine.

```go
stream := wh.From("clicks").Stream(&wavehouse.StreamOptions{
    Since: "2026-01-01T00:00:00Z",
})
defer stream.Close()
```

### `.Subscribe(sub) → func()`

Callback-based consumption. Returns an unsubscribe function. The
subscriber's `Status` callback fires immediately with the current status.

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

### Channel-based consumption — `.Events()`

The idiomatic Go alternative to the TypeScript SDK's async iterator: a
read-only channel, closed automatically when the stream shuts down.

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
Unlike the TypeScript SDK's async iterator — where breaking out of a
`for await` loop auto-closes the underlying connection — breaking a Go
`for range stream.Events()` loop only stops consuming from the channel; the
background goroutine and its HTTP connection keep running. Always pair a
stream with `defer stream.Close()` (or an explicit `stream.Close()` on every
exit path) regardless of which consumption style you use.
:::

The channel is buffered (256 events); a slow consumer that never drains it
causes the SDK to **drop** new events for that channel rather than block the
stream's read loop (`.Subscribe` callbacks still fire per event
regardless of channel backpressure).

### `.Close()`

Explicitly close the stream and release its resources. Non-blocking — safe
to call from inside a subscriber callback (which runs on the stream's own
goroutine); it signals the goroutine to stop without waiting for it to
finish.

```go
stream.Close()
```

### `.Status()`

Returns the current `StreamStatus`. A method (not a field), since Go has no
JS-style reactive property access.

```go
status := stream.Status()
```

### `.Connected(ctx)`

**Go-only addition** — not present in the TypeScript SDK. Blocks until the
stream reaches `StatusLive` or `ctx` is canceled; returns an error if the
stream closes before connecting. Useful when you need to know a stream is
live before doing something else (e.g. before starting a producer in a
test).

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

There's no `Signal`/context field here — a stream isn't canceled by passing
a `context.Context` into `.Stream()`; call `.Close()` instead (see above).

### `StreamEvent`

```go
type StreamEvent struct {
    Table     string         // table name (e.g. "clicks")
    Timestamp string         // received_timestamp (RFC3339Nano)
    Data      map[string]any // row data
}
```

:::note[`Events()` carries events only]
`Error` and `Status` are delivered exclusively through `.Subscribe(...)` —
the channel is typed `chan StreamEvent` and simply ends (closes) when the
stream closes, including on a terminal 401/403/404. Pair `Events()` with a
`Subscribe(&StreamSubscriber{Error: ..., Status: ...})` if you need to know
*why* a stream ended.
:::

:::note[`Events()` starts feeding on first call]
The channel only receives events emitted **after** the first `Events()`
call — a stream you set up but don't consume yet buffers nothing for the
channel. Call `Events()` immediately after `.Stream()` (or use
`.Subscribe`) if you can't start ranging right away.
:::

### Transport Behavior

| Transport | Reconnect | Protocol |
| --------- | --------- | -------- |
| SSE | Automatic, with exponential backoff (capped at 30s) and gap-fill replay via the last-seen event ID | HTTP/2 recommended |

Reconnect covers transport failures and retryable (5xx) responses. A
non-retryable response (401/403/404) is terminal: the error is delivered to
the subscriber's `Error` callback, status goes to `StatusClosed`, and the
stream does not reconnect — fix the cause (refresh the token, correct the
table) and open a new stream.

Auth is sent as an `Authorization: Bearer` header on every stream
(re)connection — see
[the note in the Getting Started guide](/sdk/go#creating-a-client). The
TypeScript SDK's "more than 5 concurrent connections" warning is a
browser-specific `EventSource` limit and doesn't apply here.

### Client-Side Stream Filtering

When a `*QueryBuilder` with `.Where()` filters or `.Select()` columns calls
`.Stream()`, the returned stream applies those filters client-side:

```go
stream := wh.From("clicks").
    Select("page", "button").
    Where("page", wavehouse.OpEq, "/home").
    Stream(nil)

// Only events where page == "/home" are emitted, with only page + button fields
```

Supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `like`,
`not_like` — the same `FilterOp` set `.Where()` takes everywhere. `like` /
`not_like` match SQL LIKE semantics (`%` → any run of characters, `_` → any
single character), case-insensitively. `in` accepts any Go slice type on
the right-hand side (`[]string`, `[]int`, `[]any`, ...), not just `[]any`.

---

## Live Queries

Live queries combine a historical backfill (`.FetchUntyped`) with a
real-time stream, providing a seamless initial-load + live-updates
experience. Only available on `*QueryBuilder` (there's no `TableRef.LiveQuery`
shortcut, matching the TypeScript SDK).

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
Unlike the TypeScript SDK's `initial: (result: Result<T[]>) => void`, the Go
SDK's `LiveQuery` doesn't accept a type parameter — `Initial` always
receives `[]map[string]any` plus a plain `error`, even if you'd otherwise
use `wavehouse.FetchTyped[Row]` for the same query outside a live query.
Decode into your own type inside the callback if you need one.
:::

### How it works

1. Subscribes to the stream **immediately** and buffers incoming events.
2. Runs the `.FetchUntyped(ctx)` query for historical data, calls
   `sub.Initial(rows, err)` with the result.
3. Deduplicates buffered events against the **newest** `received_timestamp`
   in the backfill — the maximum across all rows, not the last row's (an
   `OrderBy(..., "desc")` puts the *oldest* row last).
4. Flushes remaining buffered events (re-checking for anything that arrived
   mid-flush) and switches to live mode.

This "stream-first" approach ensures no events are lost between the fetch
and stream start.

:::caution[Dedup needs `received_timestamp` in the projection]
The dedup bound comes from the backfill rows' `received_timestamp` values.
`.SelectAll()` (or no projection) includes it; a `.Select(...)` projection
that omits it disables dedup, and events in the fetch/stream overlap window
are delivered twice — once in `Initial`, again via `Next`.
:::

### `.Close()`

Shuts down the live query and its underlying stream. Safe to call more than
once (idempotent via `sync.Once`).

```go
lq.Close()
```

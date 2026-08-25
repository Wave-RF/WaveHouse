---
title: "Go SDK Streaming & Live Queries"
description: "Real-time SSE streams, client-side filtering, and backfill-then-live queries in the WaveHouse Go SDK."
---

Real-time consumption with `github.com/Wave-RF/WaveHouse/clients/go`: SSE event streams from tables, builders, and pipes, plus live queries that backfill history before going live. Frames are parsed over `net/http` with no runtime dependencies. Builders and table refs come from [Queries](/sdk/go/queries). The TypeScript SDK's [Streaming & Live Queries](/sdk/streaming) implements the same protocol and mostly the same client-side filtering, but the lifecycle differs: Go streams are goroutine-backed and closed explicitly, not tied to a `context.Context` or a browser's `EventSource`.

## Streaming

### `*StreamController`

Returned by `.Stream(opts)` on `*TableRef`, `*QueryBuilder`, `*PipeRef`, and `*DLQNamespace` (DLQ is not yet functional server-side — [#197](https://github.com/Wave-RF/WaveHouse/issues/197)). `.Stream` returns immediately; the connection opens in a background goroutine.

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

// Removes this subscriber only — the connection stays open for any others
// and still needs stream.Close() when you're done with the stream itself.
defer unsub()
```

### Channel-based consumption — `.Events()`

A read-only channel, closed automatically when the stream shuts down. It is buffered (256 events) and buffers from `.Stream()` onward, so events arriving before the first `Events()` call are not lost. A slow consumer makes the SDK **drop** new events for that channel rather than block the read loop; the first drop logs via `log`, later drops are silent, and `.Subscribe` callbacks fire regardless.

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

:::note[`Events()` carries events only]
`Error` and `Status` are delivered exclusively via `.Subscribe(...)`. The channel closes on terminal errors (401/403/404), so pair `Events()` with a subscriber to learn why a stream ended.
:::

### `.Close()`

`stream.Close()` closes the stream and releases its resources. Non-blocking, and safe to call from inside a subscriber callback.

### `.Status()`

`stream.Status()` returns the current `StreamStatus`: `StatusConnecting`, `StatusLive`, `StatusReconnecting`, or `StatusClosed`.

### `.Connected(ctx)`

Blocks until the stream reaches `StatusLive` or `ctx` is canceled; returns an error if the stream closes before connecting. Useful for ensuring a stream is live (in tests, for instance).

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

There is no `Signal`/context field: a stream is not canceled by passing a `context.Context` into `.Stream()` — call `.Close()` instead.

### `StreamEvent`

```go
type StreamEvent struct {
    Table     string         // table name (e.g. "clicks")
    Timestamp string         // received_timestamp (RFC3339Nano)
    Data      map[string]any // row data
}
```

Top-level `DateTime`/`DateTime64` values inside `Data` arrive in canonical RFC 3339 UTC, byte-identical to what `/v1/query` renders for the same stored value, because the ingest handler rewrites them before publishing — a live frame and a later query can't disagree on the spelling of an instant. So a value you sent as `2026-06-21T06:00:00.123+02:00` comes back as `2026-06-21T04:00:00.123Z` (same instant, different spelling), and the canonicalization is deliberately fail-open: a value the server can't parse, or whose zone it can't resolve, is published verbatim. See [Timestamp canonicalization](/api#timestamp-canonicalization).

### Transport Behavior

SSE reconnects automatically with exponential backoff (capped at 30s) and gap-fill replay from the last event ID; HTTP/2 is recommended. Reconnect covers transport failures and retryable responses (5xx/429, plus `SSE_AUTH_ERROR` and `SSE_READ_ERROR`). `SSE_PARSE_ERROR` is retryable but does *not* reconnect — the offending frame is dropped and the same connection carries on. Terminal failures fire the `Error` callback, set status `StatusClosed`, and stop: non-retryable HTTP statuses, `SSE_CONNECT_ERROR` (bad `BaseURL`), `SSE_REDIRECT` (a credentialed request was redirected), and `SSE_BAD_CONTENT_TYPE` (a `200` that wasn't an event stream). Every error reaches the callback as a `*wavehouse.Error`, so `errors.As` and `wavehouse.IsRetryable` work on all of them — see the [error-code table](/sdk/go/reference#error-handling).

`/v1/stream` is not admin-gated, so WaveHouse itself never answers a stream with `401`; a `401` on a stream came from something in front of it. `Auth` provider errors during (re)connect are retryable (`SSE_ERROR`) and reconnects continue — `ClientOptions.MaxRetries` bounds request retries only, not stream reconnects — so call `.Close()` if the provider fails permanently. Auth goes as an `Authorization: Bearer` header on every connection, re-read from `Auth` per attempt ([note in Getting Started](/sdk/go#creating-a-client)).

Delivery across a reconnect is **at-least-once**: the server replays from the last event ID *inclusively*, so the first frame after a gap-fill is usually one you already saw. Replay reaches back only as far as the server's `mq.gap_window_minutes` (15 minutes by default); a longer outage resumes live with a hole.

### Server-Side Policy Filtering

Before anything reaches the client, the server applies the caller's policy — claims captured from the JWT at connect time — to the stream: a table the role can't `select` never opens, denied columns are stripped from every frame, and a role carrying a row `filter` has non-matching rows withheld per subscriber, on live frames and `Since` gap-fill replay alike.

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

Supported operators: `OpEq`, `OpNeq`, `OpGt`, `OpGte`, `OpLt`, `OpLte`, `OpIn`, `OpLike`, `OpNotLike` — the `FilterOp` set `.Where()` takes everywhere. `OpLike`/`OpNotLike` use SQL LIKE semantics (`%`, `_`), case-insensitively, and `OpIn` accepts any Go slice type (e.g. `[]string`, `[]int`).

#### How values are compared

The client-side evaluator mirrors the server's row-filter comparison rules rather than comparing everything as text:

- **Timestamps compare chronologically.** Since the server canonicalizes every top-level `DateTime`/`DateTime64` value to RFC 3339 UTC before publishing, a payload may read `2026-06-21T04:00:00Z` while your filter constant names the same instant as `2026-06-21T06:00:00+02:00`. Comparing those as text is wrong in both directions — lexically the payload sorts *below* the constant, so `OpGte` would miss a chronologically equal row. Both sides are parsed as instants instead.
- **Only unambiguous spellings count as instants:** RFC 3339 with an explicit offset or `Z`. A zone-less spelling like `2026-06-21 04:00:00` names an instant only relative to the column's declared timezone, which the server reads from the schema and a stream subscriber does not have, so it is not treated as a timestamp.
- **Ordering an instant against a non-instant fails closed.** If one side parses as a timestamp and the other does not, `OpGt`/`OpGte`/`OpLt`/`OpLte` withhold the row rather than falling back to text comparison, which could admit rows the query path excludes. The usual cause is a zone-less filter constant — give it an offset.
- **A missing column equals only `nil`.** A column absent from the payload does not match the string `"<nil>"`.
- **Numbers compare numerically**, so `9 < 100` as you would expect rather than as text.

:::caution[Integer precision above 2^53]
Event data decodes through `encoding/json` into `map[string]any`, so JSON numbers arrive as `float64`. An integer column beyond 2^53 has already lost exactness before any filter runs, and the server compares such columns in their exact storage domain — so a client-side filter on a very large `UInt64` can disagree with the server's verdict. Filter on a string or timestamp column instead when exactness at that magnitude matters.
:::

## Live Queries

Live queries combine a historical backfill (`.FetchUntyped`) with a real-time stream, for seamless initial loads and updates. They exist only on `*QueryBuilder` (there is no `TableRef.LiveQuery` shortcut), matching the TypeScript SDK.

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
Unlike the TypeScript SDK's `initial: (result: Result<T[]>) => void`, Go's `LiveQuery` takes no type parameter: `Initial` always receives `[]map[string]any` plus a plain `error`, even where you would use `wavehouse.FetchTyped[Row]` for the same query outside a live query. Decode inside the callback if needed.
:::

### How it works

1. Subscribes to the stream immediately and buffers events.
2. Runs `.FetchUntyped(ctx)` for historical data, then calls `sub.Initial(rows, err)`.
3. Deduplicates buffered events against the maximum `received_timestamp` in the backfill (not necessarily the last row).
4. Flushes remaining buffered events and switches to live mode.

Subscribing first is what prevents event loss between fetch and stream start.

:::caution[Dedup needs `received_timestamp` in the projection]
Dedup relies on `received_timestamp`. `.SelectAll()` (or no projection) includes it; a `.Select(...)` omitting it disables dedup, so events in the overlap window are delivered twice — once via `Initial`, once via `Next`.
:::

:::caution[`OpLike` matching differs between backfill and live]
Client-side `OpLike` is case-insensitive, but server-side backfills use ClickHouse `LIKE`, which is case-sensitive, so a live query filtering on `OpLike` may exclude rows from the backfill that it includes in the live stream ([#451](https://github.com/Wave-RF/WaveHouse/issues/451)). `OpNotLike` is rejected by `/v1/query` with a `400`, failing the `Initial` callback — see [Queries](/sdk/go/queries#wherecolumn-op-value).
:::

### `.Close()`

`lq.Close()` shuts down the live query and its underlying stream. Idempotent (guarded by `sync.Once`).

# WaveHouse Go SDK

Official Go client for [WaveHouse](https://github.com/Wave-RF/WaveHouse) — a schema-aware real-time API gateway for ClickHouse.

**Zero third-party runtime dependencies** — stdlib only.

**[Full SDK documentation on wavehouse.dev](https://wavehouse.dev/sdk/go/)**

## Install

```bash
go get github.com/Wave-RF/WaveHouse/clients/go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    wavehouse "github.com/Wave-RF/WaveHouse/clients/go"
)

func main() {
    // Create an unauthenticated client (uses the server's default_role).
    client := wavehouse.NewClient(wavehouse.Config{
        BaseURL: "http://localhost:8080",
    })

    // Health check.
    if err := client.Sys.Health(context.Background()); err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Insert a row.
    _, err := client.From("clicks").Insert(ctx, map[string]any{
        "page": "/home", "button": "cta",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Query with the fluent builder.
    page, err := client.From("clicks").
        Select("page", "button").
        Where("page", wavehouse.OpEq, "/home").
        OrderBy("page", "asc").
        Limit(10).
        FetchUntyped(ctx)
    if err != nil {
        log.Fatal(err)
    }
    for _, row := range page.Data {
        fmt.Println(row["page"], row["button"])
    }
}
```

## Authentication

```go
// Static token.
client := wavehouse.NewClient(wavehouse.Config{
    BaseURL: "http://localhost:8080",
    Auth:    wavehouse.StaticToken("your-jwt"),
})

// Dynamic token (e.g. rotated).
client = wavehouse.NewClient(wavehouse.Config{
    BaseURL: "http://localhost:8080",
    Auth: func(ctx context.Context) (string, error) {
        return fetchFreshToken(ctx)
    },
})
```

## Typed Queries (Generics)

```go
type ClickRow struct {
    Page       string `json:"page"`
    Button     string `json:"button"`
    DurationMS int    `json:"duration_ms"`
}

page, err := wavehouse.FetchTyped[ClickRow](ctx,
    client.From("clicks").Select("page", "button", "duration_ms").Limit(100),
)
// page.Data is []ClickRow
```

## Batch Insert (NDJSON)

```go
// Array of maps — serialized to NDJSON automatically.
result, _ := client.From("clicks").Insert(ctx, []map[string]any{
    {"page": "/a", "button": "cta"},
    {"page": "/b", "button": "nav"},
})
// result.OK, result.Total, result.Succeeded, result.Failed

// Pre-formatted NDJSON string.
result, _ = client.From("clicks").InsertNDJSON(ctx,
    `{"page":"/a"}`+"\n"+`{"page":"/b"}`,
)
```

## Streaming (SSE)

```go
stream := client.From("clicks").Stream(&wavehouse.StreamOptions{
    Since: "2026-01-01T00:00:00Z",
})
defer stream.Close()

// Channel-based consumption.
for event := range stream.Events() {
    fmt.Println(event.Table, event.Data)
}

// Or callback-based.
unsub := stream.Subscribe(&wavehouse.StreamSubscriber{
    Next:   func(e wavehouse.StreamEvent) { fmt.Println(e.Data) },
    Status: func(s wavehouse.StreamStatus) { fmt.Println("status:", s) },
})
defer unsub()
```

## Live Queries

```go
lq := client.From("clicks").
    SelectAll().
    OrderBy("received_timestamp", "desc").
    Limit(100).
    LiveQuery(&wavehouse.StreamSubscriber{
        Initial: func(rows []map[string]any, err error) {
            // Historical backfill.
            fmt.Println("initial rows:", len(rows))
        },
        Next: func(e wavehouse.StreamEvent) {
            // Live events after backfill.
            fmt.Println("live:", e.Data)
        },
    }, nil)
defer lq.Close()
```

## Named Pipes

```go
// Execute a pipe.
rows, _ := wavehouse.Fetch[map[string]any](ctx,
    client.Pipe("top_pages", map[string]any{"limit": 10}),
)

// Admin: manage pipes.
client.Pipes.Set(ctx, "top_pages", wavehouse.PipeDef{
    SQL:          "SELECT page, count() as views FROM clicks GROUP BY page LIMIT {{limit}}",
    AllowedRoles: []string{"viewer", "admin"},
})
pipes, _ := client.Pipes.List(ctx)
client.Pipes.Delete(ctx, "old_pipe")
```

## Admin

```go
// Schema introspection (admin-only).
schemas, _ := client.Schema.List(ctx)
client.Schema.Refresh(ctx)

// Policy management (admin-only).
policy, _ := client.Policy.Get(ctx)
client.Policy.Set(ctx, policy)
result, _ := client.Policy.Validate(ctx, policy)

// DLQ stats (admin-only).
stats, _ := client.DLQ.List(ctx)

// Raw SQL (admin-only).
rows, _ := wavehouse.SQL[map[string]any](ctx, client, "SELECT count() FROM clicks")
```

## Codegen

Generate Go structs from a running WaveHouse instance:

```bash
go run github.com/Wave-RF/WaveHouse/clients/go/cmd/wavehouse-codegen \
    --url http://localhost:8080 \
    --auth <admin-jwt> \
    --out ./db_types.go \
    --package myapp
```

The CLI reads `/v1/schema` (admin-only) and maps ClickHouse types to Go types:

| ClickHouse | Go |
|---|---|
| `String`, `UUID`, `DateTime*`, `Date*`, `Enum*`, `IPv4/6` | `string` |
| `UInt8/16/32/64` | `uint8/16/32/64` |
| `Int8/16/32/64` | `int8/16/32/64` |
| `Float32/64` | `float32/64` |
| `Bool` | `bool` |
| `Nullable(T)` | `*T` |
| `Array(T)` | `[]T` |
| `Map(K,V)` | `map[K]V` |
| `UInt128/256`, `Int128/256`, `Decimal*` | `string` |

## Error Handling

All SDK operations return `(T, error)`. Errors are `*wavehouse.Error` (use `errors.As`):

```go
page, err := client.From("clicks").Fetch(ctx)
if err != nil {
    var whErr *wavehouse.Error
    if errors.As(err, &whErr) {
        fmt.Println(whErr.Status, whErr.Code, whErr.Message, whErr.Retryable)
    }
}
```

The HTTP layer retries 5xx and network errors with exponential backoff (default 2 retries). 503 with `Retry-After` is honored. Context cancellation returns immediately with code `ABORTED`.

## License

Apache-2.0

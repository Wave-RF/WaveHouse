# WaveHouse Go SDK

Official Go client for [WaveHouse](https://github.com/Wave-RF/WaveHouse) — a schema-aware real-time API gateway for ClickHouse. Zero third-party runtime dependencies, SSE parser included.

**Full documentation: [wavehouse.dev/sdk/go](https://wavehouse.dev/sdk/go)** (setup); the usage guides below cover both SDKs, tabbed by language.

## Install

```bash
go get github.com/Wave-RF/WaveHouse/clients/go
```

Requires Go 1.24 or newer.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    wavehouse "github.com/Wave-RF/WaveHouse/clients/go"
)

func main() {
    wh := wavehouse.NewClient(wavehouse.Config{
        BaseURL: "http://localhost:8080",
        Auth:    wavehouse.StaticToken("your-jwt"), // omit for unauthenticated access
    })

    page, err := wh.From("clicks").
        Select("page", "button").
        Where("page", wavehouse.OpEq, "/home").
        Limit(10).
        FetchUntyped(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    for _, row := range page.Data {
        fmt.Println(row["page"], row["button"])
    }
}
```

## Documentation

- [Go SDK setup](https://wavehouse.dev/sdk/go) — client config, auth, typed rows via generics, error handling.
- [Queries](https://wavehouse.dev/sdk/queries) — tables, the query builder, inserts, pagination, raw SQL.
- [Streaming & Live Queries](https://wavehouse.dev/sdk/streaming) — SSE streams, client-side filtering, backfill-then-live.
- [Pipes](https://wavehouse.dev/sdk/pipes) — execute and manage named query pipes.
- [Admin & System](https://wavehouse.dev/sdk/admin) — schema, policy, DLQ stats, health.
- [Reference & CLI](https://wavehouse.dev/sdk/reference) — error codes, the full API tree, and the `wavehouse-codegen` struct generator.

## License

Apache-2.0

---
title: "Go SDK"
description: "Zero-dependency Go client SDK — query builder, real-time streaming, codegen."
---

`github.com/Wave-RF/WaveHouse/clients/go` — zero third-party runtime
dependency Go client for WaveHouse (stdlib only).

:::tip[Looking for the TypeScript SDK?]
This page and the rest of `/sdk/go/*` cover the Go client. The
JavaScript/TypeScript client (`@wavehouse/sdk`) has its own docs starting at
[SDK Overview](/sdk) — the two SDKs speak the same wire format, so anything
you learn about WaveHouse's query builder, streaming, or admin endpoints on
either page mostly carries over.
:::

## Installation

```bash
go get github.com/Wave-RF/WaveHouse/clients/go
```

Requires Go 1.24+ (the `go.mod` floor, matching supported releases rather than server's patch-pinned toolchain).

## Import

```go
import wavehouse "github.com/Wave-RF/WaveHouse/clients/go"
```

Aliasing `wavehouse` is optional but keeps call sites short; all examples here assume it.

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
    wh := wavehouse.NewClient(wavehouse.Config{
        BaseURL: "http://localhost:8080",
        Auth:    wavehouse.StaticToken("your-jwt"),
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

Find more examples in the [README](https://github.com/Wave-RF/WaveHouse/blob/main/clients/go/README.md).

## Creating a Client

```go
wh := wavehouse.NewClient(wavehouse.Config{
    BaseURL: "https://wavehouse.example.com",
    Auth: func(ctx context.Context) (string, error) {
        return myAuthProvider.GetToken(ctx)
    },
    Options: &wavehouse.ClientOptions{
        MaxRetries: 2,
    },
})
```

### `Config`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `BaseURL` | `string` | — | Required WaveHouse server URL, optionally with a path prefix. A trailing `/` is trimmed and every request path is appended on both transports, so a server under `https://app.example.com/wavehouse` works as-is. |
| `Auth` | `func(context.Context) (string, error)` | `nil` | Token provider called before each request. `nil` means unauthenticated access. |
| `Options` | `*ClientOptions` | `nil` | Transport tuning (see below). |
| `HTTPClient` | `*http.Client` | fresh `&http.Client{}` | Override for custom TLS, proxies, or test transports. |

:::caution[Timeouts: use contexts, not `http.Client.Timeout`]
The default client has no `Timeout`; use a `context.Context` deadline to prevent hangs. If supplying your own `HTTPClient`, leave `Timeout` unset, as it would kill long-lived SSE streams and force reconnect loops. Use `Transport`-level dial/TLS/response-header timeouts instead.
:::

### `ClientOptions`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MaxRetries` | `int` | `2` | Retry attempts for retryable errors (5xx, 429, network failures). |

`*Client` is safe for concurrent use; state is immutable after `NewClient` and builder chains copy. Ensure your `Auth` function is concurrency-safe.

:::caution[`Options` opts you out of the default, not just in]
The 2-retry default only applies if `Config.Options` is `nil`. If `Options` is provided, an unset `MaxRetries` field defaults to Go's int zero value (`0`), which explicitly disables retries. Passing `&wavehouse.ClientOptions{}` removes the default retry behavior.
:::

For static tokens, use `wavehouse.StaticToken(token)`:

```go
wh := wavehouse.NewClient(wavehouse.Config{
    BaseURL: "http://localhost:8080",
    Auth:    wavehouse.StaticToken("your-jwt"),
})
```

:::note[How the token is transmitted]
Unlike a browser's `EventSource`, Go's `net/http` client sets arbitrary headers on any request, so the Go SDK sends `Authorization: Bearer <token>` on every request, including SSE streams. No `?token=` query fallback (a TypeScript-in-the-browser concern; see its [equivalent note](/sdk#creating-a-client)).
:::

:::caution[Use HTTPS for authenticated non-local servers]
While the SDK allows `http://` for local development or private networks, bearer tokens over plaintext HTTP are insecure. Use `https://` for endpoints outside trusted networks.
:::

## Typed Rows (Generics)

Pass a row type parameter to decode results into your struct instead of `map[string]any`:

```go
type ClickRow struct {
    Page       string `json:"page"`
    Button     string `json:"button"`
    DurationMS int    `json:"duration_ms"`
}

page, err := wavehouse.FetchTyped[ClickRow](ctx,
    wh.From("clicks").Select("page", "button", "duration_ms").Limit(100),
)
// page.Data is []ClickRow
```

Use the [codegen CLI](/sdk/go/reference#codegen-cli) to generate row structs from a running server.

`FetchTyped`, `Fetch[Row]` (pipes), and `SQL[Row]` (raw SQL) are package-level generic functions because Go lacks generic methods. Untyped equivalents (`.FetchUntyped(ctx)`) are ordinary methods.

## Error Handling

Request-response operations (queries, ingest, pipes, admin) return `(T, error)` or just `error` if no body exists (`Pipes.Set`/`Delete`, `Policy.Set`, `Schema.Refresh`, `Sys.Health`). HTTP exchange errors are `*wavehouse.Error`; unwrap via `errors.As`. Client-side failures (e.g., `Auth` provider, marshal errors) are plain wrapped errors; handle the `errors.As == false` case. Streaming methods (`Stream`, `Subscribe`, `Close`, `Connected`) use callbacks or plain errors; see [Streaming](/sdk/go/streaming).

```go
page, err := wh.From("clicks").Fetch(ctx)
if err != nil {
    var whErr *wavehouse.Error
    if errors.As(err, &whErr) {
        fmt.Println(whErr.Status, whErr.Code, whErr.Message, whErr.Retryable)
    } else {
        fmt.Println("client-side failure:", err) // auth provider, marshal, ...
    }
    return err
}
```

See [Reference → Error Handling](/sdk/go/reference#error-handling) for retry behavior and error codes.

## Differences from the TypeScript SDK

Both SDKs share a wire format and feature set, verified by a shared `wire_cases.json` fixture in CI to ensure equivalent HTTP requests for builder calls. However, API shapes differ:

- **No `Result<T>` union.** Go returns `(T, error)`. A non-nil `error` is the only failure signal; no `{ok, data, error}` objects or `error: null` sentinels are used.
- **`context.Context` instead of `AbortSignal`.** Non-streaming calls take `ctx context.Context` as the first argument. Use timeout or `cancel()` instead of `AbortController`. See [Reference → Context Cancellation](/sdk/go/reference#context-cancellation).
- **Streams closed explicitly.** `TableRef.Stream` and `QueryBuilder.Stream` omit `context.Context`. The returned `*StreamController` manages its own goroutine and connection, torn down by `.Close()` (deferred `stream.Close()` is usual). See [Streaming](/sdk/go/streaming).
- **Generics on package functions.** Go lacks type parameters on methods; use `FetchTyped[Row]`, `Fetch[Row]`, or `SQL[Row]`.
- **No implicit "await."** Call `.FetchUntyped(ctx)` or `wavehouse.FetchTyped[Row](ctx, builder)` explicitly; `QueryBuilder` is not `PromiseLike`.
- **Any slice batches.** Reflection allows `[]ClickRow{...}` to use the same NDJSON batch path as `[]map[string]any`. See [Queries → Insert](/sdk/go/queries#insertctx-data).

## Explore the Go SDK

- [Queries](/sdk/go/queries) — Tables, chainable query builder, pagination, and raw SQL.
- [Streaming & Live Queries](/sdk/go/streaming) — SSE streams, client-side filtering, and backfill-then-live queries.
- [Pipes](/sdk/go/pipes) — Manage named query pipes.
- [Admin & System](/sdk/go/admin) — Schema introspection, access-control policy, DLQ stats, and health checks.
- [Reference & CLI](/sdk/go/reference) — Error codes, context cancellation, API tree, and codegen CLI.

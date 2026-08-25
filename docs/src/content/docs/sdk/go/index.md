---
title: "Go SDK"
description: "Zero-dependency Go client SDK — query builder, real-time streaming, codegen."
---

`github.com/Wave-RF/WaveHouse/clients/go` — a Go client for WaveHouse with zero third-party runtime dependencies, SSE parser included.

:::tip[Looking for the TypeScript SDK?]
`/sdk/go/*` covers the Go client; the JavaScript/TypeScript client (`@wavehouse/sdk`) starts at [SDK Overview](/sdk). Both speak the same wire format, so concepts carry over — only the [API shapes differ](#differences-from-the-typescript-sdk).
:::

## Installation

```bash
go get github.com/Wave-RF/WaveHouse/clients/go
```

Requires Go 1.24+ (the `go.mod` floor, which tracks supported releases rather than the server's patch-pinned toolchain).

```go
import wavehouse "github.com/Wave-RF/WaveHouse/clients/go"
```

The `wavehouse` alias is optional but keeps call sites short; all examples here assume it.

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

## Creating a Client

`Auth` is any function returning a token; `wavehouse.StaticToken(token)` wraps a fixed one, as in the Quick Start above.

```go
wh := wavehouse.NewClient(wavehouse.Config{
    BaseURL: "https://wavehouse.example.com",
    Auth: func(ctx context.Context) (string, error) {
        return myAuthProvider.GetToken(ctx)
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
The default client sets no `Timeout`; use a `context.Context` deadline to prevent hangs. Leave `Timeout` unset on a custom `HTTPClient` too — it would kill long-lived SSE streams and force reconnect loops. Use `Transport`-level dial/TLS/response-header timeouts instead.
:::

### `ClientOptions`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MaxRetries` | `int` | `2` | Retry attempts for retryable errors (5xx, 429, network failures). |
| `Headers` | `map[string]string` | `nil` | Sent on every request the client makes — REST calls and SSE streams alike. |

`*Client` is safe for concurrent use — state is immutable after `NewClient` and builder chains copy — provided your `Auth` function is concurrency-safe.

:::caution[`Options` opts you out of the default, not just in]
The 2-retry default applies only when `Config.Options` is `nil`. Passing `&wavehouse.ClientOptions{}` leaves `MaxRetries` at Go's zero value (`0`), which disables retries — set it explicitly.
:::

`Headers` is the Go analog of the TypeScript SDK's [`options.headers`](/sdk#custom-headers) — a gateway credential, a tenant selector, or tracing metadata with no first-class option. It is also how an operator sends the server's non-JWT [operator key](/api#authentication):

```go
wh := wavehouse.NewClient(wavehouse.Config{
    BaseURL: "http://localhost:8080",
    Options: &wavehouse.ClientOptions{
        MaxRetries: 2, // Options opts out of the default — set it explicitly.
        Headers:    map[string]string{"X-Operator-Key": os.Getenv("WH_OPERATOR_KEY")},
    },
})
```

The SDK's own headers win: `Authorization`, `Accept`, `Content-Type`, and the stream's `Cache-Control` are set after yours and overwrite any collision, matched case-insensitively and replacing rather than appending. The map is copied at `NewClient`, so later mutation changes nothing. There is no Go field for `options.fetch` or `options.fetchOptions` because `Config.HTTPClient` covers both — supply your own `*http.Client`, or a custom `http.RoundTripper` on its `Transport`.

:::note[How the token is transmitted]
The SDK sends `Authorization: Bearer <token>` on every request, SSE streams included, and never uses a `?token=` query fallback. The TypeScript SDK streams over `fetch` rather than `EventSource` for exactly this reason, so header auth is shared behavior rather than a Go-only property (see its [equivalent note](/sdk#creating-a-client)). The token is re-read from `Auth` on every reconnect attempt, so a rotating token keeps a long-lived stream alive.
:::

:::caution[A credentialed stream will not follow a redirect]
When the stream request carries a credential — an `Auth` token or a `ClientOptions.Headers` entry — the SDK refuses any 3xx and fails the stream with a terminal `SSE_REDIRECT`. Following it would either strip `Authorization` on a cross-host hop and silently downgrade the stream to `default_role`, or forward your configured headers to wherever the redirect points. Uncredentialed streams follow redirects normally.
:::

Use `https://` for any authenticated server outside a trusted network. The SDK allows `http://` for local development and private networks, but bearer tokens over plaintext are insecure.

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

Use the [codegen CLI](/sdk/go/reference#codegen-cli) to generate row structs from a running server. `FetchTyped`, `Fetch[Row]` (pipes), and `SQL[Row]` (raw SQL) are package-level generic functions because Go lacks generic methods; the untyped equivalents (`.FetchUntyped(ctx)`) are ordinary methods.

## Error Handling

Request-response operations (queries, ingest, pipes, admin) return `(T, error)`, or a bare `error` when there is no body (`Pipes.Set`/`Delete`, `Policy.Set`, `Schema.Refresh`, `Sys.Health`). HTTP exchange errors are `*wavehouse.Error`; unwrap via `errors.As`. Client-side failures (`Auth` provider, marshal errors) are plain wrapped errors, so handle the `errors.As == false` case too. Streaming methods (`Stream`, `Subscribe`, `Close`, `Connected`) report through callbacks or plain errors; see [Streaming](/sdk/go/streaming).

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

Both SDKs share a wire format and feature set, verified in CI against a shared `wire_cases.json` fixture that asserts equivalent HTTP requests for builder calls. The API shapes differ:

- **No `Result<T>` union.** Go returns `(T, error)`; a non-nil `error` is the only failure signal. No `{ok, data, error}` objects, no `error: null` sentinels.
- **`context.Context` instead of `AbortSignal`.** Non-streaming calls take `ctx context.Context` first; use a deadline or `cancel()`. See [Reference → Context Cancellation](/sdk/go/reference#context-cancellation).
- **Streams closed explicitly.** `TableRef.Stream` and `QueryBuilder.Stream` take no context; the returned `*StreamController` owns its goroutine and connection until `.Close()` (usually deferred). See [Streaming](/sdk/go/streaming).
- **Generics on package functions.** Go has no type parameters on methods, so use `FetchTyped[Row]`, `Fetch[Row]`, or `SQL[Row]`.
- **No implicit "await."** `QueryBuilder` is not `PromiseLike`; call `.FetchUntyped(ctx)` or `wavehouse.FetchTyped[Row](ctx, builder)` explicitly.
- **No third-party dependencies.** Stdlib only, SSE frame parser included. The TypeScript SDK carries exactly one runtime dependency (`eventsource-parser`, ~1.4 KB gzipped).
- **Any slice batches.** Reflection lets `[]ClickRow{...}` take the same NDJSON batch path as `[]map[string]any`. See [Queries → Insert](/sdk/go/queries#insertctx-data).

## Explore the Go SDK

- [Queries](/sdk/go/queries) — Tables, chainable query builder, pagination, and raw SQL.
- [Streaming & Live Queries](/sdk/go/streaming) — SSE streams, client-side filtering, and backfill-then-live queries.
- [Pipes](/sdk/go/pipes) — Manage named query pipes.
- [Admin & System](/sdk/go/admin) — Schema introspection, access-control policy, DLQ stats, and health checks.
- [Reference & CLI](/sdk/go/reference) — Error codes, context cancellation, API tree, and codegen CLI.

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

Requires Go 1.26.5 or later (the minimum pinned in the module's `go.mod`).

## Import

```go
import wavehouse "github.com/Wave-RF/WaveHouse/clients/go"
```

The package name is `wavehouse`; aliasing the import isn't required, but
keeps call sites short (`wavehouse.NewClient(...)`, `wavehouse.OpEq`, ...) —
every example on these pages assumes it.

## Quick Start

```go
import (
    "context"
    "fmt"

    wavehouse "github.com/Wave-RF/WaveHouse/clients/go"
)

wh := wavehouse.NewClient(wavehouse.Config{
    BaseURL: "http://localhost:8080",
    Auth:    wavehouse.StaticToken("your-jwt"),
})

page, err := wh.From("clicks").
    Select("page", "button").
    Where("page", wavehouse.OpEq, "/home").
    Limit(10).
    FetchUntyped(context.Background())
if err != nil { /* handle */ }
for _, row := range page.Data {
    fmt.Println(row["page"], row["button"])
}
```

See the [README](https://github.com/Wave-RF/WaveHouse/blob/main/clients/go/README.md) for more quick-start examples.

## Creating a Client

```go
import wavehouse "github.com/Wave-RF/WaveHouse/clients/go"

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
| `BaseURL` | `string` | — | WaveHouse server URL (required) |
| `Auth` | `func(context.Context) (string, error)` | `nil` | Token provider, called before each request. `nil` means unauthenticated access |
| `Options` | `*ClientOptions` | `nil` | Transport tuning (see below) |
| `HTTPClient` | `*http.Client` | fresh `&http.Client{}` | Override for custom TLS, proxies, or test transports |

### `ClientOptions`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MaxRetries` | `int` | `2` | Retry attempts for retryable errors (5xx, network failures) |

:::caution[`Options` opts you out of the default, not just in]
The default of 2 retries only applies when `Config.Options` is `nil`. If you
set `Options` to configure anything else in the future, an unset
`MaxRetries` field is Go's int zero value — `0` — which is a **valid,
explicit** "no retries" setting, not "use the default." Today `MaxRetries`
is the struct's only field, so this mostly matters if you pass
`&wavehouse.ClientOptions{}` and expect retry-by-default: you won't get it.
:::

For a static token that never rotates, use `wavehouse.StaticToken(token)`
instead of writing the closure yourself:

```go
wh := wavehouse.NewClient(wavehouse.Config{
    BaseURL: "http://localhost:8080",
    Auth:    wavehouse.StaticToken("your-jwt"),
})
```

:::note[How the token is transmitted]
Unlike a browser's `EventSource`, Go's `net/http` client can set arbitrary
headers on any request — so the Go SDK sends `Authorization: Bearer <token>`
on **every** request, including SSE streams. There's no `?token=` query
parameter fallback to worry about (that's a TypeScript-SDK-in-the-browser
concern only; see its [equivalent note](/sdk#creating-a-client)).
:::

:::caution[Use HTTPS for authenticated non-local servers]
The SDK doesn't forbid `http://` base URLs — local development and
private-network deployments rely on them — but a bearer token sent over
plaintext HTTP is readable by anything on the path. Point authenticated
clients at `https://` endpoints outside a trusted network.
:::

## Typed Rows (Generics)

Pass a row type as a type parameter to get results decoded straight into
your struct, instead of `map[string]any`:

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

Generate row structs from a running server with the
[codegen CLI](/sdk/go/reference#codegen-cli).

`FetchTyped` is a package-level generic function, not a method — Go doesn't
support generic methods, so this (and `Fetch[Row]` for pipes, and
`SQL[Row]` for raw SQL) are top-level functions that take the client or
builder as an argument. Untyped equivalents (`.FetchUntyped(ctx)`, decoding
into `map[string]any`) are ordinary methods, since they need no type
parameter.

## Error Handling

Every request-response operation (queries, ingest, pipes, admin) returns `(T, error)`. Errors originating from the HTTP exchange are `*wavehouse.Error`; unwrap with `errors.As`. Client-side failures before a request goes out (an `Auth` provider error, a request-body marshal failure) are plain wrapped errors, so handle the `errors.As == false` case too. (Streaming lifecycle methods — `Stream`, `Subscribe`, `Close`, `Connected` — deliver errors through callbacks or plain errors instead; see [Streaming](/sdk/go/streaming).)

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

The two SDKs share a wire format and mirror each other's feature set closely
(a shared `wire_cases.json` conformance fixture is replayed by a test runner
per SDK — both run in CI — asserting each produces the expected HTTP request
for equivalent builder calls), but the
languages pull the API shape in different directions:

- **No `Result<T>` union.** Go returns `(T, error)`; nothing is wrapped in
  an `{ok, data, error}` object, and there's no `error: null` sentinel to
  check — a non-nil `error` is the only signal.
- **`context.Context` instead of `AbortSignal`.** Every non-streaming call
  takes a `ctx context.Context` as its first argument; cancel it (timeout or
  `cancel()`) instead of building an `AbortController`. See
  [Reference → Context Cancellation](/sdk/go/reference#context-cancellation).
- **Streams are closed explicitly, not via `ctx`.** `TableRef.Stream` /
  `QueryBuilder.Stream` don't take a `context.Context` — the returned
  `*StreamController` manages its own background goroutine and connection,
  torn down by calling `.Close()` (deferred `stream.Close()` is the usual
  pattern). See [Streaming](/sdk/go/streaming).
- **Generics live on package-level functions, not methods** (`FetchTyped[Row]`,
  `Fetch[Row]`, `SQL[Row]`), because Go doesn't support type parameters on
  methods.
- **No implicit "await."** A `QueryBuilder` isn't `PromiseLike` — call
  `.FetchUntyped(ctx)` or `wavehouse.FetchTyped[Row](ctx, builder)`
  explicitly; there's no bare `await builder` shortcut.
- **`Insert` accepts typed row slices, not just maps.** Passing
  `[]ClickRow{...}` (any slice type, detected via reflection) batches as
  NDJSON exactly like `[]map[string]any` — see
  [Queries → Insert](/sdk/go/queries#insertctx-data).

## Explore the Go SDK

- [Queries](/sdk/go/queries) — Tables, the chainable query builder, pagination, and raw SQL.
- [Streaming & Live Queries](/sdk/go/streaming) — Real-time SSE streams, client-side filtering, and backfill-then-live queries.
- [Pipes](/sdk/go/pipes) — Execute and manage named query pipes.
- [Admin & System](/sdk/go/admin) — Schema introspection, access-control policy, DLQ stats, and health checks.
- [Reference & CLI](/sdk/go/reference) — Error codes, context cancellation, the full API tree, and the codegen CLI.

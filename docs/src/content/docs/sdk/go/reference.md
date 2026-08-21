---
title: "Go SDK Reference & CLI"
description: "Error codes, context cancellation, the full API tree, and the codegen CLI for the WaveHouse Go SDK."
---

Cross-cutting reference for `github.com/Wave-RF/WaveHouse/clients/go`: cancellation, the error model behind every request-response call's `(T, error)` return, the complete API tree at a glance, and the `wavehouse-codegen` tool that ships with the module. Compare with the TypeScript SDK's [Reference & CLI](/sdk/reference) page.

## Context Cancellation

Non-streaming operations take a `context.Context` as their first argument (similar to TypeScript's `AbortSignal`). Cancel it using a timeout or explicit `cancel()`:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

page, err := wh.From("clicks").Fetch(ctx)
var whErr *wavehouse.Error
if errors.As(err, &whErr) && whErr.Code == "ABORTED" {
    fmt.Println("Request timed out")
}
```

Cancellation returns immediately (no retry) with `&wavehouse.Error{Status: 0, Code: "ABORTED", Retryable: false}`.

`.Stream(opts)` ignores `context.Context`; the returned `*StreamController` manages its own context and goroutine, closed via `.Close()`. See [Streaming](/sdk/go/streaming#streamoptions).

## Error Handling

The SDK never panics on API or network failures. Request-response operations (queries, ingest, pipes, admin) return `(T, error)`, while result-less operations (`Pipes.Set`/`Delete`, `Policy.Set`, `Schema.Refresh`, `Sys.Health`) return a bare `error`.

HTTP exchange errors are `*wavehouse.Error` (unwrap via `errors.As`). Client-side failures (e.g., `Auth` provider, marshal failures) are plain wrapped errors; handle the `errors.As == false` case. Streaming methods (`Stream`, `Subscribe`, `Close`) do not return `(T, error)`; stream errors use the subscriber's `Error` callback. `Connected(ctx)` returns plain errors. This mirrors the TypeScript SDK's "never throws" guarantee.

| Status | Code | Retryable | Description |
|--------|------|-----------|--------------|
| 400 | `HTTP_400` | No | Bad request (validation, missing fields) |
| 401 | `HTTP_401` | No | Invalid or expired JWT (missing tokens use `default_role`, resulting in success or 403) |
| 403 | `HTTP_403` | No | Insufficient permissions |
| 404 | `HTTP_404` | No | Table or pipe not found |
| 429 | `HTTP_429` | Yes | Rate limited (auto-retries, honoring `Retry-After`, capped at 30s) |
| 500 | `HTTP_500` | Yes | Server error (retried per `ClientOptions.MaxRetries`) |
| 503 | `HTTP_503` | Yes | Service unavailable (auto-retries, honoring `Retry-After`, capped at 30s) |
| 0 | `NETWORK_ERROR` | Yes | Network failure (retried with exponential backoff) |
| 0 | `ABORTED` | No | Request canceled via `context.Context` |
| 0 | `SSE_AUTH_ERROR` | Yes | The `Auth` provider returned an error for this attempt; the stream retries, so a token endpoint having a bad minute doesn't tear down a healthy stream |
| 0 | `SSE_NETWORK_ERROR` | Yes | Transport failure opening or holding the stream connection |
| 0 | `SSE_CONNECT_ERROR` | No | `BaseURL` is unparseable, or its scheme is not `http`/`https` — retrying cannot fix it |
| *3xx* | `SSE_REDIRECT` | No | The stream endpoint redirected while the request carried a credential, and the SDK refused to follow it |
| 200 | `SSE_BAD_CONTENT_TYPE` | No | A `200` that wasn't `text/event-stream` — something between you and WaveHouse answered (a captive portal, an auth gateway's login page) |
| 0 | `SSE_PARSE_ERROR` | Yes | A frame's JSON didn't decode; the frame is dropped and the stream continues |
| 0 | `SSE_READ_ERROR` | Yes | The connection failed mid-read; the stream reconnects from the last event ID |
| 0 | `SSE_ERROR` | Yes | Stream failure the SDK could not classify further |

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

`wavehouse.IsRetryable(err)` shortcuts the `errors.As` + `.Retryable` check.

Retries apply to all HTTP methods, matching TypeScript's `http.ts`. For `/v1/ingest`, at-least-once delivery on retry is a documented contract (see API docs ["At-least-once on retry"](/api#post-v1ingesttabletable--ingest-data)); use server-side dedup for duplicate suppression. `/v1/ops/query` (raw SQL) requires `admin_role`, so repeated execution on retry is an accepted risk.

## Full API Tree

```text
NewClient(Config) → *Client
├── .From(table) → *TableRef
│   ├── .Fetch(ctx) → (*Page[map[string]any], error)
│   ├── .Select(...cols) → *QueryBuilder
│   │   ├── .Select() .SelectAll() .Where() .Count() .Sum() .Avg() .Min() .Max()
│   │   │   .CountDistinct() .Aggregate() .GroupBy() .OrderBy()
│   │   │   .Limit() .TimeRange() .CacheTTL()
│   │   ├── FetchTyped[Row](ctx, q)  → (*Page[Row], error)          // package-level generic func
│   │   ├── .FetchUntyped(ctx)       → (*Page[map[string]any], error)
│   │   ├── .Stream(opts)            → *StreamController
│   │   └── .LiveQuery(sub, opts)    → *LiveQueryHandle
│   ├── .SelectAll() → *QueryBuilder
│   ├── .Insert(ctx, data) → (*InsertResult, error)
│   ├── .InsertNDJSON(ctx, ndjson) → (*InsertResult, error)
│   ├── .Schema(ctx) → (*TableSchema, error)
│   └── .Stream(opts) → *StreamController
├── .Pipe(name, params) → *PipeRef
│   ├── Fetch[Row](ctx, p) → ([]Row, error)          // package-level generic func
│   ├── .FetchUntyped(ctx) → ([]map[string]any, error)
│   └── .Stream(opts) → *StreamController
├── .Pipes (admin) → *PipesNamespace
│   ├── .List(ctx) → ([]Pipe, error)
│   ├── .Get(ctx, name) → (*Pipe, error)
│   ├── .Set(ctx, name, PipeDef) → error
│   └── .Delete(ctx, name) → error
├── SQL[Row](ctx, client, query) → ([]Row, error)     // package-level generic func, admin-only
├── .Schema (admin) → *SchemaNamespace
│   ├── .List(ctx) → (Schemas, error)
│   └── .Refresh(ctx) → error
├── .Policy (admin) → *PolicyNamespace
│   ├── .Get(ctx) → (*Policy, error)
│   ├── .Set(ctx, *Policy) → error
│   └── .Validate(ctx, *Policy) → (*ValidationResult, error)
├── .DLQ (admin) → *DLQNamespace
│   ├── .List(ctx) → (*DLQStats, error)
│   ├── .Table(ctx, name) → (*DLQStats, error)
│   └── .Stream(opts) → *StreamController  // not yet functional server-side — #197
└── .Sys → *SysNamespace
    └── .Health(ctx) → error

*StreamController
├── .Subscribe(*StreamSubscriber) → func()  // unsubscribe
├── .Events() → <-chan StreamEvent          // idiomatic Go alternative to an async iterator
├── .Close()
├── .Status() → StreamStatus
└── .Connected(ctx) → error                 // Go-only addition, blocks until live
```

## Codegen CLI

Generate Go structs from a running WaveHouse instance using the `wavehouse-codegen` command in `cmd/`:

```bash
export WAVEHOUSE_AUTH='<admin-jwt>'   # avoids leaking the token via argv
go run github.com/Wave-RF/WaveHouse/clients/go/cmd/wavehouse-codegen@latest \
    --url http://localhost:8080 \
    --out ./db_types.go \
    --package myapp
```

Or, inside `clients/go/`:

```bash
go run ./cmd/wavehouse-codegen --url http://localhost:8080 --out ./db_types.go
```

Codegen reads the admin-only `/v1/ops/schema` endpoint; non-dev servers require an admin token or return `403`. Use `WAVEHOUSE_AUTH` instead of `--auth <jwt>` to keep tokens out of shell history and process listings.

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `--url`, `-u` | WaveHouse base URL | `http://localhost:8080` |
| `--out`, `-o` | Output `.go` file path | `./wavehouse_types.go` |
| `--auth`, `-a` | Bearer token; prefer `WAVEHOUSE_AUTH` env var | `$WAVEHOUSE_AUTH` |
| `--package`, `-p` | Go package name for the generated file | `main` |
| `--help`, `-h` | Show usage and exit | — |

Output is processed via `go/format`. If a table or column name produces invalid Go source, codegen fails loudly.

**Example output:**

```go
// Code generated by wavehouse-codegen. DO NOT EDIT.

package myapp

// ClicksRow represents a row in the "clicks" table.
type ClicksRow struct {
    Page              string  `json:"page"`
    Button            string  `json:"button"`
    Score             float64 `json:"score"`
    ReceivedTimestamp *string `json:"received_timestamp,omitempty"`
}
```

(Example for the [development quick-start](/development#quick-start) `clicks` table; `received_timestamp` is `*string` + `,omitempty` due to its `DEFAULT` clause.)

The generator does not special-case initialisms: `event_id` becomes `EventId`, not the Go-idiomatic `EventID` — each `_`-separated part just gets its first letter upper-cased. Table and column names are converted to `PascalCase`; leading digits get an `X` prefix (e.g., `2fa_events` $\rightarrow$ `X2faEventsRow`). Columns with `has_default: true` become pointer fields with `,omitempty`: `nil` uses the server default, a pointed-at value is sent — including an explicit `0`/`false`/`""`.

**ClickHouse → Go type mapping:**

| ClickHouse Type | Go Type |
|------------------|---------|
| `String`, `FixedString`, `UUID`, `DateTime*`, `Date*`, `Time`/`Time64`, `Enum8`/`Enum16`, `IPv4`/`IPv6` | `string` |
| `Bool` / `Boolean` | `bool` |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` | `uint8` / `uint16` / `uint32` / `uint64` |
| `Int8` / `Int16` / `Int32` / `Int64` | `int8` / `int16` / `int32` / `int64` |
| `Float32`, `BFloat16` | `float32` |
| `Float64` | `float64` |
| `UInt128`/`UInt256`, `Int128`/`Int256` | `json.Number` |
| `Decimal*` | `string` |
| `Nullable(T)` | `*T` |
| `LowCardinality(T)` | same as `T` |
| `Array(T)` | `[]T` (except `Array(UInt8)` $\rightarrow$ `json.RawMessage` per [#436](https://github.com/Wave-RF/WaveHouse/issues/436)) |
| `Map(K, V)` | `map[K]V` (fallback: `map[string]any`) |
| `SimpleAggregateFunction(fn, T)` | same as `T` (rollup tables from `AggregatingMergeTree`/`SummingMergeTree` generate usable structs) |
| anything unrecognized | `any` |

Unlike the TypeScript SDK, Go codegen preserves ClickHouse integer **widths** (`UInt64` → `uint64`, not a generic `number`), so 64-bit columns decode exactly where TS hits the 2^53 ceiling. Generated structs target `/v1/query` and `/v1/pipes/*`. For the raw-SQL path (`/v1/ops/query`), which quotes 64-bit+ integers, use `map[string]any` with `SQL[Row]`.

## Testing

Unit tests are colocated in `clients/go/` (module `clients/go/go.mod`), separate from the root `WaveHouse` module. The cross-language wire-format **conformance suite** uses `clients/go/conformance_test.go` to replay the shared fixture (`clients/go/testdata/wire_cases.json`), asserting correct HTTP methods, paths, content types, and bodies. The TypeScript half—`tests/conformance/conformance_ts.mjs`, run via `make test-conformance-ts` (builds TS SDK first)—uses the same fixture; CI runs both to ensure wire format consistency.

```bash
cd clients/go
go test ./...
```

E2E tests (build tag `e2e`) run against a live WaveHouse instance via a dedicated Make target:

```bash
WAVEHOUSE_URL=http://localhost:8080 WAVEHOUSE_AUTH='<jwt>' make test-go-sdk-e2e
```

`WAVEHOUSE_URL` defaults to `http://localhost:8080`; optional `WAVEHOUSE_AUTH` is for admin cases. The suite skips if the server is unreachable. Unlike the TypeScript SDK, Go isn't yet in the repo's `make test-e2e` harness (see [E2E Testing](/sdk/reference#e2e-testing)).

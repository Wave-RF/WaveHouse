---
title: "Go SDK Reference & CLI"
description: "Error codes, context cancellation, the full API tree, and the codegen CLI for the WaveHouse Go SDK."
---

Cross-cutting reference for `github.com/Wave-RF/WaveHouse/clients/go`:
cancellation, the error model behind every request-response call's `(T, error)` return,
the complete API tree at a glance, and the `wavehouse-codegen` tool that
ships with the module. Compare with the TypeScript SDK's
[Reference & CLI](/sdk/reference) page.

## Context Cancellation

Every non-streaming operation takes a `context.Context` as its first
argument — Go's equivalent of the TypeScript SDK's `AbortSignal` support.
Cancel it with a timeout or an explicit `cancel()`:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

page, err := wh.From("clicks").Fetch(ctx)
var whErr *wavehouse.Error
if errors.As(err, &whErr) && whErr.Code == "ABORTED" {
    fmt.Println("Request timed out")
}
```

Context cancellation returns immediately (no retry) with
`&wavehouse.Error{Status: 0, Code: "ABORTED", Retryable: false}`.

Streams work differently: `.Stream(opts)` doesn't take a `context.Context`
at all — the returned `*StreamController` owns its own internal context and
background goroutine, torn down explicitly via `.Close()`. See
[Streaming](/sdk/go/streaming#streamoptions).

---

## Error Handling

The SDK never panics on API or network failures — every request-response
operation (queries, ingest, pipes, admin) returns `(T, error)`, and errors
are always `*wavehouse.Error` (unwrap with `errors.As`). Streaming lifecycle
methods (`Stream`, `Subscribe`, `Close`) don't return `(T, error)`; stream
errors are delivered via the subscriber's `Error` callback. This is the
direct Go equivalent of the TypeScript SDK's "the SDK never throws"
guarantee.

| Status | Code | Retryable | Description |
|--------|------|-----------|--------------|
| 400 | `HTTP_400` | No | Bad request (validation, missing fields) |
| 401 | `HTTP_401` | No | Present-but-invalid or expired JWT (a *missing* token resolves to `default_role` and is denied with 403) |
| 403 | `HTTP_403` | No | Insufficient permissions |
| 404 | `HTTP_404` | No | Table or pipe not found |
| 500 | `HTTP_500` | Yes | Server error (retried per `ClientOptions.MaxRetries`) |
| 503 | `HTTP_503` | Yes | Service unavailable (auto-retries, honoring `Retry-After`) |
| 0 | `NETWORK_ERROR` | Yes | Network failure (retried with exponential backoff) |
| 0 | `ABORTED` | No | Request canceled via `context.Context` |

```go
page, err := wh.From("clicks").Fetch(ctx)
if err != nil {
    var whErr *wavehouse.Error
    if errors.As(err, &whErr) {
        fmt.Println(whErr.Status, whErr.Code, whErr.Message, whErr.Retryable)
    }
    return err
}
```

`wavehouse.IsRetryable(err)` is a shortcut for the `errors.As` + `.Retryable`
check above.

Retries apply uniformly to every HTTP method the SDK issues (not just GET) —
matching the TypeScript SDK's `http.ts` behavior. For `/v1/ingest`,
at-least-once delivery on retry is a documented contract (see the API
docs' ["At-least-once on retry"](/api#post-v1ingesttabletable--ingest-data)
note); dedup is the prescribed server-side safety net when duplicate
suppression matters. `/v1/admin/query` (raw SQL) is gated by `admin_role`,
so repeated execution on retry is an accepted risk for admin-only usage.

---

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

Generate Go structs from a running WaveHouse instance. The module ships a
`wavehouse-codegen` command under `cmd/`:

```bash
export WAVEHOUSE_AUTH=<admin-jwt>   # avoids leaking the token via argv
go run github.com/Wave-RF/WaveHouse/clients/go/cmd/wavehouse-codegen \
    --url http://localhost:8080 \
    --out ./db_types.go \
    --package myapp
```

Or, working inside this repo (`clients/go/`):

```bash
go run ./cmd/wavehouse-codegen --url http://localhost:8080 --out ./db_types.go
```

Codegen reads `/v1/schema`, which is **admin-only**. Against a non-dev
server, provide an admin-role token or the request is denied with `403`.
Prefer the `WAVEHOUSE_AUTH` environment variable — a token passed with
`--auth <jwt>` ends up in shell history and process listings.

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `--url`, `-u` | WaveHouse base URL | `http://localhost:8080` |
| `--out`, `-o` | Output `.go` file path | `./wavehouse_types.go` |
| `--auth`, `-a` | Bearer token (if auth required); prefer `WAVEHOUSE_AUTH` env var | `$WAVEHOUSE_AUTH` |
| `--package`, `-p` | Go package name for the generated file | `main` |
| `--help`, `-h` | Show usage and exit | — |

The output is run through `go/format` before being written — if a table or
column name would produce invalid Go source (rare, but possible with exotic
names), codegen fails loudly instead of writing broken code.

**Example output:**

```go
// Code generated by wavehouse-codegen. DO NOT EDIT.

package myapp

// ClicksRow represents a row in the "clicks" table.
type ClicksRow struct {
    Page              string  `json:"page"`
    Button            string  `json:"button"`
    Score             float64 `json:"score"`
    ReceivedTimestamp string  `json:"received_timestamp,omitempty"`
}
```

(That's the exact output for the `clicks` table from the
[development quick-start](/development#quick-start) — `received_timestamp`
gets `,omitempty` because it has a `DEFAULT` clause.)

Note the generator does **not** special-case initialisms: `event_id` becomes
`EventId`, not the Go-idiomatic `EventID` — each `_`-separated part simply
gets its first letter upper-cased.

Table and column names are converted to `PascalCase` for Go field/type names
(a leading digit gets an `X` prefix — e.g. a table named `2fa_events`
becomes `X2faEventsRow` — to stay a valid Go identifier). A column with
`has_default: true` in the schema gets `,omitempty` appended to its JSON
tag.

**ClickHouse → Go type mapping:**

| ClickHouse Type | Go Type |
|------------------|---------|
| `String`, `FixedString`, `UUID`, `DateTime*`, `Date*`, `Time`/`Time64`, `Enum8`/`Enum16`, `IPv4`/`IPv6` | `string` |
| `Bool` / `Boolean` | `bool` |
| `UInt8` / `UInt16` / `UInt32` | `uint8` / `uint16` / `uint32` |
| `Int8` / `Int16` / `Int32` | `int8` / `int16` / `int32` |
| `Float32`, `BFloat16` | `float32` |
| `Float64` | `float64` |
| `UInt64`/`Int64`, `Decimal*`, `UInt128`/`UInt256`, `Int128`/`Int256` | `string` (ClickHouse quotes 64-bit-and-wider integers in JSON output — `output_format_json_quote_64bit_integers` — and the server forwards them verbatim) |
| `Nullable(T)` | `*T` |
| `LowCardinality(T)` | same as `T` |
| `Array(T)` | `[]T` |
| `Map(K, V)` | `map[K]V` (falls back to `map[string]any` if `K`/`V` can't be split) |
| `SimpleAggregateFunction(fn, T)` | same as `T` (rollup tables from `AggregatingMergeTree`/`SummingMergeTree` generate usable structs) |
| anything unrecognized | `any` |

This differs from the TypeScript SDK's mapping in one notable way: Go's
codegen preserves ClickHouse's integer **widths** up to 32 bits (`UInt32` →
`uint32`, not a generic `number`), since Go — unlike TypeScript — has
native fixed-width integer types. 64-bit integers stay `string` because
that is what actually arrives on the wire.

## Testing

The Go SDK ships with unit tests colocated in `clients/go/` (its own Go
module — `clients/go/go.mod` — separate from the root `WaveHouse` module),
plus the Go half of the cross-language wire-format **conformance suite**:
`clients/go/conformance_test.go` replays the shared fixture
(`clients/go/testdata/wire_cases.json`) and asserts the Go SDK produces the
expected HTTP method, path, content type, and body for each case. The
TypeScript half — `tests/conformance/conformance_ts.mjs`, run with
`make test-conformance-ts` (it builds the TS SDK first) — replays the same
fixture, and CI runs both, keeping the two clients honest about the wire
format they both speak.

```bash
cd clients/go
go test ./...
```

E2E tests (build tag `e2e`) run against a live WaveHouse instance and have
their own Make target, separate from the repo's `make test-e2e`:

```bash
WAVEHOUSE_URL=http://localhost:8080 WAVEHOUSE_AUTH=<jwt> make test-go-sdk-e2e
```

`WAVEHOUSE_URL` defaults to `http://localhost:8080`; `WAVEHOUSE_AUTH` is
optional (admin-only cases skip without it). When the server is unreachable
the suite skips instead of failing.

Unlike the TypeScript SDK, the Go SDK isn't (yet) wired into the repo's
`make test-e2e` harness — see the TypeScript SDK's
[E2E Testing](/sdk/reference#e2e-testing) section for that suite's
architecture, which the Go client doesn't currently participate in.

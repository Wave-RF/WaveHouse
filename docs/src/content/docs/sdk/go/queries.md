---
title: "Go SDK Queries"
description: "Tables, the chainable query builder, pagination, and raw SQL in the WaveHouse Go SDK."
---

Reading and writing data with `github.com/Wave-RF/WaveHouse/clients/go`: table references, the chainable query builder, cursor pagination, and the admin-only raw-SQL escape hatch. Every request-response operation takes a `context.Context` as its first argument and returns `(T, error)`; the chainable builder methods and `.Stream(opts)` are the exceptions — see [Error Handling](/sdk/go#error-handling). Compare with the TypeScript SDK's [Queries](/sdk/queries) page, which covers the same surface with a `Result<T>`-returning, `PromiseLike` builder.

## Tables — `client.From(table)`

`From` returns a `*TableRef`. It performs no request, making it safe to store or pass around.

```go
clicks := wh.From("clicks")
```

### `.Fetch(ctx)`

Shortcut for "select every column" with a default limit of 1000 (`wavehouse.DefaultLimit`). Internally it is `t.SelectAll().Limit(DefaultLimit).FetchUntyped(ctx)`. Unlike the TypeScript SDK's `.fetch(opts?)`, there is no options struct to override the limit or attach anything per-call; chain `.SelectAll().Limit(n)` yourself ([Query Builder](#query-builder)).

Access-control policies restrict returned columns; `.Fetch()` cannot bypass `deny_columns`/`allow_columns` (see [Access control](/access-control#column-permissions)).

```go
page, err := clicks.Fetch(ctx)
if err != nil {
    log.Fatal(err)
}
for _, row := range page.Data {
    fmt.Println(row["page"])
}
```

For pagination, use the query builder with `.OrderBy()` (see [Pagination](#pagination)).

### `.Insert(ctx, data)`

Inserts one or many rows based on the input type:

- **Map or struct** (excluding slices and `[]byte`): Sent as JSON via `POST /v1/ingest?table={table}`. For raw NDJSON, use `.InsertNDJSON`.
- **Any slice** (`[]map[string]any`, `[]ClickRow`, etc.): Serialized to NDJSON via reflection and sent as one `application/x-ndjson` request. Per-record outcomes are returned in the result.

```go
// Single row → InsertResult{OK: true} (or Duplicate: &true when dedup skips it)
res, err := clicks.Insert(ctx, map[string]any{"page": "/home", "button": "cta"})

// Many rows (map slice) → one NDJSON request, per-record summary
res, err = clicks.Insert(ctx, []map[string]any{
    {"page": "/home", "button": "cta"},
    {"page": "/about", "button": "nav"},
})
// res.OK, res.Total, res.Succeeded, res.Failed, res.Duplicates, res.Results

// Many rows (typed slice) — same NDJSON path, via reflection
type ClickRow struct {
    Page   string `json:"page"`
    Button string `json:"button"`
}
res, err = clicks.Insert(ctx, []ClickRow{
    {Page: "/home", Button: "cta"},
    {Page: "/about", Button: "nav"},
})
```

For batches, `res.OK` is `true` only if all records succeeded (`*res.Failed == 0`). Check `res.Failed` and `res.Results` (each `InsertRecordResult{Index, OK, Duplicate, Error}`, 1-based `Index`) for partial failures. The returned `error` indicates whole-request failures (network, `404`, `403`, `503`). Empty slices are no-ops.

> The server is format-agnostic: `POST /v1/ingest` also accepts a raw JSON array or a single object (`Content-Type` is only a hint). See [API reference](/api#post-v1ingesttabletable--ingest-data).

### `.InsertNDJSON(ctx, ndjson)`

Inserts pre-formatted NDJSON as a `string` without parsing it into Go values. Returns the same summary as slice `Insert`.

```go
// From a literal string.
res, err := clicks.InsertNDJSON(ctx, `{"page":"/a"}`+"\n"+`{"page":"/b"}`)

// From a file on disk.
raw, err := os.ReadFile("events.ndjson")
if err != nil {
    log.Fatal(err)
}
res, err = clicks.InsertNDJSON(ctx, string(raw))
```

### `.Schema(ctx)`

Fetch table column definitions from ClickHouse. Admin-only.

```go
schema, err := clicks.Schema(ctx)
// schema.Name == "clicks"
// schema.Columns: []Column{{Name: "page", Type: "String", IsNullable: false, HasDefault: false}, ...}
```

### `.Select(...columns)`

Start a query builder chain. See [Query Builder](#query-builder).

```go
page, err := clicks.Select("page", "button").
    Where("page", wavehouse.OpEq, "/home").
    Limit(10).
    FetchUntyped(ctx)
```

### `.SelectAll()`

Selects every column your role is allowed to read. This is the explicit version of `.Fetch()`. It is mutually exclusive with `.Select(...)` and aggregations (`.Count()`, `.Sum()`). For restricted roles, the server expands this to allowed columns rather than a bare `SELECT *`; it never bypasses `deny_columns`/`allow_columns` (see [Access control → Column permissions](/access-control#column-permissions)).

```go
page, err := clicks.SelectAll().Where("country", wavehouse.OpEq, "US").Limit(10).FetchUntyped(ctx)
```

### `.Stream(opts)`

Open a real-time event subscription. See [Streaming](/sdk/go/streaming).

```go
stream := clicks.Stream(&wavehouse.StreamOptions{Since: "2026-01-01T00:00:00Z"})
```

## Query Builder

Returned by `tableRef.Select(...)` or `tableRef.SelectAll()`. Immutable—every chain method returns a new `*QueryBuilder`. Unlike the TypeScript SDK, Go builders do not auto-execute; call `.FetchUntyped(ctx)` or `wavehouse.FetchTyped[Row](ctx, builder)` explicitly:

```go
page, err := clicks.Select("page").Limit(10).FetchUntyped(ctx)
```

### Chain Methods

All methods return a new `*QueryBuilder`; the original remains unchanged.

#### `.Select(...columns)`

Append columns to the SELECT clause. A literal `"*"` is treated as a column named `*`—use `.SelectAll()` for all columns.

```go
q := clicks.Select("page").Select("button") // SELECT page, button
```

#### `.SelectAll()`

Selects every readable column (expanded server-side based on role). Mutually exclusive with `.Select(...)` and aggregations (`.Count()`, `.Sum()`, etc.).

```go
q := clicks.Select().SelectAll().Where("country", wavehouse.OpEq, "US")
```

#### `.Where(column, op, value)`

Add a filter using `FilterOp` constants:

```go
clicks.Select("page").
    Where("score", wavehouse.OpGt, 10).
    Where("page", wavehouse.OpLike, "/home%")
```

| `FilterOp` constant | Backend wire token | Description |
|----------------------|---------------------|--------------|
| `wavehouse.OpEq` | `eq` | Equal |
| `wavehouse.OpNeq` | `neq` | Not equal |
| `wavehouse.OpGt` | `gt` | Greater than |
| `wavehouse.OpGte` | `gte` | Greater than or equal |
| `wavehouse.OpLt` | `lt` | Less than |
| `wavehouse.OpLte` | `lte` | Less than or equal |
| `wavehouse.OpIn` | `in` | Value in array (accepts any Go slice) |
| `wavehouse.OpLike` | `like` | SQL LIKE pattern |
| `wavehouse.OpNotLike` | `not_like` | SQL NOT LIKE — **client-side only**; `/v1/query` rejects this token |

#### Aggregations

```go
clicks.Select("page").
    Count("*", "total").                  // COUNT(*)
    Sum("score", "total_score").          // SUM(score)
    Avg("score", "avg_score").            // AVG(score)
    Min("score", "min_score").            // MIN(score)
    Max("score", "max_score").            // MAX(score)
    CountDistinct("page", "unique_pages").
    Aggregate("uniqExact", "user_id", "unique_users") // allowlisted fn
```

Custom functions via `.Aggregate(fn, column, alias)` are validated server-side (case-insensitive). Allowlist: `count`, `sum`, `avg`, `min`, `max`, `countDistinct`, `uniq`, `uniqExact`, `any`, `anyLast`, `argMin`, `argMax`, `groupArray`, `median`, `quantile`, `stddevPop`, `stddevSamp`, `varPop`, `varSamp`. Others return `400 unsupported aggregation function`.

`Count`/`Sum`/`Avg`/`Min`/`Max`/`CountDistinct` take `(column, alias)`; `Aggregate` takes `(fn, column, alias)`. Empty-alias defaults: `Count` → `count` (and `column=""` becomes `*`); `Sum`/`Avg`/`Min`/`Max` → `sum_<column>`/`avg_<column>`/`min_<column>`/`max_<column>`; `CountDistinct` uses `count_distinct_<column>`. `Aggregate` has no default; pass one or it is sent as `""`.

#### `.GroupBy(...columns)`

```go
clicks.Select("page").Count("", "").GroupBy("page")
```

#### `.OrderBy(column, dir)`

```go
clicks.Select("page").Count("", "total").OrderBy("total", "desc")
```

`dir` defaults to `"asc"` if `""`.

#### `.Limit(n)`

```go
clicks.Select().Limit(100)
```

If unspecified, `wavehouse.DefaultLimit` (1000) is applied. The server also enforces a maximum (`query.default_max_rows`, default 10,000).

#### `.TimeRange(column, since, until)`

Filter by time window. `since`/`until` accept RFC3339 timestamps or relative durations (`"1h"`, `"30m"`, `"7d"`, `"2w"`; day/week suffixes expand to hours, so `"7d"` is `"168h"`). Pass `""` for `until` for open-ended ranges.

```go
clicks.Select("page").TimeRange("received_timestamp", "1h", "")
clicks.Select("page").TimeRange(
    "received_timestamp", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z",
)
```

#### `.CacheTTL(seconds)`

Sets a desired result-cache TTL. **Currently client-side only**; the server derives TTL adaptively from execution time. See [#280](https://github.com/Wave-RF/WaveHouse/issues/280).

```go
clicks.Select("page").Count("", "").CacheTTL(300) // not yet honored server-side — see #280
```

### `wavehouse.FetchTyped[Row](ctx, q)`

Executes the query and decodes rows into `[]Row`.

```go
type PageCount struct {
    Page  string `json:"page"`
    Count int    `json:"total"`
}

page, err := wavehouse.FetchTyped[PageCount](ctx,
    clicks.Select("page").Count("*", "total").GroupBy("page"),
)
// page.Data is []PageCount
```

### `.FetchUntyped(ctx)`

Executes the query and decodes rows into `[]map[string]any`.

```go
page, err := clicks.Select("page").OrderBy("page", "asc").Limit(50).FetchUntyped(ctx)
if err != nil {
    return err
}

if page.HasMore && page.Next != nil {
    page, err = page.Next(ctx) // cursor-based pagination — needs OrderBy
    if err != nil {
        return err
    }
}
```

### `.Stream(opts)`

Opens a live stream from the builder's table with client-side filtering and projection. See [Streaming](/sdk/go/streaming).

### Pagination

`Page[T]`:

```go
type Page[T any] struct {
    Data    []T
    HasMore bool
    Next    func(ctx context.Context) (*Page[T], error) // nil when no cursor is available
}
```

If `Limit` is set and results meet that limit, `HasMore` is `true`. `Next` walks the **first** `.OrderBy()` column using a filter on the last row's value; thus, `Next` requires an explicit `.OrderBy()`. Without one, `Next` is `nil`. If the order column is omitted from `.Select(...)`, `Next` returns an empty page.

The cursor filter is strict (`gt`/`lt` on the first `.OrderBy()` column, no tie-breaker), so rows sharing a boundary value with the last row are skipped. Paginate on a per-row-unique column, or accept dropped ties; the TypeScript SDK's `next()` has the same limitation ([#452](https://github.com/Wave-RF/WaveHouse/issues/452)).

On the untyped path (`FetchUntyped` / `TableRef.Fetch`), JSON numbers decode as `float64`, so integer cursors lose exactness past 2^53 and pagination can repeat or skip a row. `FetchTyped` with an `int64` field, or codegen structs, keep it exact.

```go
page, err := clicks.Select().
    OrderBy("received_timestamp", "desc").
    Limit(100).
    FetchUntyped(ctx)
if err != nil {
    log.Fatal(err)
}

allRows := append([]map[string]any(nil), page.Data...)
for page.HasMore && page.Next != nil {
    page, err = page.Next(ctx)
    if err != nil {
        log.Fatal(err)
    }
    allRows = append(allRows, page.Data...)
}
```

## Raw SQL — `wavehouse.SQL[Row](ctx, client, query)`

Execute a raw SQL query via `/v1/ops/query`. This endpoint is admin-only: JWT tokens must resolve to the admin role (`admin_role`, default `"admin"`). Requests without valid tokens fall back to `default_role` and are rejected unless `default_role` is set to admin (dev-only). Alternatively, an operator key (`Authorization: Operator <key>` or `X-Operator-Key`) authorizes `/v1/ops/*`. Since `Config.Auth` uses `Bearer <token>`, provide a `Config.HTTPClient` with a `Transport` that sets the `X-Operator-Key` header to use an operator key. Use `map[string]any` for dynamic schemas.

```go
rows, err := wavehouse.SQL[map[string]any](ctx, wh,
    "SELECT page, count() AS total FROM clicks GROUP BY page LIMIT 10")

// Or decode into a struct that matches the projected columns/aliases.
// NOTE: this path forwards ClickHouse's own JSON, which QUOTES 64-bit
// integers (count() is UInt64) — decode them with the `,string` tag, or
// use map[string]any. See Reference → Codegen CLI for the full story.
type PageTotal struct {
    Page  string `json:"page"`
    Total uint64 `json:"total,string"`
}
typed, err := wavehouse.SQL[PageTotal](ctx, wh,
    "SELECT page, count() AS total FROM clicks GROUP BY page LIMIT 10")
```

:::note[No parameter binding through the SDK]
Positional `?` substitution is unsupported. The SDK cannot forward ClickHouse named params (`WHERE id = {id:UInt32}` + `param_id=42`) because the proxy blocks arbitrary query-string params and `SQL[Row]` lacks a hook to add them. Use inline literals or the structured query builder (`wh.From(table)...`) for safe binding of user input.
:::

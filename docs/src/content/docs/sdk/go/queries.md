---
title: "Go SDK Queries"
description: "Tables, the chainable query builder, pagination, and raw SQL in the WaveHouse Go SDK."
---

Reading and writing data with `github.com/Wave-RF/WaveHouse/clients/go`: table references, the chainable query builder, cursor pagination, and the admin-only raw-SQL escape hatch. Every request-response operation takes a `context.Context` first and returns `(T, error)`, the chainable builder methods and `.Stream(opts)` excepted — see [Error Handling](/sdk/go#error-handling). The TypeScript SDK covers the same surface on its [Queries](/sdk/queries) page, with a `Result<T>`-returning, `PromiseLike` builder.

## Tables — `client.From(table)`

`From` returns a `*TableRef`. It performs no request, so it is safe to store or pass around.

```go
clicks := wh.From("clicks")
```

### `.Fetch(ctx)`

Shortcut for "select every column" with a default limit of 1000 (`wavehouse.DefaultLimit`) — internally `t.SelectAll().Limit(DefaultLimit).FetchUntyped(ctx)`. There is no options struct as in the TypeScript SDK's `.fetch(opts?)`: to override the limit or paginate, chain `.SelectAll().Limit(n).OrderBy(...)` yourself ([Query Builder](#query-builder)). Access-control policies restrict the returned columns, and `.Fetch()` cannot bypass `deny_columns`/`allow_columns` (see [Access control](/access-control#column-permissions)).

```go
page, err := clicks.Fetch(ctx)
if err != nil {
    log.Fatal(err)
}
for _, row := range page.Data {
    fmt.Println(row["page"])
}
```

### `.Insert(ctx, data)`

Inserts one or many rows based on the input type:

- **Map or struct** (excluding slices and `[]byte`): sent as JSON via `POST /v1/ingest?table={table}`. For raw NDJSON, use `.InsertNDJSON`.
- **Any slice** (`[]map[string]any`, `[]ClickRow`, etc.): serialized to NDJSON via reflection and sent as one `application/x-ndjson` request, with per-record outcomes in the result.

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

For batches, `res.OK` is `true` only if every record succeeded (`*res.Failed == 0`); check `res.Failed` and `res.Results` (each an `InsertRecordResult{Index, OK, Duplicate, Error}` with a 1-based `Index`) for partial failures. The returned `error` signals whole-request failures instead (network, `404`, `403`, `503`), and empty slices are no-ops. The server itself is format-agnostic — `POST /v1/ingest` also accepts a raw JSON array or a single object, since `Content-Type` is only a hint ([API reference](/api#post-v1ingesttabletable--ingest-data)).

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

Starts a query builder chain — see [Query Builder](#query-builder) for the chainable methods and how to execute it.

### `.SelectAll()`

The explicit version of `.Fetch()`: selects every column your role may read. Mutually exclusive with `.Select(...)` and aggregations (`.Count()`, `.Sum()`). For restricted roles the server expands it to the allowed columns rather than a bare `SELECT *`, so it never bypasses `deny_columns`/`allow_columns` ([Access control](/access-control#column-permissions)).

```go
page, err := clicks.SelectAll().Where("country", wavehouse.OpEq, "US").Limit(10).FetchUntyped(ctx)
```

### `.Stream(opts)`

Opens a real-time event subscription on the table, e.g. `clicks.Stream(&wavehouse.StreamOptions{Since: "2026-01-01T00:00:00Z"})`. See [Streaming](/sdk/go/streaming).

## Query Builder

Returned by `tableRef.Select(...)` or `tableRef.SelectAll()`. Immutable — every chain method returns a new `*QueryBuilder`, leaving the original unchanged. Unlike the TypeScript SDK, Go builders do not auto-execute; call `.FetchUntyped(ctx)` or `wavehouse.FetchTyped[Row](ctx, builder)` explicitly:

```go
page, err := clicks.Select("page").Limit(10).FetchUntyped(ctx)
```

### Chain Methods

#### `.Select(...columns)`

Append columns to the SELECT clause. A literal `"*"` is treated as a column named `*` — use `.SelectAll()` for all columns.

```go
q := clicks.Select("page").Select("button") // SELECT page, button
```

#### `.SelectAll()`

Same expansion rules as [`tableRef.SelectAll()`](#selectall), from an existing builder.

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

`Count`/`Sum`/`Avg`/`Min`/`Max`/`CountDistinct` take `(column, alias)`; `.Aggregate(fn, column, alias)` runs a custom function, validated server-side (case-insensitively) against the allowlist `count`, `sum`, `avg`, `min`, `max`, `countDistinct`, `uniq`, `uniqExact`, `any`, `anyLast`, `argMin`, `argMax`, `groupArray`, `median`, `quantile`, `stddevPop`, `stddevSamp`, `varPop`, `varSamp` — anything else returns `400 unsupported aggregation function`. With an empty alias, `Count` defaults to `count` (and `column=""` becomes `*`), `Sum`/`Avg`/`Min`/`Max` to `sum_<column>`/`avg_<column>`/`min_<column>`/`max_<column>`, and `CountDistinct` to `count_distinct_<column>`; `Aggregate` has no default and sends `""`.

#### `.GroupBy(...columns)`

Group the result set, as in `clicks.Select("page").Count("", "").GroupBy("page")`.

#### `.OrderBy(column, dir)`

`dir` defaults to `"asc"` if `""`.

```go
clicks.Select("page").Count("", "total").OrderBy("total", "desc")
```

#### `.Limit(n)`

Caps the row count, as in `clicks.Select().Limit(100)`. Defaults to `wavehouse.DefaultLimit` (1000); the server also enforces a maximum (`query.default_max_rows`, default 10,000).

#### `.TimeRange(column, since, until)`

Filter by time window. `since`/`until` accept RFC3339 timestamps or relative durations (`"1h"`, `"30m"`, `"7d"`, `"2w"`; day/week suffixes expand to hours, so `"7d"` is `"168h"`). Pass `""` for `until` for an open-ended range.

```go
clicks.Select("page").TimeRange("received_timestamp", "1h", "")
clicks.Select("page").TimeRange(
    "received_timestamp", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z",
)
```

#### `.CacheTTL(seconds)`

Sets a desired result-cache TTL. **Currently client-side only**; the server derives TTL adaptively from execution time ([#280](https://github.com/Wave-RF/WaveHouse/issues/280)).

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
```

### `.Stream(opts)`

Opens a live stream from the builder's table with client-side filtering and projection. See [Streaming](/sdk/go/streaming).

### Pagination

Both fetch methods return a `Page[T]`:

```go
type Page[T any] struct {
    Data    []T
    HasMore bool
    Next    func(ctx context.Context) (*Page[T], error) // nil when no cursor is available
}
```

`HasMore` is `true` when a `Limit` is set and the results meet it. `Next` walks the **first** `.OrderBy()` column by filtering on the last row's value, so it requires an explicit `.OrderBy()` — without one `Next` is `nil`, and if the order column is missing from `.Select(...)` it returns an empty page. The cursor filter is strict (`gt`/`lt`, no tie-breaker), so rows sharing a boundary value with the last row are skipped: paginate on a per-row-unique column or accept dropped ties, a limitation the TypeScript SDK's `next()` shares ([#452](https://github.com/Wave-RF/WaveHouse/issues/452)). On the untyped path (`FetchUntyped` / `TableRef.Fetch`), JSON numbers decode as `float64`, so integer cursors lose exactness past 2^53 and pagination can repeat or skip a row; `FetchTyped` with an `int64` field, or codegen structs, keep it exact.

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

Executes a raw SQL query via the admin-only `/v1/ops/query`. A JWT must resolve to the admin role (`admin_role`, default `"admin"`); requests without a valid token fall back to `default_role` and are rejected unless that role is admin (dev-only). An operator key authorizes `/v1/ops/*` as well, but since `Config.Auth` always sends `Bearer <token>`, pass it by giving `Config.HTTPClient` a `Transport` that sets the `X-Operator-Key` header ([API authentication](/api#authentication)). Use `map[string]any` for dynamic schemas.

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
Positional `?` substitution is unsupported, and the SDK cannot forward ClickHouse named params (`WHERE id = {id:UInt32}` + `param_id=42`) because the proxy blocks arbitrary query-string params and `SQL[Row]` has no hook to add them. Use inline literals, or the structured query builder (`wh.From(table)...`) for safe binding of user input.
:::

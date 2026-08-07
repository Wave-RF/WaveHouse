---
title: "Go SDK Queries"
description: "Tables, the chainable query builder, pagination, and raw SQL in the WaveHouse Go SDK."
---

Reading and writing data with `github.com/Wave-RF/WaveHouse/clients/go`:
table references, the chainable query builder, cursor pagination, and the
admin-only raw-SQL escape hatch. Every call takes a `context.Context` as its
first argument and returns `(T, error)` — see
[Error Handling](/sdk/go#error-handling). Compare with the TypeScript SDK's
[Queries](/sdk/queries) page, which covers the same surface with a
`Result<T>`-returning, `PromiseLike` builder.

## Tables — `client.From(table)`

`From` returns a `*TableRef` — a reference to a table. It performs no
request by itself, so it's safe to store in a variable or pass around.

```go
clicks := wh.From("clicks")
```

### `.Fetch(ctx)`

Shortcut for "select every column", with a default limit of 1000
(`wavehouse.DefaultLimit`). Internally it's
`t.SelectAll().Limit(DefaultLimit).FetchUntyped(ctx)` — unlike the
TypeScript SDK's `.fetch(opts?)`, there's no options struct to override the
limit or attach anything per-call; chain `.SelectAll().Limit(n)` yourself
(see [Query Builder](#query-builder)) if you need a different limit.

When an access-control policy restricts your role's columns, the server
returns only the columns your role is allowed to read — `.Fetch()` is never
a way around `deny_columns`/`allow_columns` (see
[Access control](/access-control#column-permissions)).

```go
page, err := clicks.Fetch(ctx)
if err != nil {
    log.Fatal(err)
}
for _, row := range page.Data {
    fmt.Println(row["page"])
}
```

To paginate, use the query builder with an explicit `.OrderBy()` instead —
see [Pagination](#pagination).

### `.Insert(ctx, data)`

Insert one row or many. What you pass determines the wire format:

- A single **map or struct** (anything that isn't a slice, and isn't
  `[]byte`) is sent as JSON: `POST /v1/ingest?table={table}`.
- **Any slice** — `[]map[string]any`, a generated/user-defined row type like
  `[]ClickRow`, etc. — is serialized to NDJSON (one record per line, via
  reflection for non-`[]map[string]any` slices) and sent as a single
  `application/x-ndjson` request, so a bad record doesn't fail or hide the
  rest of the batch. Per-record outcomes come back in the result.

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

For a batch insert, `res.OK` is `true` only when every record succeeded
(`*res.Failed == 0`). Inspect `res.Failed` and `res.Results` (each
`InsertRecordResult{Index, OK, Duplicate, Error}`, 1-based `Index`) for
partial failures — the returned `error` is reserved for whole-request
failures (network, `404` unknown table, `403` forbidden, `503`
backpressure). An empty slice is a no-op and sends no request.

> The server itself is format-agnostic: `POST /v1/ingest` also accepts a raw
> JSON array or a single object directly (the `Content-Type` is only a
> hint), so non-SDK clients can send whichever shape is convenient. See the
> [API reference](/api#post-v1ingesttabletable--ingest-data).

### `.InsertNDJSON(ctx, ndjson)`

Insert pre-formatted NDJSON you already have, as a plain `string` — a file
you've read, or a string you built yourself — without first parsing it into
Go values. Returns the same per-record summary as a slice `Insert`.

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

Fetch the table's column definitions from ClickHouse. Admin-only.

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

Start a query that selects **every column your role is allowed to read** —
the explicit form of what `.Fetch()` does. Mutually exclusive with
`.Select(...)` and with aggregations (`.Count()`, `.Sum()`, etc.); the
server expands it to your allowed columns (never a raw `SELECT *`) and never
bypasses `deny_columns`/`allow_columns`. See
[Access control → Column permissions](/access-control#column-permissions).

```go
page, err := clicks.SelectAll().Where("country", wavehouse.OpEq, "US").Limit(10).FetchUntyped(ctx)
```

### `.Stream(opts)`

Open a real-time event subscription. See [Streaming](/sdk/go/streaming).

```go
stream := clicks.Stream(&wavehouse.StreamOptions{Since: "2026-01-01T00:00:00Z"})
```

---

## Query Builder

Returned by `tableRef.Select()`. Immutable — every chain method returns a
new `*QueryBuilder`, so intermediate values can be reused safely. Unlike the
TypeScript SDK's `PromiseLike` builder, a Go `*QueryBuilder` doesn't
auto-execute — call `.FetchUntyped(ctx)` or the package-level
`wavehouse.FetchTyped[Row](ctx, builder)` explicitly:

```go
page, err := clicks.Select("page").Limit(10).FetchUntyped(ctx)
```

### Chain Methods

All methods return a new `*QueryBuilder` — the original is unchanged.

#### `.Select(...columns)`

Append columns to the SELECT clause. A literal `"*"` is the column *named*
`*`, not a wildcard — use `.SelectAll()` for all columns.

```go
q := clicks.Select("page").Select("button") // SELECT page, button
```

#### `.SelectAll()`

Select every column your role may read (the all-columns wildcard, expanded
server-side to your allowed columns). Mutually exclusive with `.Select(...)`
and with aggregations (`.Count()`, `.Sum()`, etc.).

```go
q := clicks.Select().SelectAll().Where("country", wavehouse.OpEq, "US")
```

#### `.Where(column, op, value)`

Add a filter condition, using the `FilterOp` constants:

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
| `wavehouse.OpIn` | `in` | Value in array — accepts a Go slice of any element type (`[]string`, `[]int`, `[]any`, ...) |
| `wavehouse.OpLike` | `like` | SQL LIKE pattern |
| `wavehouse.OpNotLike` | — | SQL NOT LIKE — **client-side only** (live-query / stream filtering); the `/v1/query` backend rejects it |

#### Aggregations

```go
clicks.Select("page").
    Count("*", "total").                  // COUNT(*)
    Sum("score", "total_score").          // SUM(score)
    Avg("score", "avg_score").            // AVG(score)
    Min("score", "min_score").            // MIN(score)
    Max("score", "max_score").            // MAX(score)
    CountDistinct("page", "unique_pages").
    Aggregate("uniqExact", "user_id", "unique_users") // custom fn
```

`Count`/`Sum`/`Avg`/`Min`/`Max`/`CountDistinct` take `(column, alias
string)`; `Aggregate` takes `(fn, column, alias string)`. Empty-alias
defaults: `Count` → `count` (and `column=""` becomes `*`); `Sum`/`Avg`/
`Min`/`Max` → `sum_<column>`/`avg_<column>`/`min_<column>`/`max_<column>`;
`CountDistinct` → `count_distinct_<column>`. `Aggregate` has **no** alias
default — pass one explicitly or the query is sent with `"alias": ""`.

#### `.GroupBy(...columns)`

```go
clicks.Select("page").Count("", "").GroupBy("page")
```

#### `.OrderBy(column, dir)`

```go
clicks.Select("page").Count("", "total").OrderBy("total", "desc")
```

`dir` defaults to `"asc"` when passed as `""`.

#### `.Limit(n)`

```go
clicks.Select().Limit(100)
```

If no limit is specified, `wavehouse.DefaultLimit` (1000) is applied
automatically to prevent unbounded result sets. The server also enforces the
configured maximum (`query.default_max_rows`, default 10,000 rows).

#### `.TimeRange(column, since, until)`

Filter by a time window. `since` and `until` accept RFC3339 timestamps or
relative durations (`"1h"`, `"30m"`, `"7d"`, `"2w"` — day and week suffixes
expand to hours, so `"7d"` is `"168h"`). Pass `""` for `until` to leave it
open-ended.

```go
clicks.Select("page").TimeRange("received_timestamp", "1h", "")
clicks.Select("page").TimeRange(
    "received_timestamp", "2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z",
)
```

#### `.CacheTTL(seconds)`

Records a desired result-cache TTL on the builder. **Currently client-side
state only** — the value is never sent to the server, which derives each
result's cache TTL adaptively from query execution time. Wiring it through
the wire format is tracked in
[#280](https://github.com/Wave-RF/WaveHouse/issues/280).

```go
clicks.Select("page").Count("", "").CacheTTL(300) // not yet honored server-side — see #280
```

### `wavehouse.FetchTyped[Row](ctx, q)`

Execute the query and decode rows into `[]Row`. Package-level generic
function (Go has no generic methods) — takes the builder as its argument.

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

Execute the query and decode rows into `[]map[string]any`. The ordinary
(non-generic) method form of `FetchTyped`.

```go
page, err := clicks.Select("page").OrderBy("page", "asc").Limit(50).FetchUntyped(ctx)

if page.HasMore && page.Next != nil {
    page2, err := page.Next(ctx) // cursor-based pagination — needs OrderBy
}
```

### `.Stream(opts)`

Open a live stream from the builder's table, applying `.Where()`/`.Select()`
filters and column projection client-side. See
[Streaming](/sdk/go/streaming).

### Pagination

`Page[T]`:

```go
type Page[T any] struct {
    Data    []T
    HasMore bool
    Next    func(ctx context.Context) (*Page[T], error) // nil when no cursor is available
}
```

When `Limit` is set and the result contains at least that many rows,
`HasMore` is `true`. Cursor-based pagination's `Next` walks the **first**
`.OrderBy()` column — it adds a filter on that column using the last row's
value — so `Next` is only attached when the query has an explicit
`.OrderBy()`. With no order column the result still reports `HasMore`
honestly, but `Next` is `nil` (there is no deterministic cursor to build) —
add an `.OrderBy()` to paginate. If the order column was left out of an
explicit `.Select(...)` projection, `Next` quietly returns an empty page
instead of erroring (there is no cursor value to read).

One precision caveat on the untyped path (`FetchUntyped` / `TableRef.Fetch`):
rows decode into `map[string]any`, where JSON numbers become `float64`, so an
integer cursor column loses exactness past 2^53 and pagination can repeat or
skip a row at that scale — the same ceiling the TypeScript SDK has with JS
numbers. `FetchTyped` with an `int64` field keeps the cursor exact, and
codegen structs are unaffected (their 64-bit integer columns are `string`).

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

---

## Raw SQL — `wavehouse.SQL[Row](ctx, client, query)`

Execute a raw SQL query. `/v1/admin/query` is admin-only: for JWT callers,
the token must resolve to the policy admin role (`admin_role`, `"admin"` by
default) — a JWT request with no token, or an invalid/expired one, falls
back to the `default_role` and is rejected. Alternatively, a configured
operator key (`Authorization: Operator <key>` or `X-Operator-Key`)
authorizes `/v1/admin/*` without a JWT. Package-level generic function — use
`map[string]any` for a dynamic/unknown schema.

```go
rows, err := wavehouse.SQL[map[string]any](ctx, wh,
    "SELECT page, count() AS total FROM clicks GROUP BY page LIMIT 10")

// Or decode into a struct that matches the projected columns/aliases:
type PageTotal struct {
    Page  string `json:"page"`
    Total int    `json:"total"`
}
typed, err := wavehouse.SQL[PageTotal](ctx, wh,
    "SELECT page, count() AS total FROM clicks GROUP BY page LIMIT 10")
```

:::note[No parameter binding through the SDK]
Positional `?` substitution is not supported, and the SDK has no way to
forward ClickHouse-style named params (the `WHERE id = {id:UInt32}` +
`param_id=42` query-string combo) — the proxy doesn't forward arbitrary
query-string params and `SQL[Row]` doesn't expose a hook to add them. Inline
literals into the SQL, or — for safe binding from user-supplied input — use
the structured query builder (`wh.From(table)...`).
:::

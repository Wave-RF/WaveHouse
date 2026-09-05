---
title: "SDK Queries"
description: "Tables, the chainable query builder, pagination, and raw SQL in @wavehouse/sdk."
---

Reading and writing data with `@wavehouse/sdk`: table references, the chainable query builder, cursor pagination, and the admin-only raw-SQL escape hatch. Every call returns the SDK's [`Result<T>`](/sdk#result-type) — nothing throws for anything the server returns (see [Error Handling](/sdk/reference#error-handling) for the caller and environment errors that do). Examples import from `@wavehouse/sdk` or `https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

## Tables — `wh.from(table)`

`from()` returns a `TableRef` — a reference to a table. It is **NOT thenable**, so it's safe to pass around or store in a variable without triggering requests.

```ts
const clicks = wh.from('clicks');
```

### `.fetch(opts?)`

Shortcut for "select every column", with a default limit of 1000. When an access-control policy restricts your role's columns, the server returns only the columns your role is allowed to read — `.fetch()` is never a way around `deny_columns`/`allow_columns` (see [Access control](/access-control#column-permissions)).

To paginate, chain an explicit `.orderBy()` — a bare `.fetch()` sends no default order (see [Pagination](#pagination)). Ordering, grouping, or filtering by a column your role can't read is rejected, so a column-restricted role must reference only readable columns in those clauses.

```ts
const { data, error, hasMore, next } = await clicks.fetch();
const { data } = await clicks.fetch({ limit: 50, signal: controller.signal });
```

### `.insert(data, opts?)`

Insert one row or many. A single object is sent as a JSON `POST /v1/ingest?table={table}`. An **array** is serialized to NDJSON (one record per line) and sent as a single `application/x-ndjson` request, so a bad record no longer fails or hides the rest of the batch — per-record outcomes come back in the result.

```ts
// Single row → { ok: true } (or { ok: true, duplicate: true } when dedup skips it)
const { data, error } = await clicks.insert({ page: '/home', button: 'cta' });

// Many rows → one NDJSON request, per-record summary
const { data } = await clicks.insert([
  { page: '/home', button: 'cta' },
  { page: '/about', button: 'nav' },
]);
// data: { ok, total, succeeded, failed, duplicates, results? }
```

For an array insert, `data.ok` is `true` only when every record succeeded (`failed === 0`). Inspect `data.failed` and `data.results` (each `{ index, ok|duplicate|error }`, 1-based `index`) for partial failures — the call's top-level `error` is reserved for whole-request failures (network, `404` unknown table, `403` forbidden, `503` backpressure). An empty array is a no-op and sends no request. The array path sends one request regardless of size, so it is bound by the same 16 MiB [request-body cap](/reverse-proxy#request-body-size-limits); bounded-concurrency chunking of very large arrays is tracked in [#196](https://github.com/Wave-RF/WaveHouse/issues/196).

> `POST /v1/ingest` also accepts a raw JSON array or a single object directly, so non-SDK clients can send whichever shape is convenient — but the `Content-Type` is **required** and decides the format (`application/json` or `application/x-ndjson`); a request without one is rejected with `415`. The SDK always sets it. See the [API reference](/api#post-v1ingesttabletable--ingest-data).

### `.insertNDJSON(source, opts?)`

Insert pre-formatted NDJSON you already have — a `.ndjson` file, a byte stream, or a string — without first parsing it into objects. Accepts a `string`, `Uint8Array`, `Blob`/`File`, or `ReadableStream<Uint8Array>`; non-string sources are read fully into memory before sending. The server buffers the whole body too, so the 16 MiB [request-body cap](/reverse-proxy#request-body-size-limits) applies to NDJSON exactly as to any other shape — an upload over it is a `413` with nothing ingested; split it across several calls. Returns the same per-record summary as an array `insert`. See [Batch Ingest](/api#batch-ingest).

```ts
// From a string
await clicks.insertNDJSON('{"page":"/a"}\n{"page":"/b"}\n');

// From a browser <input type="file"> (a File is a Blob)
await clicks.insertNDJSON(fileInput.files[0]);

// From a Node file (fs.openAsBlob; or read it to a string)
import { openAsBlob } from 'node:fs';
await clicks.insertNDJSON(await openAsBlob('events.ndjson'));
```

### `.schema(opts?)`

Fetch the table's column definitions from ClickHouse. `.schema()` hits `/v1/ops/schema`, an **admin-only** endpoint: the caller must pass the admin gate — resolve to the policy admin role or present the non-JWT [operator key](/api#authentication) via [`options.headers`](/sdk#custom-headers) — or this returns `403`.

```ts
const { data } = await clicks.schema();
// data: { name: 'clicks', columns: [
//   { name: 'page', type: 'String', is_nullable: false, has_default: false, position: 1 }, ...
//   { name: 'received_timestamp', type: "DateTime64(3, 'UTC')", is_nullable: false,
//     has_default: true, default_kind: 'DEFAULT',
//     default_expression: "now64(3, 'UTC')", position: 4 }
// ] }
// `position` is the declaration ordinal; `default_kind`/`default_expression`
// appear only when the column declares a default. A MATERIALIZED or ALIAS
// column is computed by ClickHouse and cannot be inserted.
```

### `.select(...columns)`

Start a query builder chain. See [Query Builder](#query-builder).

```ts
const { data } = await clicks.select('page', 'button').where('page', '=', '/home').limit(10);
```

### `.selectAll()`

Selects **every column your role is allowed to read** — the explicit form of what a bare `.fetch()` does. Mutually exclusive with `.select(...)` and with aggregations (`.count()`, `.sum()`, etc.); the server expands it to your allowed columns (never a raw `SELECT *`) and never bypasses `deny_columns`/`allow_columns`. See [Access control → Column permissions](/access-control#column-permissions).

```ts
const { data } = await clicks.selectAll().where('country', '=', 'US').limit(10);
```

### `.stream(opts?)`

Open a real-time event subscription. See [Streaming](/sdk/streaming).

```ts
const stream = clicks.stream({ since: '2026-01-01T00:00:00Z' });
```

---

## Query Builder

Returned by `tableRef.select()`. Immutable — every chain method returns a new `QueryBuilder`. The builder is **PromiseLike**, so `await builder` auto-executes `.fetch()`.

```ts
// These are equivalent:
const result = await clicks.select('page').limit(10).fetch();
const result = await clicks.select('page').limit(10); // PromiseLike shortcut
```

### Chain Methods

All methods return a new `QueryBuilder` — the original is unchanged.

#### `.select(...columns)`

Append columns to the SELECT clause. A literal `'*'` is the column *named* `*`, not a wildcard — use `.selectAll()` for all columns.

```ts
const q = clicks.select('page').select('button'); // SELECT page, button
```

#### `.selectAll()`

Select every column your role may read (the all-columns wildcard, expanded server-side to your allowed columns). Mutually exclusive with `.select(...)` and with aggregations (`.count()`, `.sum()`, etc.).

```ts
const q = clicks.selectAll().where('country', '=', 'US');
```

#### `.where(column, op, value)`

Add a filter condition. SDK operators are translated to backend format.

```ts
clicks.select('page').where('score', '>', 10).where('page', 'like', '/home%')
```

| SDK Operator | Backend | Description |
|-------------|---------|-------------|
| `'='` | `eq` | Equal |
| `'!='` | `neq` | Not equal |
| `'>'` | `gt` | Greater than |
| `'>='` | `gte` | Greater than or equal |
| `'<'` | `lt` | Less than |
| `'<='` | `lte` | Less than or equal |
| `'in'` | `in` | Value in array |
| `'like'` | `like` | SQL LIKE pattern. Case-**sensitive** on `/v1/query`; the client-side filter used by `.stream()` / `.liveQuery()` matches case-**insensitively** |
| `'not_like'` | — | SQL NOT LIKE — **client-side only** (live-query / stream filtering); the `/v1/query` backend rejects it |

#### Aggregations

```ts
clicks.select('page')
  .count('*', 'total')           // COUNT(*)
  .sum('score', 'total_score')   // SUM(score)
  .avg('score', 'avg_score')     // AVG(score)
  .min('score', 'min_score')     // MIN(score)
  .max('score', 'max_score')     // MAX(score)
  .countDistinct('page', 'unique_pages')
  .aggregate('uniqExact', 'user_id', 'unique_users') // custom fn
```

Each aggregation method signature: `(column: string, alias?: string)`. `count()` defaults to `column='*'`, `alias='count'`.

#### `.groupBy(...columns)`

```ts
clicks.select('page').count().groupBy('page')
```

#### `.orderBy(column, dir?)`

```ts
clicks.select('page').count('*', 'total').orderBy('total', 'desc')
```

`dir` defaults to `'asc'`.

#### `.limit(n)`

```ts
clicks.select().limit(100)
```

If no limit is specified, `QueryBuilder.DEFAULT_LIMIT` (1000) is applied automatically to prevent unbounded result sets. The server also enforces the configured maximum (`query.default_max_rows`, default 10,000 rows).

#### `.timeRange(column, since, until?)`

Filter by a time window. `since` and `until` accept RFC3339 timestamps or relative durations (`'1h'`, `'30m'`, `'7d'`, `'2w'` — day and week suffixes expand to hours, so `'7d'` is `'168h'`).

```ts
clicks.select('page').timeRange('received_timestamp', '1h')
clicks.select('page').timeRange(
  'received_timestamp', '2026-01-01T00:00:00Z', '2026-02-01T00:00:00Z'
)
```

#### `.cacheTTL(seconds)`

Records a desired result-cache TTL on the builder. **Currently client-side state only** — the value is never sent to the server, which derives each result's cache TTL adaptively from query execution time. Wiring it through the wire format is tracked in [#280](https://github.com/Wave-RF/WaveHouse/issues/280).

```ts
clicks.select('page').count().cacheTTL(300) // not yet honored server-side — see #280
```

### `.fetch(opts?)`

Execute the query. Returns `Result<Row[]>` with optional pagination.

```ts
const { data, error, hasMore, next } = await clicks.select('page').limit(50).fetch();

if (hasMore && next) {
  const page2 = await next(); // cursor-based pagination
}
```

**Options** — `RequestOptions`:

| Field | Type | Description |
|-------|------|-------------|
| `signal` | `AbortSignal` | Cancel the request |
| `limit` | `number` | Override builder limit for this fetch |

A pipe's `.fetch()` takes the narrower `PipeRequestOptions` instead — see [Pipes](/sdk/pipes#fetchopts).

### `.stream(opts?)`

Open a live stream from the builder's table. See [Streaming](/sdk/streaming).

### Pagination

When `limit` is set and the result contains at least `limit` rows, `hasMore` is `true`. Cursor-based pagination's `next()` walks an **order column** — it adds a filter on that column using the last row's value — so `next()` is only attached when the query has an explicit `.orderBy()`. With no order column the result still reports `hasMore` honestly, but `next` is `undefined` (there is no deterministic cursor to build) — add an `.orderBy()` to paginate.

```ts
let result = await clicks.select().orderBy('received_timestamp', 'desc').limit(100).fetch();

const allRows = [...result.data!];
while (result.hasMore && result.next) {
  result = await result.next();
  if (result.data) allRows.push(...result.data);
}
```

---

## Raw SQL — `wh.sql(query, opts?)`

Execute a raw SQL query. `/v1/ops/query` is admin-only: the caller must resolve to the policy admin role (`admin_role`, `"admin"` by default). A tokenless request falls back to the `default_role`, so it is rejected with `403` on any policy that doesn't deliberately set `default_role` to the admin role (a loudly-warned dev-only setting); an invalid or expired token is rejected with `401`. The SDK has no first-class option for the server's non-JWT [operator key](/api#authentication) — an operator can send its `X-Operator-Key` header via [`options.headers`](/sdk#custom-headers).

```ts
const { data, error } = await wh.sql('SELECT page, count() FROM clicks GROUP BY page LIMIT 10');
```

:::note[No parameter binding through the SDK]
Positional `?` substitution is not supported, and the SDK has no way to forward ClickHouse-style named params (the `WHERE id = {id:UInt32}` + `param_id=42` query-string combo) — the proxy doesn't forward arbitrary query-string params and `wh.sql()` doesn't expose a hook to add them. Inline literals into the SQL, or — for safe binding from user-supplied input — use the structured query builder (`wh.from(table)…`).
:::

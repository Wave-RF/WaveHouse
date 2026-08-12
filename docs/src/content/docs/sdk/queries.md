---
title: "SDK Queries"
description: "Tables, the chainable query builder, pagination, and raw SQL in @wavehouse/sdk."
---

Reading and writing data with `@wavehouse/sdk`: table references, the
chainable query builder, cursor pagination, and the admin-only raw-SQL
escape hatch. Every call returns the SDK's
[`Result<T>`](/sdk#result-type) — nothing throws for anything the server
returns (see [Error Handling](/sdk/reference#error-handling) for the caller and
environment errors that do).
Examples import from `@wavehouse/sdk`; using the CDN instead, import from
`https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

## Tables — `wh.from(table)`

`from()` returns a `TableRef`. **NOT thenable** — safe to store without triggering requests.

```ts
const clicks = wh.from('clicks');
```

### `.fetch(opts?)`

Shortcut for "select every column", default limit 1000. Server-side `deny_columns`/`allow_columns` policies restrict returned columns and `.fetch()` cannot bypass them ([Access control](/access-control#column-permissions)).

Chain `.orderBy()` to paginate — bare `.fetch()` sends no default order (see [Pagination](#pagination)). Roles may only reference readable columns when ordering, grouping, or filtering.

```ts
const { data, error, hasMore, next } = await clicks.fetch();
const { data } = await clicks.fetch({ limit: 50, signal: controller.signal });
```

### `.insert(data, opts?)`

Insert one or many rows. Single objects use JSON `POST /v1/ingest?table={table}`. Arrays serialize to NDJSON in one `application/x-ndjson` request returning per-record outcomes, so bad records don't fail the batch.

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

For arrays, `data.ok` is `true` only if `failed === 0`. Use `data.failed` and `data.results` (each `{ index, ok|duplicate|error }`, 1-based `index`) for partial failures. Top-level `error` covers whole-request failures (network, `404` unknown table, `403` forbidden, `503` backpressure). Empty arrays are no-ops; chunking large arrays is tracked in [#196](https://github.com/Wave-RF/WaveHouse/issues/196).

> The server is format-agnostic: `POST /v1/ingest` also accepts a raw JSON array or a single object (`Content-Type` is only a hint). See the [API reference](/api#post-v1ingesttabletable--ingest-data).

### `.insertNDJSON(source, opts?)`

Insert pre-formatted NDJSON (`string`, `Uint8Array`, `Blob`/`File`, or `ReadableStream<Uint8Array>`) without parsing into objects. Non-string sources are read fully into memory. Returns the same per-record summary as array `insert`.

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

Fetch table column definitions from ClickHouse.

```ts
const { data } = await clicks.schema();
// data: { name: 'clicks', columns: [
//   { name: 'page', type: 'String', is_nullable: false, has_default: false }, ...
// ] }
```

### `.select(...columns)`

Start a query builder chain. See [Query Builder](#query-builder).

```ts
const { data } = await clicks.select('page', 'button').where('page', '=', '/home').limit(10);
```

### `.selectAll()`

Selects every column your role may read — the explicit `.fetch()`. Mutually exclusive with `.select(...)` and aggregations (`.count()`, `.sum()`, etc.). The server expands it to allowed columns, never a raw `SELECT *` ([Column permissions](/access-control#column-permissions)).

```ts
const { data } = await clicks.selectAll().where('country', '=', 'US').limit(10);
```

### `.stream(opts?)`

Open a real-time event subscription. See [Streaming](/sdk/streaming).

```ts
const stream = clicks.stream({ since: '2026-01-01T00:00:00Z' });
```

## Query Builder

Returned by `tableRef.select()`. Immutable — every chain method returns a new `QueryBuilder`. **PromiseLike**: `await builder` auto-executes `.fetch()`.

```ts
// These are equivalent:
const result = await clicks.select('page').limit(10).fetch();
const result = await clicks.select('page').limit(10); // PromiseLike shortcut
```

### Chain Methods

All return a new `QueryBuilder`; the original is unchanged.

#### `.select(...columns)`

Append columns to the SELECT clause. A literal `'*'` is a column *named* `*`; use `.selectAll()` for all columns.

```ts
const q = clicks.select('page').select('button'); // SELECT page, button
```

#### `.selectAll()`

Selects every column your role may read (expanded server-side). Mutually exclusive with `.select(...)` and aggregations (`.count()`, `.sum()`, etc.).

```ts
const q = clicks.selectAll().where('country', '=', 'US');
```

#### `.where(column, op, value)`

Add a filter condition. SDK operators translate to backend format.

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
| `'like'` | `like` | SQL LIKE pattern |
| `'not_like'` | — | SQL NOT LIKE — **client-side only** (live-query/stream); `/v1/query` rejects it |

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

Signature: `(column: string, alias?: string)`. `count()` defaults to `column='*'`, `alias='count'`.

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

Defaults to `QueryBuilder.DEFAULT_LIMIT` (1000); server max is `query.default_max_rows` (10,000).

#### `.timeRange(column, since, until?)`

Filter by time window. `since` and `until` take RFC3339 timestamps or relative durations (`'1h'`, `'30m'`, `'7d'`, `'2w'`; day/week suffixes expand to hours, so `'7d'` is `'168h'`).

```ts
clicks.select('page').timeRange('received_timestamp', '1h')
clicks.select('page').timeRange(
  'received_timestamp', '2026-01-01T00:00:00Z', '2026-02-01T00:00:00Z'
)
```

#### `.cacheTTL(seconds)`

Desired result-cache TTL. **Client-side only**; the server derives TTL adaptively from execution time. See [#280](https://github.com/Wave-RF/WaveHouse/issues/280).

```ts
clicks.select('page').count().cacheTTL(300) // not yet honored server-side — see #280
```

### `.fetch(opts?)`

Execute query. Returns `Result<Row[]>` with optional pagination.

```ts
const { data, error, hasMore, next } = await clicks.select('page').limit(50).fetch();

if (hasMore && next) {
  const page2 = await next(); // cursor-based pagination
}
```

**Options:**

| Field | Type | Description |
|-------|------|-------------|
| `signal` | `AbortSignal` | Cancel request |
| `limit` | `number` | Override builder limit for this fetch |

### `.stream(opts?)`

Open a live stream from the table. See [Streaming](/sdk/streaming).

### Pagination

`hasMore` is `true` when results meet the set `limit`. Cursor pagination's `next()` needs an explicit `.orderBy()`; without it `next` is `undefined` though `hasMore` stays true.

```ts
let result = await clicks.select().orderBy('received_timestamp', 'desc').limit(100).fetch();

const allRows = [...result.data!];
while (result.hasMore && result.next) {
  result = await result.next();
  if (result.data) allRows.push(...result.data);
}
```

## Raw SQL — `wh.sql(query, opts?)`

Execute raw SQL. `/v1/admin/query` needs a JWT resolving to the policy admin role (`admin_role`, default `"admin"`). Missing, invalid, or expired tokens fall back to `default_role` and are rejected.

```ts
const { data, error } = await wh.sql('SELECT page, count() FROM clicks GROUP BY page LIMIT 10');
```

:::note[No parameter binding through the SDK]
Positional `?` substitution is unsupported. The SDK can't forward ClickHouse named params (`WHERE id = {id:UInt32}` + `param_id=42`): the proxy blocks arbitrary query-string params and `wh.sql()` has no hook for them. Use inline literals or the structured builder (`wh.from(table)…`) to bind user input safely.
:::

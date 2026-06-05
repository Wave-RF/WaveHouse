---
title: "TypeScript SDK"
description: "Zero-dependency client SDK — query builder, real-time streaming, codegen."
sidebar:
  order: 8
---

`@wavehouse/sdk` — Zero-dependency TypeScript client for WaveHouse.

## Installation

```bash
npm install @wavehouse/sdk
```

## Quick Start

```ts
import { createClient } from '@wavehouse/sdk';

const wh = createClient({
  baseURL: 'http://localhost:8080',
  auth: async () => getAccessToken(), // omit for public/unauthenticated
});

// Query
const { data, error } = await wh.from('clicks').select('page').limit(10);

// Insert
await wh.from('clicks').insert({ page: '/home', button: 'signup' });

// Stream
const stream = wh.from('clicks').stream();
const unsub = stream.subscribe({
  next: (event) => console.log(event.data),
  status: (s) => console.log('Stream:', s),
});
```

## Creating a Client

```ts
import { createClient } from '@wavehouse/sdk';
import type { Database } from './my-types'; // optional hand-written types

const wh = createClient<Database>({
  baseURL: 'https://wavehouse.example.com',
  auth: async () => myAuthProvider.getToken(),
  options: {
    maxRetries: 2,
  },
});
```

### `ClientConfig<DB>`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `baseURL` | `string` | — | WaveHouse server URL (required) |
| `auth` | `() => Promise<string> \| string` | — | Token provider. Omit for public access |
| `options.maxRetries` | `number` | `2` | Retry attempts for failed/5xx requests |

> **How the token is transmitted.** The SDK attaches your `auth` token as an `Authorization: Bearer` header on REST calls, and — because the browser `EventSource` API can't set headers — as a `?token=` query parameter on streaming connections. When both are present the server reads the header in preference to the query parameter, and strips the `?token=` value from the URL after extraction so it can't leak into logs.

### Type-Safe Tables

Pass a `Database` type to get autocomplete on table names and row types:

```ts
interface Database {
  clicks: { page: string; button: string; score: number; received_timestamp: string };
  users: { id: string; name: string; email: string };
}

const wh = createClient<Database>({ baseURL: '...' });
const clicks = wh.from('clicks'); // ✅ autocomplete
const { data } = await clicks.select('page', 'button').limit(10);
// data is Array<{ page: string; button: string; score: number; received_timestamp: string }> | null
```

---

## Result Type

Every async SDK operation returns `Result<T>` — a discriminated union that never throws:

```ts
type Result<T> =
  | { ok: true;  data: T;    error: null; hasMore?: boolean; next?: () => Promise<Result<T>> }
  | { ok: false; data: null; error: WaveHouseError }

interface WaveHouseError {
  status: number;     // HTTP status (0 for network errors)
  code: string;       // e.g. 'HTTP_400', 'NETWORK_ERROR', 'ABORTED'
  message: string;    // Human-readable error message
  details?: unknown;  // Raw response body
  retryable: boolean; // Whether SDK would retry this error
}
```

Usage pattern — branch on `result.ok` (or destructure `{ data, error }` if you prefer):

```ts
const result = await wh.from('clicks').select('page').limit(10);
if (result.ok) {
  console.log(result.data); // Row[] — TypeScript knows data is non-null here
} else {
  console.error(result.error.message); // never throws
}
```

---

## Tables — `wh.from(table)`

`from()` returns a `TableRef` — a reference to a table. It is **NOT thenable**, so it's safe to pass around or store in a variable without triggering requests.

```ts
const clicks = wh.from('clicks');
```

### `.fetch(opts?)`

Shortcut for `SELECT *` with a default limit of 1000.

```ts
const { data, error, hasMore, next } = await clicks.fetch();
const { data } = await clicks.fetch({ limit: 50, signal: controller.signal });
```

### `.insert(data, opts?)`

Insert one row or multiple rows. Each row is sent as a separate `POST /v1/ingest?table={table}`.

```ts
// Single row
const { data, error } = await clicks.insert({ page: '/home', button: 'cta' });
// data: { ok: true } or { ok: true, duplicate: true }

// Multiple rows
const { error } = await clicks.insert([
  { page: '/home', button: 'cta' },
  { page: '/about', button: 'nav' },
]);
```

### `.schema(opts?)`

Fetch the table's column definitions from ClickHouse.

```ts
const { data } = await clicks.schema();
// data: { name: 'clicks', columns: [{ name: 'page', type: 'String', is_nullable: false, has_default: false }, ...] }
```

### `.select(...columns)`

Start a query builder chain. See [Query Builder](#query-builder).

```ts
const { data } = await clicks.select('page', 'button').where('page', '=', '/home').limit(10);
```

### `.stream(opts?)`

Open a real-time event subscription. See [Streaming](#streaming).

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

Append columns to the SELECT clause.

```ts
const q = clicks.select('page').select('button'); // SELECT page, button
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
| `'like'` | `like` | SQL LIKE pattern |
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

Each aggregation method signature: `(column: string, alias?: string)`.  
`count()` defaults to `column='*'`, `alias='count'`.

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

If no limit is specified, `QueryBuilder.DEFAULT_LIMIT` (1000) is applied automatically to prevent unbounded result sets. The server also enforces a maximum of 10,000 rows (`DefaultMaxRows`).

#### `.timeRange(column, since, until?)`

Filter by a time window. `since` accepts RFC3339 timestamps or relative durations (`'1h'`, `'30m'`, `'7d'`).

```ts
clicks.select('page').timeRange('received_timestamp', '1h')
clicks.select('page').timeRange('received_timestamp', '2026-01-01T00:00:00Z', '2026-02-01T00:00:00Z')
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

**Options:**

| Field | Type | Description |
|-------|------|-------------|
| `signal` | `AbortSignal` | Cancel the request |
| `limit` | `number` | Override builder limit for this fetch |

### `.stream(opts?)`

Open a live stream from the builder's table. See [Streaming](#streaming).

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

Execute a raw SQL query. `/v1/admin/query` is admin-only: the caller's JWT must resolve to the policy admin role (`admin_role`, `"admin"` by default). A request with no token, or an invalid/expired one, falls back to the `default_role` and is rejected.

```ts
const { data, error } = await wh.sql('SELECT page, count() FROM clicks GROUP BY page LIMIT 10');
```

> **No parameter binding through the SDK.** Positional `?` substitution is not supported, and the SDK has no way to forward ClickHouse-style named params (the `WHERE id = {id:UInt32}` + `param_id=42` query-string combo) — the proxy doesn't forward arbitrary query-string params and `wh.sql()` doesn't expose a hook to add them. Inline literals into the SQL, or — for safe binding from user-supplied input — use the structured query builder (`wh.from(table)…`).

---

## Named Pipes — `wh.pipe(name, params?)`

Execute a pre-defined named query pipe. Returns a `PipeRef` which is **PromiseLike**.

```ts
// These are equivalent:
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 }).fetch();
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 }); // PromiseLike
```

### `.fetch(opts?)`

Execute and return results.

### `.stream(opts?)`

Open a live stream. See [Streaming](#streaming).

---

## Pipes Admin — `wh.pipes`

Manage named query pipes. Requires the admin role (`policy.admin_role`).

```ts
// List all pipes
const { data: pipes } = await wh.pipes.list();

// Get a single pipe definition
const { data: pipe } = await wh.pipes.get('top_pages');

// Create or update
await wh.pipes.set('top_pages', {
  sql: 'SELECT page, count() as views FROM clicks GROUP BY page LIMIT {{limit}}',
  parameters: [{ name: 'limit', type: 'number', required: false, default: 100 }],
  description: 'Top pages by view count',
  allowed_roles: ['viewer', 'admin'],
});

// Delete
await wh.pipes.delete('old_pipe');
```

---

## Schema — `wh.schema`

Introspect ClickHouse table schemas.

```ts
// List all table schemas
const { data: schemas } = await wh.schema.list();
// schemas: { clicks: { name: 'clicks', columns: [...] }, users: { ... } }

// Force refresh from ClickHouse
await wh.schema.refresh();
```

Individual table schema is also available via `wh.from('clicks').schema()`.

> `wh.schema.list()`, `wh.schema.refresh()`, and `wh.from(t).schema()` hit `/v1/schema*`, which are **admin-only** endpoints. Against any non-dev policy (anything but `default_role: admin`), construct the client with an admin-role token or these calls return `403`.

---

## Policy — `wh.policy`

Manage Hasura-style access control policies. Requires the admin role (`policy.admin_role`).

```ts
// Get current policy
const { data: policy } = await wh.policy.get();

// Update policy
await wh.policy.set({
  default_role: 'viewer',
  tables: {
    clicks: {
      select: {
        viewer: {
          allow_columns: ['page', 'button', 'received_timestamp'],
          filter: { tenant_id: { _eq: '{{ jwt.app_metadata.tenant_id }}' } },
        },
        admin: { allow_columns: ['*'] },
      },
    },
  },
});

// Validate without applying (dry run)
const { data } = await wh.policy.validate(policyDraft);
// data: { valid: true } or error with validation details
```

---

## DLQ — `wh.dlq`

Dead Letter Queue operations.

```ts
// Get DLQ statistics
const { data } = await wh.dlq.list();
// data: { tables: { "clicks": 3, "users": 0 }, total: 3 }

// Stats for a specific table
const { data } = await wh.dlq.table('clicks');

// Stream DLQ events
const stream = wh.dlq.stream();
```

---

## System — `wh.sys`

Content-free server-online check.

```ts
// health() hits the public, content-free /v1/health route — 200/503, no body.
// Use it to check a server is reachable before sending data.
const result = await wh.sys.health();
if (result.ok) {
  // server is up and past boot
}
// on failure, result.error carries the reason (network vs. server error)
```

> Readiness (`/readyz`) is intentionally **not** exposed through the SDK — it runs a ClickHouse query per call and is a load-balancer / reverse-proxy concern, not the client's. Probe `/readyz` directly from your orchestrator if you need it.

---

## Streaming

Streams use SSE (Server-Sent Events) for both unauthenticated connections and for authenticated ones.

### `StreamController`

Returned by `.stream()` on `TableRef`, `QueryBuilder`, `PipeRef`, and `DLQNamespace`. It is **NOT thenable**.

```ts
const stream = wh.from('clicks').stream({ since: '2026-01-01T00:00:00Z' });
```

### `.subscribe(subscriber)` → `unsubscribe()`

Callback-based consumption. Returns a cleanup function.

```ts
const unsub = stream.subscribe({
  next: (event) => {
    // event: { table: 'clicks', timestamp: '2026-...', data: { page: '/', ... } }
    console.log('New event:', event.data);
  },
  status: (state) => {
    // state: 'connecting' | 'live' | 'reconnecting' | 'closed'
    updateIndicator(state);
  },
  error: (err) => {
    console.error('Stream error:', err.message);
  },
});

// Cleanup — closes the connection if no other subscribers remain
unsub();
```

### Async Iterator

```ts
const stream = wh.from('clicks').stream();

for await (const event of stream) {
  console.log(event.table, event.data);
  if (shouldStop) break; // breaking auto-closes the stream
}
```

### `.close()`

Explicitly close the stream and release all resources.

```ts
stream.close();
```

### `.status`

Current connection status: `'connecting' | 'live' | 'reconnecting' | 'closed'`.

### `StreamOptions`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `since` | `string` | RFC3339 timestamp for gap-fill replay |
| `signal` | `AbortSignal` | Cancel/close the stream. Wired via `attachSignal()` internally. |

### `StreamEvent<T>`

```ts
interface StreamEvent<T> {
  table: string;     // table name (e.g. 'clicks')
  timestamp: string; // received_timestamp (RFC3339Nano)
  data: T;           // row data
}
```

### Transport Behavior

| Transport | Reconnect | Protocol |
| --------- | --------- | -------- |
| SSE | Automatic (native `EventSource` with `Last-Event-ID`) | HTTP/2 recommended |
<!-- | TBD | Automatic (retries?) | HTTP/2 recommended | -->
<!-- TODO: Fill in above ^ for SSE fallback, likely polling? -->

> **SSE connection limit:** The SDK warns when more than 5 concurrent SSE connections are open (browser limit per domain).

### Client-Side Stream Filtering

When a `QueryBuilder` with `.where()` filters or `.select()` columns calls `.stream()`, the returned stream applies those filters client-side:

```ts
const stream = wh.from('clicks')
  .select('page', 'button')
  .where('page', '=', '/home')
  .stream();

// Only events where page === '/home' are emitted, with only page + button columns
```

Supported operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `like`, `not_like` — the same `FilterOp` set `.where()` takes everywhere (the SDK maps them to wire tokens such as `eq`/`neq` internally).

---

## Live Queries

Live queries combine a historical backfill (`.fetch()`) with a real-time stream, providing a seamless initial load + live updates experience.

```ts
const lq = wh.from('clicks')
  .where('page', '=', '/home')
  .orderBy('received_timestamp', 'desc')
  .limit(100)
  .liveQuery({
    initial: (result) => {
      // Called once with the historical data
      setRows(result.data ?? []);
    },
    next: (event) => {
      // Called for each live event after backfill
      addRow(event.data);
    },
    error: (err) => console.error(err),
  });

// Cleanup
lq.close();
```

### `StreamSubscriber<T>`

```ts
interface StreamSubscriber<T> {
  initial: (result: Result<T[]>) => void;  // Historical backfill result
  next: (event: StreamEvent<T>) => void;    // Live events
  status?: (state: string) => void;         // Connection state changes
  error?: (err: WaveHouseError) => void;    // Errors
}
```

### How it works

1. Opens the stream **immediately** and buffers incoming events.
2. Runs the `.fetch()` query for historical data, calls `subscriber.initial()` with the result.
3. Deduplicates buffered events by comparing timestamps against the latest historical timestamp.
4. Flushes remaining buffered events and switches to live mode.

This "stream-first" approach ensures no events are lost between the fetch and stream start.

---

## AbortController Support

All async operations accept an `AbortSignal` for cancellation:

```ts
const controller = new AbortController();
setTimeout(() => controller.abort(), 5000); // 5s timeout

const { data, error } = await wh.from('clicks').fetch({ signal: controller.signal });
if (error?.code === 'ABORTED') {
  console.log('Request timed out');
}
```

---

## Error Handling

The SDK **never throws**. All errors are returned in `Result.error`.

| Status | Code | Retryable | Description |
|--------|------|-----------|-------------|
| 400 | `HTTP_400` | No | Bad request (validation, missing fields) |
| 401 | `HTTP_401` | No | Missing or invalid JWT |
| 403 | `HTTP_403` | No | Insufficient permissions |
| 404 | `HTTP_404` | No | Table or pipe not found |
| 500 | `HTTP_500` | Yes | Server error (retried per `maxRetries`) |
| 503 | `HTTP_503` | Yes | Service unavailable (auto-retries with `Retry-After`) |
| 0 | `NETWORK_ERROR` | Yes | Network failure (retried with exponential backoff) |
| 0 | `ABORTED` | No | Request canceled via `AbortSignal` |

---

## Full API Tree

```text
createClient<DB>(config) → WaveHouseClient
├── .from(table) → TableRef (NOT thenable)
│   ├── .fetch(opts?) → Promise<Result<Row[]>>
│   ├── .select(...cols?) → QueryBuilder (PromiseLike)
│   │   ├── .select() .where() .count() .sum() .avg() .min() .max()
│   │   │   .countDistinct() .aggregate() .groupBy() .orderBy()
│   │   │   .limit() .timeRange() .cacheTTL()
│   │   ├── .fetch(opts?) → Promise<Result<Row[]>>
│   │   ├── .stream(opts?) → StreamController
│   │   └── .liveQuery(subscriber, opts?) → LiveQuery
│   ├── .insert(data) → Promise<Result<InsertResult>>
│   ├── .schema() → Promise<Result<TableSchema>>
│   └── .stream(opts?) → StreamController
├── .pipe(name, params?) → PipeRef (PromiseLike)
│   ├── .fetch(opts?) → Promise<Result<Row[]>>
│   └── .stream(opts?) → StreamController
├── .pipes (admin)
│   ├── .list() → Promise<Result<Pipe[]>>
│   ├── .get(name) → Promise<Result<Pipe>>
│   ├── .set(name, def) → Promise<Result<void>>
│   └── .delete(name) → Promise<Result<void>>
├── .sql(query, opts?) → Promise<Result<Row[]>>
├── .schema
│   ├── .list() → Promise<Result<Schemas>>
│   └── .refresh() → Promise<Result<void>>
├── .policy (admin)
│   ├── .get() → Promise<Result<Policy>>
│   ├── .set(policy) → Promise<Result<void>>
│   └── .validate(policy) → Promise<Result<ValidationResult>>
├── .dlq
│   ├── .list() → Promise<Result<DLQStats>>
│   ├── .table(name) → Promise<Result<DLQStats>>
│   └── .stream() → StreamController
└── .sys
    └── .health() → Promise<Result<void>>

StreamController (NOT thenable)
├── .subscribe({ next, status?, error? }) → unsubscribe()
├── .close()
├── .status → StreamStatus
└── [Symbol.asyncIterator]() → AsyncIterableIterator<StreamEvent>
```

## Codegen CLI

Generate TypeScript types from a running WaveHouse instance:

```bash
# From the SDK package (clients/ts/):
pnpm codegen --url http://localhost:8080 --out ./src/db.d.ts
# or directly:
npx tsx src/cli/codegen.ts --url http://localhost:8080 --out ./src/db.d.ts
```

Codegen reads `/v1/schema`, which is **admin-only**. Against a non-dev server, pass an admin-role token with `--auth <jwt>` or the request is denied with `403`.

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `--url`, `-u` | WaveHouse base URL | `http://localhost:8080` |
| `--out`, `-o` | Output .d.ts file path | `./wavehouse.d.ts` |
| `--auth`, `-a` | Bearer token (if auth required) | — |

**Example output:**

```ts
// Auto-generated by @wavehouse/sdk codegen
export interface Database {
  clicks: ClicksRow;
  events: EventsRow;
}

export interface ClicksRow {
  event_id: string;
  page: string;
  user_id: string;
  duration_ms: number;
  received_timestamp: string;
}
```

**ClickHouse → TypeScript type mapping:**

| ClickHouse Type | TypeScript Type |
|----------------|-----------------|
| `String`, `FixedString`, `UUID`, `DateTime*`, `Date*`, `Enum*`, `IPv4/6` | `string` |
| `UInt*`, `Int*`, `Float*`, `Decimal*` | `number` |
| `Bool` | `boolean` |
| `Nullable(T)` | `T \| null` |
| `Array(T)` | `T[]` |
| `Map(K, V)` | `Record<K, V>` |
| `LowCardinality(T)` | same as `T` |

## E2E Testing

The SDK doubles as the E2E integration test harness. Tests in `tests/e2e/sdk/` exercise the full pipeline (ingest → ClickHouse → query) through the SDK, validating both the backend and the client library in one pass.

```bash
make test-e2e          # Run all E2E tests (the orchestrator boots a ClickHouse testcontainer + the wavehouse-cov binary, then runs the SDK suite)
```

Test files live in `tests/e2e/sdk/`: `admin`, `auth`, `batching`, `cache`, `dlq`, `ingest`, `query`, `streaming`, `stress` (each `*.test.ts`).

See [Development Guide — E2E Tests via SDK](/development#e2e-tests-via-sdk) for architecture details and workflow tips.

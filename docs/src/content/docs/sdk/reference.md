---
title: "SDK Reference & CLI"
description: "Error codes, AbortController, the full API tree, the codegen CLI, and E2E testing with @wavehouse/sdk."
---

Cross-cutting reference for `@wavehouse/sdk`: cancellation, the error model
behind every [`Result<T>`](/sdk#result-type), the complete API tree at a
glance, and the tooling that ships in the package.

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

The SDK **never throws** for anything the server returns — all API errors come back in `Result.error`. It does throw on caller and environment errors: a non-absolute `baseURL` (REST calls reject with a `TypeError`; streams report `SSE_CONNECT_ERROR` to the subscriber's `error` callback — see [Serving under a path prefix](/sdk#serving-under-a-path-prefix)), `.stream()` / `.liveQuery()` in a runtime with no `EventSource` (see [Runtime support](/sdk#runtime-support)), and an `auth` callback that rejects — a token-refresh failure propagates out of the REST call, and surfaces on a stream as `SSE_CONNECT_ERROR`. [`StreamController.connected()`](/sdk/streaming#connectedtimeoutms) also rejects — when the stream is already closed, closes before connecting, or doesn't reach `live` within `timeoutMs` (default `5_000` ms) — so `await` it inside a `try`/`catch`.

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
| 0 | `SSE_CONNECT_ERROR` | Yes | Stream failed to connect (e.g. a non-absolute `baseURL`) |
| 0 | `SSE_ERROR` | Yes | Stream connection error |

The two `SSE_*` codes arrive on the subscriber's `error` callback rather than in a `Result.error`, since a stream has no single result to carry them. Their `retryable: true` is advisory: unlike the REST codes above, the SDK never re-dials a stream itself. After the connection is open, drops surface through the `status` callback (`reconnecting` → `live`, or `closed`) while the native `EventSource` re-dials on its own; `SSE_ERROR` is a defensive fallback for a transport left in an unexpected state. A failure *before* the `EventSource` is constructed — a non-absolute `baseURL`, a rejecting `auth` callback — is terminal (`SSE_CONNECT_ERROR`), so fix the cause and start a new stream.

---

## Full API Tree

```text
createClient<DB>(config) → WaveHouseClient
├── .from(table) → TableRef (NOT thenable)
│   ├── .fetch(opts?) → Promise<Result<Row[]>>
│   ├── .select(...cols?) → QueryBuilder (PromiseLike)
│   │   ├── .select() .selectAll() .where() .count() .sum() .avg() .min() .max()
│   │   │   .countDistinct() .aggregate() .groupBy() .orderBy()
│   │   │   .limit() .timeRange() .cacheTTL()
│   │   ├── .fetch(opts?) → Promise<Result<Row[]>>
│   │   ├── .stream(opts?) → StreamController
│   │   └── .liveQuery(subscriber, opts?) → LiveQuery
│   ├── .selectAll() → QueryBuilder (PromiseLike)
│   ├── .insert(data) → Promise<Result<InsertResult>>
│   ├── .insertNDJSON(source) → Promise<Result<InsertResult>>
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
│   └── .stream() → StreamController  // not yet functional server-side — #197
└── .sys
    └── .health() → Promise<Result<void>>

StreamController (NOT thenable)
├── .subscribe({ next, status?, error? }) → unsubscribe()
├── .connected(timeoutMs?) → Promise<void>
├── .close()
├── .status → StreamStatus
└── [Symbol.asyncIterator]() → AsyncIterableIterator<StreamEvent>
```

## Codegen CLI

Generate TypeScript types from a running WaveHouse instance. The package ships a `wavehouse-codegen` bin, so after installing `@wavehouse/sdk` you can run it with `npx`:

```bash
npx wavehouse-codegen --url http://localhost:8080 --out ./src/db.d.ts

# Or, working inside this repo (clients/ts/):
pnpm codegen --url http://localhost:8080 --out ./src/db.d.ts
```

Codegen reads `/v1/schema`, which is **admin-only**. Against a non-dev server, pass an admin-role token with `--auth <jwt>` or the request is denied with `403`.

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `--url`, `-u` | WaveHouse base URL (may include a path prefix) | `http://localhost:8080` |
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
# Run all E2E tests: the orchestrator boots a ClickHouse testcontainer +
# the wavehouse-cov binary, then runs the SDK suite
make test-e2e
```

Test files live in `tests/e2e/sdk/`: `admin`, `auth`, `batching`, `cache`, `dlq`, `ingest`, `ndjson`, `query`, `streaming`, `stress` (each `*.test.ts`).

See [Development Guide — E2E Tests via SDK](/development#e2e-tests-via-sdk) for architecture details and workflow tips.

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

The SDK **never throws** for anything the server returns — all API errors come back in `Result.error`. It does throw on caller and environment errors: a non-absolute `baseURL` (REST calls reject with a `TypeError`; streams report `SSE_CONNECT_ERROR` to the subscriber's `error` callback — see [Serving under a path prefix](/sdk#serving-under-a-path-prefix)), `.stream()` / `.liveQuery()` in a runtime with no global `fetch` and no `options.fetch` (see [Runtime support](/sdk#runtime-support)), and an `auth` callback that rejects — a token-refresh failure propagates out of the REST call, and on a stream is reported as a retryable `SSE_AUTH_ERROR`.

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
| 0 | `SSE_CONNECT_ERROR` | No | Stream could not be started (e.g. a non-absolute `baseURL`) |
| 0 | `SSE_AUTH_ERROR` | Yes | The `auth` callback threw while minting a token for an attempt |
| 0 | `SSE_NETWORK_ERROR` | Yes | Stream request failed to reach the server |
| 0 | `SSE_READ_ERROR` | Yes | Stream was interrupted mid-read |
| 0 | `SSE_PARSE_ERROR` | Yes (usually nothing to re-dial) | Unparseable frame — reported and skipped, the connection continues. The buffer cap reports here too, then ends the connection (see below) |
| *(response status)* | `SSE_NO_STREAM_BODY` | No | A configured `options.fetch` returned a response with no readable body |
| *(response status)* | `SSE_REDIRECT` | No | The stream endpoint redirected; point `baseURL` at the final URL |
| *(response status)* | `SSE_BAD_CONTENT_TYPE` | No | A `200` that wasn't `text/event-stream` — usually a gateway's login page |
| *(response status)* | `HTTP_4xx` / `HTTP_5xx` | Per status | The stream request was rejected — same codes as REST |

The `SSE_*` codes arrive on the subscriber's `error` callback rather than in a `Result.error`, since a stream has no single result to carry them. The difference from REST is *when* you see the flag, not whether it drives retries — REST retries on it too, within `maxRetries`. On REST you only ever see `retryable` after the SDK has exhausted its own attempts, so acting on it further is your call; on a stream it is live, and a retryable failure means the transport is about to re-dial. A retryable failure is reported and then re-dialed on a jittered exponential backoff (capped at 30s, and reset only once a connection has held for a few seconds — so a server that accepts and instantly closes still backs off), with the `status` callback moving `reconnecting` → `live`. A non-retryable one is terminal: the stream reports the error, goes `closed`, and stays closed.

Rejected requests surface the real status and message rather than an opaque connection failure, and any `4xx` ends the stream, since repeating the request won't usually talk whatever rejected it round — the exception being a `429` or `408` from a fronting rate limiter, which is transient even though the stream still ends, so catch it and open a new one after a delay ([#469](https://github.com/Wave-RF/WaveHouse/issues/469)). Note that **WaveHouse never rejects a stream for authentication**: `/v1/stream` is ungated, so an expired or missing token resolves to `default_role` and you get a `200` with a filtered view, not a `401`. The one 4xx it raises itself is `400` for a missing or empty `table`; any other 4xx comes from something in front — an auth gateway, a proxy. That silent-downgrade behavior is exactly why `auth` is re-read on every connection attempt, and [#239](https://github.com/Wave-RF/WaveHouse/issues/239) tracks enforcing expiry server-side. `SSE_CONNECT_ERROR` and `SSE_NO_STREAM_BODY` are configuration faults, so fix the cause and start a new stream.

`SSE_PARSE_ERROR` is the one code that isn't a connection outcome: it's reported and *skipped*, and the connection keeps reading — one bad frame shouldn't cost you the stream. Its `retryable: true` is therefore vestigial; there is nothing to re-dial. Two things it does **not** cover: a frame whose `data` isn't valid JSON is logged with `console.warn` and dropped, never reaching your `error` callback; and the parser's 16 MiB buffer cap is reported here first, after which the now-terminated parser fails the next read as `SSE_READ_ERROR`, which does reconnect.

`SSE_AUTH_ERROR` is the exception that proves the rule: because `auth` is now invoked on every connection attempt rather than once per stream, a token endpoint having a bad minute is treated as transient and retried, rather than tearing down a stream that is otherwise healthy.

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
│   ├── .fetch(opts?) → Promise<Result<Row[]>>   // { signal } only — no limit
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
# Run all E2E tests: the orchestrator boots a ClickHouse testcontainer +
# the wavehouse-cov binary, then runs the SDK suite
make test-e2e
```

Test files live in `tests/e2e/sdk/` (each `*.test.ts`): `admin`, `auth`, `batching`, `cache`, `dlq`, `ingest`, `ndjson`, `query`, `streaming`, `stress`, plus `helpers` — a stack-free unit test of the harness's own `waitForCondition` poll helper rather than a pipeline test.

See [Development Guide — E2E Tests via SDK](/development#e2e-tests-via-sdk) for architecture details and workflow tips.

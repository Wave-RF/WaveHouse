# WaveHouse TypeScript SDK v1 — Implementation Plan

## Status: APPROVED — READY FOR IMPLEMENTATION

## TL;DR

Zero-dependency TypeScript client SDK for WaveHouse. Unified `.from()` entry point, immutable query builders that are `PromiseLike`, SSE for public/unauth streams, multiplexed WebSocket for authenticated streams, cursor-based pagination, stream-first-then-fetch backfill pattern.

## Design Decisions (Agreed)

1. **One entry point**: `.from(table)` → `TableRef` for all table operations
2. **TableRef is NOT thenable** — safe to `await` accidentally (returns itself)
3. **QueryBuilder IS thenable** — `await builder` auto-executes as `.fetch()`
4. **Immutable builders** — every chain method returns a new instance
5. **`{ data, error }` tuples** for all async operations — never throws
6. **Typed errors**: `{ status, code, message, details?, retryable }`
7. **Transport**: no auth → native EventSource (SSE), auth → single multiplexed WebSocket
8. **Manual override**: `transport: 'auto' | 'sse' | 'ws'`
9. **Stream-first ordering**: open subscription → fetch historical → deduplicate → deliver
10. **Cursor pagination**: results have `{ data, error, hasMore, next? }`
11. **Where clause**: `where('col', '>', value)` with `FilterOp` union type
12. **No client-side caching** in v1 (server cache sufficient)
13. **No React hooks** in v1 (separate `@wavehouse/react` in v2)
14. **No codegen CLI** in v1 (defer to v2, accept `Database` generic from hand-written or external types)
15. **AbortController support** on all async operations
16. **Token refresh during streams**: auto-reconnect with fresh token from `auth()` function
17. **Backend sends `received_timestamp` as SSE `id:` field** (backend change needed)
18. **WS accepts `?token=<jwt>` for auth during upgrade** (backend change needed)
19. **WS supports in-band subscribe/unsubscribe messages** (backend change needed)

## API Surface

```
createClient<DB>(config) → WaveHouseClient
├── .from(table) → TableRef (NOT thenable)
│   ├── .fetch(opts?) → Promise<Result<Row[]>>  (SELECT * shortcut, supports pagination)
│   ├── .select(...cols?) → QueryBuilder (thenable → {data, error, hasMore, next?})
│   │   ├── .where(col, op, val) .count(col,alias) .sum(col,alias)
│   │   │   .avg(col,alias) .min(col,alias) .max(col,alias)
│   │   │   .countDistinct(col,alias) .aggregate(fn,col,alias)
│   │   │   .groupBy(...cols) .orderBy(col,dir) .limit(n)
│   │   │   .timeRange(col, since, until?)
│   │   ├── .fetch(opts?) → Promise<Result<Row[]>>
│   │   └── .stream(opts?) → StreamController
│   ├── .insert(data | data[]) → Promise<Result<InsertResult>>
│   ├── .schema() → Promise<Result<Schema>>
│   └── .stream(opts?) → StreamController (raw event subscription)
├── .pipe(name, params?) → PipeRef (thenable → {data, error, hasMore, next?})
│   ├── .fetch(opts?) → Promise<Result<Row[]>>
│   └── .stream(opts?) → StreamController
├── .pipes
│   ├── .list() → Promise<Result<Pipe[]>>
│   ├── .set(name, def) → Promise<Result<void>>
│   └── .delete(name) → Promise<Result<void>>
├── .sql(query) → Promise<Result<Row[]>>
├── .schema
│   ├── .list() → Promise<Result<Schemas>>
│   └── .refresh() → Promise<Result<void>>
├── .policy
│   ├── .get() → Promise<Result<Policy>>
│   ├── .set(policy) → Promise<Result<void>>
│   └── .validate(policy) → Promise<Result<ValidationResult>>
├── .dlq
│   ├── .list() → Promise<Result<DLQEntry[]>>
│   ├── .table(name) → Promise<Result<DLQEntry[]>>
│   ├── .retry(table) → Promise<Result<void>>
│   └── .stream() → StreamController
└── .sys
    ├── .health() → Promise<Result<Health>>
    └── .ready() → Promise<Result<Ready>>

StreamController (NOT thenable)
├── .subscribe({ initial?, next, status?, error? }) → unsubscribe()
└── [Symbol.asyncIterator]() → AsyncIterableIterator
```

## Type System

```ts
Result<T> = { data: T; error: null; hasMore?: boolean; next?: () => Promise<Result<T>> }
           | { data: null; error: WaveHouseError; hasMore?: false; next?: undefined }

WaveHouseError = { status: number; code: string; message: string; details?: unknown; retryable: boolean }

FilterOp = '=' | '!=' | '>' | '>=' | '<' | '<=' | 'in' | 'like' | 'not_like'
  // SDK translates to backend: '>' → 'gt', etc.

StreamStatus = 'connecting' | 'live' | 'reconnecting' | 'closed'

StreamSubscriber<T> = {
  initial?: (result: Result<T[]>) => void;  // historical backfill
  next: (event: StreamEvent<T>) => void;    // each live event
  status?: (state: StreamStatus) => void;
  error?: (err: WaveHouseError) => void;
}

StreamEvent<T> = { table: string; timestamp: string; data: T }

ClientConfig<DB> = {
  baseURL: string;
  auth?: () => Promise<string>;  // omit for public/no-auth
  transport?: 'auto' | 'sse' | 'ws';
  options?: { maxRetries?: number }
}
```

## Implementation Phases (Documentation → Types → Code → Tests)

### Phase 1: API Documentation & Type Contracts
- src/types.ts — All public types with JSDoc (Result, WaveHouseError, FilterOp, interfaces for TableRef/QueryBuilder/StreamController/PipeRef)
- docs/sdk.md — Complete SDK API reference rewritten from scratch
- clients/ts/README.md — Replace drafts with clean Quick Start
Dependencies: None. Starting point.

### Phase 2: Core Implementation
- src/errors.ts — WaveHouseError construction from HTTP responses
- src/http.ts — Fetch wrapper: auth injection, retry, Retry-After, AbortController, backoff
- src/client.ts — WaveHouseClient class, namespace wiring, createClient()
- src/table.ts — TableRef: .fetch(), .select(), .insert(), .schema(), .stream()
- src/query-builder.ts — Immutable QueryBuilder (PromiseLike, operator translation, pagination)
- src/pipes.ts — PipeRef + PipesNamespace
- src/sql.ts, src/schema.ts, src/policy.ts, src/dlq.ts, src/sys.ts
- src/index.ts — Public exports
Dependencies: Phase 1 types.

### Phase 3: Streaming Implementation
- src/stream/controller.ts — StreamController: subscribe, async iterator, ref counting, dedup
- src/stream/sse.ts — Native EventSource adapter (no-auth mode)
- src/stream/ws.ts — Multiplexed WebSocket (auth mode, shared connection, in-band sub/unsub)
- src/stream/live-query.ts — Stream-first backfill, buffering, dedup, client-side filtering
Dependencies: Phase 2.

### Phase 4: Polish & Verify (deferred)
- Unit tests (Vitest)
- npm run typecheck + build verification
- Bundle size audit (< 15KB gzipped)
- Playground / examples
- Codegen CLI

## Key Implementation Details

### Immutable QueryBuilder
- Internal state: { table, columns, aggregations, filters, groupBy, orderBy, limit, timeRange, cacheTTL }
- Each method: `return new QueryBuilder({ ...this.state, [field]: newValue })`
- .then() method makes it PromiseLike: calls this.fetch() internally
- .fetch() builds the AST JSON, POSTs to /v1/tables/{table}/query

### Pagination Cursor
- After fetch, inspect result length vs limit
- If result.length === effectiveLimit → hasMore=true
- next() adds where(timestampCol, '<', lastRow[timestampCol]) to a clone of the builder
- Default ordering: received_timestamp DESC (if no orderBy specified)
- Server may apply role-based limit cap — SDK pages through transparently

### SSE Transport (Public/No-Auth)
- new EventSource(url) where url = `${baseURL}/v1/stream/sse?topic=ingest.${table}&since=${since}`
- onmessage: parse JSON, extract id (=received_timestamp), emit to StreamController
- onerror: EventSource auto-reconnects with Last-Event-ID
- onopen: emit status='live'

### WebSocket Transport (Authenticated)
- Single connection: new WebSocket(`${baseURL}/v1/stream/ws?token=${token}`)
- Subscribe: send JSON `{ action: 'subscribe', topic: 'ingest.events', since: '...' }`
- Unsubscribe: send JSON `{ action: 'unsubscribe', topic: 'ingest.events' }`
- Messages: parse JSON, route to correct StreamController by table_name
- Token refresh: onclose/onerror → call auth() → reconnect → resubscribe all active topics
- Reconnection: exponential backoff (1s, 2s, 4s, 8s, max 30s)
- Connection state exposed globally via wh.connection if needed

### Stream-First Backfill Flow
1. Open SSE/WS subscription for table (events start buffering)
2. Execute historical fetch (POST /v1/tables/{table}/query with timeRange/limit)
3. Collect buffered events that arrived during step 2
4. Deduplicate: remove buffered events whose received_timestamp exists in historical data
5. Fire initial callback with historical result (includes hasMore/next for pagination)
6. Fire next callback for each deduped buffered event (chronological order)
7. Fire next callback for each subsequent live event

### Error Handling
- All HTTP errors parsed into WaveHouseError: { status, code, message, details, retryable }
- 503 + Retry-After: auto-retry after specified delay (retryable=true)
- 400/403/404: return error, no retry (retryable=false)
- 500: return error, retryable per maxRetries config
- Network errors: retryable=true, exponential backoff

### Backend Assumptions (changes needed for backend team)
1. POST /v1/ingest/{table} accepts both flat JSON objects AND arrays
2. SSE endpoint sends received_timestamp as id: field
3. WS endpoint accepts ?token= query param for auth
4. WS endpoint supports in-band { action: 'subscribe'/'unsubscribe', topic, since? } messages
5. DLQ has streaming support
6. Server may return X-Limit-Applied header when role limit caps the request

## Relevant Files
- clients/ts/src/ — all new files go here
- clients/ts/package.json — already configured (@wavehouse/sdk, dual ESM/CJS, zero deps)
- clients/ts/tsconfig.json — already configured (ES2022, strict, bundler resolution)
- clients/ts/tsup.config.ts — already configured (dual format build)

## Verification
1. Phase 1: Docs reviewed by user for DX / completeness, types pass typecheck
2. Phase 2-3: `npm run typecheck` + `npm run build` — clean output
3. Phase 4: Unit tests pass, bundle < 15KB gzipped

## Tooling Decision
- Keep: TypeScript 5.5 + tsup (esbuild) + tsx. Standard for npm library publishing.
- Don't switch to Bun/Deno: this is a library consumed by all runtimes (browsers, Node, Deno, Bun, CF Workers). Build with the most compatible toolchain.
- Add later: Vitest for tests, optionally Biome for lint/format.
- Add prepublishOnly script to package.json.

## Excluded from v1 (v2+)
- Codegen CLI (npx wavehouse codegen)
- React hooks (@wavehouse/react)
- Client-side caching (IndexedDB/memory)
- Smart aggregation classification (incrementable/decomposable/poll-required)
- Tree-shaking sub-path exports
- Playground / example scripts

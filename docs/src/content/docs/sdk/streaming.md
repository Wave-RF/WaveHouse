---
title: "SDK Streaming & Live Queries"
description: "Real-time SSE streams, client-side filtering, and backfill-then-live queries in @wavehouse/sdk."
---

Real-time consumption with `@wavehouse/sdk`: SSE event streams from tables,
builders, and pipes, plus live queries that backfill history before going
live. Builders and table refs come from [Queries](/sdk/queries).
Examples import from `@wavehouse/sdk`; using the CDN instead, import from
`https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

## Streaming

Streams are Server-Sent Events over `fetch`. The `auth` token rides in an
`Authorization: Bearer` header — the same as every other request, on browsers
and servers alike — and is re-read on each connection attempt, so a stream that
outlives its token picks up a fresh one. Unauthenticated streams work the same
way, minus the header.

### `StreamController`

Returned by `.stream()` on `TableRef`, `QueryBuilder`, `PipeRef`, and `DLQNamespace` (the DLQ variant is not yet functional server-side — [#197](https://github.com/Wave-RF/WaveHouse/issues/197)). It is **NOT thenable**.

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

A handler that throws *during delivery* doesn't end the stream; the exception is
logged and the connection keeps running. But delivery of that event stops at the
handler that threw — your *later* subscribers, and any concurrent `for await`,
do not get it. The first `status` call, the synchronous one `.subscribe()` makes
before returning, isn't caught at all and throws back out at you. Wrap your
handler bodies in your own `try`/`catch`; see
[Error Handling](/sdk/reference#error-handling) for the carve-outs.

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

### `.connected(timeoutMs?)` → `Promise<void>`

Resolves once the stream reaches `live`. Rejects if it is already `closed`, if
it closes before connecting, or after `timeoutMs` (default `5000`). Useful when
you need the subscription established before doing something that depends on it
— inserting a row you expect to see come back, for instance.

```ts
const stream = wh.from('clicks').stream();
const unsub = stream.subscribe({ next: (e) => console.log(e) });
await stream.connected();          // wait until the transport is live
await wh.from('clicks').insert({ page: '/home' });
```

A *timeout* rejection does **not** stop the transport — reconnection is
unbounded, so it means "not live yet", not "given up"; call `.close()` if you
want it to stop. A rejection because the stream closed is different: there the
transport has already stopped.

One way this rejects against a perfectly healthy stream: if a subscriber you
registered *before* calling `.connected()` has a `status` handler that throws,
the throw aborts the fan-out before `connected()`'s internal watcher sees `live`,
so it times out while `.status` already reads `live`. See
[Error Handling](/sdk/reference#error-handling).

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

Row values of top-level `DateTime`/`DateTime64` columns inside `data` (not
timestamps nested in `Array`/`Map`/`Tuple` columns) arrive in canonical RFC 3339
UTC (`2026-06-21T04:00:00.123Z`), matching what `/v1/query` returns for the
same row — `new Date(value)` parses correctly with no zone fix-up.
Values WaveHouse couldn't canonicalize (ingest is fail-open) stream in the
producer's original spelling, and the `/v1/query` match doesn't hold for them:
a spelling ClickHouse accepted anyway still queries back in canonical UTC (one
it rejected never lands in the table at all), and a zone-less date-time is
what `new Date()` reads as *local* time — though a date-only `YYYY-MM-DD`
string is read as UTC, an ECMAScript quirk
(see [Timestamp canonicalization](/api#timestamp-canonicalization)).

### Transport Behavior

| Transport | Reconnect | Protocol |
| --------- | --------- | -------- |
| SSE over `fetch` | Automatic, jittered backoff, resumes via `Last-Event-ID` | HTTP/2 recommended |

:::note[SSE connection limit]
The SDK warns above 5 concurrent SSE connections — just under the browser's 6-per-domain limit for HTTP/1.1. HTTP/2 multiplexes over one connection, so the limit doesn't apply there.
:::

A dropped stream reconnects on a jittered exponential backoff, capped at 30s, and
resumes with `Last-Event-ID` so the server gap-fills what was missed. The
schedule resets only after a connection has *held* for a few seconds, so a
server accepting and immediately closing — slow-consumer eviction, a half-broken
upstream — can't pin the client at sub-second retries. Reconnection is
**unbounded**: a stream keeps re-dialing until it hits a terminal error or you
`close()` it. `options.maxRetries` bounds REST requests only and is never
consulted here.

:::caution[Resumption is at-least-once, and time-bounded]
Delivery across a reconnect is **at-least-once**. The `Last-Event-ID` the client
sends is the last event's `received_timestamp`, and the server replays from that
instant *inclusively* — so the last event you already saw, and anything sharing
its timestamp, arrives again. The SDK does not deduplicate live frames —
`liveQuery()` makes one pass at the backfill seam, and only under an ascending
order ([#449](https://github.com/Wave-RF/WaveHouse/issues/449)) — so key on
`timestamp` plus your
own row identity if duplicates matter.

Replay is also bounded by the server's
[`mq.gap_window_minutes`](/configuration#message-queue-nats) — 15 minutes by default.
A drop longer than that resumes with a hole and no signal, because the purged
messages are simply gone.
:::

A `4xx` is terminal and surfaces through `error` with the real status code
rather than an opaque connection failure — in a browser going cross-origin, only
when the rejection passes CORS and the gateway answered the `Authorization`
preflight; otherwise it arrives as a retryable network error instead, the way
`EventSource` reported everything. It won't be an *authentication* rejection
from WaveHouse, which leaves `/v1/stream` ungated and answers an expired token
with a filtered view rather than a `401`; the only 4xx it raises itself is `400`
for a missing or empty table name, and only on the stream route — a `404` or
`405` means the request never reached it, usually a `baseURL` path prefix the
proxy didn't strip. Anything else means something in front of it (an auth
gateway, a proxy) turned the request away — and note the
exception to "retrying wouldn't help": a `429` or `408` from a rate limiter *is*
transient, but the stream still ends, so catch it and open a new one after a
delay ([#469](https://github.com/Wave-RF/WaveHouse/issues/469)). See
[Error Handling](/sdk/reference#error-handling) for every code a stream can
report and which ones re-dial.

Streams go through `options.fetch`, `options.headers`, and
`options.fetchOptions` like every other request — which is what lets a stream
reach a header-gated origin. A custom `fetch` is asked more of on this
path; see [Supplying your own fetch](/sdk#supplying-your-own-fetch).

### Server-Side Policy Filtering

Access-control policy applies on the server before anything reaches the client: tables the connection's role can't `select` are skipped, denied columns are stripped from each event, and the role's row-level `filter` is evaluated per subscriber against the connection's JWT claims. A stream on a row-policied table therefore delivers only the rows the policy admits for that connection — and, where the server's in-memory comparison can't prove a match, fewer; see [Access control — where each rule is enforced](/access-control#where-each-rule-is-enforced). Claims are captured when the connection opens: a policy change applies from the next live event (an in-flight gap-fill replay finishes under the policy snapshot taken at connect), while token expiry or claim changes take effect on reconnect.

### Client-Side Stream Filtering

On top of that, when a `QueryBuilder` with `.where()` filters or `.select()` columns calls `.stream()`, the returned stream applies those filters client-side:

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
  .selectAll()
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
  initial?: (result: Result<T[]>) => void; // Historical backfill result
  next: (event: StreamEvent<T>) => void;    // Live events
  status?: (state: StreamStatus) => void;   // Connection state changes
  error?: (err: WaveHouseError) => void;    // Errors
}
```

### How it works

1. Opens the stream **immediately** and buffers incoming events.
2. Runs the `.fetch()` query for historical data, calls `subscriber.initial()` with the result — unless the fetch itself throws, in which case neither happens (see below).
3. Deduplicates buffered events against the **last row** of the backfill result — which is the newest only when the query orders ascending. A `desc` query (like the example above) puts the oldest row last, so for live frames the boundary is the oldest timestamp and this step filters nothing. With a `since` gap-fill in flight it works the other way — replay lands in the same buffer, so replayed events at or older than that row are dropped before reaching `next()`. A projection that omits `received_timestamp` skips the pass entirely. Tracked in [#449](https://github.com/Wave-RF/WaveHouse/issues/449).
4. Flushes remaining buffered events and switches to live mode.

This "stream-first" approach is what closes the window between the fetch and the
stream starting — events arriving during the fetch are buffered rather than
missed. It holds only when the backfill completes cleanly.

#### When the backfill doesn't complete

Here is how it doesn't, and what each case looks like from your subscriber.
"Buffered events" means the ones that arrived during the fetch window, which
step 4 would otherwise have flushed; the column reports how many of them reach
you.

| What happened | `initial()` | Live events | `error` | Buffered events |
|---|---|---|---|---|
| Backfill returns an error `Result` | fires, `result.error` set | delivered | — | none delivered |
| Your `initial()` throws | fires, then throws | delivered | — | none delivered — the flush never starts |
| Your `next()` throws during the flush | fires normally | delivered, after the flush stops | — | none after the throw |
| `auth` rejects, the stream connects anyway | never fires | delivered | — | none delivered |
| `auth` rejects for a few attempts, then recovers | never fires | delivered once it recovers | `SSE_AUTH_ERROR` per failed attempt | none delivered |
| `auth` keeps rejecting | never fires | none — never reaches `live` | `SSE_AUTH_ERROR` per attempt | none delivered |
| Relative `baseURL` | never fires | none — the stream is terminated too | terminal `SSE_CONNECT_ERROR` | none delivered |
| Your `status` handler throws on the first, synchronous call | never fires | none — buffered where you can't reach them | — | none delivered |

**The `status`-throw row is the odd one out**, and everything below is written
for the others. It is the one case where the backfill never *starts*: the throw
escapes `liveQuery()` before the constructor reaches it, so you get no handle
back, nothing can `.close()` the stream, and — because the buffering phase never
ends — every event accumulates in an object you have no reference to,
indefinitely. `next()` is never called at all. Passing `opts.signal` is the only
way to stop it; not throwing is the fix. Throwing on any *later* `status`
transition is isolated and logged like `next`. See
[Error Handling](/sdk/reference#error-handling).

**What to do about the rest.** Check `result.error` inside `initial()`, keep the
flush handlers total, and — if a missing backfill matters — treat `initial()`
never firing as its own failure. Where `auth` rejects and the stream connects
anyway nothing else will tell you, and where the other rows *do* raise an error
it names `auth` or the URL, never the backfill. Re-run the fetch then; you never
have to work out which row you hit. Leave it a moment first: events reach a
stream from the message queue before the ingest worker lands them in ClickHouse,
and its per-table batcher flushes on size or a deadline with the insert still to
complete after that (see [Ingest pipeline](/ingest-pipeline)), so an immediate
re-fetch can miss the newest rows.

**Why `auth` splits the way it does.** The backfill makes the *first* `auth()`
call and gets exactly one shot — the token is minted above the REST retry loop,
so a rejection there is never retried and the whole backfill is gone. The stream
calls `auth()` again on every connection attempt and treats the same rejection as
transient. That asymmetry is the entire reason a live query can end up running
normally with no snapshot behind it, and why nothing announces it: `error` never
fires, and the only trace is that `initial()` didn't. That is the case worth
guarding against, because it is the one that looks like success.

**What the live stream can lose.** In every row where the backfill runs at all:
nothing, except in one window — events arriving after the stream goes `live` but
before the backfill fails are buffered, and the failure discards the buffer. On
the `auth` rows that window opens only if the rejection arrives *after* the
connection does: the backfill fails as soon as its own `auth()` call rejects,
while the stream needs a second `auth()` call resolved **and** a connection
opened. Which way that goes depends on your token provider, and you don't need to
know — the recovery above covers both.

Tracked in [#473](https://github.com/Wave-RF/WaveHouse/issues/473).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FetchLike, StreamEvent, StreamStatus, WaveHouseError } from "../types.js";
import { MAX_BUFFER_CHARS, SSETransport } from "./sse.js";

/** One recorded call into the injected fetch. */
interface Attempt {
  url: string;
  init: RequestInit;
}

/**
 * Headers are always handed to fetch as a plain object by the transport.
 *
 * Asserted rather than assumed: a `Headers` instance or an entry array would
 * cast to an object with no string keys, quietly turning every
 * `expect(headers[x]).toBeUndefined()` below into a tautology.
 */
function headersOf(attempt: Attempt): Record<string, string> {
  const h = attempt.init.headers ?? {};
  expect(h).not.toBeInstanceOf(Headers);
  expect(Array.isArray(h)).toBe(false);
  return h as Record<string, string>;
}

/**
 * A response whose body is a stream the test drives frame by frame, standing in
 * for a connection the server holds open.
 */
function streamingResponse(): {
  res: Response;
  push: (chunk: string) => void;
  close: () => void;
  fail: (reason: Error) => void;
} {
  const encoder = new TextEncoder();
  let ctrl!: ReadableStreamDefaultController<Uint8Array>;
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      ctrl = c;
    },
  });
  return {
    res: new Response(body, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" },
    }),
    push: (chunk) => ctrl.enqueue(encoder.encode(chunk)),
    close: () => ctrl.close(),
    fail: (reason: Error) => ctrl.error(reason),
  };
}

/** A scripted fetch: each queued entry answers one connection attempt. */
function makeFetch() {
  const attempts: Attempt[] = [];
  const queue: Array<() => Response> = [];
  const impl: FetchLike = async (url, init) => {
    attempts.push({ url, init: init ?? {} });
    const next = queue.shift();
    if (!next) return streamingResponse().res; // idle, never-ending
    return next();
  };
  return { impl, attempts, queue };
}

/** Collector wired to a transport's three callbacks. */
function collect<T>(t: SSETransport<T>) {
  const events: StreamEvent<T>[] = [];
  const statuses: StreamStatus[] = [];
  const errors: WaveHouseError[] = [];
  t.onEvent = (e) => events.push(e);
  t.onStatus = (s) => statuses.push(s);
  t.onError = (e) => errors.push(e);
  return { events, statuses, errors };
}

const BASE = "http://localhost:8080";

/** Let the transport's async connect path reach its next await. */
const flush = () => new Promise((r) => setTimeout(r, 0));

const frame = (id: string, payload: unknown) => `id: ${id}\ndata: ${JSON.stringify(payload)}\n\n`;

/** An `event: schema` frame announcing `cols`. It carries no `id:` line. */
const schemaFrame = (cols: string[], table = "clicks") =>
  `event: schema\ndata: ${JSON.stringify({ table_name: table, columns: cols })}\n\n`;

/**
 * The pair of frames the server sends for one row: the `event: schema`
 * announcement of the column list, then the positional row. Rows travel
 * positionally, so a client that never saw the announcement cannot read one —
 * pushing them together is what a real connection does. Re-announcing an
 * unchanged list is harmless, so each call site stays self-contained.
 */
const rowFrames = (id: string, ts: string, data: Record<string, unknown>, table = "clicks") =>
  schemaFrame(Object.keys(data), table) +
  frame(id, { table_name: table, received_timestamp: ts, row: Object.values(data) });

// A failed assertion aborts the test body, so cleanup cannot live at the end of
// one: leaked fake timers stall every later test that awaits `flush()`, turning
// a single real failure into a cascade of timeouts that buries it.
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("SSETransport request construction", () => {
  it("authenticates with a Bearer header and keeps the token out of the URL", async () => {
    const f = makeFetch();
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      auth: () => "jwt-abc",
      fetch: f.impl,
    });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    const url = new URL(f.attempts[0].url);
    expect(url.pathname).toBe("/v1/stream");
    expect(url.searchParams.get("table")).toBe("clicks");
    expect(url.searchParams.get("token")).toBeNull();
    expect(url.search).not.toContain("jwt-abc");
    expect(headersOf(f.attempts[0]).Authorization).toBe("Bearer jwt-abc");

    t.disconnect();
  });

  it("preserves a base path prefix", async () => {
    const f = makeFetch();
    const t = new SSETransport({
      baseURL: "https://app.example.com/api/warehouse",
      table: "clicks",
      fetch: f.impl,
    });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    expect(new URL(f.attempts[0].url).pathname).toBe("/api/warehouse/v1/stream");
    t.disconnect();
  });

  it("sets the stream-critical init fields the SDK owns", async () => {
    const f = makeFetch();
    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    const { init } = f.attempts[0];
    expect(headersOf(f.attempts[0]).Accept).toBe("text/event-stream");
    // As an init field, never a Cache-Control header — the header form is not
    // in the server's Access-Control-Allow-Headers and would fail preflight.
    expect(init.cache).toBe("no-store");
    expect(headersOf(f.attempts[0])["Cache-Control"]).toBeUndefined();
    // No credential on this request, so a redirect costs nothing to follow.
    expect(init.redirect).toBe("follow");

    t.disconnect();
  });

  it("refuses redirects once a credential is attached", async () => {
    const f = makeFetch();
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      headers: { "CF-Access-Client-Secret": "shh" },
      fetch: f.impl,
    });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    // Configured headers are forwarded across a cross-origin hop even though
    // Authorization is stripped, so a secret would land wherever it points.
    expect(f.attempts[0].init.redirect).toBe("manual");
    t.disconnect();
  });

  it("neutralizes a body from fetchOptions", async () => {
    const f = makeFetch();
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      fetch: f.impl,
      fetchOptions: { body: "leaked" },
    });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    // A body on a GET makes the real fetch throw, which would read as a network
    // failure and be retried against a request that can never be built.
    expect(f.attempts[0].init.body).toBeUndefined();
    t.disconnect();
  });

  it("does not set credentials outside a browser, even when configured", async () => {
    const f = makeFetch();
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      fetch: f.impl,
      fetchOptions: { credentials: "include" },
    });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    expect(f.attempts[0].init.credentials).toBeUndefined();
    t.disconnect();
  });

  it("merges configured headers but keeps Authorization for auth", async () => {
    const f = makeFetch();
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      auth: () => "real-token",
      headers: { "CF-Access-Client-Id": "svc", authorization: "Bearer smuggled" },
      fetch: f.impl,
    });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    const headers = headersOf(f.attempts[0]);
    expect(headers["CF-Access-Client-Id"]).toBe("svc");
    expect(headers.Authorization).toBe("Bearer real-token");
    expect(headers.authorization).toBeUndefined();

    t.disconnect();
  });

  it("passes `since` on the first connect and no Last-Event-ID", async () => {
    const f = makeFetch();
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      since: "2026-08-01T00:00:00Z",
      fetch: f.impl,
    });
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    expect(new URL(f.attempts[0].url).searchParams.get("since")).toBe("2026-08-01T00:00:00Z");
    expect(headersOf(f.attempts[0])["Last-Event-ID"]).toBeUndefined();
    t.disconnect();
  });

  it("uses the global fetch when none is configured", async () => {
    const spy = vi.fn(async () => streamingResponse().res);
    vi.stubGlobal("fetch", spy);
    const t = new SSETransport({ baseURL: BASE, table: "clicks" });
    t.connect();
    await vi.waitFor(() => expect(spy).toHaveBeenCalledTimes(1));
    t.disconnect();
  });
});

describe("SSETransport framing", () => {
  it("emits an event per frame", async () => {
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(rowFrames("2026-08-01T00:00:01Z", "2026-08-01T00:00:01Z", { a: 1 }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    expect(seen.events[0]).toEqual({
      table: "clicks",
      timestamp: "2026-08-01T00:00:01Z",
      data: { a: 1 },
    });
    t.disconnect();
  });

  it("ignores comment frames — the connect preamble and keepalives", async () => {
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(": connected\n\n");
    conn.push(": keepalive\n\n");
    await flush();

    expect(seen.events).toHaveLength(0);
    expect(seen.errors).toHaveLength(0);
    t.disconnect();
  });

  it("reassembles a frame split across chunk boundaries", async () => {
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    // A single frame delivered one character at a time — the boundary case a
    // naive `prefix + chunk` parser corrupts.
    const whole = rowFrames("id-1", "2026-08-01T00:00:02Z", { split: true });
    for (const ch of whole) conn.push(ch);
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    expect(seen.events[0].data).toEqual({ split: true });
    t.disconnect();
  });

  it("warns on a malformed payload without emitting or erroring", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push("id: x\ndata: {not json\n\n");
    await vi.waitFor(() => expect(warn).toHaveBeenCalled());

    expect(seen.events).toHaveLength(0);
    expect(seen.errors).toHaveLength(0);
    t.disconnect();
  });
});

describe("SSETransport positional rows", () => {
  it("zips a positional row into an object using the announced columns", async () => {
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(schemaFrame(["page", "country", "score"]));
    conn.push(
      frame("t1", { table_name: "clicks", received_timestamp: "t1", row: ["/home", "US", 42] }),
    );
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    // The public API still yields row objects: the positional wire shape is an
    // implementation detail of the transport.
    expect(seen.events[0]).toEqual({
      table: "clicks",
      timestamp: "t1",
      data: { page: "/home", country: "US", score: 42 },
    });
    t.disconnect();
  });

  it("re-announced columns change the mapping for the rows that follow", async () => {
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(schemaFrame(["page"]));
    conn.push(frame("t1", { table_name: "clicks", received_timestamp: "t1", row: ["/a"] }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    // A schema change mid-stream moves the positions; without honoring it the
    // client would label "/b" as `page` and drop the new column entirely.
    conn.push(schemaFrame(["page", "country"]));
    conn.push(frame("t2", { table_name: "clicks", received_timestamp: "t2", row: ["/b", "GB"] }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(2));

    expect(seen.events[0].data).toEqual({ page: "/a" });
    expect(seen.events[1].data).toEqual({ page: "/b", country: "GB" });
    t.disconnect();
  });

  it("drops a row it has no column list for rather than guessing", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(frame("t1", { table_name: "clicks", received_timestamp: "t1", row: ["/a"] }));
    await flush();

    expect(seen.events).toHaveLength(0);
    expect(seen.errors).toHaveLength(0);
    expect(warn).toHaveBeenCalled();

    // The stream keeps going: the announcement arrives and rows flow again.
    conn.push(schemaFrame(["page"]));
    conn.push(frame("t2", { table_name: "clicks", received_timestamp: "t2", row: ["/b"] }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));
    expect(seen.events[0].data).toEqual({ page: "/b" });

    t.disconnect();
    warn.mockRestore();
  });

  it("drops a row whose length disagrees with the announced columns", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(schemaFrame(["page", "country"]));
    // Too short and too long are the same class of bug: there is no way to say
    // which value is which, so neither is delivered under guessed names.
    conn.push(frame("t1", { table_name: "clicks", received_timestamp: "t1", row: ["/a"] }));
    conn.push(
      frame("t2", { table_name: "clicks", received_timestamp: "t2", row: ["/a", "US", "extra"] }),
    );
    conn.push(frame("t3", { table_name: "clicks", received_timestamp: "t3", row: "not-an-array" }));
    await flush();

    expect(seen.events).toHaveLength(0);
    expect(warn).toHaveBeenCalledTimes(3);
    t.disconnect();
    warn.mockRestore();
  });

  it("bounds skew warnings per cause instead of one per row", async () => {
    // A server that predates the positional wire sends every row with no schema
    // event, so the unbounded warn was one console line per row for the whole
    // connection. The volume is bounded, but per CAUSE — a flat boolean would
    // have hidden the short/long/non-array cases behind whichever fired first.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    // No schema frame at all — 10 rows, one cause.
    for (let i = 0; i < 10; i++) {
      conn.push(frame(`t${i}`, { table_name: "clicks", received_timestamp: `t${i}`, row: ["/a"] }));
    }
    await flush();

    expect(seen.events).toHaveLength(0);
    // 3 warnings + 1 "further occurrences suppressed", not 10.
    expect(warn).toHaveBeenCalledTimes(4);
    expect(String(warn.mock.calls[3][0])).toContain("suppressed");

    // A DIFFERENT cause still gets its own budget — the point of keying by cause.
    warn.mockClear();
    conn.push(schemaFrame(["page", "country"]));
    conn.push(frame("s1", { table_name: "clicks", received_timestamp: "s1", row: ["/a"] }));
    await flush();
    expect(warn).toHaveBeenCalledTimes(1);

    // The two non-skew causes are bounded on the same budget. Both flood
    // identically against a bad server, and sdk/reference.md states the bound as
    // a contract for every cause — so each needs its own case, or reverting the
    // bound breaks nothing.
    warn.mockClear();
    for (let i = 0; i < 10; i++) {
      conn.push(`event: schema\ndata: {"table_name":"clicks","columns":"not-an-array"}\n\n`);
    }
    await flush();
    expect(warn).toHaveBeenCalledTimes(4);
    expect(String(warn.mock.calls[3][0])).toContain("suppressed");

    warn.mockClear();
    for (let i = 0; i < 10; i++) {
      conn.push(`data: {not json\n\n`);
    }
    await flush();
    expect(warn).toHaveBeenCalledTimes(4);
    expect(String(warn.mock.calls[3][0])).toContain("suppressed");

    t.disconnect();
    warn.mockRestore();
  });

  it("refuses a schema frame that names a column twice", async () => {
    // Zipping a positional row against a duplicate name keeps the later value
    // and drops the earlier one, with no signal. Refusing the announcement is
    // the loud failure: rows drop until the next valid one.
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t2 = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t2);
    t2.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(schemaFrame(["tenant", "tenant"]));
    conn.push(frame("t1", { table_name: "clicks", received_timestamp: "t1", row: ["a", "b"] }));
    await flush();

    expect(seen.events).toHaveLength(0);
    expect(warn).toHaveBeenCalled();

    // A valid announcement afterwards recovers the stream.
    conn.push(schemaFrame(["page", "button"]));
    conn.push(frame("t2", { table_name: "clicks", received_timestamp: "t2", row: ["/a", "go"] }));
    await flush();
    expect(seen.events).toHaveLength(1);
    expect(seen.events[0].data).toEqual({ page: "/a", button: "go" });

    t2.disconnect();
    warn.mockRestore();
  });

  it("drops a malformed schema event rather than keeping a stale column list", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(schemaFrame(["page"]));
    conn.push(frame("t1", { table_name: "clicks", received_timestamp: "t1", row: ["/a"] }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    // Keeping the previous list here would label the next row's values with
    // names the server no longer claims — worse than delivering nothing.
    conn.push(`event: schema\ndata: {"table_name":"clicks","columns":"nope"}\n\n`);
    conn.push(frame("t2", { table_name: "clicks", received_timestamp: "t2", row: ["/b"] }));
    await flush();

    expect(seen.events).toHaveLength(1);
    expect(warn).toHaveBeenCalled();
    t.disconnect();
    warn.mockRestore();
  });
});

describe("SSETransport reconnect and resumption", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it("resumes from the last non-empty id and re-mints the token", async () => {
    const f = makeFetch();
    const first = streamingResponse();
    const second = streamingResponse();
    f.queue.push(
      () => first.res,
      () => second.res,
    );

    let issued = 0;
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      auth: () => `token-${++issued}`,
      fetch: f.impl,
    });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    first.push(rowFrames("2026-08-01T00:00:03Z", "2026-08-01T00:00:03Z", { n: 1 }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    // A frame with a blank id. Per the SSE spec an empty id field CLEARS the
    // last-event-id, which would silently lose the resumption point — so the
    // transport ignores it and keeps the previous one.
    first.push(frame("", { table_name: "clicks", received_timestamp: "later", row: [2] }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(2));

    first.close();
    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(2));

    expect(headersOf(f.attempts[1])["Last-Event-ID"]).toBe("2026-08-01T00:00:03Z");
    expect(headersOf(f.attempts[1]).Authorization).toBe("Bearer token-2");
    expect(seen.statuses).toContain("reconnecting");

    t.disconnect();
  });

  it("escalates backoff against a server that accepts and instantly closes", async () => {
    const attempts: number[] = [];
    // Slow-consumer eviction looks exactly like this: a clean 200 that closes
    // immediately. Resetting the schedule on any connection that merely opened
    // would pin the client at sub-second retries forever.
    const impl: FetchLike = async () => {
      attempts.push(Date.now());
      const conn = streamingResponse();
      conn.push(": connected\n\n");
      conn.close();
      return conn.res;
    };

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: impl });
    collect(t);
    t.connect();
    await vi.advanceTimersByTimeAsync(6_000);

    // Escalating, the cheapest possible schedule is 0.5 + 1 + 2 + 4s, so six
    // seconds buys at most four attempts. Flat sub-second retries gave eight.
    expect(attempts.length).toBeGreaterThan(1);
    expect(attempts.length).toBeLessThanOrEqual(5);
    // The count alone would also pass on a flat ~1.2s schedule. Jitter spans
    // [n/2, n) and n doubles, so consecutive gaps cannot overlap and each must
    // strictly exceed the last.
    const gaps = attempts.slice(1).map((at, i) => at - attempts[i]);
    for (let i = 1; i < gaps.length; i++) {
      expect(gaps[i]).toBeGreaterThan(gaps[i - 1]);
    }

    t.disconnect();
  });

  it("honors a server `retry:` as a backoff floor, clamped to the ceiling", async () => {
    const f = makeFetch();
    const first = streamingResponse();
    f.queue.push(() => first.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    // Server-controlled input into client timing, so the clamp matters: an
    // hour would otherwise strand the stream far past the 30s ceiling.
    first.push("retry: 3600000\n\n");
    await vi.advanceTimersByTimeAsync(1);
    first.close();

    // Well past the un-clamped floor's first attempt, still inside an hour.
    await vi.advanceTimersByTimeAsync(120_000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(2));

    t.disconnect();
  });

  it("reports an unparseable frame without dropping the connection", async () => {
    const f = makeFetch();
    const first = streamingResponse();
    f.queue.push(() => first.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    first.push("bogus: x\n\n");
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));
    expect(seen.errors[0].code).toBe("SSE_PARSE_ERROR");

    // Documented as reported-and-skipped: one bad frame must not cost the
    // stream, so the same connection keeps delivering.
    expect(seen.statuses).not.toContain("reconnecting");
    first.push(rowFrames("2026-08-01T00:00:11Z", "2026-08-01T00:00:11Z", { ok: true }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));
    expect(f.attempts).toHaveLength(1);

    t.disconnect();
  });

  it("honors a `retry:` floor exactly, without jittering below it", async () => {
    // Deterministic: with random() pinned to 0 the pre-fix code drew the
    // bottom of [nominal/2, nominal) — half the floor the server asked for.
    vi.spyOn(Math, "random").mockReturnValue(0);
    const f = makeFetch();
    const first = streamingResponse();
    f.queue.push(() => first.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    first.push("retry: 20000\n\n");
    await vi.advanceTimersByTimeAsync(1);
    first.close();

    // Pre-fix this re-dialed at 10s; the floor says 20s.
    await vi.advanceTimersByTimeAsync(19_999);
    expect(f.attempts).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(2);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(2));

    t.disconnect();
  });

  it("ends the connection on a buffer overflow, with no second error", async () => {
    const f = makeFetch();
    const first = streamingResponse();
    const second = streamingResponse();
    f.queue.push(
      () => first.res,
      () => second.res,
    );

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    // One unterminated line past the cap. The parser reports the overflow and
    // terminates itself; feeding it again throws and would surface as a second,
    // misleading SSE_READ_ERROR blaming the read for a cause already reported.
    first.push(`data: ${"x".repeat(MAX_BUFFER_CHARS)}`);
    // Enqueued before the first read completes, because the early return makes
    // `_pump`'s finally cancel the reader — a later push would throw on a
    // closed stream. Without the fix the loop feeds this to the terminated
    // parser, which throws and adds a second, misleading SSE_READ_ERROR.
    first.push("data: {}\n\n");
    await vi.waitFor(() => expect(seen.errors.length).toBeGreaterThan(0));

    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(2));

    // Asserted only after the reconnect, so a second error would have had
    // every chance to land. Checked before the reconnect this cannot fail.
    expect(seen.errors.map((e) => e.code)).toEqual(["SSE_PARSE_ERROR"]);

    t.disconnect();
  });

  it("retries an AbortError from a custom fetch that the SDK did not raise", async () => {
    const f = makeFetch();
    // The motivating case: an `options.fetch` enforcing its own per-attempt
    // deadline aborts an internal controller. The caller never cancelled, so
    // this is transient — matching on the error type ended the stream
    // terminally and emitted nothing at all.
    f.queue.push(() => {
      throw new DOMException("Aborted", "AbortError");
    });

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_NETWORK_ERROR");
    expect(seen.errors[0].retryable).toBe(true);

    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(2));

    t.disconnect();
  });

  it("reports an AbortError raised mid-read instead of swallowing it", async () => {
    const f = makeFetch();
    const first = streamingResponse();
    const second = streamingResponse();
    f.queue.push(
      () => first.res,
      () => second.res,
    );

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    // Same rule on the read path: an abort we did not raise is a dropped
    // connection to report and re-dial, not a silent teardown.
    first.fail(new DOMException("Aborted", "AbortError"));
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_READ_ERROR");
    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(2));

    t.disconnect();
  });

  it("reconnects after the body errors mid-read, resuming from the last id", async () => {
    const f = makeFetch();
    const first = streamingResponse();
    const second = streamingResponse();
    f.queue.push(
      () => first.res,
      () => second.res,
    );

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    first.push(rowFrames("2026-08-01T00:00:09Z", "2026-08-01T00:00:09Z", { n: 1 }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    // A reset connection, not a clean close — the read rejects rather than
    // reporting done, which is the branch that has to tell a genuine failure
    // apart from an abort before deciding to re-dial.
    first.fail(new Error("ECONNRESET"));
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_READ_ERROR");
    expect(seen.errors[0].retryable).toBe(true);

    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(2));
    expect(headersOf(f.attempts[1])["Last-Event-ID"]).toBe("2026-08-01T00:00:09Z");

    t.disconnect();
  });

  it("stops for good on a 401 rather than retrying a rejected token", async () => {
    const f = makeFetch();
    f.queue.push(() => new Response(JSON.stringify({ error: "invalid token" }), { status: 401 }));

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].status).toBe(401);
    expect(seen.errors[0].message).toBe("invalid token");
    expect(seen.statuses).toContain("closed");

    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.attempts).toHaveLength(1);
  });

  it("retries a 503", async () => {
    const f = makeFetch();
    f.queue.push(() => new Response("{}", { status: 503 }));

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].retryable).toBe(true);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts.length).toBeGreaterThan(1));

    t.disconnect();
  });

  it("retries when the connection itself fails", async () => {
    const f = makeFetch();
    f.queue.push(() => {
      throw new TypeError("connect ECONNREFUSED");
    });

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_NETWORK_ERROR");
    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts.length).toBeGreaterThan(1));

    t.disconnect();
  });
});

describe("SSETransport lifecycle", () => {
  it("does not leave an unhandled rejection when releasing an errored body", async () => {
    // Cancelling an errored stream returns a *rejected* promise. Unhandled,
    // that takes the host process down under Node's default
    // `--unhandled-rejections=throw` — so a connection reset arriving between
    // the headers and a terminal branch would kill the consumer's app.
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => rejections.push(reason);
    process.on("unhandledRejection", onRejection);

    try {
      const f = makeFetch();
      f.queue.push(() => {
        const conn = streamingResponse();
        // Wrong content type, so this takes a terminal branch that releases
        // the body rather than reading it...
        const res = new Response(conn.res.body, {
          status: 200,
          headers: { "Content-Type": "text/html" },
        });
        conn.fail(new Error("ECONNRESET")); // ...and the body is already errored.
        return res;
      });

      const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
      const seen = collect(t);
      t.connect();
      await vi.waitFor(() => expect(seen.errors).toHaveLength(1));
      expect(seen.errors[0].code).toBe("SSE_BAD_CONTENT_TYPE");

      // Unhandled rejections surface a turn after the microtask queue drains.
      await new Promise((r) => setTimeout(r, 20));
      expect(rejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", onRejection);
    }
  });

  it("rejects a response whose body was already consumed", async () => {
    const f = makeFetch();
    f.queue.push(() => {
      const res = new Response("id: x\ndata: {}\n\n", {
        status: 200,
        headers: { "Content-Type": "text/event-stream" },
      });
      // What a wrapper that read the body before returning hands back. The
      // body is still non-null — merely locked — so an `if (!body)` guard
      // sails past it, reports "live", and then throws out of the read loop.
      res.body?.getReader();
      return res;
    });

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_NO_STREAM_BODY");
    expect(seen.statuses).not.toContain("live");
    expect(seen.statuses).toContain("closed");
  });

  it("rejects a fetch that cannot stream instead of hanging", async () => {
    const f = makeFetch();
    // Exactly what a wrapper that buffers or logs the body hands back: the
    // server's headers intact, the stream itself already consumed.
    f.queue.push(
      () =>
        new Response(null, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" },
        }),
    );

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_NO_STREAM_BODY");
    expect(seen.errors[0].retryable).toBe(false);
    expect(seen.statuses).toContain("closed");
  });

  it("emits closed once, however many times disconnect is called", async () => {
    const f = makeFetch();
    const t = new SSETransport({ baseURL: "http://x", table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    t.disconnect();
    t.disconnect();
    t.disconnect();

    // The emit deliberately bypasses `_emitStatus` (which returns early once
    // `_closed` is set), so it has to sit inside the teardown guard or every
    // redundant call re-fires it.
    expect(seen.statuses.filter((s) => s === "closed")).toHaveLength(1);
  });

  it("ends the stream on a baseURL that cannot resolve", async () => {
    vi.useFakeTimers();
    const f = makeFetch();
    const t = new SSETransport({ baseURL: "not-a-url", table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_CONNECT_ERROR");
    expect(seen.errors[0].retryable).toBe(false);
    expect(seen.statuses).toContain("closed");

    // Deterministic failure — retrying would only reproduce it.
    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.attempts).toHaveLength(0);
  });

  it("ends the stream on a non-http baseURL scheme", async () => {
    vi.useFakeTimers();
    const f = makeFetch();
    const t = new SSETransport({ baseURL: "ws://localhost:8080", table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_CONNECT_ERROR");
    expect(seen.errors[0].message).toContain("http or https");
    // `fetch` would reject with an opaque failure the loop reads as transient.
    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.attempts).toHaveLength(0);
  });

  it("retries an AbortError from the token provider that the SDK did not raise", async () => {
    vi.useFakeTimers();
    const f = makeFetch();
    let calls = 0;
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      auth: () => {
        calls++;
        // A refresh call cancelled by its own controller. The SDK never asked
        // to cancel, so this is transient — matching on the error type instead
        // of `_closed` ended the stream terminally, and emitted nothing.
        if (calls === 1) throw new DOMException("Aborted", "AbortError");
        return "recovered";
      },
      fetch: f.impl,
    });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_AUTH_ERROR");
    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));
    expect(headersOf(f.attempts[0]).Authorization).toBe("Bearer recovered");

    t.disconnect();
  });

  it("keeps retrying when the token provider throws", async () => {
    vi.useFakeTimers();
    const f = makeFetch();
    let calls = 0;
    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      auth: () => {
        calls++;
        if (calls === 1) throw new Error("refresh endpoint down");
        return "recovered";
      },
      fetch: f.impl,
    });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    // Transient by assumption: `auth` runs per attempt now, so one bad minute
    // at the token endpoint must not tear down a long-lived stream.
    expect(seen.errors[0].code).toBe("SSE_AUTH_ERROR");
    expect(seen.errors[0].retryable).toBe(true);
    expect(f.attempts).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(2000);
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));
    expect(headersOf(f.attempts[0]).Authorization).toBe("Bearer recovered");

    t.disconnect();
  });

  it("refuses a redirect instead of retrying it forever", async () => {
    vi.useFakeTimers();
    const f = makeFetch();
    // What Node hands back under `redirect: "manual"`; a browser would give an
    // opaque status-0 response, handled by the same branch.
    f.queue.push(
      () => new Response(null, { status: 302, headers: { Location: "https://elsewhere/" } }),
    );

    const t = new SSETransport({
      baseURL: BASE,
      table: "clicks",
      auth: () => "jwt",
      fetch: f.impl,
    });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    // A bearer token alone must make the request credentialed. Asserting only
    // the resulting error would not pin this: the scripted fetch returns its
    // 302 regardless of `init.redirect`, so the error fires either way.
    expect(f.attempts[0].init.redirect).toBe("manual");
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_REDIRECT");
    expect(seen.errors[0].retryable).toBe(false);
    expect(seen.statuses).toContain("closed");

    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.attempts).toHaveLength(1);
  });

  it("rejects a 200 that is not an event stream", async () => {
    vi.useFakeTimers();
    const f = makeFetch();
    // An auth gateway answering with its login page: 200, streaming, and
    // entirely devoid of SSE frames.
    f.queue.push(
      () =>
        new Response("<html>sign in</html>", {
          status: 200,
          headers: { "Content-Type": "text/html; charset=utf-8" },
        }),
    );

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_BAD_CONTENT_TYPE");
    expect(seen.errors[0].message).toContain("text/html");
    expect(seen.statuses).toContain("closed");
    expect(seen.statuses).not.toContain("live");

    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.attempts).toHaveLength(1);
  });

  it("aborts the in-flight request on disconnect", async () => {
    const f = makeFetch();
    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    const signal = f.attempts[0].init.signal as AbortSignal;
    expect(signal.aborted).toBe(false);

    t.disconnect();
    expect(signal.aborted).toBe(true);
    expect(seen.statuses).toContain("closed");
  });

  it("does not reconnect after disconnect", async () => {
    vi.useFakeTimers();
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    collect(t);
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));

    t.disconnect();
    conn.close();
    await vi.advanceTimersByTimeAsync(60_000);

    expect(f.attempts).toHaveLength(1);
  });

  it("does not report live for a disconnect that lands mid-handshake", async () => {
    let settle: ((r: Response) => void) | undefined;
    const impl: FetchLike = () =>
      new Promise<Response>((resolve) => {
        settle = resolve;
      });

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(settle).toBeTypeOf("function"));

    // Aborting doesn't retract a Response that is already on its way back, so
    // the happy-path continuation still runs. Left unguarded it flips a closed
    // controller to "live" and strands it there — the loop exits without
    // emitting anything further.
    t.disconnect();
    settle?.(streamingResponse().res);
    await flush();

    expect(seen.statuses).toContain("closed");
    expect(seen.statuses).not.toContain("live");
    expect(seen.statuses[seen.statuses.length - 1]).toBe("closed");
  });

  it("strands no timer when closed from inside the reconnecting handler", async () => {
    vi.useFakeTimers();
    // A server that accepts and immediately closes, so the loop reaches its
    // backoff and emits "reconnecting".
    const impl: FetchLike = async () => {
      const conn = streamingResponse();
      conn.push(": connected\n\n");
      conn.close();
      return conn.res;
    };

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: impl });
    const seen = collect(t);
    t.onStatus = (st) => {
      seen.statuses.push(st);
      // `_wake` is still null at this instant — the timer does not exist yet —
      // so a naive implementation installs one nothing can cancel.
      if (st === "reconnecting") t.disconnect();
    };
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("reconnecting"));
    await vi.advanceTimersByTimeAsync(0);

    expect(vi.getTimerCount()).toBe(0);
    expect(seen.statuses).toContain("closed");
  });

  it("stops delivering when a subscriber closes from inside its own handler", async () => {
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    // "Read until I see my event, then stop" — the ordinary pattern, and what
    // EventSource honored: close() inside a handler ended delivery.
    t.onEvent = (e) => {
      seen.events.push(e);
      t.disconnect();
    };
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    // Both frames arrive in one chunk, as gap-fill replay delivers them, and
    // the parser dispatches every complete frame in a chunk synchronously.
    const two = rowFrames("a", "a", { n: 1 }) + rowFrames("b", "b", { n: 2 });
    conn.push(two);
    await flush();

    expect(seen.events).toHaveLength(1);
    expect(seen.events[0].data).toEqual({ n: 1 });
  });

  it("blames the handler, not the frame, when next() throws", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    let first = true;
    t.onEvent = (e) => {
      if (first) {
        first = false;
        throw new Error("consumer render blew up");
      }
      seen.events.push(e);
    };
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    conn.push(rowFrames("id-1", "t1", { n: 1 }));
    await vi.waitFor(() => expect(err).toHaveBeenCalled());

    // A shared catch reported this as "SSE received malformed message",
    // blaming the server for a well-formed frame the consumer choked on.
    expect(warn).not.toHaveBeenCalled();
    expect(err.mock.calls[0][0]).toContain("event handler threw");

    // The property that actually matters: one bad handler call must not cost
    // the stream. Asserting only the log message pins the symptom, not this.
    conn.push(rowFrames("id-2", "t2", { n: 2 }));
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));
    expect(seen.events[0].data).toEqual({ n: 2 });

    t.disconnect();
    warn.mockRestore();
    err.mockRestore();
  });

  it("survives a subscriber whose status handler throws", async () => {
    const warn = vi.spyOn(console, "error").mockImplementation(() => {});
    const rejections: unknown[] = [];
    const onRejection = (r: unknown) => rejections.push(r);
    process.on("unhandledRejection", onRejection);

    try {
      const f = makeFetch();
      const conn = streamingResponse();
      f.queue.push(() => conn.res);

      const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
      const seen = collect(t);
      // Unguarded, this unwinds _run into connect()'s terminal handler, which
      // emits a spurious SSE_CONNECT_ERROR and then calls disconnect() — whose
      // own onStatus("closed") throws again, out of the .catch(), fatally.
      t.onStatus = (st) => {
        seen.statuses.push(st);
        throw new Error("subscriber blew up");
      };
      t.connect();
      await vi.waitFor(() => expect(seen.statuses).toContain("live"));

      conn.push(rowFrames("id-1", "t", { n: 1 }));
      await vi.waitFor(() => expect(seen.events).toHaveLength(1));

      // The stream is still delivering, and nothing spurious was reported.
      expect(seen.errors).toHaveLength(0);
      await new Promise((r) => setTimeout(r, 20));
      expect(rejections).toEqual([]);

      t.disconnect();
    } finally {
      process.off("unhandledRejection", onRejection);
      warn.mockRestore();
    }
  });

  it("survives a subscriber whose error handler throws", async () => {
    const warn = vi.spyOn(console, "error").mockImplementation(() => {});
    const f = makeFetch();
    f.queue.push(() => new Response(JSON.stringify({ error: "nope" }), { status: 401 }));

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.onError = (e) => {
      seen.errors.push(e);
      throw new Error("subscriber blew up");
    };
    t.connect();
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    // A throwing handler must not turn a clean terminal 401 into something else.
    expect(seen.errors[0].status).toBe(401);
    await flush();
    expect(seen.errors).toHaveLength(1);
    warn.mockRestore();
  });

  it("connect() twice does not start a second reconnect loop", async () => {
    const f = makeFetch();
    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    t.connect();
    t.connect();
    await vi.waitFor(() => expect(f.attempts).toHaveLength(1));
    await flush();

    expect(f.attempts).toHaveLength(1);
    t.disconnect();
  });

  it("connect() after disconnect() is inert", async () => {
    const f = makeFetch();
    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    t.disconnect();
    t.connect();
    await flush();
    expect(f.attempts).toHaveLength(0);
  });
});

describe("SSETransport hostile column names", () => {
  it("makes a column named __proto__ an own property instead of losing it", async () => {
    const f = makeFetch();
    const conn = streamingResponse();
    f.queue.push(() => conn.res);

    const t = new SSETransport({ baseURL: BASE, table: "clicks", fetch: f.impl });
    const seen = collect(t);
    t.connect();
    await vi.waitFor(() => expect(seen.statuses).toContain("live"));

    // A ClickHouse column may legitimately be named __proto__. On a normal
    // object literal that assignment hits the inherited setter: the value
    // vanishes from the row rather than becoming a key.
    conn.push(schemaFrame(["__proto__", "page"]));
    conn.push(
      frame("t1", {
        table_name: "clicks",
        received_timestamp: "t1",
        row: [{ polluted: true }, "/a"],
      }),
    );
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    const row = seen.events[0].data as Record<string, unknown>;
    // Read it as a plain key, never through the accessor the name shadows.
    expect(Object.hasOwn(row, "__proto__")).toBe(true);
    expect(Object.getOwnPropertyDescriptor(row, "__proto__")?.value).toEqual({ polluted: true });
    expect(row.page).toBe("/a");
    // Nothing leaked onto Object.prototype.
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();

    t.disconnect();
  });
});

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

    conn.push(
      frame("2026-08-01T00:00:01Z", {
        table_name: "clicks",
        received_timestamp: "2026-08-01T00:00:01Z",
        data: { a: 1 },
      }),
    );
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
    const whole = frame("id-1", {
      table_name: "clicks",
      received_timestamp: "2026-08-01T00:00:02Z",
      data: { split: true },
    });
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

    first.push(
      frame("2026-08-01T00:00:03Z", {
        table_name: "clicks",
        received_timestamp: "2026-08-01T00:00:03Z",
        data: { n: 1 },
      }),
    );
    await vi.waitFor(() => expect(seen.events).toHaveLength(1));

    // A passthrough payload carries a blank id. Per the SSE spec that clears
    // the last-event-id, which would silently lose the resumption point.
    first.push("id: \ndata: {}\n\n");
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
    first.push(
      frame("2026-08-01T00:00:11Z", {
        table_name: "clicks",
        received_timestamp: "2026-08-01T00:00:11Z",
        data: { ok: true },
      }),
    );
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

    first.push(
      frame("2026-08-01T00:00:09Z", {
        table_name: "clicks",
        received_timestamp: "2026-08-01T00:00:09Z",
        data: { n: 1 },
      }),
    );
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
    const two =
      frame("a", { table_name: "clicks", received_timestamp: "a", data: { n: 1 } }) +
      frame("b", { table_name: "clicks", received_timestamp: "b", data: { n: 2 } });
    conn.push(two);
    await flush();

    expect(seen.events).toHaveLength(1);
    expect(seen.events[0].data).toEqual({ n: 1 });
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

      conn.push(frame("id-1", { table_name: "clicks", received_timestamp: "t", data: { n: 1 } }));
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

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FetchLike, StreamEvent, StreamStatus, WaveHouseError } from "../types.js";
import { SSETransport } from "./sse.js";

/** One recorded call into the injected fetch. */
interface Attempt {
  url: string;
  init: RequestInit;
}

/** Headers are always handed to fetch as a plain object by the transport. */
function headersOf(attempt: Attempt): Record<string, string> {
  return (attempt.init.headers ?? {}) as Record<string, string>;
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
    vi.unstubAllGlobals();
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
    warn.mockRestore();
  });
});

describe("SSETransport reconnect and resumption", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
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
    vi.useRealTimers();
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
    vi.useRealTimers();
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
    vi.useRealTimers();
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
    await vi.waitFor(() => expect(seen.errors).toHaveLength(1));

    expect(seen.errors[0].code).toBe("SSE_REDIRECT");
    expect(seen.errors[0].retryable).toBe(false);
    expect(seen.statuses).toContain("closed");

    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.attempts).toHaveLength(1);
    vi.useRealTimers();
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
    vi.useRealTimers();
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
    vi.useRealTimers();
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

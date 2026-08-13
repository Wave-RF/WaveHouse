import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { request } from "./http.js";
import type { HttpContext } from "./types.js";

function makeCtx(overrides?: Partial<HttpContext>): HttpContext {
  return {
    baseURL: "http://localhost:8080",
    options: { maxRetries: 0 },
    ...overrides,
  };
}

describe("request", () => {
  let fetchSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("makes a successful GET request", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ status: "ok" }), { status: 200 }));

    const result = await request<{ status: string }>(makeCtx(), {
      method: "GET",
      path: "/health",
    });

    expect(result.data).toEqual({ status: "ok" });
    expect(result.error).toBeNull();
    expect(fetchSpy).toHaveBeenCalledOnce();

    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/health");
    expect(init.method).toBe("GET");
  });

  it("makes a POST request with JSON body", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    await request(makeCtx(), {
      method: "POST",
      path: "/v1/ingest?table=clicks",
      body: { page: "/home" },
    });

    const [, init] = fetchSpy.mock.calls[0];
    expect(init.method).toBe("POST");
    expect(init.body).toBe(JSON.stringify({ page: "/home" }));
    expect(init.headers["Content-Type"]).toBe("application/json");
  });

  it("sends rawBody verbatim with a custom Content-Type", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ total: 1 }), { status: 200 }));

    const ndjson = '{"page":"/a"}\n{"page":"/b"}';
    await request(makeCtx(), {
      method: "POST",
      path: "/v1/ingest?table=clicks",
      rawBody: ndjson,
      contentType: "application/x-ndjson",
    });

    const [, init] = fetchSpy.mock.calls[0];
    // rawBody is sent as-is — no JSON.stringify wrapping.
    expect(init.body).toBe(ndjson);
    expect(init.headers["Content-Type"]).toBe("application/x-ndjson");
  });

  it("injects auth token when auth function provided", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx({ auth: async () => "my-token" }), {
      method: "GET",
      path: "/v1/schema",
    });

    const [, init] = fetchSpy.mock.calls[0];
    expect(init.headers.Authorization).toBe("Bearer my-token");
  });

  it("does not inject auth header when auth returns empty", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx({ auth: async () => "" }), {
      method: "GET",
      path: "/v1/schema",
    });

    const [, init] = fetchSpy.mock.calls[0];
    expect(init.headers.Authorization).toBeUndefined();
  });

  it("returns error for 4xx responses", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: "unknown table: foo" }), { status: 404 }),
    );

    const result = await request(makeCtx(), { method: "GET", path: "/v1/schema?table=foo" });

    expect(result.data).toBeNull();
    expect(result.error?.status).toBe(404);
    expect(result.error?.message).toBe("unknown table: foo");
    expect(result.error?.retryable).toBe(false);
  });

  it("returns error for 500 without retry when maxRetries=0", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: "internal" }), { status: 500 }),
    );

    const result = await request(makeCtx({ options: { maxRetries: 0 } }), {
      method: "GET",
      path: "/health",
    });

    expect(result.error?.status).toBe(500);
    expect(fetchSpy).toHaveBeenCalledOnce();
  });

  it("retries on 5xx up to maxRetries", async () => {
    fetchSpy
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "err" }), { status: 500 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "err" }), { status: 500 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const result = await request(makeCtx({ options: { maxRetries: 2 } }), {
      method: "GET",
      path: "/health",
    });

    expect(result.data).toEqual({ ok: true });
    expect(fetchSpy).toHaveBeenCalledTimes(3);
  });

  it("retries on network error", async () => {
    fetchSpy
      .mockRejectedValueOnce(new TypeError("Failed to fetch"))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const result = await request(makeCtx({ options: { maxRetries: 1 } }), {
      method: "GET",
      path: "/health",
    });

    expect(result.data).toEqual({ ok: true });
    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it("returns abort error when signal is aborted", async () => {
    fetchSpy.mockRejectedValue(new DOMException("Aborted", "AbortError"));

    const ac = new AbortController();
    ac.abort();

    const result = await request(makeCtx(), {
      method: "GET",
      path: "/health",
      signal: ac.signal,
    });

    expect(result.error?.code).toBe("ABORTED");
    expect(result.error?.retryable).toBe(false);
  });

  it("returns ABORTED when the abort lands during a retry backoff", async () => {
    // The existing abort test rejects the *fetch*, which is caught by the
    // branch at the top of the catch. This one aborts during the backoff that
    // runs inside that same catch — the one sleep whose rejection had no
    // handler, so it escaped `request()` as a raw DOMException and reached the
    // caller as an unhandled rejection instead of the documented Result.
    fetchSpy.mockRejectedValue(new TypeError("fetch failed"));

    const ac = new AbortController();
    setTimeout(() => ac.abort(), 20);

    // maxRetries must be > 0 or there is no backoff to abort during —
    // the default fixture is 0, which returns NETWORK_ERROR on the first pass.
    const result = await request(makeCtx({ options: { maxRetries: 1 } }), {
      method: "GET",
      path: "/health",
      signal: ac.signal,
    });

    expect(result.error?.code).toBe("ABORTED");
    expect(result.error?.retryable).toBe(false);
  });

  it("appends query params to URL", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx(), {
      method: "GET",
      path: "/v1/dlq/stats",
      params: { table: "clicks" },
    });

    const [url] = fetchSpy.mock.calls[0];
    expect(url).toContain("table=clicks");
  });

  it("keeps a base path prefix on the request URL", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx({ baseURL: "https://app.example.com/api/warehouse" }), {
      method: "POST",
      path: "/v1/query?table=clicks",
    });

    const [url] = fetchSpy.mock.calls[0];
    expect(url).toBe("https://app.example.com/api/warehouse/v1/query?table=clicks");
  });

  it("keeps a base path prefix when appending params", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx({ baseURL: "https://app.example.com/api/warehouse/" }), {
      method: "GET",
      path: "/v1/dlq/stats",
      params: { table: "clicks" },
    });

    const [url] = fetchSpy.mock.calls[0];
    expect(url).toBe("https://app.example.com/api/warehouse/v1/dlq/stats?table=clicks");
  });

  it("handles empty response body", async () => {
    fetchSpy.mockResolvedValue(new Response("", { status: 200 }));

    const result = await request(makeCtx(), {
      method: "POST",
      path: "/v1/schema/refresh",
    });

    expect(result.data).toBeUndefined();
    expect(result.error).toBeNull();
  });
});

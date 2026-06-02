import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PipeRef, PipesNamespace } from "./pipes.js";
import type { HttpContext } from "./types.js";

let fetchSpy: ReturnType<typeof vi.fn>;
const mockCreateStream = vi.fn();

function makeCtx(): HttpContext {
  return { baseURL: "http://localhost:8080", options: { maxRetries: 0 } };
}

describe("PipeRef", () => {
  beforeEach(() => {
    fetchSpy = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify([{ count: 42 }]), { status: 200 }));
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("fetch() POSTs to /v1/pipes/{name}", async () => {
    const ref = new PipeRef(makeCtx(), "top_pages", { limit: 10 }, mockCreateStream);
    const result = await ref.fetch();

    expect(result.data).toEqual([{ count: 42 }]);
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/pipes/top_pages");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({ limit: 10 });
  });

  it("fetch() sends empty object when no params", async () => {
    const ref = new PipeRef(makeCtx(), "simple", undefined, mockCreateStream);
    await ref.fetch();

    expect(JSON.parse(fetchSpy.mock.calls[0][1].body)).toEqual({});
  });

  it("is PromiseLike — await auto-executes fetch", async () => {
    const ref = new PipeRef(makeCtx(), "top_pages", undefined, mockCreateStream);
    const result = await ref;

    expect(result.data).toEqual([{ count: 42 }]);
  });

  it("returns error on failure", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: "pipe not found" }), { status: 404 }),
    );

    const ref = new PipeRef(makeCtx(), "missing", undefined, mockCreateStream);
    const result = await ref.fetch();

    expect(result.error?.status).toBe(404);
  });

  it("stream() delegates to createStream", () => {
    const ctrl = {} as any;
    mockCreateStream.mockReturnValue(ctrl);

    const ref = new PipeRef(makeCtx(), "live", undefined, mockCreateStream);
    const result = ref.stream();

    expect(mockCreateStream).toHaveBeenCalledWith("live", undefined);
    expect(result).toBe(ctrl);
  });
});

describe("PipesNamespace", () => {
  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("list() GETs /v1/admin/pipes", async () => {
    const pipes = [{ name: "p1", sql: "SELECT 1" }];
    fetchSpy.mockResolvedValue(new Response(JSON.stringify(pipes), { status: 200 }));

    const ns = new PipesNamespace(makeCtx());
    const result = await ns.list();

    expect(result.data).toEqual(pipes);
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/admin/pipes");
  });

  it("get() GETs /v1/admin/pipes/{name}", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ name: "p1", sql: "SELECT 1" }), { status: 200 }),
    );

    const ns = new PipesNamespace(makeCtx());
    const result = await ns.get("p1");

    expect(result.data?.name).toBe("p1");
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/admin/pipes/p1");
  });

  it("set() PUTs /v1/admin/pipes/{name}", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const ns = new PipesNamespace(makeCtx());
    const result = await ns.set("p1", { sql: "SELECT 1" });

    expect(result.error).toBeNull();
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/admin/pipes/p1");
    expect(init.method).toBe("PUT");
  });

  it("delete() DELETEs /v1/admin/pipes/{name}", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const ns = new PipesNamespace(makeCtx());
    const result = await ns.delete("p1");

    expect(result.error).toBeNull();
    expect(fetchSpy.mock.calls[0][1].method).toBe("DELETE");
  });
});

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TableRef } from "./table.js";
import type { HttpContext } from "./types.js";

let fetchSpy: ReturnType<typeof vi.fn>;
const mockCreateStream = vi.fn();

function makeCtx(): HttpContext {
  return { baseURL: "http://localhost:8080", options: { maxRetries: 0 } };
}

function table(name = "clicks"): TableRef {
  return new TableRef(makeCtx(), name, mockCreateStream);
}

describe("TableRef", () => {
  beforeEach(() => {
    fetchSpy = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify([{ page: "/home" }]), { status: 200 }));
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // --- fetch ---

  it("fetch() sends a structured query with default limit 1000", async () => {
    const result = await table().fetch();

    expect(result.data).toEqual([{ page: "/home" }]);
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/query?table=clicks");
    const body = JSON.parse(init.body);
    expect(body.limit).toBe(1000);
    // A bare fetch is a full-row read → select_all (not an empty projection).
    expect(body.select_all).toBe(true);
  });

  it("fetch() respects limit option", async () => {
    await table().fetch({ limit: 50 });

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.limit).toBe(50);
  });

  // --- select ---

  it("select() returns a QueryBuilder", async () => {
    const qb = table().select("page", "button");
    // QueryBuilder is PromiseLike
    expect(typeof qb.then).toBe("function");
    expect(typeof qb.where).toBe("function");
  });

  it("selectAll() returns a QueryBuilder that sends select_all", async () => {
    const qb = table().selectAll();
    expect(typeof qb.where).toBe("function");
    await qb.fetch();
    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.select_all).toBe(true);
    expect(body.columns).toBeUndefined();
  });

  // --- insert ---

  it("insert() single row sends POST to /v1/ingest?table={table}", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const result = await table().insert({ page: "/home", score: 42 });

    expect(result.data).toEqual({ ok: true });
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/ingest?table=clicks");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({ page: "/home", score: 42 });
  });

  it("insert() array sends one request per row", async () => {
    fetchSpy.mockImplementation(() =>
      Promise.resolve(new Response(JSON.stringify({ ok: true }), { status: 200 })),
    );

    const result = await table().insert([{ page: "/a" }, { page: "/b" }]);

    expect(fetchSpy).toHaveBeenCalledTimes(2);
    expect(result.data).toEqual({ ok: true });
  });

  it("insert() returns error if any row fails", async () => {
    fetchSpy
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "invalid json" }), { status: 400 }),
      );

    const result = await table().insert([{ page: "/a" }, { page: "/b" }]);

    expect(result.data).toBeNull();
    expect(result.error?.status).toBe(400);
  });

  it("insert() returns duplicate info", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ duplicate: true }), { status: 200 }));

    const result = await table().insert({ page: "/dup" });

    expect(result.data?.duplicate).toBe(true);
  });

  // --- schema ---

  it("schema() sends GET to /v1/schema?table={table}", async () => {
    const schema = {
      name: "clicks",
      columns: [{ name: "page", type: "String", is_nullable: false, has_default: false }],
    };
    fetchSpy.mockResolvedValue(new Response(JSON.stringify(schema), { status: 200 }));

    const result = await table().schema();

    expect(result.data).toEqual(schema);
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/schema?table=clicks");
  });

  // --- stream ---

  it("stream() delegates to createStream", () => {
    const ctrl = {} as any;
    mockCreateStream.mockReturnValue(ctrl);

    const result = table().stream({ since: "2026-01-01" });

    expect(mockCreateStream).toHaveBeenCalledWith("clicks", { since: "2026-01-01" });
    expect(result).toBe(ctrl);
  });

  // --- URL encoding ---

  it("encodes table name in URL", async () => {
    fetchSpy.mockResolvedValue(new Response("[]", { status: 200 }));

    await table("my table").fetch();

    expect(fetchSpy.mock.calls[0][0]).toContain("my%20table");
  });
});

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
    // Pins the header that reaches the server, NOT the explicit declaration at
    // the call site. Those are indistinguishable here: http.ts defaults to
    // `opts.contentType ?? "application/json"`, so deleting the declaration
    // produces byte-identical requests and every test still passes (verified).
    // What this does catch is the default drifting while the declaration is
    // absent — the two together are what make the request wrong. The NDJSON
    // paths differ from the default, so theirs are genuine pins; this one is
    // not, and a real one would have to observe the options handed to
    // request() rather than the fetch call.
    expect(init.headers["Content-Type"]).toBe("application/json");
    expect(JSON.parse(init.body)).toEqual({ page: "/home", score: 42 });
  });

  it("insert() array sends a single NDJSON request", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ total: 2, succeeded: 2, failed: 0, duplicates: 0 }), {
        status: 200,
      }),
    );

    const result = await table().insert([{ page: "/a" }, { page: "/b" }]);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/ingest?table=clicks");
    expect(init.method).toBe("POST");
    expect(init.headers["Content-Type"]).toBe("application/x-ndjson");
    // One JSON record per line, no trailing newline.
    expect(init.body).toBe('{"page":"/a"}\n{"page":"/b"}');
    expect(result.ok).toBe(true);
    expect(result.data).toMatchObject({ ok: true, total: 2, succeeded: 2, failed: 0 });
  });

  it("insert() empty array is a no-op with no request", async () => {
    const result = await table().insert([]);

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(result.ok).toBe(true);
    expect(result.data).toMatchObject({ ok: true, total: 0, succeeded: 0, failed: 0 });
  });

  it("insert() array surfaces per-record failures without erroring", async () => {
    fetchSpy.mockResolvedValue(
      new Response(
        JSON.stringify({
          total: 2,
          succeeded: 1,
          failed: 1,
          duplicates: 0,
          results: [
            { index: 1, ok: true },
            { index: 2, error: "validation failed" },
          ],
        }),
        { status: 200 },
      ),
    );

    const result = await table().insert([{ page: "/a" }, { bad: "row" } as never]);

    // The request itself succeeded...
    expect(result.ok).toBe(true);
    expect(result.error).toBeNull();
    // ...but not every record did.
    expect(result.data?.ok).toBe(false);
    expect(result.data?.failed).toBe(1);
    expect(result.data?.results).toEqual([
      { index: 1, ok: true },
      { index: 2, error: "validation failed" },
    ]);
  });

  it("insert() array tolerates a 200 with an empty body without throwing", async () => {
    // An intermediary could strip the body off a 200; the Result contract must
    // hold (never throws) and degrade to a zeroed summary.
    fetchSpy.mockResolvedValue(new Response("", { status: 200 }));

    const result = await table().insert([{ page: "/a" }]);

    expect(result.ok).toBe(true);
    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true, total: 0, succeeded: 0, failed: 0 });
  });

  it("insert() array returns the error arm on a whole-request failure", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: "unknown table: clicks" }), { status: 404 }),
    );

    const result = await table().insert([{ page: "/a" }]);

    expect(result.ok).toBe(false);
    expect(result.data).toBeNull();
    expect(result.error?.status).toBe(404);
  });

  it("insertNDJSON() sends a raw NDJSON string verbatim", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ total: 2, succeeded: 2, failed: 0, duplicates: 0 }), {
        status: 200,
      }),
    );

    const ndjson = '{"page":"/a"}\n{"page":"/b"}\n';
    const result = await table().insertNDJSON(ndjson);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/ingest?table=clicks");
    expect(init.headers["Content-Type"]).toBe("application/x-ndjson");
    expect(init.body).toBe(ndjson);
    expect(result.data).toMatchObject({ succeeded: 2 });
  });

  it("insertNDJSON() reads a non-string source (Blob) before sending", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ total: 1, succeeded: 1, failed: 0, duplicates: 0 }), {
        status: 200,
      }),
    );

    const blob = new Blob(['{"page":"/a"}\n'], { type: "application/x-ndjson" });
    const result = await table().insertNDJSON(blob);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [, init] = fetchSpy.mock.calls[0];
    expect(init.body).toBe('{"page":"/a"}\n');
    expect(result.data?.succeeded).toBe(1);
  });

  it("insert() returns duplicate info", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ duplicate: true }), { status: 200 }));

    const result = await table().insert({ page: "/dup" });

    expect(result.data?.duplicate).toBe(true);
  });

  // --- schema ---

  it("schema() sends GET to /v1/ops/schema?table={table}", async () => {
    const schema = {
      name: "clicks",
      columns: [{ name: "page", type: "String", is_nullable: false, has_default: false }],
    };
    fetchSpy.mockResolvedValue(new Response(JSON.stringify(schema), { status: 200 }));

    const result = await table().schema();

    expect(result.data).toEqual(schema);
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/ops/schema?table=clicks");
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

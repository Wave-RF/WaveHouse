import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DLQNamespace } from "./dlq.js";
import { PolicyNamespace } from "./policy.js";
import { SchemaNamespace } from "./schema.js";
import { sql } from "./sql.js";
import { SysNamespace } from "./sys.js";
import type { HttpContext } from "./types.js";

let fetchSpy: ReturnType<typeof vi.fn>;

function makeCtx(): HttpContext {
  return { baseURL: "http://localhost:8080", options: { maxRetries: 0 } };
}

describe("sql", () => {
  beforeEach(() => {
    fetchSpy = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify([{ count: 10 }]), { status: 200 }));
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => vi.restoreAllMocks());

  it("POSTs to /v1/admin/query with sql field", async () => {
    const result = await sql(makeCtx(), "SELECT count() FROM clicks");

    expect(result.data).toEqual([{ count: 10 }]);
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/admin/query");
    expect(JSON.parse(init.body)).toEqual({ sql: "SELECT count() FROM clicks" });
  });

  // The proxy endpoint doesn't accept positional `?` params — ClickHouse's
  // HTTP interface uses a different binding model entirely. The SDK no
  // longer accepts a params array; callers inline literals or use
  // ClickHouse's named-param syntax in the SQL string. This test pins that
  // contract: the request body never carries a `params` field, even if the
  // SQL string itself contains a `?` (which would now be passed to
  // ClickHouse verbatim and trigger a parse error there — the admin's
  // responsibility).
  it("never sends a params field", async () => {
    // Use SQL that actually contains `?` so the test exercises what its
    // comment claims — that the SDK passes the `?` through to ClickHouse
    // verbatim instead of trying to bind it to a missing params array.
    await sql(makeCtx(), "SELECT ?");

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body).toEqual({ sql: "SELECT ?" });
    expect(body.params).toBeUndefined();
  });

  it("returns error on failure", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: "forbidden" }), { status: 403 }),
    );

    const result = await sql(makeCtx(), "DROP TABLE foo");
    expect(result.error?.status).toBe(403);
  });
});

describe("SchemaNamespace", () => {
  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => vi.restoreAllMocks());

  it("list() GETs /v1/schema", async () => {
    const schemas = { clicks: { name: "clicks", columns: [] } };
    fetchSpy.mockResolvedValue(new Response(JSON.stringify(schemas), { status: 200 }));

    const ns = new SchemaNamespace(makeCtx());
    const result = await ns.list();

    expect(result.data).toEqual(schemas);
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/schema");
  });

  it("refresh() POSTs to /v1/schema/refresh", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    const ns = new SchemaNamespace(makeCtx());
    const result = await ns.refresh();

    expect(result.error).toBeNull();
    expect(fetchSpy.mock.calls[0][1].method).toBe("POST");
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/schema/refresh");
  });
});

describe("PolicyNamespace", () => {
  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => vi.restoreAllMocks());

  it("get() GETs /v1/admin/policy", async () => {
    const policy = { tables: {} };
    fetchSpy.mockResolvedValue(new Response(JSON.stringify(policy), { status: 200 }));

    const ns = new PolicyNamespace(makeCtx());
    const result = await ns.get();

    expect(result.data).toEqual(policy);
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/admin/policy");
  });

  it("set() PUTs /v1/admin/policy", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const ns = new PolicyNamespace(makeCtx());
    const result = await ns.set({ tables: {} });

    expect(result.error).toBeNull();
    expect(fetchSpy.mock.calls[0][1].method).toBe("PUT");
  });

  it("validate() POSTs /v1/admin/policy/validate", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ valid: true }), { status: 200 }));

    const ns = new PolicyNamespace(makeCtx());
    const result = await ns.validate({ tables: {} });

    expect(result.data).toEqual({ valid: true });
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/admin/policy/validate");
  });
});

describe("DLQNamespace", () => {
  const mockStream = vi.fn();

  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => vi.restoreAllMocks());

  it("list() GETs /v1/dlq/stats", async () => {
    const stats = { tables: { clicks: 5 }, total: 5 };
    fetchSpy.mockResolvedValue(new Response(JSON.stringify(stats), { status: 200 }));

    const ns = new DLQNamespace(makeCtx(), mockStream);
    const result = await ns.list();

    expect(result.data).toEqual(stats);
  });

  it("table() adds table param", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ tables: { clicks: 3 }, total: 3 }), { status: 200 }),
    );

    const ns = new DLQNamespace(makeCtx(), mockStream);
    await ns.table("clicks");

    expect(fetchSpy.mock.calls[0][0]).toContain("table=clicks");
  });

  it("stream() delegates to createStream", () => {
    const ctrl = {} as any;
    mockStream.mockReturnValue(ctrl);

    const ns = new DLQNamespace(makeCtx(), mockStream);
    expect(ns.stream()).toBe(ctrl);
    expect(mockStream).toHaveBeenCalledWith("dlq", undefined);
  });
});

describe("SysNamespace", () => {
  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => vi.restoreAllMocks());

  it("health() GETs /v1/health (content-free)", async () => {
    fetchSpy.mockResolvedValue(new Response(null, { status: 200 }));

    const ns = new SysNamespace(makeCtx());
    const result = await ns.health();

    expect(result.error).toBeNull();
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/health");
  });

  it("ready() GETs /readyz", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ status: "ready" }), { status: 200 }));

    const ns = new SysNamespace(makeCtx());
    const result = await ns.ready();

    expect(result.data).toEqual({ status: "ready" });
    expect(fetchSpy.mock.calls[0][0]).toContain("/readyz");
  });

  it("ready() returns error when service is down", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: "clickhouse unreachable" }), { status: 503 }),
    );

    const ns = new SysNamespace(makeCtx());
    const result = await ns.ready();

    expect(result.error?.status).toBe(503);
    expect(result.error?.retryable).toBe(true);
  });
});

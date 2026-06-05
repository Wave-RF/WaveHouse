import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryBuilder } from "./query-builder.js";
import type { HttpContext, OrderClause, QueryFilter } from "./types.js";

let fetchSpy: ReturnType<typeof vi.fn>;
const mockCreateStream = vi.fn();

function makeCtx(): HttpContext {
  return { baseURL: "http://localhost:8080", options: { maxRetries: 0 } };
}

function builder(table = "clicks"): QueryBuilder {
  return new QueryBuilder(
    makeCtx(),
    {
      table,
      columns: [],
      aggregations: [],
      filters: [],
      groupBy: [],
      orderBy: [],
    },
    mockCreateStream,
  );
}

/** A builder carrying a client-configured `defaultOrderBy` (4th ctor arg). */
function builderWithDefault(defaultOrderBy: OrderClause[], table = "clicks"): QueryBuilder {
  return new QueryBuilder(
    makeCtx(),
    {
      table,
      columns: [],
      aggregations: [],
      filters: [],
      groupBy: [],
      orderBy: [],
    },
    mockCreateStream,
    defaultOrderBy,
  );
}

describe("QueryBuilder", () => {
  beforeEach(() => {
    fetchSpy = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify([{ page: "/home" }]), { status: 200 }));
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // --- Immutability ---

  it("returns a new instance on every chain call", () => {
    const b1 = builder();
    const b2 = b1.select("page");
    const b3 = b2.where("score", ">", 10);
    expect(b1).not.toBe(b2);
    expect(b2).not.toBe(b3);
  });

  // --- Select ---

  it("builds AST with selected columns", async () => {
    await builder().select("page", "button").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.columns).toEqual(["page", "button"]);
  });

  it("accumulates columns across multiple select calls", async () => {
    await builder().select("page").select("button").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.columns).toEqual(["page", "button"]);
  });

  // --- Where ---

  it("builds filters with operator translation", async () => {
    await builder().where("score", ">", 10).fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.filters).toEqual([{ column: "score", op: "gt", value: 10 }]);
  });

  it("translates all operators correctly", async () => {
    const ops = [
      ["=", "eq"],
      ["!=", "neq"],
      [">", "gt"],
      [">=", "gte"],
      ["<", "lt"],
      ["<=", "lte"],
      ["in", "in"],
      ["like", "like"],
      ["not_like", "not_like"],
    ] as const;

    for (const [sdkOp, backendOp] of ops) {
      fetchSpy.mockResolvedValue(new Response("[]", { status: 200 }));
      await builder().where("col", sdkOp, "v").fetch();
      const body = JSON.parse(fetchSpy.mock.calls.at(-1)![1].body);
      expect(body.filters[0].op).toBe(backendOp);
    }
  });

  it("accumulates multiple where clauses", async () => {
    await builder().where("score", ">", 10).where("page", "=", "/home").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.filters).toHaveLength(2);
  });

  // --- Aggregations ---

  it("builds count aggregation", async () => {
    await builder().count("*", "total").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.aggregations).toEqual([{ fn: "count", column: "*", alias: "total" }]);
  });

  it("builds sum/avg/min/max aggregations", async () => {
    await builder().sum("score").avg("score").min("score").max("score").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.aggregations).toEqual([
      { fn: "sum", column: "score", alias: "sum_score" },
      { fn: "avg", column: "score", alias: "avg_score" },
      { fn: "min", column: "score", alias: "min_score" },
      { fn: "max", column: "score", alias: "max_score" },
    ]);
  });

  it("builds countDistinct aggregation", async () => {
    await builder().countDistinct("page").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.aggregations).toEqual([
      { fn: "countDistinct", column: "page", alias: "count_distinct_page" },
    ]);
  });

  it("builds custom aggregate", async () => {
    await builder().aggregate("quantile(0.95)", "latency", "p95").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.aggregations).toEqual([{ fn: "quantile(0.95)", column: "latency", alias: "p95" }]);
  });

  // --- GroupBy ---

  it("builds groupBy", async () => {
    await builder().groupBy("page", "button").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.group_by).toEqual(["page", "button"]);
  });

  // --- OrderBy ---

  it("builds orderBy with default asc", async () => {
    await builder().orderBy("page").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.order_by).toEqual([{ column: "page", dir: "asc" }]);
  });

  it("builds orderBy desc", async () => {
    await builder().orderBy("score", "desc").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.order_by).toEqual([{ column: "score", dir: "desc" }]);
  });

  // --- Limit ---

  it("builds limit", async () => {
    await builder().limit(50).fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.limit).toBe(50);
  });

  // --- TimeRange ---

  it("builds timeRange with since only", async () => {
    await builder().timeRange("received_timestamp", "1h").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.time_range).toEqual({ column: "received_timestamp", since: "1h" });
  });

  it("builds timeRange with since and until", async () => {
    await builder().timeRange("ts", "2026-01-01", "2026-02-01").fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.time_range).toEqual({ column: "ts", since: "2026-01-01", until: "2026-02-01" });
  });

  // --- Empty AST ---

  it("omits empty arrays and undefined fields from AST", async () => {
    await builder().fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.columns).toBeUndefined();
    expect(body.aggregations).toBeUndefined();
    expect(body.filters).toBeUndefined();
    expect(body.group_by).toBeUndefined();
    // A bare query carries no order_by: the SDK no longer hardcodes a
    // received_timestamp default, so .fetch() stays valid on any schema (#270).
    expect(body.order_by).toBeUndefined();
    expect(body.time_range).toBeUndefined();
  });

  // --- Fetch endpoint ---

  it("POSTs to /v1/query?table={table}", async () => {
    await builder("events").select("user_id").fetch();

    const [url] = fetchSpy.mock.calls[0];
    expect(url).toContain("/v1/query?table=events");
  });

  it("returns data on successful fetch", async () => {
    const result = await builder().select("page").fetch();

    expect(result.data).toEqual([{ page: "/home" }]);
    expect(result.error).toBeNull();
  });

  it("returns error on failed fetch", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: "bad query" }), { status: 400 }),
    );

    const result = await builder().fetch();

    expect(result.data).toBeNull();
    expect(result.error?.status).toBe(400);
  });

  // --- Pagination ---

  it("sets hasMore=true but no next() when limit is hit with no order", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify([{ page: "a" }, { page: "b" }]), { status: 200 }),
    );

    const result = await builder().limit(2).fetch();

    // hasMore is reported honestly from the row count, but without an order
    // column there's no deterministic cursor to paginate by, so next() is absent.
    expect(result.hasMore).toBe(true);
    expect(result.next).toBeUndefined();
  });

  it("sets hasMore=false when result length is less than limit", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify([{ page: "a" }]), { status: 200 }));

    const result = await builder().limit(10).fetch();

    expect(result.hasMore).toBe(false);
    expect(result.next).toBeUndefined();
  });

  it("next() adds cursor filter for pagination", async () => {
    fetchSpy
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify([
            { page: "a", received_timestamp: "2026-01-01T12:00:00Z" },
            { page: "b", received_timestamp: "2026-01-01T11:00:00Z" },
          ]),
          { status: 200 },
        ),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify([{ page: "c" }]), { status: 200 }));

    const result = await builder().orderBy("received_timestamp", "desc").limit(2).fetch();

    expect(result.next).toBeDefined();
    await result.next!();

    const body = JSON.parse(fetchSpy.mock.calls[1][1].body);
    const cursorFilter = body.filters.find((f: QueryFilter) => f.op === "lt");
    expect(cursorFilter).toEqual({
      column: "received_timestamp",
      op: "lt",
      value: "2026-01-01T11:00:00Z",
    });
  });

  // --- Configured defaultOrderBy (issue #270) ---

  it("applies a configured defaultOrderBy when no explicit orderBy is set", async () => {
    await builderWithDefault([{ column: "id", dir: "asc" }]).fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.order_by).toEqual([{ column: "id", dir: "asc" }]);
  });

  it("lets an explicit orderBy override the configured defaultOrderBy", async () => {
    await builderWithDefault([{ column: "id", dir: "asc" }])
      .orderBy("score", "desc")
      .fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.order_by).toEqual([{ column: "score", dir: "desc" }]);
  });

  it("exposes next() when a defaultOrderBy is configured and the page is full", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify([{ id: "a" }, { id: "b" }]), { status: 200 }),
    );

    const result = await builderWithDefault([{ column: "id", dir: "asc" }])
      .limit(2)
      .fetch();

    expect(result.hasMore).toBe(true);
    expect(result.next).toBeDefined();
  });

  it("drives the _fetchNext cursor from the configured defaultOrderBy", async () => {
    fetchSpy
      .mockResolvedValueOnce(
        new Response(JSON.stringify([{ id: "a" }, { id: "b" }]), { status: 200 }),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify([{ id: "c" }]), { status: 200 }));

    const result = await builderWithDefault([{ column: "id", dir: "asc" }])
      .limit(2)
      .fetch();
    await result.next!();

    const body = JSON.parse(fetchSpy.mock.calls[1][1].body);
    const cursorFilter = body.filters.find((f: QueryFilter) => f.op === "gt");
    expect(cursorFilter).toEqual({ column: "id", op: "gt", value: "b" });
    // The next page pins the same order the cursor assumes.
    expect(body.order_by).toEqual([{ column: "id", dir: "asc" }]);
  });

  it("does not apply defaultOrderBy to aggregation queries", async () => {
    await builderWithDefault([{ column: "id", dir: "asc" }])
      .count("*", "total")
      .fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body.order_by).toBeUndefined();
  });

  // --- PromiseLike ---

  it("is thenable — await auto-executes fetch", async () => {
    const result = await builder().select("page");

    expect(result.data).toEqual([{ page: "/home" }]);
  });

  // --- Stream ---

  it("delegates to createStream", () => {
    const ctrl = {} as any;
    mockCreateStream.mockReturnValue(ctrl);

    const result = builder().stream({ since: "2026-01-01" });

    expect(mockCreateStream).toHaveBeenCalledWith("clicks", { since: "2026-01-01" });
    expect(result).toBe(ctrl);
  });

  // --- Complex chain ---

  it("builds a complex query", async () => {
    await builder()
      .select("page")
      .where("score", ">", 10)
      .count("*", "total")
      .groupBy("page")
      .orderBy("total", "desc")
      .limit(50)
      .timeRange("received_timestamp", "1h")
      .cacheTTL(60)
      .fetch();

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body);
    expect(body).toEqual({
      columns: ["page"],
      filters: [{ column: "score", op: "gt", value: 10 }],
      aggregations: [{ fn: "count", column: "*", alias: "total" }],
      group_by: ["page"],
      order_by: [{ column: "total", dir: "desc" }],
      limit: 50,
      time_range: { column: "received_timestamp", since: "1h" },
    });
  });
});

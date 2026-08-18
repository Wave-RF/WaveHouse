import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { chTypeToTS, fetchSchemas, generateTypes } from "./codegen.js";

// The server's /v1/ops/schema wire shape: a JSON ARRAY of tables (Go
// SchemaRegistry.List() → []TableSchema), not a name-keyed map. Regression
// fixture for the map-parse bug that generated non-compiling `0: 0Row`
// output against a live server (#388). Deliberately unsorted to pin the
// by-name output ordering.
const wireSchemas = [
  {
    name: "user_events",
    columns: [
      { name: "id", type: "UInt64", is_nullable: false, has_default: false },
      { name: "note", type: "Nullable(String)", is_nullable: true, has_default: false },
    ],
  },
  {
    name: "clicks",
    columns: [
      { name: "page", type: "String", is_nullable: false, has_default: false },
      { name: "count", type: "UInt32", is_nullable: false, has_default: true },
    ],
  },
];

describe("fetchSchemas", () => {
  let fetchSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("parses the array the server returns", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify(wireSchemas), { status: 200 }));

    const schemas = await fetchSchemas("http://localhost:8080");

    expect(schemas).toEqual(wireSchemas);
    const [url] = fetchSpy.mock.calls[0];
    expect(String(url)).toContain("/v1/ops/schema");
  });

  it("sends the bearer token", async () => {
    fetchSpy.mockResolvedValue(new Response("[]", { status: 200 }));

    await fetchSchemas("http://localhost:8080", "tok");

    const [, init] = fetchSpy.mock.calls[0];
    expect(init.headers.Authorization).toBe("Bearer tok");
  });

  it("rejects a non-array body instead of generating garbage", async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ clicks: wireSchemas[1] }), { status: 200 }),
    );

    await expect(fetchSchemas("http://localhost:8080")).rejects.toThrow(/JSON array/);
  });

  it("surfaces an HTTP error status", async () => {
    fetchSpy.mockResolvedValue(new Response('{"error":"forbidden"}', { status: 403 }));

    await expect(fetchSchemas("http://localhost:8080")).rejects.toThrow(/403/);
  });
});

describe("generateTypes", () => {
  it("keys the Database interface by table name, sorted, from the wire-shape array", () => {
    const out = generateTypes(wireSchemas);

    expect(out).toContain("export interface Database {");
    expect(out).toContain("  clicks: ClicksRow;");
    expect(out).toContain("  user_events: UserEventsRow;");
    expect(out.indexOf("clicks: ClicksRow")).toBeLessThan(
      out.indexOf("user_events: UserEventsRow"),
    );
    // The old map-parse bug keyed rows by array index ("0: 0Row;").
    expect(out).not.toMatch(/^\s*\d+\??:/m);

    expect(out).toContain("export interface ClicksRow {");
    expect(out).toContain("  page: string;");
    expect(out).toContain("  count?: number;"); // has_default → optional
    expect(out).toContain("export interface UserEventsRow {");
    expect(out).toContain("  id: number;");
    expect(out).toContain("  note: string | null;"); // Nullable → | null
  });

  it("orders tables by code unit, independent of the host locale", () => {
    // "_" (0x5F) sorts before "b" (0x62) by code unit; many ICU locales
    // collate punctuation-insensitively and would flip the pair.
    const out = generateTypes([
      { name: "ab", columns: [] },
      { name: "a_b", columns: [] },
    ]);

    expect(out.indexOf("  a_b: ABRow;")).toBeGreaterThan(-1);
    expect(out.indexOf("  a_b: ABRow;")).toBeLessThan(out.indexOf("  ab: AbRow;"));
  });
});

describe("chTypeToTS", () => {
  it.each([
    ["String", "string"],
    ["Nullable(String)", "string | null"],
    ["LowCardinality(String)", "string"],
    ["UInt64", "number"],
    ["Bool", "boolean"],
    ["DateTime64(3, 'UTC')", "string"],
    ["Array(UInt8)", "number[]"],
    ["Map(String, UInt64)", "Record<string, number>"],
    ["Tuple(String, UInt8)", "unknown[]"],
    ["AggregateFunction(sum, UInt64)", "unknown"],
  ])("%s → %s", (ch, ts) => {
    expect(chTypeToTS(ch)).toBe(ts);
  });
});

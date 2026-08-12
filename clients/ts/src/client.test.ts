import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient, WaveHouseClient } from "./client.js";
import { DLQNamespace } from "./dlq.js";
import { PipeRef, PipesNamespace } from "./pipes.js";
import { PolicyNamespace } from "./policy.js";
import { SchemaNamespace } from "./schema.js";
import { SysNamespace } from "./sys.js";
import { TableRef } from "./table.js";

let fetchSpy: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchSpy = vi.fn().mockResolvedValue(new Response(JSON.stringify([]), { status: 200 }));
  vi.stubGlobal("fetch", fetchSpy);
});

afterEach(() => vi.restoreAllMocks());

describe("createClient", () => {
  it("returns a WaveHouseClient instance", () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    expect(client).toBeInstanceOf(WaveHouseClient);
  });

  it("strips trailing slashes from baseURL", () => {
    const client = createClient({ baseURL: "http://localhost:8080///" });
    expect(client._ctx.baseURL).toBe("http://localhost:8080");
  });

  it("keeps a path prefix on baseURL, minus trailing slashes", () => {
    const client = createClient({ baseURL: "https://app.example.com/api/warehouse/" });
    expect(client._ctx.baseURL).toBe("https://app.example.com/api/warehouse");
  });

  it("sends requests under the baseURL path prefix", async () => {
    const client = createClient({ baseURL: "https://app.example.com/api/warehouse" });
    await client.from("clicks").select("page").limit(1);

    const [url] = fetchSpy.mock.calls[0];
    expect(new URL(url as string).pathname).toBe("/api/warehouse/v1/query");
  });

  it("defaults maxRetries to 2", () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    expect(client._ctx.options.maxRetries).toBe(2);
  });

  it("respects custom maxRetries", () => {
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { maxRetries: 5 },
    });
    expect(client._ctx.options.maxRetries).toBe(5);
  });

  it("passes auth function to context", () => {
    const authFn = () => Promise.resolve("token");
    const client = createClient({ baseURL: "http://localhost:8080", auth: authFn });
    expect(client._ctx.auth).toBe(authFn);
  });
});

describe("WaveHouseClient namespaces", () => {
  const client = createClient({ baseURL: "http://localhost:8080" });

  it("has schema namespace", () => {
    expect(client.schema).toBeInstanceOf(SchemaNamespace);
  });

  it("has policy namespace", () => {
    expect(client.policy).toBeInstanceOf(PolicyNamespace);
  });

  it("has dlq namespace", () => {
    expect(client.dlq).toBeInstanceOf(DLQNamespace);
  });

  it("has sys namespace", () => {
    expect(client.sys).toBeInstanceOf(SysNamespace);
  });

  it("has pipes namespace", () => {
    expect(client.pipes).toBeInstanceOf(PipesNamespace);
  });
});

describe("WaveHouseClient.from()", () => {
  it("returns a TableRef", () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    const table = client.from("clicks");
    expect(table).toBeInstanceOf(TableRef);
  });

  it("passes the table name through", async () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    client.from("events").fetch();

    // Wait for the fetch to be called
    await vi.waitFor(() => expect(fetchSpy).toHaveBeenCalledTimes(1));
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/query?table=events");
  });
});

describe("WaveHouseClient.pipe()", () => {
  it("returns a PipeRef", () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    const pipe = client.pipe("top_pages");
    expect(pipe).toBeInstanceOf(PipeRef);
  });

  it("PipeRef is PromiseLike", () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    const pipe = client.pipe("top_pages", { limit: 10 });
    expect(typeof pipe.then).toBe("function");
  });
});

describe("WaveHouseClient.sql()", () => {
  it("delegates to sql() and POSTs to /v1/admin/query", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify([{ count: 42 }]), { status: 200 }));

    const client = createClient({ baseURL: "http://localhost:8080" });
    const result = await client.sql("SELECT count() FROM clicks");

    expect(result.data).toEqual([{ count: 42 }]);
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/admin/query");
  });

  it("throws a migration-clear error when called with a legacy params array", () => {
    // The second argument used to be a positional-`?` params array.
    // TS callers get a compile-time error; JS callers (or `any`-typed
    // call sites) would silently pass the array as `opts` and get
    // confusing downstream SQL errors. The runtime guard catches
    // that case and points at the migration.
    const client = createClient({ baseURL: "http://localhost:8080" });
    // Force-cast so the test compiles under strict TS.
    const callWithLegacyParams = () =>
      (client.sql as unknown as (q: string, p: unknown) => unknown)(
        "SELECT * FROM clicks WHERE id = ?",
        ["some-id"],
      );

    expect(callWithLegacyParams).toThrow(/client\.sql\(sql, params\) was removed/);
  });
});

describe("options.fetch", () => {
  it("routes requests through a supplied fetch instead of the global", async () => {
    const custom = vi.fn().mockResolvedValue(new Response(JSON.stringify([]), { status: 200 }));
    const client = createClient({
      baseURL: "http://localhost:8080",
      auth: () => "test-token",
      options: { fetch: custom as unknown as typeof fetch },
    });

    await client.from("clicks").select("*").limit(1);

    expect(custom).toHaveBeenCalledTimes(1);
    expect(fetchSpy).not.toHaveBeenCalled();
    // The documented contract: a string URL and a complete RequestInit —
    // proxy/middleware consumers rely on the whole request reaching them.
    const [url, init] = custom.mock.calls[0];
    expect(typeof url).toBe("string");
    expect(String(url)).toContain("http://localhost:8080");
    expect(init.method).toBe("POST");
    expect(init.headers).toMatchObject({
      "Content-Type": "application/json",
      Authorization: "Bearer test-token",
    });
    expect(typeof init.body).toBe("string");
  });

  it("falls back to the global fetch when no override is given", async () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    await client.from("clicks").select("*").limit(1);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("keeps the global late-bound so it can be swapped after construction", async () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    const replacement = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify([]), { status: 200 }));
    vi.stubGlobal("fetch", replacement);

    await client.from("clicks").select("*").limit(1);

    expect(replacement).toHaveBeenCalledTimes(1);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("applies the override to retries too, not just the first attempt", async () => {
    const custom = vi
      .fn()
      .mockResolvedValueOnce(new Response("boom", { status: 500 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([]), { status: 200 }));
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { fetch: custom as unknown as typeof fetch, maxRetries: 1 },
    });

    await client.from("clicks").select("*").limit(1);

    expect(custom).toHaveBeenCalledTimes(2);
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});

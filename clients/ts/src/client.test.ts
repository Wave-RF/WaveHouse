import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient, WaveHouseClient } from "./client.js";
import { DLQNamespace } from "./dlq.js";
import { PipeRef, PipesNamespace } from "./pipes.js";
import { PolicyNamespace } from "./policy.js";
import { SchemaNamespace } from "./schema.js";
import { SysNamespace } from "./sys.js";
import { TableRef } from "./table.js";
import type { FetchLike, PipeRequestOptions, RequestOptions } from "./types.js";

let fetchSpy: ReturnType<typeof vi.fn>;

beforeEach(() => {
  // A fresh Response per call: a Response body can only be read once, so a
  // shared instance makes the second request fail and retry, and a test
  // asserting on `mock.calls[1]` would silently be reading that retry.
  fetchSpy = vi
    .fn()
    .mockImplementation(() => Promise.resolve(new Response(JSON.stringify([]), { status: 200 })));
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

  it("rejects a per-call limit, which the pipes endpoint cannot honour", async () => {
    const client = createClient({ baseURL: "http://localhost:8080" });
    // Compile-time assertions: accepting `limit` here would silently drop it,
    // since the endpoint binds the body as the pipe's parameters. A row cap
    // belongs in the pipe's own SQL, supplied via `wh.pipe(name, { limit })`.

    // @ts-expect-error — as a fresh literal
    await client.pipe("top_pages").fetch({ limit: 10 });

    // ...and as a named value. This is the case a plain `Pick<>` would let
    // through, since excess-property checking only rejects literals — so the
    // limit would reach the endpoint, be ignored, and never be flagged.
    const shared: RequestOptions = { signal: AbortSignal.timeout(1000), limit: 10 };
    // @ts-expect-error — `limit` is not part of PipeRef.fetch's options
    await client.pipe("top_pages").fetch(shared);

    // `signal` alone is accepted, literal or named.
    await client.pipe("top_pages").fetch({ signal: AbortSignal.timeout(1000) });
    const signalOnly: PipeRequestOptions = { signal: AbortSignal.timeout(1000) };
    await client.pipe("top_pages").fetch(signalOnly);
  });
});

describe("WaveHouseClient.sql()", () => {
  it("delegates to sql() and POSTs to /v1/ops/query", async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify([{ count: 42 }]), { status: 200 }));

    const client = createClient({ baseURL: "http://localhost:8080" });
    const result = await client.sql("SELECT count() FROM clicks");

    expect(result.data).toEqual([{ count: 42 }]);
    expect(fetchSpy.mock.calls[0][0]).toContain("/v1/ops/query");
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
    // Typed as FetchLike rather than cast: a consumer's override needs no cast,
    // so the test shouldn't need one either.
    const custom = vi.fn<FetchLike>(async () => new Response(JSON.stringify([]), { status: 200 }));
    const client = createClient({
      baseURL: "http://localhost:8080",
      auth: () => "test-token",
      options: { fetch: custom },
    });

    await client.from("clicks").select("*").limit(1);

    expect(custom).toHaveBeenCalledTimes(1);
    expect(fetchSpy).not.toHaveBeenCalled();
    // The documented contract: a string URL and a complete RequestInit —
    // proxy/middleware consumers rely on the whole request reaching them.
    const [url, init] = custom.mock.calls[0];
    expect(typeof url).toBe("string");
    expect(url).toContain("http://localhost:8080");
    expect(init?.method).toBe("POST");
    expect(init?.headers).toMatchObject({
      "Content-Type": "application/json",
      Authorization: "Bearer test-token",
    });
    expect(typeof init?.body).toBe("string");
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
      .fn<FetchLike>()
      .mockResolvedValueOnce(new Response("boom", { status: 500 }))
      .mockResolvedValueOnce(new Response(JSON.stringify([]), { status: 200 }));
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { fetch: custom, maxRetries: 1 },
    });

    await client.from("clicks").select("*").limit(1);

    expect(custom).toHaveBeenCalledTimes(2);
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});

describe("options.headers", () => {
  const headersOf = (call: number) =>
    fetchSpy.mock.calls[call][1].headers as Record<string, string>;

  it("adds configured headers to every request", async () => {
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { headers: { "CF-Access-Client-Id": "abc.access", "X-Tenant": "acme" } },
    });

    await client.from("clicks").select("*").limit(1);
    await client.from("clicks").select("*").limit(1);

    // Exactly two — proves call 1 is a second request, not a retry of the first.
    expect(fetchSpy).toHaveBeenCalledTimes(2);
    for (const call of [0, 1]) {
      expect(headersOf(call)).toMatchObject({
        "CF-Access-Client-Id": "abc.access",
        "X-Tenant": "acme",
      });
    }
  });

  it("cannot override the Content-Type a request needs", async () => {
    // A global Content-Type outranking the request's own is a documented way to
    // break uploads — it must lose, not merge.
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { headers: { "Content-Type": "text/plain" } },
    });

    await client.from("clicks").select("*").limit(1);

    expect(headersOf(0)["Content-Type"]).toBe("application/json");
    expect(Object.values(headersOf(0))).not.toContain("text/plain");
  });

  it("cannot displace the auth token, even under different casing", async () => {
    const client = createClient({
      baseURL: "http://localhost:8080",
      auth: () => "real-token",
      options: { headers: { authorization: "Bearer impostor" } },
    });

    await client.from("clicks").select("*").limit(1);

    const sent = headersOf(0);
    expect(sent.Authorization).toBe("Bearer real-token");
    // The lowercase spelling must not ride along beside the canonical one.
    expect(sent.authorization).toBeUndefined();
  });

  it("collapses two configured spellings of one header instead of sending both", async () => {
    // Left as separate keys, the Headers constructor comma-joins them at fetch
    // time — `x-tenant: acme, beta` — which is the corruption this guards.
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { headers: { "x-tenant": "acme", "X-Tenant": "beta" } },
    });

    await client.from("clicks").select("*").limit(1);

    const sent = headersOf(0);
    const spellings = Object.keys(sent).filter((k) => k.toLowerCase() === "x-tenant");
    expect(spellings).toHaveLength(1);
    expect(new Headers(sent).get("x-tenant")).toBe("beta");
  });

  it("still applies when no auth is configured", async () => {
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { headers: { "X-Tenant": "acme" } },
    });

    await client.from("clicks").select("*").limit(1);

    expect(headersOf(0)["X-Tenant"]).toBe("acme");
    expect(headersOf(0).Authorization).toBeUndefined();
  });
});

describe("options.fetchOptions", () => {
  it("merges configured RequestInit fields into every request", async () => {
    const client = createClient({
      baseURL: "http://localhost:8080",
      options: { fetchOptions: { credentials: "include", cache: "no-store" } },
    });

    await client.from("clicks").select("*").limit(1);

    const init = fetchSpy.mock.calls[0][1];
    expect(init.credentials).toBe("include");
    expect(init.cache).toBe("no-store");
  });

  it("cannot corrupt the fields the SDK controls", async () => {
    const client = createClient({
      baseURL: "http://localhost:8080",
      auth: () => "test-token",
      options: {
        headers: { "X-Tenant": "acme" },
        fetchOptions: {
          method: "DELETE",
          body: "hijacked",
          headers: { "X-Tenant": "overridden", Authorization: "Bearer impostor" },
        } as RequestInit,
      },
    });

    await client.from("clicks").select("*").limit(1);

    const init = fetchSpy.mock.calls[0][1];
    expect(init.method).toBe("POST");
    expect(init.body).not.toBe("hijacked");
    // options.headers is the header channel; fetchOptions.headers is discarded
    // wholesale rather than merged, so it can't smuggle an Authorization past auth.
    expect(init.headers).toMatchObject({
      "X-Tenant": "acme",
      Authorization: "Bearer test-token",
    });
  });
});

describe("stream error delivery", () => {
  it("delivers a start-up failure to a subscriber attached after .stream() returns", async () => {
    // Regression: the transport used to detect an unresolvable `baseURL`
    // synchronously inside `connect()`, which runs in the StreamController
    // constructor — so the error fired before `.stream()` had returned and no
    // subscriber could ever see it. Every transport-level test attaches its
    // callbacks before `connect()`, which is exactly why they missed it.
    const wh = createClient({ baseURL: "not-a-url" });
    const stream = wh.from("clicks").stream();

    const errors: unknown[] = [];
    const statuses: string[] = [];
    stream.subscribe({
      next: () => {},
      status: (s) => statuses.push(s),
      error: (e) => errors.push(e),
    });

    await vi.waitFor(() => expect(errors).toHaveLength(1));
    expect((errors[0] as { code: string }).code).toBe("SSE_CONNECT_ERROR");
    expect(statuses).toContain("closed");
  });
});

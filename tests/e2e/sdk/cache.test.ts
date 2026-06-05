import { describe, expect, it } from "vitest";
import { chQuery, dataClient, makeJWT, testId, WH_URL, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Cache", () => {
  const wh = dataClient();
  const T = suiteTables("cache");

  it("verifies X-Cache headers and invalidation lifecycle", async () => {
    // We use raw fetch here because the SDK abstracts away HTTP headers
    const token = makeJWT({
      sub: "cache-test",
      role: "viewer",
      tenant_id: "acme",
    });
    const reqOpts = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      // A deterministic query body to ensure consistent cache key generation
      body: JSON.stringify({ columns: ["*"], limit: 69 }), // must make sure we never use this anywhere else in our tests!
    };

    const url = `${WH_URL}/v1/query?table=${T.clicks}`;

    // 1. Prime the cache (MISS)
    const res1 = await fetch(url, reqOpts);
    if (!res1.ok) throw new Error(`Query failed: ${await res1.text()}`);
    expect(res1.ok).toBe(true);
    expect(res1.headers.get("x-cache")).toEqual("MISS");

    // 2. Fetch again immediately (HIT)
    const res2 = await fetch(url, reqOpts);
    expect(res2.ok).toBe(true);
    expect(res2.headers.get("x-cache")).toBe("HIT");

    // 3. Invalidate the cache by ingesting a new row.
    // (We can use the SDK here since we don't care about the ingest headers)
    const eventId = testId();
    const insertRes = await wh.from(T.clicks).insert({
      event_id: eventId,
      page: "/cache-header-test",
      user_id: "u-cache",
      session_id: "s-cache",
      duration_ms: 100,
    });
    expect(insertRes.error).toBeNull();

    // 4. Wait for the async worker to flush to ClickHouse and invalidate the cache.
    // By querying ClickHouse directly, we don't accidentally trigger a cache re-prime!
    await waitForCondition(async () => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE event_id = '${eventId}'`,
      );
      return r.length === 1;
    }, 10_000);

    // 5. Fetch a third time (MISS - cache was invalidated)
    const res3 = await fetch(url, reqOpts);
    expect(res3.ok).toBe(true);
    expect(res3.headers.get("x-cache")).toEqual("MISS");
  });

  it("expires cache naturally based on TTL", async () => {
    const token = makeJWT({
      sub: "ttl-test",
      role: "viewer",
      tenant_id: "acme",
    });
    const reqOpts = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ columns: ["*"], limit: 420 }), // Unique body for this test
    };

    const url = `${WH_URL}/v1/query?table=${T.clicks}`;

    // Prime the cache (MISS) and test how long the query takes
    const start = Date.now();
    const res1 = await fetch(url, reqOpts);
    if (!res1.ok) throw new Error(`Query failed: ${await res1.text()}`);
    const queryTime = Date.now() - start;
    expect(res1.ok).toBe(true);
    expect(res1.headers.get("x-cache")).toEqual("MISS");

    const ttl = Math.max(10_000, queryTime * 1000); // queryTime * 1000 is what our cache.QueryTimeToTTL does
    console.log(`Query [MISS] took ${queryTime}ms, so expected TTL: ${ttl}`);
    if (ttl > 20_000) {
      console.warn(
        "Query took a longer time than expected, this could be flakiness or a fundamental regression in some part of the query api flow. Please continue monitoring.",
      );
    }

    // Fetch again immediately (HIT)
    const hotFetch = Date.now();
    const res2 = await fetch(url, reqOpts);
    if (!res2.ok) throw new Error(`Query failed: ${await res2.text()}`);
    const hotTime = Date.now() - hotFetch;
    expect(res2.headers.get("x-cache")).toBe("HIT");
    console.log(`Query [HIT] took ${hotTime}ms`);

    // Wait for the ttl to expire.
    // Wait an extra 1 second to be safe.
    await new Promise((resolve) => setTimeout(resolve, ttl + 1000));

    // Fetch a third time (MISS - cache naturally expired)
    const res3 = await fetch(url, reqOpts);
    expect(res3.ok).toBe(true);
    expect(res3.headers.get("x-cache")).toEqual("MISS");
  }, 60_000);
});

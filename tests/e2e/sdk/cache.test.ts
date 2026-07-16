import { describe, expect, it } from "vitest";
import {
  adminClient,
  chQuery,
  dataClient,
  makeJWT,
  testId,
  WH_URL,
  waitForCondition,
} from "./helpers.js";
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
      body: JSON.stringify({ select_all: true, limit: 69 }), // must make sure we never use this anywhere else in our tests!
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

  it("invalidates a pipe reading a materialized view's TO target on source ingest", async () => {
    const admin = adminClient();
    const stamp = Date.now();
    const src = `mv_src_${stamp}`;
    const tgt = `mv_tgt_${stamp}`;
    const mv = `mv_${stamp}`;
    const pipeName = `pipe_mv_target_${stamp}`;

    // Source + TO target are both ordinary tables; ClickHouse populates the
    // target through the MV on every insert into the source. Nothing ever
    // writes the target directly, so the pipe below stays fresh only if
    // WaveHouse carries a source write through the MV into the target's
    // cache version (the trigger→target cascade).
    await chQuery(
      `CREATE TABLE IF NOT EXISTS default.\`${src}\` (event_id String) ENGINE = MergeTree() ORDER BY event_id`,
    );
    await chQuery(
      `CREATE TABLE IF NOT EXISTS default.\`${tgt}\` (event_id String) ENGINE = MergeTree() ORDER BY event_id`,
    );
    await chQuery(
      `CREATE MATERIALIZED VIEW IF NOT EXISTS default.\`${mv}\` TO default.\`${tgt}\` AS SELECT event_id FROM default.\`${src}\``,
    );

    // Discover the MV (and its trigger→target edge) before the pipe first runs.
    const refreshRes = await admin.schema.refresh();
    expect(refreshRes.error).toBeNull();

    const setRes = await admin.pipes.set(pipeName, {
      sql: `SELECT count() AS c FROM default.\`${tgt}\``,
      description: "E2E: MV TO-target invalidation",
    });
    expect(setRes.error).toBeNull();

    // Raw fetch (as in the header test above) so X-Cache is visible. Admin
    // bypasses the pipe's allowed_roles allowlist, so no policy edits needed.
    const token = makeJWT({ sub: "cache-mv-test", role: "admin", tenant_id: "acme" });
    const exec = () =>
      fetch(`${WH_URL}/v1/pipes/${pipeName}`, {
        headers: { Authorization: `Bearer ${token}` },
      });

    const res1 = await exec();
    if (!res1.ok) throw new Error(`Pipe failed: ${await res1.text()}`);
    expect(res1.headers.get("x-cache")).toEqual("MISS");
    expect(await res1.json()).toEqual([{ c: 0 }]); // target starts empty

    const res2 = await exec();
    expect(res2.headers.get("x-cache")).toBe("HIT");

    // Ingest into the SOURCE. The worker's flush inserts into ClickHouse
    // (firing the MV into the target) and then invalidates the source's
    // namespace; the cascade must carry that bump into the target the pipe
    // folded.
    const eventId = testId();
    const insertRes = await admin.from(src).insert({ event_id: eventId });
    expect(insertRes.error).toBeNull();

    // The row landing in the TARGET proves the MV fired and the flush ran...
    await waitForCondition(async () => {
      const r = await chQuery(
        `SELECT event_id FROM default.\`${tgt}\` WHERE event_id = '${eventId}'`,
      );
      return r.length === 1;
    }, 10_000);

    // ...and the eviction follows the flush, so poll to MISS (a HIT poll
    // doesn't re-prime anything; the first MISS is the eviction).
    await waitForCondition(async () => (await exec()).headers.get("x-cache") === "MISS", 5_000);

    // Whatever we fetch now is post-eviction: the pipe sees the new row.
    const res3 = await exec();
    expect(await res3.json()).toEqual([{ c: 1 }]);

    await admin.pipes.delete(pipeName);
    await chQuery(`DROP VIEW IF EXISTS default.\`${mv}\``);
    await chQuery(`DROP TABLE IF EXISTS default.\`${tgt}\``);
    await chQuery(`DROP TABLE IF EXISTS default.\`${src}\``);
  }, 30_000);

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
      body: JSON.stringify({ select_all: true, limit: 420 }), // Unique body for this test
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

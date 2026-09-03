import { beforeAll, describe, expect, it } from "vitest";
import {
  adminClient,
  chQuery,
  dataClient,
  makeJWT,
  testId,
  WH_URL,
  waitForCondition,
} from "./helpers.js";
import { readPolicyFile, setPolicy } from "./settings.js";
import { suiteTables } from "./tables.js";

describe("Query", () => {
  const wh = dataClient();
  const T = suiteTables("query");

  // Seed a known set of rows before the query tests run
  const seededIds: string[] = [];

  beforeAll(async () => {
    for (let i = 0; i < 10; i++) {
      const id = testId();
      seededIds.push(id);
      await wh.from(T.clicks).insert({
        event_id: id,
        page: i < 5 ? "/query-a" : "/query-b",
        user_id: `u-query-${i}`,
        session_id: `s-query-${i}`,
        country: i % 2 === 0 ? "US" : "GB",
        duration_ms: (i + 1) * 100,
      });
    }
    // Poll until all seeded rows are visible in ClickHouse
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT count() as cnt FROM default.${T.clicks} WHERE event_id IN ('${seededIds.join("','")}')`,
        signal,
      );
      return Number((r[0] as any).cnt) === seededIds.length;
    }, 15_000);
  });

  it("fetches rows with default limit", async () => {
    const result = await wh.from(T.clicks).fetch();
    expect(result.error).toBeNull();
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data!.length).toBeGreaterThan(0);
  });

  it("select + where + orderBy + limit", async () => {
    const result = await wh
      .from(T.clicks)
      .select("page", "user_id", "duration_ms")
      .where("country", "=", "US")
      .orderBy("duration_ms", "desc")
      .limit(5);

    expect(result.error).toBeNull();
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data!.length).toBeLessThanOrEqual(5);

    // All returned rows should have country=US filtered at the backend
    // (columns selected don't include country, but verify the query succeeded)
    for (const row of result.data!) {
      expect(row).toHaveProperty("page");
      expect(row).toHaveProperty("duration_ms");
    }
  });

  it("time_range over a DateTime64 column does not 500 (#238)", async () => {
    const jwt = makeJWT({ sub: "test-viewer", role: "viewer", tenant_id: "acme" });
    const res = await fetch(`${WH_URL}/v1/query?table=${encodeURIComponent(T.clicks)}`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${jwt}` },
      body: JSON.stringify({
        columns: ["event_id", "page"],
        time_range: { column: "received_timestamp", since: "24h" },
      }),
    });

    // The bug surfaced as HTTP 500 (ClickHouse code 53); the fix returns 200.
    expect(res.status).toBe(200);
    const rows = (await res.json()) as unknown[];
    expect(Array.isArray(rows)).toBe(true);
    // The rows seeded in beforeAll all carry received_timestamp = now64(3), so a
    // "last 24h" window must return at least those — proving the bound actually
    // matched DateTime64 values rather than just returning an empty 200.
    expect(rows.length).toBeGreaterThanOrEqual(seededIds.length);
  });

  it("aggregation: count with groupBy", async () => {
    const result = await wh
      .from(T.clicks)
      .select("page")
      .count("page", "click_count")
      .groupBy("page")
      .orderBy("click_count", "desc")
      .limit(20);

    expect(result.error).toBeNull();
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data!.length).toBeGreaterThan(0);

    for (const row of result.data!) {
      expect(row).toHaveProperty("page");
      expect(row).toHaveProperty("click_count");
      expect(Number((row as any).click_count)).toBeGreaterThan(0);
    }
  });

  it("pagination: limit + next page", async () => {
    // Paginate by the unique event_id so the keyset cursor can't skip rows
    // (received_timestamp ties on batch-inserted rows would — see #175).
    const page1 = await wh.from(T.clicks).select().orderBy("event_id", "asc").limit(3).fetch();
    expect(page1.error).toBeNull();
    expect(page1.data).toHaveLength(3);
    expect(page1.hasMore).toBe(true);
    expect(page1.next).toBeDefined();

    const page2 = await page1.next!();
    expect(page2.error).toBeNull();
    expect(page2.data).toBeInstanceOf(Array);
    expect(page2.data!.length).toBeGreaterThan(0);
  });

  // #175: the old received_timestamp default cursor skipped rows tied on a page
  // boundary (batch inserts share one now64(3) ms). #270 removed that default, so
  // a bare .fetch() with no .orderBy() now reports hasMore but offers no next() —
  // deterministic pagination needs an explicit .orderBy() (see the test above).
  it("reports hasMore without next() when no order is set (#270, sidesteps #175)", async () => {
    const page1 = await wh.from(T.clicks).select().limit(3).fetch();
    expect(page1.error).toBeNull();
    expect(page1.data).toHaveLength(3);
    expect(page1.hasMore).toBe(true);
    expect(page1.next).toBeUndefined();
  });

  // TODO(#274): re-enable once the backend supplies a per-table default sort order
  // (plus a resumable cursor). Then a bare .fetch() — no explicit .orderBy() —
  // should paginate end-to-end again, the intent of the old received_timestamp
  // default but without the #175 tie bug.
  it.skip("paginates a bare .fetch() once the backend supplies a default order (#274)", async () => {
    const page1 = await wh.from(T.clicks).select().limit(3).fetch();
    expect(page1.error).toBeNull();
    expect(page1.data).toHaveLength(3);
    expect(page1.hasMore).toBe(true);
    expect(page1.next).toBeDefined();

    const page2 = await page1.next!();
    expect(page2.error).toBeNull();
    expect(page2.data!.length).toBeGreaterThan(0);
  });

  it("raw SQL query", async () => {
    // Scope to seededIds so the SQL string is unique per run. /v1/ops/query
    // itself never caches (Cache-Control: no-store on every response) — the
    // uniqueness here avoids confusing test output when this and admin.test.ts
    // each independently SELECT count() from the same table and the suites
    // race, not a cache concern.
    const admin = adminClient();
    const result = await admin.sql(
      `SELECT count() as cnt FROM default.${T.clicks} WHERE event_id IN ('${seededIds.join("','")}')`,
    );
    expect(result.error).toBeNull();
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data!.length).toBe(1);
    expect(Number((result.data![0] as any).cnt)).toBe(seededIds.length);
  });

  it("query non-existent table returns error", async () => {
    const result = await wh.from("no_such_table").fetch();
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(404);
  });

  it("queries a table with special characters in its name", async () => {
    const admin = adminClient();
    const weirdName = "read-test; 2026";

    // 1. Create table and refresh schema
    await chQuery(
      `CREATE TABLE IF NOT EXISTS \`${weirdName}\` (id String, received_timestamp DateTime) ENGINE = Memory`,
    );
    await admin.schema.refresh();

    // 2. Add it to the policy
    const currentPolicy = readPolicyFile();
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [weirdName]: {
          viewer: { select: { allow_columns: ["*"] } },
        },
      },
    });

    // 3. Query it using the SDK
    const result = await wh.from(weirdName).fetch();
    expect(result.error).toBeNull();

    await chQuery(`DROP TABLE IF EXISTS \`${weirdName}\``);
  });

  it("fetches from a bring-your-own-schema table lacking received_timestamp (#270)", async () => {
    const admin = adminClient();
    // A table with NO received_timestamp column. The SDK used to hardcode
    // ORDER BY received_timestamp DESC, so a bare .fetch() here 500'd; #270
    // dropped that default, so the query is now valid (regression guard).
    const noTsTable = `no_ts_${testId().replace(/-/g, "_")}`;

    // 1. Create the table and refresh schema so WaveHouse discovers it.
    await chQuery(
      `CREATE TABLE IF NOT EXISTS default.\`${noTsTable}\` (id String, label String) ENGINE = Memory`,
    );
    await admin.schema.refresh();

    // 2. Grant the viewer role read access via policy.
    const currentPolicy = readPolicyFile();
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [noTsTable]: {
          viewer: { select: { allow_columns: ["*"] } },
        },
      },
    });

    // 3. A bare .fetch() must succeed (no 500) — the regression guard for #270.
    try {
      const result = await wh.from(noTsTable).fetch();
      expect(result.error).toBeNull();
      expect(result.data).toBeInstanceOf(Array);
    } finally {
      await chQuery(`DROP TABLE IF EXISTS default.\`${noTsTable}\``);
    }
  });

  it("rejects queries to unauthorized tables (403)", async () => {
    const admin = adminClient();

    // Create a table but DO NOT add it to the policy
    await chQuery(`CREATE TABLE IF NOT EXISTS top_secret_data (id String) ENGINE = Memory`);
    await admin.schema.refresh();

    // The dataClient (viewer role) should be blocked completely
    const result = await wh.from("top_secret_data").fetch();
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(403);

    await chQuery(`DROP TABLE IF EXISTS top_secret_data`);
  });

  it("rejects queries requesting unauthorized columns (403)", async () => {
    const currentPolicy = readPolicyFile();

    // Temporarily restrict this suite's clicks table so 'user_id' can't be selected
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [T.clicks]: {
          ...(currentPolicy.tables[T.clicks] || {}),
          viewer: {
            ...(currentPolicy.tables[T.clicks]?.viewer || {}),
            select: { allow_columns: ["page", "duration_ms"] },
          },
        },
      },
    });

    // Try to explicitly select the forbidden column
    const result = await wh.from(T.clicks).select("user_id").fetch();
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(403);

    // Restore the baseline policy using the non-null assertion (!)
    await setPolicy(currentPolicy);
  });

  // ── #223: the column allowlist is a hard cap on EVERY read shape ────────────
  //
  // The original bug only checked the columns a caller *explicitly* listed, so
  // omitting `columns` (SELECT *), sending `["*"]`, or referencing a denied
  // column via group_by / filter / order_by all bypassed the allowlist. These
  // tests drive the real pipeline (SDK → WaveHouse → ClickHouse) under a
  // restricted `viewer` policy and assert denied columns never come back, and
  // that the inference-leak clauses are rejected outright.

  // Snapshot the policy, restrict viewer's clicks SELECT to `perms`, run body,
  // then always restore — a failed assertion must not leak a restricted policy
  // into later tests. The suite runs sequentially (tables.ts), so this is
  // race-free.
  async function withViewerSelect(
    perms: Record<string, unknown>,
    body: () => Promise<void>,
  ): Promise<void> {
    const snapshot = readPolicyFile();
    const tables = snapshot.tables;
    await setPolicy({
      tables: {
        ...tables,
        [T.clicks]: {
          ...(tables[T.clicks] || {}),
          viewer: { ...(tables[T.clicks]?.viewer || {}), select: perms },
        },
      },
    });
    try {
      await body();
    } finally {
      await setPolicy(snapshot);
    }
  }

  it("omitting columns returns only the allowed columns, never SELECT * (#223)", async () => {
    // The point under test is the *projection*: a bare .fetch() with no .select()
    // must not silently widen to SELECT *. received_timestamp is just a third
    // allowed column here — since #270 a bare .fetch() emits no implicit order, so
    // it carries no special weight; the returned keys must equal the allow-list.
    await withViewerSelect(
      { allow_columns: ["page", "duration_ms", "received_timestamp"] },
      async () => {
        const result = await wh.from(T.clicks).fetch();
        expect(result.error).toBeNull();
        expect(result.data!.length).toBeGreaterThan(0);
        for (const row of result.data!) {
          // Exactly the allowed columns — the sensitive ones (user_id, country,
          // session_id, event_id) never leak through the omitted-columns path.
          expect(Object.keys(row as object).sort()).toEqual([
            "duration_ms",
            "page",
            "received_timestamp",
          ]);
        }
      },
    );
  });

  it("selectAll() expands to allowed columns under a deny-list, not raw SELECT *", async () => {
    await withViewerSelect({ deny_columns: ["user_id", "session_id"] }, async () => {
      const result = await wh.from(T.clicks).selectAll().fetch();
      expect(result.error).toBeNull();
      expect(result.data!.length).toBeGreaterThan(0);
      for (const row of result.data!) {
        expect(row).not.toHaveProperty("user_id");
        expect(row).not.toHaveProperty("session_id");
        expect(row).toHaveProperty("page"); // non-denied columns still come back
      }
    });
  });

  it("group_by on a denied column is rejected (403), not leaked", async () => {
    await withViewerSelect({ allow_columns: ["page", "duration_ms"] }, async () => {
      // GROUP BY user_id would otherwise enumerate every distinct user_id.
      const result = await wh
        .from(T.clicks)
        .select("page")
        .count("page", "n")
        .groupBy("user_id")
        .fetch();
      expect(result.error).not.toBeNull();
      expect(result.error!.status).toBe(403);
    });
  });

  it("filtering on a denied column is rejected (403), not an inference oracle", async () => {
    await withViewerSelect({ allow_columns: ["page", "duration_ms"] }, async () => {
      // WHERE user_id = ? would let a caller probe values they can't read.
      const result = await wh
        .from(T.clicks)
        .select("page")
        .where("user_id", "=", "u-query-1")
        .fetch();
      expect(result.error).not.toBeNull();
      expect(result.error!.status).toBe(403);
    });
  });

  it("order_by on a denied column is rejected (403)", async () => {
    await withViewerSelect({ allow_columns: ["page", "duration_ms"] }, async () => {
      const result = await wh.from(T.clicks).select("page").orderBy("user_id", "desc").fetch();
      expect(result.error).not.toBeNull();
      expect(result.error!.status).toBe(403);
    });
  });

  it("a role with no readable columns gets 403 from a fetch, never a fail-open SELECT *", async () => {
    // allow_columns names only a non-existent column, so the role can read nothing
    // on this table. The read fails closed (403) rather than degrading to SELECT *.
    await withViewerSelect({ allow_columns: ["does_not_exist"] }, async () => {
      const result = await wh.from(T.clicks).fetch();
      expect(result.error).not.toBeNull();
      expect(result.error!.status).toBe(403);
    });
  });

  it("a column-restricted bare .fetch() succeeds, returning only allowed columns (#270 × #223)", async () => {
    // Cross-PR regression guard. Before #270 a bare .fetch() emitted an implicit
    // ORDER BY received_timestamp DESC; once #223 authorizes order_by, that implicit
    // order would make a bare .fetch() by a role that can't read received_timestamp
    // a 403. #270 removed the implicit order, so a bare .fetch() carries no denied-
    // column reference and succeeds — returning exactly the role's allowed columns.
    // received_timestamp is intentionally NOT in the allow-list here: pre-#270 this
    // very call 403'd; it must now pass. Ordering by a denied column is still
    // rejected when the caller asks for it explicitly (see the order_by test above).
    await withViewerSelect({ allow_columns: ["page", "duration_ms"] }, async () => {
      const result = await wh.from(T.clicks).fetch(); // no implicit order since #270
      expect(result.error).toBeNull();
      expect(result.data!.length).toBeGreaterThan(0);
      for (const row of result.data!) {
        expect(Object.keys(row as object).sort()).toEqual(["duration_ms", "page"]);
      }

      // An explicit .orderBy() over a readable column still paginates fine.
      const ok = await wh.from(T.clicks).select().orderBy("page", "asc").fetch();
      expect(ok.error).toBeNull();
      for (const row of ok.data!) {
        expect(Object.keys(row as object).sort()).toEqual(["duration_ms", "page"]);
      }
    });
  });

  it("enforces max_execution_time policy limit", async () => {
    const currentPolicy = readPolicyFile();

    // Temporarily restrict viewer queries to an impossibly fast 1ms timeout
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [T.clicks]: {
          ...(currentPolicy.tables[T.clicks] || {}),
          viewer: {
            ...(currentPolicy.tables[T.clicks]?.viewer || {}),
            select: {
              allow_columns: ["*"],
              max_execution_time: "1ms", // human-readable duration
            },
          },
        },
      },
    });

    try {
      // The 1ms budget races the ClickHouse roundtrip: this zero-row query on
      // a tiny table CAN finish sub-millisecond before the deadline is ever
      // observed, so a single attempt is a coin flip (flaked on 2-core CI,
      // #283). The enforced property is existential — a 1ms budget must
      // produce deadline 500s — so retry until one fires; if enforcement is
      // broken, every attempt succeeds and the wait times out the test.
      // IMPORTANT: unique event_id per attempt so no attempt is cache-served!
      let deadlineError: { status: number } | null = null;
      await waitForCondition(
        async () => {
          const result = await wh
            .from(T.clicks)
            .selectAll()
            .where("event_id", "=", testId())
            .limit(999)
            .fetch();
          deadlineError = result.error;
          return result.error !== null;
        },
        10_000,
        100,
      );
      expect(deadlineError).not.toBeNull();
      expect(deadlineError!.status).toBe(500);
    } finally {
      // Restore policy even if test fails so that others don't too
      await setPolicy(currentPolicy);
    }
  });

  // ── #316: row/memory caps are enforced by ClickHouse server-side ────────────
  //
  // Before #316 the policy's resource caps reached ClickHouse only as a client
  // context deadline — never as max_rows_to_read / max_memory_usage — so a
  // capped role's query returned the full result set instead of being rejected.
  // These drive the real public path (SDK → WaveHouse → ClickHouse) under a
  // viewer policy whose cap is impossibly small, and assert the server rejects
  // the read (500 carrying the ClickHouse limit error). Unlike the
  // execution-time race above, both are deterministic: a full scan always blows
  // past a 1-row / 1-byte budget on the first attempt. The unique event_id
  // filter keeps each query's SQL out of the shared result cache, so a cached
  // success can't mask a broken cap.

  it("enforces max_rows_to_read policy limit server-side (#316)", async () => {
    await withViewerSelect({ allow_columns: ["*"], max_rows_to_read: 1 }, async () => {
      const result = await wh.from(T.clicks).selectAll().where("event_id", "=", testId()).fetch();
      expect(result.error).not.toBeNull();
      expect(result.error!.status).toBe(500);
    });
  });

  it("enforces max_memory_usage policy limit server-side (#316)", async () => {
    // A bare number is bytes, so 1 = a 1-byte cap (also exercises the
    // number-input form alongside the "1ms" string above).
    await withViewerSelect({ allow_columns: ["*"], max_memory_usage: 1 }, async () => {
      const result = await wh.from(T.clicks).selectAll().where("event_id", "=", testId()).fetch();
      expect(result.error).not.toBeNull();
      expect(result.error!.status).toBe(500);
    });
  });
});

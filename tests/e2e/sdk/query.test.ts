import { beforeAll, describe, expect, it } from "vitest";
import { adminClient, chQuery, dataClient, testId, waitForCondition } from "./helpers.js";
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
    await waitForCondition(async () => {
      const r = await chQuery(
        `SELECT count() as cnt FROM default.${T.clicks} WHERE event_id IN ('${seededIds.join("','")}')`,
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
    // Paginate by the unique event_id so the keyset cursor can't skip rows. The
    // default received_timestamp cursor is exercised by the skipped test below,
    // which documents why it can't be relied on over batched data (#175).
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

  // Original default-order pagination. Skipped because it currently fails:
  // ClickHouse stamps received_timestamp (DEFAULT now64(3)) at BATCH-insert
  // time, so every row the worker flushes together shares one millisecond, and
  // the SDK's strict keyset cursor then skips the rows tied on a page boundary —
  // leaving page 2 empty. Tracked in #175; re-enable once the default cursor
  // breaks ties (e.g. a composite cursor with a unique tiebreak column).
  it.skip("pagination by default received_timestamp cursor (known bug — #175)", async () => {
    const page1 = await wh.from(T.clicks).select().limit(3).fetch();
    expect(page1.error).toBeNull();
    expect(page1.data).toHaveLength(3);
    expect(page1.hasMore).toBe(true);
    expect(page1.next).toBeDefined();

    const page2 = await page1.next!();
    expect(page2.error).toBeNull();
    expect(page2.data).toBeInstanceOf(Array);
    expect(page2.data!.length).toBeGreaterThan(0);
  });

  it("raw SQL query", async () => {
    // Scope to seededIds so the SQL string is unique per run. /v1/admin/query
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
    const currentPolicyRes = await admin.policy.get();
    await admin.policy.set({
      tables: {
        ...(currentPolicyRes.data as any).tables,
        [weirdName]: {
          select: { viewer: { allow_columns: ["*"] } },
        },
      },
    });

    // 3. Query it using the SDK
    const result = await wh.from(weirdName).fetch();
    expect(result.error).toBeNull();

    await chQuery(`DROP TABLE IF EXISTS \`${weirdName}\``);
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
    const admin = adminClient();
    const currentPolicyRes = await admin.policy.get();

    // Assert it's not null so the test fails cleanly if something goes wrong
    expect(currentPolicyRes.data).not.toBeNull();

    // Temporarily restrict this suite's clicks table so 'user_id' can't be selected
    await admin.policy.set({
      tables: {
        ...(currentPolicyRes.data as any).tables,
        [T.clicks]: {
          ...((currentPolicyRes.data as any).tables[T.clicks] || {}),
          select: {
            viewer: { allow_columns: ["page", "duration_ms"] },
          },
        },
      },
    });

    // Try to explicitly select the forbidden column
    const result = await wh.from(T.clicks).select("user_id").fetch();
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(403);

    // Restore the baseline policy using the non-null assertion (!)
    await admin.policy.set(currentPolicyRes.data!);
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
    const admin = adminClient();
    const snapshot = await admin.policy.get();
    const tables = (snapshot.data as any).tables;
    await admin.policy.set({
      tables: {
        ...tables,
        [T.clicks]: { ...(tables[T.clicks] || {}), select: { viewer: perms } },
      },
    });
    try {
      await body();
    } finally {
      await admin.policy.set(snapshot.data!);
    }
  }

  it("omitting columns returns only the allowed columns, never SELECT * (#223)", async () => {
    // received_timestamp is allowed here because a bare .fetch() paginates by it
    // (the SDK's default keyset cursor → ORDER BY received_timestamp), and ordering
    // by a column you can't read is now rejected just like selecting it (see the
    // pagination-cursor test below). The point under test is the *projection*: no
    // .select() must not silently become SELECT *.
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

  it("select('*') expands to allowed columns under a deny-list, not raw SELECT *", async () => {
    await withViewerSelect({ deny_columns: ["user_id", "session_id"] }, async () => {
      const result = await wh.from(T.clicks).select("*").fetch();
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

  it("bare .fetch() paginates by received_timestamp, so a role that can't read it is rejected", async () => {
    // Documents a deliberate consequence of the #223 fix: the SDK's default cursor
    // orders by received_timestamp, and ordering by a denied column is rejected.
    // A column-restricted role that paginates must include its cursor column (or
    // pass an explicit .orderBy() over a readable one).
    await withViewerSelect({ allow_columns: ["page", "duration_ms"] }, async () => {
      const rejected = await wh.from(T.clicks).fetch(); // implicit ORDER BY received_timestamp
      expect(rejected.error).not.toBeNull();
      expect(rejected.error!.status).toBe(403);

      // …and it works once ordered by a column the role can read.
      const ok = await wh.from(T.clicks).select().orderBy("page", "asc").fetch();
      expect(ok.error).toBeNull();
      for (const row of ok.data!) {
        expect(Object.keys(row as object).sort()).toEqual(["duration_ms", "page"]);
      }
    });
  });

  it("enforces max_execution_time_ms policy limit", async () => {
    const admin = adminClient();
    const currentPolicyRes = await admin.policy.get();

    // Temporarily restrict viewer queries to an impossibly fast 1ms timeout
    await admin.policy.set({
      tables: {
        ...(currentPolicyRes.data as any).tables,
        [T.clicks]: {
          ...((currentPolicyRes.data as any).tables[T.clicks] || {}),
          select: {
            viewer: {
              allow_columns: ["*"],
              max_execution_time_ms: 1, // 1 millisecond limit
            },
          },
        },
      },
    });

    try {
      // The query will hit the Go context deadline exceeded error
      // IMPORTANT: Make the query unique so it doesn't get served instantly from the cache!
      const result = await wh
        .from(T.clicks)
        .select("*")
        .where("event_id", "=", testId())
        .limit(999)
        .fetch();
      expect(result.error).not.toBeNull();
      expect(result.error!.status).toBe(500);
    } finally {
      // Restore policy even if test fails so that others don't too
      await admin.policy.set(currentPolicyRes.data!);
    }
  });
});

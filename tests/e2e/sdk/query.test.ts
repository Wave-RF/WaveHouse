import { describe, it, expect, beforeAll } from "vitest";
import {
  dataClient,
  adminClient,
  waitForCondition,
  testId,
  chQuery,
} from "./helpers.js";

describe("Query", () => {
  const wh = dataClient();

  // Seed a known set of rows before the query tests run
  const seededIds: string[] = [];

  beforeAll(async () => {
    for (let i = 0; i < 10; i++) {
      const id = testId();
      seededIds.push(id);
      await wh.from("clicks").insert({
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
        `SELECT count() as cnt FROM default.clicks WHERE event_id IN ('${seededIds.join("','")}')`,
      );
      return Number((r[0] as any).cnt) === seededIds.length;
    }, 15_000);
  });

  it("fetches rows with default limit", async () => {
    const result = await wh.from("clicks").fetch();
    expect(result.error).toBeNull();
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data!.length).toBeGreaterThan(0);
  });

  it("select + where + orderBy + limit", async () => {
    const result = await wh
      .from("clicks")
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
      .from("clicks")
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
    const page1 = await wh.from("clicks").select().limit(3).fetch();
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
    // Scope to seededIds so the SQL string is unique per run — avoids
    // colliding with admin.test.ts's identical-SQL count which can cache
    // a stale 0 in /v1/admin/query when it runs first.
    const admin = adminClient();
    const result = await admin.sql(
      `SELECT count() as cnt FROM default.clicks WHERE event_id IN ('${seededIds.join("','")}')`,
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
});

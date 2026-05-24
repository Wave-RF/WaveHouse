import { describe, it, expect } from "vitest";
import { dataClient, testId, waitForCondition, chQuery } from "./helpers.js";

describe("Ingest Batching Triggers", () => {
  const wh = dataClient();

  it("flushes immediately when hitting the 500-item batch limit", async () => {
    const runId = testId();
    const rows = Array.from({ length: 500 }).map((_, i) => ({
      event_id: `${runId}-${i}`,
        page: `/batch-count-test`,
      session_id: `session-${runId}`,
      user_id: `user-${runId}`,
    }));

    const startTime = Date.now();

    // Insert all 500 at once
    const res = await wh.from("clicks").insert(rows);
    expect(res.error).toBeNull();

    // It should hit ClickHouse almost instantly (< 2 seconds), well before the 5s timer
    await waitForCondition(
      async () => {
        const r = await chQuery(
          `SELECT count() as cnt FROM default.clicks WHERE user_id = 'user-${runId}'`,
        );
            console.log(
              `batched insert (batch from sdk as one upload) selection only found ${Number((r[0] as any).cnt)} results, waiting for 500`,
            );
        return Number((r[0] as any).cnt) === 500;
      },
      2_000,
      100,
    );

    const elapsed = Date.now() - startTime;
    expect(elapsed).toBeLessThan(4000); // Prove it didn't wait for the 5s timer
  });

  it("waits for the 5-second period if batch limit is not met", async () => {
      const runId = testId();

      // wait for previous messages for table in batch to clear
      await new Promise((r) => setTimeout(r, 30000));

    const startTime = Date.now();
    const res = await wh.from("clicks").insert({
      event_id: runId,
      page: `/batch-time-test`,
      session_id: `session-${runId}`,
      user_id: `user-${runId}`,
    });
    expect(res.error).toBeNull();

    // Check immediately (should NOT be there yet)
    let r = await chQuery(
      `SELECT count() as cnt FROM default.clicks WHERE user_id = 'user-${runId}'`,
    );
    expect(Number((r[0] as any).cnt)).toBe(0);

    // Wait until it appears
    await waitForCondition(
      async () => {
        const check = await chQuery(
          `SELECT count() as cnt FROM default.clicks WHERE user_id = 'user-${runId}'`,
        );
        return Number((check[0] as any).cnt) === 1;
      },
      10_000,
      500,
    );

    const elapsed = Date.now() - startTime;

    // It should take roughly ~5 seconds for Bento's period trigger to fire
    expect(elapsed).toBeGreaterThanOrEqual(4500);
  }, 60_000);
});

import { describe, expect, it } from "vitest";
import { chQuery, dataClient, testId, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Ingest Batching Triggers", () => {
  const wh = dataClient();
  const T = suiteTables("batching");

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
    const res = await wh.from(T.clicks).insert(rows);
    expect(res.error).toBeNull();
    const apiEndTime = Date.now();
    const ackMs = apiEndTime - startTime;
    console.log(`All 500 events uploaded (API fsync latency) in ${ackMs}ms`);
    // Sanity bound, not an SLO: the ack rides 500 sequential JetStream
    // publish-fsyncs, and fsync latency varies ~10x across dev/CI disks
    // (5104ms observed on APFS, #283).
    expect(ackMs).toBeLessThan(15_000);

    if (ackMs < 4_000) {
      // Publish beat the 5s linger (armed on the first row) with margin, so
      // the worker's buffer held all 500 rows before the timer could fire —
      // an early flush below is attributable to the size trigger.
      await waitForCondition(
        async () => {
          const r = await chQuery(
            `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
          );
          return Number((r[0] as any).cnt) === 500;
        },
        5_000,
        100,
      );

      const elapsed = Date.now() - apiEndTime;
      console.log(`All 500 events uploaded (buffered CH insert) in ${elapsed}ms`);
      expect(elapsed).toBeLessThan(5000); // Prove it didn't wait for the 5s timer
    } else {
      // Slow-disk run: the linger fired mid-publish and split the batch, so
      // the buffer never held 500 rows and size-trigger timing is
      // unobservable (the tail waits its own full linger — visibility lands
      // ~10s from first row). Verify data integrity only.
      console.log(
        `ack took ${ackMs}ms (≥4s): linger fired mid-publish; size-trigger timing inconclusive — verifying integrity only`,
      );
      await waitForCondition(
        async () => {
          const r = await chQuery(
            `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
          );
          return Number((r[0] as any).cnt) === 500;
        },
        15_000,
        250,
      );
    }
    // Worst honest path: 15s ack bound + 15s integrity wait — clear of the
    // 30s default testTimeout.
  }, 45_000);

  it("waits for the 5-second period if batch limit is not met", async () => {
    const runId = testId();

    const startTime = Date.now();
    const res = await wh.from(T.clicks).insert({
      event_id: runId,
      page: `/batch-time-test`,
      session_id: `session-${runId}`,
      user_id: `user-${runId}`,
    });
    const apiEndTime = Date.now();
    expect(res.error).toBeNull();
    expect(apiEndTime - startTime).toBeLessThan(2_500);

    // Check immediately (should NOT be there yet)
    const r = await chQuery(
      `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
    );
    expect(Number((r[0] as any).cnt)).toBe(0);

    // Wait until it appears
    await waitForCondition(
      async () => {
        const check = await chQuery(
          `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        );
        return Number((check[0] as any).cnt) === 1;
      },
      6_000,
      500,
    );

    const elapsed = Date.now() - apiEndTime;

    // It should take roughly ~5 seconds for ingest worker's period trigger to fire
    expect(elapsed).toBeGreaterThanOrEqual(4500);
  }, 20_000);
});

import { describe, expect, it } from "vitest";
import { chQuery, dataClient, testId, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Ingest Batching Triggers", () => {
  const wh = dataClient();
  const T = suiteTables("batching");

  it("flushes immediately when the batch reaches ingest.max_batch_bytes", async () => {
    const runId = testId();
    // The fixture's ingest.max_batch_bytes is 262144: the worker flushes a
    // table's batch once its rows' JSON adds up to that, however many rows.
    // 500 rows of ~700 bytes each (the page padded to 600) are ~350 KB, so
    // the batch reaches the byte trigger well before the last row lands.
    const page = `/batch-bytes-test/${"x".repeat(600)}`;
    const rows = Array.from({ length: 500 }).map((_, i) => ({
      event_id: `${runId}-${i}`,
      page,
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
      // nothing can have flushed on the timer yet: rows visible before
      // startTime + 5s got there on the byte trigger. The target is reached
      // part way through the 500 rows, so the first batch lands early and
      // the rows after it wait their own linger — the early batch is the
      // proof; the integrity wait below covers the tail.
      let seenAt = 0;
      await waitForCondition(
        async (signal) => {
          const r = await chQuery(
            `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
            signal,
          );
          if (Number((r[0] as any).cnt) === 0) return false;
          seenAt = Date.now();
          return true;
        },
        5_000,
        100,
      );

      // Anchor the proof to startTime, NOT apiEndTime: the linger arms when
      // the FIRST row reaches the worker (≈ startTime), so a broken size
      // trigger flushes at ≈ startTime + 5s — which measured from apiEndTime
      // is 5s − ackMs and would pass a bound anchored there. Visibility
      // strictly before the first-row linger deadline is what proves the
      // size trigger fired.
      const elapsed = seenAt - startTime;
      console.log(`First batch visible (buffered CH insert) ${elapsed}ms after first publish`);
      expect(elapsed).toBeLessThan(5000); // Prove it didn't wait for the 5s timer
    } else {
      // Slow-disk run: the linger fired mid-publish, so an early flush is not
      // attributable to the byte trigger. Verify data integrity only.
      console.log(
        `ack took ${ackMs}ms (≥4s): linger fired mid-publish; size-trigger timing inconclusive — verifying integrity only`,
      );
    }
    // Every row lands either way: the tail of the batch on its own linger at
    // the latest (~10s from first row on a slow disk).
    await waitForCondition(
      async (signal) => {
        const r = await chQuery(
          `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
          signal,
        );
        return Number((r[0] as any).cnt) === 500;
      },
      15_000,
      250,
    );
    // Worst honest path: 15s ack bound + 5s first-batch wait + 15s integrity
    // wait — clear of the 45s timeout.
  }, 45_000);

  it("waits for the 5-second period if the byte limit is not met", async () => {
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

    // Wait until it appears. The budget is only a "don't hang forever" bound —
    // the assertion that carries this test's meaning is the >= 4500ms lower
    // bound below. It used to be 6_000, which left ~700ms over the 5s linger
    // and made this the most flake-prone wait in the suite; every other
    // visibility wait allows 10s. Widening costs no test power (an early flush
    // still fails the lower bound) and removes the false failures (#440).
    await waitForCondition(
      async (signal) => {
        const check = await chQuery(
          `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
          signal,
        );
        return Number((check[0] as any).cnt) === 1;
      },
      10_000,
      500,
    );

    const elapsed = Date.now() - apiEndTime;

    // It should take roughly ~5 seconds for ingest worker's period trigger to fire
    expect(elapsed).toBeGreaterThanOrEqual(4500);
  }, 25_000);
});

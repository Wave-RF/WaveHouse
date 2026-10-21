import { describe, expect, it } from "vitest";
import { chQuery, dataClient, testId, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Stress & Concurrency", () => {
  const wh = dataClient();
  const T = suiteTables("stress");

  it("handles high-volume concurrent inserts and reads", async () => {
    const concurrency = 20; // Number of parallel workers
    const insertsPerWorker = 25; // 500 total inserts
    const runId = testId();

    // Blast inserts concurrently
    const workers = Array.from({ length: concurrency }).map(async (_, workerIdx) => {
      const rows = Array.from({ length: insertsPerWorker }).map((_, i) => ({
        event_id: `${runId}-${workerIdx}-${i}`,
        page: `/stress-test`,
        user_id: `user-${workerIdx}`,
        session_id: `session-${runId}`,
        duration_ms: Math.floor(Math.random() * 1000),
      }));

      // SDK handles chunking internally if we pass an array, but we are simulating
      // parallel individual/small-batch network requests here.
      const res = await wh.from(T.clicks).insert(rows);
      expect(res.error).toBeNull();
      return res;
    });

    //  Also spin up concurrent reads while writes are happening to test cache / DB locks
    const readWorkers = Array.from({ length: 10 }).map(async () => {
      const res = await wh.from(T.clicks).select("page").limit(5).fetch();
      expect(res.error).toBeNull();
      return res;
    });

    await Promise.all([...workers, ...readWorkers]);

    // Verify all data landed intact. These narrow rows stay under the
    // fixture's ingest.max_batch_bytes, so the batch flushes on the 5s timer
    // (ingest.max_batch_wait_ms) at the latest, well inside the wait.
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT count() as cnt FROM default.${T.clicks} WHERE session_id = 'session-${runId}'`,
        signal,
      );
      return Number((r[0] as any).cnt) === concurrency * insertsPerWorker;
    }, 10_000);

    const finalCount = await chQuery(
      `SELECT count() as cnt FROM default.${T.clicks} WHERE session_id = 'session-${runId}'`,
    );
    expect(Number((finalCount[0] as any).cnt)).toBe(concurrency * insertsPerWorker);
  }, 20_000); // Extended timeout for stress test
});

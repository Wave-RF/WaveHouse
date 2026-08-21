import { describe, expect, it } from "vitest";
import { adminClient, chQuery, dataClient, testId, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Dead Letter Queue (DLQ) & Failures", () => {
  const wh = dataClient();
  const admin = adminClient();
  const T = suiteTables("dlq");

  it("routes only the failed row to DLQ while valid rows are inserted", async () => {
    const runId = testId();

    // Get baseline DLQ stats before we pollute them. DLQ stats are keyed by
    // table name, and this suite owns T.clicks exclusively, so the count is
    // isolated from every other test file.
    const initialDlq = await admin.dlq.list();
    const initialClicksDlq = (initialDlq.data?.tables as any)?.[T.clicks] || 0;

    // We are going to send 9 perfectly valid rows, and 1 critically malformed row.
    const rows = Array.from({ length: 10 }).map((_, i) => {
      if (i === 9) {
        return {
          event_id: `${runId}-bad`,
          page: "/bag-page",
          session_id: `session-${runId}`,
          user_id: `user-${runId}`,
          // Go accepts strings for numerics, but ClickHouse cannot parse this into an Int/Float.
          // This successfully bypasses API validation but fails database insertion.
          duration_ms: "definitely-not-a-number",
        };
      }
      return {
        event_id: `${runId}-${i}`,
        page: `/good-page`,
        session_id: `session-${runId}`,
        user_id: `user-${runId}`,
      };
    });

    const res = await wh.from(T.clicks).insert(rows as any);
    expect(res.error).toBeNull(); // API accepts it (schema validation is loose by design)

    // 9 good rows must land. Budget is 10s (the suite norm), not the 6s a plain
    // timer-flush uses: the bad row forces the worker into 1-by-1 isolation —
    // ~11 sequential CH round-trips after the 5s maxWait timer — which is far
    // more post-timer work than a single-insert flush, so 6s sat right at the
    // edge under CI load. The structural fix is a lower e2e maxWait (deferred
    // config PR), which drops the 5s-timer dependency; this budget can shrink
    // back once that lands.
    await waitForCondition(async (signal) => {
      const chRows = await chQuery(
        `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        signal,
      );
      return Number((chRows[0] as any).cnt) === 9;
    }, 10_000);

    // Verify exactly 1 message was added to the DLQ for this suite's clicks table
    await waitForCondition(async () => {
      const dlqRes = await admin.dlq.list();
      const currentClicksDlq = (dlqRes.data?.tables as any)?.[T.clicks] || 0;
      return currentClicksDlq === initialClicksDlq + 1;
    }, 5_000);

    const finalDlq = await admin.dlq.list();
    const finalClicksDlq = (finalDlq.data?.tables as any)?.[T.clicks] || 0;

    // Only 1 was rejected and routed to the DLQ
    expect(finalClicksDlq).toBe(initialClicksDlq + 1);
  }, 20_000);
});

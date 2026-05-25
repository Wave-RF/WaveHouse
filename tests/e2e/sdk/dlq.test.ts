import { describe, it, expect } from "vitest";
import {
  dataClient,
  adminClient,
  testId,
  waitForCondition,
  chQuery,
} from "./helpers.js";

describe("Dead Letter Queue (DLQ) & Failures", () => {
  const wh = dataClient();
  const admin = adminClient();

  it("routes an entire batch to DLQ if one row fails strict ClickHouse validation", async () => {
      const runId = testId();

    // Get baseline DLQ stats before we pollute them
    const initialDlq = await admin.dlq.list();
    const initialClicksDlq =
      (initialDlq.data?.tables as any)?.["dlq.clicks"] || 0;

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

    const res = await wh.from("clicks").insert(rows as any);
      expect(res.error).toBeNull(); // API accepts it (schema validation is loose by design)

      await waitForCondition(async () => {
          // Verify 9 of the rows made it to ClickHouse
          const chRows = await chQuery(
              `SELECT count() as cnt FROM default.clicks WHERE user_id = 'user-${runId}'`,
          );
          return Number((chRows[0] as any).cnt) == 9;
      }, 6_000);

    // Verify exactly 1 message was added to the DLQ for the clicks table
    await waitForCondition(async () => {
      const dlqRes = await admin.dlq.list();
      const currentClicksDlq =
            (dlqRes.data?.tables as any)?.["dlq.clicks"] || 0;
        return currentClicksDlq === initialClicksDlq + 1;
    }, 5_000);

    const finalDlq = await admin.dlq.list();
    const finalClicksDlq = (finalDlq.data?.tables as any)?.["dlq.clicks"] || 0;

    // The entire batch of 10 was rejected and routed to the DLQ
    expect(finalClicksDlq).toBe(initialClicksDlq + 1);
  }, 20_000);
});

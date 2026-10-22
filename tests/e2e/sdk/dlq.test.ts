import { describe, expect, it } from "vitest";
import { adminClient, chQuery, dataClient, testId, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Dead Letter Queue (DLQ) & Failures", () => {
  const wh = dataClient();
  const admin = adminClient();
  const T = suiteTables("dlq");

  // This case used to assert the opposite: a row that "bypasses API validation
  // but fails database insertion" landing in the DLQ. That gap is what the type
  // layer closed — ingest now asks ClickHouse's own parser before publishing,
  // so an unparseable value is refused at the gateway with the server's real
  // code and never enters the queue. The DLQ still exists for failures that
  // only surface at INSERT time (covered by tests/integration/dlq_test.go and
  // internal/ingest/worker_test.go); what this pins is that bad data no longer
  // gets that far.
  it("refuses the unparseable row at ingest, so the DLQ never sees it", async () => {
    const runId = testId();

    // Baseline before we touch it. DLQ stats are keyed by table name and this
    // suite owns T.clicks exclusively, so the count is isolated from every
    // other test file.
    const initialDlq = await admin.dlq.list();
    const initialClicksDlq = (initialDlq.data?.tables as any)?.[T.clicks] || 0;

    // Nine valid rows and one whose duration_ms no ClickHouse parser can read.
    const rows = Array.from({ length: 10 }).map((_, i) => {
      if (i === 9) {
        return {
          event_id: `${runId}-bad`,
          page: "/bad-page",
          session_id: `session-${runId}`,
          user_id: `user-${runId}`,
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
    // ClickHouse's parser refuses the batch whole, as its INSERT would: a 400
    // with its own code, naming the record, and nothing published.
    expect(res.error).not.toBeNull();
    expect(res.error!.status).toBe(400);
    expect(res.error!.code).toBe("clickhouse.rejected");
    expect((res.error!.details as { exception_code?: number }).exception_code).toBeGreaterThan(0);
    expect(res.error!.message).toMatch(/^record 10: /);

    // The nine good rows, resent alone, land: the refused batch claimed none of
    // their ids.
    const good = await wh.from(T.clicks).insert(rows.slice(0, 9) as any);
    expect(good.error).toBeNull();
    expect(good.data?.succeeded).toBe(9);
    await waitForCondition(async (signal) => {
      const chRows = await chQuery(
        `SELECT count() as cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        signal,
      );
      return Number((chRows[0] as any).cnt) === 9;
    }, 10_000);

    // The refused row was never published, so nothing can have reached the DLQ.
    // The wait above already proves the worker drained this batch.
    const finalDlq = await admin.dlq.list();
    const finalClicksDlq = (finalDlq.data?.tables as any)?.[T.clicks] || 0;
    expect(finalClicksDlq).toBe(initialClicksDlq);
  }, 20_000);
});

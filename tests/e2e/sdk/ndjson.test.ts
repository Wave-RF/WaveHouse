import { describe, expect, it } from "vitest";
import { chQuery, dataClient, makeJWT, testId, WH_URL, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

/**
 * End-to-end coverage for NDJSON batch ingest (issue #195): the API
 * `application/x-ndjson` path, the SDK array `insert([...])` path that now rides
 * it, and the raw `insertNDJSON()` helper — including a body ClickHouse refuses.
 */
describe("NDJSON ingest", () => {
  const wh = dataClient();
  const T = suiteTables("ndjson");

  it("inserts an array as a single NDJSON request and lands every row in ClickHouse", async () => {
    const runId = testId();
    const rows = [1, 2, 3].map((n) => ({
      event_id: `${runId}-${n}`,
      page: "/ndjson-array",
      user_id: `user-${runId}`,
      session_id: `s-${runId}`,
    }));

    const result = await wh.from(T.clicks).insert(rows);
    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true, total: 3, succeeded: 3, failed: 0 });

    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT count() AS cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        signal,
      );
      return Number((r[0] as { cnt: number }).cnt) === 3;
    }, 10_000);
  });

  it("refuses a batch with an unknown column, persisting none of it", async () => {
    const runId = testId();
    const rows = [
      { event_id: `${runId}-a`, page: "/ok", user_id: `user-${runId}`, session_id: `s-${runId}` },
      // Unknown column: ClickHouse refuses it, which refuses the whole batch.
      {
        event_id: `${runId}-bad`,
        page: "/bad",
        user_id: `user-${runId}`,
        session_id: `s-${runId}`,
        totally_fake_field: "nope",
      },
      { event_id: `${runId}-b`, page: "/ok", user_id: `user-${runId}`, session_id: `s-${runId}` },
    ];

    const result = await wh.from(T.clicks).insert(rows);
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(400);
    expect(result.error!.code).toBe("clickhouse.rejected");
    expect((result.error!.details as { exception_code?: number }).exception_code).toBe(117);
    expect(result.error!.message).toMatch(/^record 2: /);

    // Without the bad record, the batch lands whole: the refusal claimed none
    // of its ids.
    const retry = await wh.from(T.clicks).insert([rows[0], rows[2]]);
    expect(retry.error).toBeNull();
    expect(retry.data).toMatchObject({ ok: true, total: 2, succeeded: 2, failed: 0 });
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        signal,
      );
      return r.length === 2;
    }, 10_000);
    const inCH = await chQuery<{ event_id: string }>(
      `SELECT event_id FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
    );
    expect(inCH.map((r) => r.event_id).sort()).toEqual([`${runId}-a`, `${runId}-b`].sort());
  });

  it("insertNDJSON() sends a raw NDJSON string", async () => {
    const runId = testId();
    const ndjson = `${[1, 2]
      .map((n) =>
        JSON.stringify({
          event_id: `${runId}-${n}`,
          page: "/ndjson-string",
          user_id: `user-${runId}`,
          session_id: `s-${runId}`,
        }),
      )
      .join("\n")}\n`;

    const result = await wh.from(T.clicks).insertNDJSON(ndjson);
    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ total: 2, succeeded: 2, failed: 0 });

    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT count() AS cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        signal,
      );
      return Number((r[0] as { cnt: number }).cnt) === 2;
    }, 10_000);
  });

  it("refuses an NDJSON body with a malformed line, ingesting none of it", async () => {
    const runId = testId();
    const good = `${runId}-ok`;
    const ndjson = [
      JSON.stringify({
        event_id: good,
        page: "/ok",
        user_id: `user-${runId}`,
        session_id: `s-${runId}`,
      }),
      "{ this is not valid json",
    ].join("\n");

    // ClickHouse's own parse refusal, with its exception_code and the record it
    // failed on; the good line is not inserted either, as with a ClickHouse
    // INSERT.
    const result = await wh.from(T.clicks).insertNDJSON(ndjson);
    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(400);
    expect(result.error!.code).toBe("clickhouse.rejected");
    expect(typeof (result.error!.details as { exception_code?: number }).exception_code).toBe(
      "number",
    );
    expect(result.error!.message).toMatch(/^record 2: /);

    // Resent without the bad line, the good record lands: the refused body
    // claimed nothing.
    const retry = await wh.from(T.clicks).insertNDJSON(ndjson.split("\n")[0]);
    expect(retry.error).toBeNull();
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE event_id = '${good}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);
  });

  // A SINGLE-LINE JSON array with one bad record is refused whole, as a
  // ClickHouse INSERT of the array is: none of its records land.
  it("refuses a compact JSON array with one bad record, ingesting none of it", async () => {
    const runId = testId();
    const body =
      "[" +
      [
        { event_id: `${runId}-a`, page: "/a", user_id: `user-${runId}`, session_id: `s-${runId}` },
        {
          event_id: `${runId}-b`,
          page: "/b",
          user_id: `user-${runId}`,
          session_id: `s-${runId}`,
          totally_fake_field: "nope",
        },
        { event_id: `${runId}-c`, page: "/c", user_id: `user-${runId}`, session_id: `s-${runId}` },
      ]
        .map((r) => JSON.stringify(r))
        .join(",") +
      "]";
    expect(body.includes("\n")).toBe(false);

    const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${makeJWT({ sub: "test-viewer", role: "viewer", tenant_id: "acme" })}`,
      },
      body,
    });
    expect(res.status).toBe(400);
    const parsed = (await res.json()) as { error?: string; code?: string; exception_code?: number };
    expect(parsed.code).toBe("clickhouse.rejected");
    expect(parsed.exception_code).toBe(117);
    expect(parsed.error).toMatch(/^record 2: /);

    // A record sent after it lands; none of the array does.
    const after = `${runId}-after`;
    const ok = await wh.from(T.clicks).insert({
      event_id: after,
      page: "/after",
      user_id: `user-${runId}`,
      session_id: `s-${runId}`,
    });
    expect(ok.error).toBeNull();
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE event_id = '${after}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);
    const inCH = await chQuery<{ event_id: string }>(
      `SELECT event_id FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
    );
    expect(inCH.map((r) => r.event_id)).toEqual([after]);
  });

  it("accepts a raw JSON array body (Content-Type: application/json) and lands every row", async () => {
    const runId = testId();
    const rows = [1, 2].map((n) => ({
      event_id: `${runId}-${n}`,
      page: "/json-array",
      user_id: `user-${runId}`,
      session_id: `s-${runId}`,
    }));

    // The SDK serializes arrays to NDJSON, so hit the server's JSON-array
    // decoder directly with a raw fetch to prove the wire path end-to-end.
    const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${makeJWT({ sub: "test-viewer", role: "viewer", tenant_id: "acme" })}`,
      },
      body: JSON.stringify(rows),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { total: number; succeeded: number; failed: number };
    expect(body).toMatchObject({ total: 2, succeeded: 2, failed: 0 });

    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT count() AS cnt FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        signal,
      );
      return Number((r[0] as { cnt: number }).cnt) === 2;
    }, 10_000);
  });
});

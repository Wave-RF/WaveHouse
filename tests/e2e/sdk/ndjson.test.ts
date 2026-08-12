import { describe, expect, it } from "vitest";
import { chQuery, dataClient, makeJWT, testId, WH_URL, waitForCondition } from "./helpers.js";
import { suiteTables } from "./tables.js";

/**
 * End-to-end coverage for NDJSON batch ingest (issue #195): the API
 * `application/x-ndjson` path, the SDK array `insert([...])` path that now rides
 * it, and the raw `insertNDJSON()` helper — including partial-failure handling.
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

  it("surfaces per-record validation failures while still persisting the good rows", async () => {
    const runId = testId();
    const goodA = `${runId}-good-a`;
    const goodB = `${runId}-good-b`;

    const rows = [
      { event_id: goodA, page: "/ok", user_id: `user-${runId}`, session_id: `s-${runId}` },
      // Unknown column → the server rejects this one line only.
      {
        event_id: `${runId}-bad`,
        page: "/bad",
        user_id: `user-${runId}`,
        session_id: `s-${runId}`,
        totally_fake_field: "nope",
      },
      { event_id: goodB, page: "/ok", user_id: `user-${runId}`, session_id: `s-${runId}` },
    ];

    const result = await wh.from(T.clicks).insert(rows);
    // The request itself succeeded — the bad record is reported, not thrown.
    expect(result.error).toBeNull();
    expect(result.data?.ok).toBe(false);
    expect(result.data?.total).toBe(3);
    expect(result.data?.succeeded).toBe(2);
    expect(result.data?.failed).toBe(1);
    const failed = result.data?.results?.find((r) => r.error);
    expect(failed?.index).toBe(2);

    // Exactly the two good rows reach ClickHouse; the bad one does not.
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
    expect(inCH.map((r) => r.event_id).sort()).toEqual([goodA, goodB].sort());
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

  it("reports a malformed NDJSON line and still ingests the rest", async () => {
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

    const result = await wh.from(T.clicks).insertNDJSON(ndjson);
    expect(result.error).toBeNull();
    expect(result.data?.total).toBe(2);
    expect(result.data?.succeeded).toBe(1);
    expect(result.data?.failed).toBe(1);
    const failed = result.data?.results?.find((r) => r.error);
    expect(failed?.index).toBe(2);
    expect(failed?.error).toContain("invalid json");

    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE event_id = '${good}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);
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

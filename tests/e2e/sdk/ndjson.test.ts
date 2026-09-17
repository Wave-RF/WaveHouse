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
    // CONTRACT CHANGE: the message is ClickHouse's own parse refusal, carrying
    // its code, where it used to be the Go decoder's flat "invalid json".
    expect(typeof failed?.code).toBe("number");
    expect(failed?.error).toBeTruthy();

    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE event_id = '${good}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);
  });

  // The §0.2 regression guard on the wire: a SINGLE-LINE JSON array with one
  // bad record used to lose the whole batch — chtypes rejects it outright and
  // exports no bytes, so the records that parsed perfectly went with it.
  // Ingest rewrites the array's depth-1 commas to newlines in place, which
  // restores #195's promise that one bad record never obscures the rest.
  it("salvages the good records of a compact JSON array with one bad record", async () => {
    const runId = testId();
    const good = [`${runId}-a`, `${runId}-c`];
    const body =
      "[" +
      [
        { event_id: good[0], page: "/a", user_id: `user-${runId}`, session_id: `s-${runId}` },
        {
          event_id: `${runId}-b`,
          page: "/b",
          user_id: `user-${runId}`,
          session_id: `s-${runId}`,
          totally_fake_field: "nope",
        },
        { event_id: good[1], page: "/c", user_id: `user-${runId}`, session_id: `s-${runId}` },
      ]
        .map((r) => JSON.stringify(r))
        .join(",") +
      "]";
    // Deliberately one line: JSON.stringify of the array would be too, but
    // spelling it out is what makes the framing the subject of the test.
    expect(body.includes("\n")).toBe(false);

    const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${makeJWT({ sub: "test-viewer", role: "viewer", tenant_id: "acme" })}`,
      },
      body,
    });
    expect(res.status).toBe(200);
    const parsed = (await res.json()) as {
      total: number;
      succeeded: number;
      failed: number;
      results: Array<{ index: number; code?: number }>;
    };
    expect(parsed).toMatchObject({ total: 3, succeeded: 2, failed: 1 });
    expect(parsed.results[1].code).toBe(117);

    await waitForCondition(async (signal) => {
      const r = await chQuery<{ event_id: string }>(
        `SELECT event_id FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
        signal,
      );
      return r.length === 2;
    }, 10_000);
    const inCH = await chQuery<{ event_id: string }>(
      `SELECT event_id FROM default.${T.clicks} WHERE user_id = 'user-${runId}'`,
    );
    expect(inCH.map((r) => r.event_id).sort()).toEqual([...good].sort());
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

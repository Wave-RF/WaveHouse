import { describe, it, expect } from "vitest";
import {
  dataClient,
  viewerClient,
  waitForCondition,
  testId,
  chQuery,
} from "./helpers.js";

describe("Ingest", () => {
  const wh = dataClient();

  it("inserts a valid row and verifies via query", async () => {
    const id = testId();
    const result = await wh.from("clicks").insert({
      event_id: id,
      page: "/test-ingest",
      user_id: "u1",
      session_id: "s1",
      country: "US",
      duration_ms: 42,
    });

    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // Poll ClickHouse — pipeline flush timing varies
    await waitForCondition(async () => {
      const r = await chQuery(
        `SELECT event_id FROM default.clicks WHERE event_id = '${id}'`,
      );
      return r.length === 1;
    }, 15_000);

    const rows = await chQuery(
      `SELECT event_id FROM default.clicks WHERE event_id = '${id}'`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveProperty("event_id", id);
  });

  it("inserts a batch of rows", async () => {
    const ids = [testId(), testId(), testId()];
    const rows = ids.map((id) => ({
      event_id: id,
      page: "/batch",
      user_id: "u-batch",
      session_id: "s-batch",
    }));

    const result = await wh.from("clicks").insert(rows);
    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // Poll ClickHouse — pipeline flush timing varies
    await waitForCondition(async () => {
      const r = await chQuery(
        `SELECT event_id FROM default.clicks WHERE event_id IN ('${ids.join("','")}')`,
      );
      return r.length === 3;
    }, 15_000);

    const inCH = await chQuery(
      `SELECT event_id FROM default.clicks WHERE event_id IN ('${ids.join("','")}')`,
    );
    expect(inCH).toHaveLength(3);
  });

  it("rejects unknown fields with a validation error", async () => {
    const result = await wh.from("clicks").insert({
      event_id: testId(),
      page: "/bad",
      user_id: "u1",
      session_id: "s1",
      totally_fake_field: "nope",
    } as any);

    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(400);
  });

  it("rejects ingest to a non-existent table", async () => {
    const result = await wh.from("this_table_does_not_exist").insert({
      some_field: "value",
    });

    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(404);
  });

  it("inserts with viewer role", async () => {
    const viewer = viewerClient();
    const id = testId();

    const result = await viewer.from("events").insert({
      event_id: id,
      type: "page_view",
      user_id: "viewer-1",
      source: "web",
    });

    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // Poll ClickHouse — on cold-start the pipeline may take longer than 4s
    await waitForCondition(async () => {
      const r = await chQuery(
        `SELECT event_id FROM default.events WHERE event_id = '${id}'`,
      );
      return r.length === 1;
    }, 15_000);

    const rows = await chQuery(
      `SELECT event_id FROM default.events WHERE event_id = '${id}'`,
    );
    expect(rows).toHaveLength(1);
  });
});


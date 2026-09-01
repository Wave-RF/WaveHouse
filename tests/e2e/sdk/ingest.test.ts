import { describe, expect, it } from "vitest";
import {
  adminClient,
  chQuery,
  dataClient,
  makeJWT,
  testId,
  viewerClient,
  WH_URL,
  waitForCondition,
} from "./helpers.js";
import { readPolicyFile, setPolicy } from "./settings.js";
import { suiteTables } from "./tables.js";

describe("Ingest", () => {
  const wh = dataClient();
  const T = suiteTables("ingest");

  it("inserts a valid row and verifies via query", async () => {
    const id = testId();
    const result = await wh.from(T.clicks).insert({
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
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE event_id = '${id}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);

    const rows = await chQuery(`SELECT event_id FROM default.${T.clicks} WHERE event_id = '${id}'`);
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

    const result = await wh.from(T.clicks).insert(rows);
    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // Poll ClickHouse — pipeline flush timing varies
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.clicks} WHERE event_id IN ('${ids.join("','")}')`,
        signal,
      );
      return r.length === 3;
    }, 10_000);

    const inCH = await chQuery(
      `SELECT event_id FROM default.${T.clicks} WHERE event_id IN ('${ids.join("','")}')`,
    );
    expect(inCH).toHaveLength(3);
  });

  it("rejects unknown fields with a validation error", async () => {
    const result = await wh.from(T.clicks).insert({
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

    const result = await viewer.from(T.events).insert({
      event_id: id,
      type: "page_view",
      user_id: "viewer-1",
      source: "web",
    });

    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // Poll ClickHouse — on cold-start the pipeline may take longer than 4s
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.events} WHERE event_id = '${id}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);

    const rows = await chQuery(`SELECT event_id FROM default.${T.events} WHERE event_id = '${id}'`);
    expect(rows).toHaveLength(1);
  });

  it("tests dedupe behavior by inserting the same event_id twice", async () => {
    const viewer = viewerClient();
    const id = testId();

    const result = await viewer.from(T.events).insert({
      event_id: id,
      type: "page_view",
      user_id: "dupe-test",
      source: "web",
    });
    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // Insert the same event_id again
    const result2 = await viewer.from(T.events).insert({
      event_id: id,
      type: "page_view",
      user_id: "dupe-test",
      source: "web",
    });
    expect(result2.error).toBeNull();
    expect(result2.data).toMatchObject({ duplicate: true });

    // Poll ClickHouse — on cold-start the pipeline may take longer than 4s
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.${T.events} WHERE event_id = '${id}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);

    const rows = await chQuery(`SELECT event_id FROM default.${T.events} WHERE event_id = '${id}'`);
    expect(rows).toHaveLength(1);
  });

  it("allows ingestion into tables with special characters (hyphens/dots)", async () => {
    const weirdTableName = "events-2026.data";
    const id = testId();
    const admin = adminClient(); // Need admin to manage schema/policy

    // 1. Create the table in ClickHouse using backticks
    await chQuery(`
      CREATE TABLE IF NOT EXISTS \`${weirdTableName}\` (
        event_id String,
        value Int32
      ) ENGINE = MergeTree() ORDER BY event_id
    `);

    // 2. Force WaveHouse to refresh its schema cache to see the new table
    await admin.schema.refresh();

    // 3. Update the policy to allow the viewer client to insert into this new table
    const currentPolicy = readPolicyFile();
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [weirdTableName]: {
          viewer: { insert: { allow_columns: ["*"] } },
        },
      },
    });

    // 4. Insert data using the standard SDK client (viewer)
    const result = await wh.from(weirdTableName).insert({
      event_id: id,
      value: 42,
    });

    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // 5. Verify it successfully made it through NATS, ingest worker, and into ClickHouse
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.\`${weirdTableName}\` WHERE event_id = '${id}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);

    const rows = await chQuery(
      `SELECT event_id FROM default.\`${weirdTableName}\` WHERE event_id = '${id}'`,
    );
    expect(rows).toHaveLength(1);

    // Clean up
    await chQuery(`DROP TABLE IF EXISTS \`${weirdTableName}\``);
  });

  it("safely parameterizes table names that look like SQL injection payloads", async () => {
    const maliciousName = "users; DROP TABLE clicks;";
    const id = testId();
    const admin = adminClient();

    // 1. Create the literal table in ClickHouse. We use backticks to safely create it.
    await chQuery(`
      CREATE TABLE IF NOT EXISTS \`${maliciousName}\` (
        event_id String,
        value Int32
      ) ENGINE = MergeTree() ORDER BY event_id
    `);

    // 2. Refresh schema
    await admin.schema.refresh();

    // 3. Update the policy to allow inserts into this weird table
    const currentPolicy = readPolicyFile();
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [maliciousName]: {
          viewer: { insert: { allow_columns: ["*"] } },
        },
      },
    });

    // 4. Push the malicious payload through the SDK
    const result = await wh.from(maliciousName).insert({
      event_id: id,
      value: 99,
    });

    expect(result.error).toBeNull();
    expect(result.data).toMatchObject({ ok: true });

    // 5. Verify it landed in the weirdly named table (proving it was treated as a literal string)
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT event_id FROM default.\`${maliciousName}\` WHERE event_id = '${id}'`,
        signal,
      );
      return r.length === 1;
    }, 10_000);

    // 6. ULTIMATE ASSERTION: Verify a real table was NOT dropped!
    // In ClickHouse, EXISTS returns a row with { result: 1 } if it exists.
    const clicksExists = await chQuery(`EXISTS TABLE default.${T.clicks}`);
    expect(clicksExists[0]).toHaveProperty("result", 1);

    // Clean up
    await chQuery(`DROP TABLE IF EXISTS \`${maliciousName}\``);
  });

  it("rejects ingest containing the reserved received_timestamp field", async () => {
    const result = await wh.from(T.clicks).insert({
      event_id: testId(),
      received_timestamp: "2026-01-01T00:00:00Z",
    } as any);

    expect(result.error).not.toBeNull();
    expect(result.error!.status).toBe(400);
  });

  it("rejects invalid JSON payloads", async () => {
    const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}`,
        // Required since the format became the caller's declaration: without
        // it the request is refused as a 415 before the body is even read
        // (covered by the next case).
        "Content-Type": "application/json",
      },
      body: "{ bad json",
    });
    expect(res.status).toBe(400);
  });

  it("rejects an ingest that declares no readable Content-Type", async () => {
    for (const headers of [
      // `fetch` supplies text/plain for a string body when none is set.
      { Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}` },
      {
        Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}`,
        "Content-Type": "text/csv",
      },
    ]) {
      const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
        method: "POST",
        headers,
        body: JSON.stringify({ event_id: testId(), page: "/undeclared" }),
      });
      expect(res.status).toBe(415);
      // The message names every type ingest reads, so a caller can fix the
      // request from the response alone.
      const body = (await res.json()) as { error?: string };
      expect(body.error).toContain("application/json");
      expect(body.error).toContain("application/x-ndjson");
    }
  });

  it("enforces policy check clauses (reject and auto-inject)", async () => {
    const currentPolicy = readPolicyFile();
    // Restrict this suite's clicks inserts so the 'country' column MUST be 'US'
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [T.clicks]: {
          ...(currentPolicy.tables[T.clicks] || {}),
          // Override only viewer's INSERT side; its select grant rides through
          // the spread untouched. There is no `*` any-role wildcard, and setup
          // seeds only viewer and admin, so viewer is the whole story here.
          viewer: {
            ...(currentPolicy.tables[T.clicks]?.viewer || {}),
            insert: {
              allow_columns: ["*"],
              check: { country: { _eq: "US" } },
            },
          },
        },
      },
    });

    // Reject if we explicitly send country=GB
    const badRes = await wh.from(T.clicks).insert({
      page: "/policy-check",
      user_id: "u-policy",
      session_id: "s-policy",
      event_id: testId(),
      country: "GB",
    });
    expect(badRes.error).not.toBeNull();
    expect(badRes.error!.status).toBe(403);

    // 2. Should auto-inject country=US if we omit it entirely
    const autoId = testId();
    const goodRes = await wh.from(T.clicks).insert({
      event_id: autoId,
      page: "/auto-inject",
      user_id: "u-policy",
      session_id: "s-policy",
    });
    expect(goodRes.error).toBeNull();

    // Verify the auto-injected row in CH has country=US
    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT country FROM default.${T.clicks} WHERE event_id = '${autoId}'`,
        signal,
      );
      return r.length === 1 && r[0].country === "US";
    }, 10_000);

    // Restore policy
    await setPolicy(currentPolicy);
  });

  it("rejects invalid JSON queries", async () => {
    const res = await fetch(`${WH_URL}/v1/query?table=${T.clicks}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}` },
      body: "{ bad json",
    });
    expect(res.status).toBe(400);
  });

  it("enforces max_rows policy limit", async () => {
    const currentPolicy = readPolicyFile();
    // Restrict viewer to only return 2 rows max from this suite's clicks
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [T.clicks]: {
          ...(currentPolicy.tables[T.clicks] || {}),
          viewer: {
            ...(currentPolicy.tables[T.clicks]?.viewer || {}),
            select: { allow_columns: ["*"], max_rows: 2 },
          },
        },
      },
    });

    // Even if we ask for 10 rows via the SDK, the policy should cap it at 2 at the backend
    const result = await wh.from(T.clicks).selectAll().limit(10).fetch();
    expect(result.error).toBeNull();
    expect(result.data).toHaveLength(2);

    // Restore policy
    await setPolicy(currentPolicy);
  });
});

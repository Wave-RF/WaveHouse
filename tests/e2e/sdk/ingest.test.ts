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

  // The unknown field is refused by ClickHouse's own parser (code 117): the
  // compile profile pins input_format_skip_unknown_fields=0 precisely so this
  // stays a verdict the caller hears rather than silent data loss.
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

  it("accepts an explicit received_timestamp and stores the caller's instant", async () => {
    // This case used to assert a 400 and pass for the wrong reason: the payload
    // omitted three columns that are neither nullable nor defaulted, and
    // WaveHouse's own validator called that "missing required column".
    // ClickHouse does not — an omitted JSONEachRow field takes the type's
    // default — and the gateway now gives the server's answer. Nothing is
    // reserved about received_timestamp on the way in: it is an ordinary column
    // with a DEFAULT, and a caller that supplies one keeps it.
    const id = testId();
    const result = await wh.from(T.clicks).insert({
      event_id: id,
      received_timestamp: "2026-01-01T00:00:00.000",
    } as any);
    expect(result.error).toBeNull();

    await waitForCondition(async () => {
      const rows = await chQuery(
        `SELECT page, toString(received_timestamp) AS ts FROM default.${T.clicks} WHERE event_id = '${id}'`,
      );
      if (rows.length !== 1) return false;
      expect(rows[0].ts).toBe("2026-01-01 00:00:00.000");
      expect(rows[0].page).toBe("");
      return true;
    }, 15_000);
  });

  it("rejects a value ClickHouse cannot parse, with ClickHouse's own code", async () => {
    // The replacement for the "reserved field" case above: a real per-record
    // refusal, carrying the server's error code rather than a gateway guess.
    const result = await wh.from(T.clicks).insert({
      event_id: testId(),
      page: "/bad-duration",
      user_id: "u1",
      session_id: "s1",
      duration_ms: "not-a-number",
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
    // The refusal is ClickHouse's own parser now, so the body carries its code
    // rather than a flat gateway "invalid json".
    const body = (await res.json()) as { error?: string; code?: number };
    expect(typeof body.code).toBe("number");
  });

  it("rejects an ingest that declares no readable Content-Type", async () => {
    for (const headers of [
      // `fetch` supplies text/plain for a string body when none is set.
      { Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}` },
      {
        Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}`,
        // text/csv USED to sit here; it is an accepted format now, so the
        // refusal case needs a type ingest genuinely does not read.
        "Content-Type": "application/xml",
      },
    ]) {
      const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
        method: "POST",
        headers,
        body: JSON.stringify({ event_id: testId(), page: "/undeclared" }),
      });
      expect(res.status).toBe(415);
      // The message names every type ingest reads, in order, so a caller can fix
      // the request from the response alone. Asserted as one literal: checking
      // the entries individually is vacuous for two of them, since
      // `application/json` and `application/jsonl` are substrings of
      // `application/jsonlines`.
      const body = (await res.json()) as { error?: string };
      expect(body.error).toContain(
        "application/json, application/x-ndjson, application/ndjson, application/jsonl, application/jsonlines, text/csv, text/tab-separated-values",
      );
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

  // CONTRACT CHANGE (AUDIT D1): a column the caller's role may not write is no
  // longer a gateway 403 `column "x" not allowed for insert`. Column policy is
  // enforced by compiling the role's own schema WITHOUT the denied columns, so
  // naming one is ClickHouse's own per-record UNKNOWN_FIELD — a 400 with
  // `code: 117`, whose message does not say whether the column exists.
  it("refuses a denied insert column with ClickHouse's code 117", async () => {
    const currentPolicy = readPolicyFile();
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [T.clicks]: {
          ...(currentPolicy.tables[T.clicks] || {}),
          viewer: {
            ...(currentPolicy.tables[T.clicks]?.viewer || {}),
            insert: { allow_columns: ["*"], deny_columns: ["country"] },
          },
        },
      },
    });

    try {
      const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          event_id: testId(),
          page: "/denied-column",
          user_id: "u1",
          session_id: "s1",
          country: "GB",
        }),
      });
      expect(res.status).toBe(400);
      const body = (await res.json()) as { error?: string; code?: number };
      expect(body.code).toBe(117);
      expect(body.error).toContain("country");

      // The same role writing only permitted columns still succeeds, and the
      // denied column takes the server's own default.
      const okId = testId();
      const okRes = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          event_id: okId,
          page: "/denied-column-ok",
          user_id: "u1",
          session_id: "s1",
        }),
      });
      expect(okRes.status).toBe(200);
      await waitForCondition(async (signal) => {
        const r = await chQuery(
          `SELECT country FROM default.${T.clicks} WHERE event_id = '${okId}'`,
          signal,
        );
        return r.length === 1 && r[0].country === "US";
      }, 10_000);
    } finally {
      await setPolicy(currentPolicy);
    }
  });

  // CSV and TSV are new accepted formats. They are HEADER-LESS and positional
  // in the table's declaration order — every wire column, in that order, with
  // an empty field meaning "take the DEFAULT". An end-to-end assertion is the
  // only one that catches a column-order bug: a mis-ordered body still answers
  // 200.
  it("ingests a complete positional CSV row end to end", async () => {
    const id = testId();
    // Every wire column, in declaration order. An empty field takes the
    // column's DEFAULT — which is how received_timestamp gets now64(3).
    const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}`,
        "Content-Type": "text/csv",
      },
      body: `"${id}","/csv-full","u-csv","s-csv","","GB",7,\n`,
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { succeeded: number; results?: unknown[] };
    expect(body.succeeded).toBe(1);

    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT page, country, duration_ms FROM default.${T.clicks} WHERE event_id = '${id}'`,
        signal,
      );
      if (r.length !== 1) return false;
      expect(r[0].page).toBe("/csv-full");
      expect(r[0].country).toBe("GB");
      expect(Number(r[0].duration_ms)).toBe(7);
      return true;
    }, 10_000);
  });

  it("ingests a complete positional TSV row, and a short row is code 27", async () => {
    const id = testId();
    // TSV spells "take the DEFAULT" as ClickHouse's own \\N (null, which
    // input_format_null_as_default turns into the column default). An EMPTY TSV
    // field is the empty string, which a DateTime64 cannot read — unlike CSV,
    // where an empty field IS the default.
    const res = await fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer" })}`,
        "Content-Type": "text/tab-separated-values",
      },
      // Row 1 is complete; row 2 is short, which is a PER-RECORD refusal with
      // ClickHouse's code 27 — not a whole-request failure.
      body: `${id}\t/tsv\tu-tsv\ts-tsv\t\tGB\t7\t\\N\nshort\t/tsv\n`,
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      total: number;
      succeeded: number;
      failed: number;
      results: Array<{ index: number; code?: number }>;
    };
    expect(body.total).toBe(2);
    expect(body.succeeded).toBe(1);
    expect(body.failed).toBe(1);
    expect(body.results[1].code).toBe(27);

    await waitForCondition(async (signal) => {
      const r = await chQuery(
        `SELECT page, country FROM default.${T.clicks} WHERE event_id = '${id}'`,
        signal,
      );
      return r.length === 1 && r[0].page === "/tsv" && r[0].country === "GB";
    }, 10_000);
  });

  // CONTRACT CHANGE (AUDIT D3): an `_in` check has no single value to inject,
  // so a record omitting the column is judged on the TABLE's own default rather
  // than failing closed on absence. country DEFAULTs to 'US' here, so a token
  // whose allowed set contains 'US' admits the record and one without it does
  // not — the old behaviour refused both.
  it("tests the table default when an _in check column is absent", async () => {
    const currentPolicy = readPolicyFile();
    // `_in` takes a claim TEMPLATE, not a literal set: the allowed values come
    // from the token, which is the multi-tenant case the check exists for.
    await setPolicy({
      tables: {
        ...currentPolicy.tables,
        [T.clicks]: {
          ...(currentPolicy.tables[T.clicks] || {}),
          viewer: {
            ...(currentPolicy.tables[T.clicks]?.viewer || {}),
            insert: { allow_columns: ["*"], check: { country: { _in: "{{ jwt.countries }}" } } },
          },
        },
      },
    });

    const post = async (id: string, countries: string[]) =>
      fetch(`${WH_URL}/v1/ingest?table=${T.clicks}`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${makeJWT({ sub: "test", role: "viewer", countries })}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          event_id: id,
          page: "/in-default",
          user_id: "u-in",
          session_id: "s-in",
        }),
      });

    try {
      // country DEFAULTs to 'US'; the token allows it, so the record is admitted
      // and stored with that default — the behaviour change.
      const okId = testId();
      const ok = await post(okId, ["US", "CA"]);
      expect(ok.status).toBe(200);
      await waitForCondition(async (signal) => {
        const r = await chQuery(
          `SELECT country FROM default.${T.clicks} WHERE event_id = '${okId}'`,
          signal,
        );
        return r.length === 1 && r[0].country === "US";
      }, 10_000);

      // The same absent column with a token that does NOT allow the default is
      // refused — the check is really being evaluated, not skipped.
      const badId = testId();
      const bad = await post(badId, ["CA", "MX"]);
      expect(bad.status).toBe(403);
      const body = (await bad.json()) as { error?: string };
      expect(body.error).toContain("check failed");
    } finally {
      await setPolicy(currentPolicy);
    }
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

import type { Policy } from "@wavehouse/sdk";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { adminClient, makeJWT, testId, WH_URL } from "./helpers.js";
import { suiteTables } from "./tables.js";

describe("Admin", () => {
  const wh = adminClient();
  const T = suiteTables("admin");

  // Track resources created during tests for cleanup
  const createdPipes: string[] = [];
  let policyWasSet = false;
  // Snapshot of the real (multi-suite) baseline policy, captured before any
  // mutation so afterAll restores exactly what setup.ts bootstrapped — not a
  // hand-written reconstruction, which would wipe every other suite's entries.
  let baselinePolicy: Policy | undefined;

  beforeAll(async () => {
    const res = await wh.policy.get();
    if (res.error) throw new Error(`Failed to fetch baseline policy: ${res.error.message}`);
    baselinePolicy = structuredClone(res.data);
  });

  afterAll(async () => {
    // Clean up test pipes
    for (const name of createdPipes) {
      await wh.pipes.delete(name);
    }
    // Restore the captured baseline policy if we modified it.
    if (policyWasSet && baselinePolicy) {
      await wh.policy.set(baselinePolicy);
    }
  });

  describe("Schema", () => {
    it("lists all schemas with column metadata", async () => {
      const result = await wh.schema.list();
      expect(result.error).toBeNull();
      expect(result.data).toBeDefined();

      const tables = result.data!;
      // setup.ts creates this suite's clicks/events/users tables
      expect(tables).toHaveProperty(T.clicks);
      expect(tables).toHaveProperty(T.events);
      expect(tables).toHaveProperty(T.users);

      // Verify column metadata on clicks
      const clicks = tables[T.clicks];
      expect(clicks.columns).toBeInstanceOf(Array);
      const colNames = clicks.columns.map((c: any) => c.name);
      expect(colNames).toContain("event_id");
      expect(colNames).toContain("page");
      expect(colNames).toContain("duration_ms");
    });

    it("refreshes schema without error", async () => {
      const result = await wh.schema.refresh();
      expect(result.error).toBeNull();
    });

    it("gets per-table schema", async () => {
      const result = await wh.from(T.clicks).schema();
      expect(result.error).toBeNull();
      expect(result.data).toBeDefined();
      expect((result.data as any).columns).toBeInstanceOf(Array);
    });
  });

  describe("Policy", () => {
    const testPolicy = {
      tables: {
        [T.clicks]: {
          select: {
            viewer: {
              allow_columns: ["page", "country", "duration_ms", "received_timestamp"],
              filter: { country: { _eq: "{{ jwt.country }}" } },
            },
            admin: {
              allow_columns: ["*"] as string[],
            },
          },
          insert: {
            viewer: { allow_columns: ["*"] as string[] },
            admin: { allow_columns: ["*"] as string[] },
          },
        },
      },
    };

    it("validates a policy (dry run)", async () => {
      const result = await wh.policy.validate(testPolicy);
      expect(result.error).toBeNull();
      expect(result.data).toBeDefined();
    });

    it("sets and gets a policy", async () => {
      // Spread the baseline so we override only this suite's clicks entry —
      // setting the bare testPolicy would wipe every other suite's tables from
      // the live policy until afterAll restores it.
      const setResult = await wh.policy.set({
        tables: { ...(baselinePolicy?.tables ?? {}), ...testPolicy.tables },
      });
      expect(setResult.error).toBeNull();
      policyWasSet = true;

      const getResult = await wh.policy.get();
      expect(getResult.error).toBeNull();
      expect(getResult.data).toBeDefined();
      expect(getResult.data).toHaveProperty("tables");
    });
  });

  describe("Pipes", () => {
    const pipeName = `test_pipe_${Date.now()}`;

    it("creates a pipe", async () => {
      const result = await wh.pipes.set(pipeName, {
        sql: `SELECT page, count() as views FROM default.${T.clicks} GROUP BY page ORDER BY views DESC LIMIT {{limit:10}}`,
        description: "E2E test pipe",
        allowed_roles: ["admin", "viewer"],
      });
      expect(result.error).toBeNull();
      createdPipes.push(pipeName);
    });

    it("lists pipes and finds the created one", async () => {
      const result = await wh.pipes.list();
      expect(result.error).toBeNull();
      expect(result.data).toBeInstanceOf(Array);

      const names = result.data!.map((p: any) => p.name);
      expect(names).toContain(pipeName);
    });

    it("executes a pipe with params", async () => {
      const result = await wh.pipe(pipeName, { limit: 3 });
      expect(result.error).toBeNull();

      // Fresh DB may have 0 rows — data can be null or empty array
      const rows = result.data ?? [];
      expect(rows).toBeInstanceOf(Array);
      expect(rows.length).toBeLessThanOrEqual(3);

      if (rows.length > 0) {
        expect(rows[0]).toHaveProperty("page");
        expect(rows[0]).toHaveProperty("views");
      }
    });

    it("executes a pipe with an array (IN list) param", async () => {
      const inPipe = `test_pipe_in_${Date.now()}`;
      const created = await wh.pipes.set(inPipe, {
        sql: `SELECT page, count() AS views FROM default.${T.clicks} WHERE page IN {{pages}} GROUP BY page`,
        parameters: [{ name: "pages", type: "array", required: true }],
        description: "E2E test pipe (IN list)",
        allowed_roles: ["admin", "viewer"],
      });
      expect(created.error).toBeNull();
      createdPipes.push(inPipe);

      // The array renders to `page IN ('/home', '/about', 'o''brien')` — every
      // element escaped (#317). The single quote in `o'brien` must stay inside
      // its literal; if escaping regressed, ClickHouse would error here.
      const result = await wh.pipe(inPipe, { pages: ["/home", "/about", "o'brien"] });
      expect(result.error).toBeNull();

      const rows = result.data ?? [];
      expect(rows).toBeInstanceOf(Array);
      if (rows.length > 0) {
        expect(rows[0]).toHaveProperty("page");
        expect(rows[0]).toHaveProperty("views");
      }
    });

    it("deletes a pipe", async () => {
      const result = await wh.pipes.delete(pipeName);
      expect(result.error).toBeNull();

      // Remove from cleanup list since we already deleted it
      const idx = createdPipes.indexOf(pipeName);
      if (idx >= 0) createdPipes.splice(idx, 1);

      // Verify it's gone
      const list = await wh.pipes.list();
      const names = (list.data ?? []).map((p: any) => p.name);
      expect(names).not.toContain(pipeName);
    });
  });

  describe("DLQ", () => {
    it("returns DLQ stats", async () => {
      const result = await wh.dlq.list();
      expect(result.error).toBeNull();
      expect(result.data).toBeDefined();
    });
  });

  describe("System", () => {
    it("health endpoint reports the server is online", async () => {
      const result = await wh.sys.health();
      // /v1/health is content-free (200/503, no body) — a null error means online.
      expect(result.error).toBeNull();
    });

    it("raw SQL: row counts", async () => {
      for (const table of [T.clicks, T.events, T.users]) {
        const result = await wh.sql(`SELECT count() as cnt FROM default.${table}`);
        expect(result.error).toBeNull();
        expect(result.data).toBeInstanceOf(Array);
      }
    });
  });

  // Runtime settings are an operator surface, deliberately not in the SDK
  // (docs/sdk/admin.md) — so this block exercises the HTTP API directly.
  // Settings are server-global; test files run sequentially (maxWorkers: 1),
  // and the DELETE below (plus the afterAll backstop) restores compiled
  // defaults so later files never see the require_id=true window.
  describe("Settings", () => {
    const settingsURL = `${WH_URL}/v1/admin/settings`;
    const adminHeaders = (extra?: Record<string, string>) => ({
      Authorization: `Bearer ${makeJWT({ sub: "e2e-settings", role: "admin" })}`,
      "Content-Type": "application/json",
      ...extra,
    });
    type SectionState = { section: string; source: "db" | "default"; revision?: number };
    type SettingsBody = {
      values: { dedupe: { id_field: string; require_id: boolean } };
      sources: SectionState[];
    };
    const dedupeSource = (body: SettingsBody): SectionState => {
      const s = body.sources.find((x) => x.section === "dedupe");
      if (!s) throw new Error(`no dedupe section in ${JSON.stringify(body.sources)}`);
      return s;
    };

    afterAll(async () => {
      // Revert to compiled defaults even if an assertion failed mid-flow.
      await fetch(`${settingsURL}/dedupe`, { method: "DELETE", headers: adminHeaders() });
    });

    it("reports compiled defaults when no override is stored", async () => {
      const res = await fetch(settingsURL, { headers: adminHeaders() });
      expect(res.status).toBe(200);
      const body: SettingsBody = await res.json();
      expect(body.values.dedupe).toEqual({ id_field: "event_id", require_id: false });
      expect(dedupeSource(body).source).toBe("default");
    });

    it("is admin-gated", async () => {
      const res = await fetch(settingsURL, {
        headers: { Authorization: `Bearer ${makeJWT({ sub: "e2e-viewer", role: "viewer" })}` },
      });
      expect(res.status).toBe(403);
    });

    it("PUT with If-Match applies live: require_id rejects id-less rows at the next request", async () => {
      const put = await fetch(`${settingsURL}/dedupe`, {
        method: "PUT",
        headers: adminHeaders({ "If-Match": "0" }), // 0 = no override stored yet
        body: JSON.stringify({ id_field: "event_id", require_id: true }),
      });
      expect(put.status).toBe(200);

      // No restart, no signal: the very next ingest request must enforce it.
      // users tables have no event_id column, so an id-less row passes schema
      // validation but can never be deduped — the case require_id turns into
      // a 400 (clicks/events declare event_id required, so schema validation
      // already rejects id-less rows there before dedupe is consulted).
      const missing = await wh
        .from(T.users)
        .insert({ user_id: testId(), name: "Settings", email: "s@e2e.test" });
      expect(missing.error).not.toBeNull();
      expect(missing.error!.status).toBe(400);
      expect(missing.error!.message).toContain("dedupe");

      // Rows that carry the id are unaffected.
      const withId = await wh.from(T.clicks).insert({
        event_id: testId(),
        page: "/settings",
        user_id: "u-settings",
        session_id: "s-settings",
      });
      expect(withId.error).toBeNull();
    });

    it("stale If-Match gets 409 and the write changes nothing", async () => {
      const res = await fetch(`${settingsURL}/dedupe`, {
        method: "PUT",
        headers: adminHeaders({ "If-Match": "0" }), // override now exists — 0 is stale
        body: JSON.stringify({ id_field: "event_id", require_id: false }),
      });
      expect(res.status).toBe(409);

      const body: SettingsBody = await (await fetch(settingsURL, { headers: adminHeaders() })).json();
      expect(body.values.dedupe.require_id).toBe(true); // survived the lost race
      expect(dedupeSource(body).source).toBe("db");
      expect(dedupeSource(body).revision).toBeGreaterThan(0);
    });

    it("rejects an invalid document and an unknown field with 400", async () => {
      const invalid = await fetch(`${settingsURL}/dedupe`, {
        method: "PUT",
        headers: adminHeaders(),
        body: JSON.stringify({ id_field: "", require_id: true }),
      });
      expect(invalid.status).toBe(400);

      const typo = await fetch(`${settingsURL}/dedupe`, {
        method: "PUT",
        headers: adminHeaders(),
        body: JSON.stringify({ id_field: "event_id", requireid: true }),
      });
      expect(typo.status).toBe(400);
    });

    it("DELETE reverts to compiled defaults, applied live", async () => {
      const del = await fetch(`${settingsURL}/dedupe`, {
        method: "DELETE",
        headers: adminHeaders(),
      });
      expect(del.status).toBe(200);

      const body: SettingsBody = await (await fetch(settingsURL, { headers: adminHeaders() })).json();
      expect(body.values.dedupe).toEqual({ id_field: "event_id", require_id: false });
      expect(dedupeSource(body).source).toBe("default");

      // Back to the default posture: the id-less row is WARN'd and accepted.
      const ok = await wh
        .from(T.users)
        .insert({ user_id: testId(), name: "Settings", email: "s@e2e.test" });
      expect(ok.error).toBeNull();
    });
  });
});

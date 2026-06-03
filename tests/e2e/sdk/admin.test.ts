import type { Policy } from "@wavehouse/sdk";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { adminClient } from "./helpers.js";
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
    it("health endpoint returns status", async () => {
      const result = await wh.sys.health();
      expect(result.error).toBeNull();
      expect(result.data).toHaveProperty("status");
    });

    it("raw SQL: row counts", async () => {
      for (const table of [T.clicks, T.events, T.users]) {
        const result = await wh.sql(`SELECT count() as cnt FROM default.${table}`);
        expect(result.error).toBeNull();
        expect(result.data).toBeInstanceOf(Array);
      }
    });
  });
});

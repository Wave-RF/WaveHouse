import type { Pipe, Policy } from "@wavehouse/sdk";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { adminClient } from "./helpers.js";
import { readPipesFile, readPolicyFile, setPipes, setPolicy } from "./settings.js";
import { suiteTables } from "./tables.js";

describe("Admin", () => {
  const wh = adminClient();
  const T = suiteTables("admin");

  // Snapshot of pipes.json before this suite adds any, restored in afterAll.
  let baselinePipes: Pipe[] | undefined;
  let policyWasSet = false;
  // Snapshot of the real (multi-suite) baseline policy, captured before any
  // mutation so afterAll restores exactly what setup.ts bootstrapped — not a
  // hand-written reconstruction, which would wipe every other suite's entries.
  let baselinePolicy: Policy | undefined;

  beforeAll(async () => {
    baselinePolicy = structuredClone(readPolicyFile());
    baselinePipes = readPipesFile();
  });

  afterAll(async () => {
    // Restore the captured baseline policy if we modified it.
    if (policyWasSet && baselinePolicy) {
      await setPolicy(baselinePolicy);
    }
    // Write pipes.json back to what it was before this suite's pipes.
    if (baselinePipes) {
      await setPipes(baselinePipes);
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
      default_role: "viewer",
      admin_role: "admin",
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

    it("adopts a policy from policies.json", async () => {
      // Spread the baseline so we override only this suite's clicks entry —
      // writing the bare testPolicy would wipe every other suite's tables from
      // the live policy until afterAll restores it.
      policyWasSet = true;
      await setPolicy({
        ...testPolicy,
        tables: { ...(baselinePolicy?.tables ?? {}), ...testPolicy.tables },
      });

      // setPolicy throws unless the reload reported adopted, so reaching this
      // line means the server took the document; the file on disk IS the
      // adopted policy (there is no HTTP read of it).
      expect(readPolicyFile()).toMatchObject({
        default_role: "viewer",
        admin_role: "admin",
      });
    });
  });

  describe("Pipes", () => {
    const pipeName = `test_pipe_${Date.now()}`;
    const inPipe = `test_pipe_in_${Date.now()}`;

    it("adopts a pipe from pipes.json", async () => {
      await setPipes([
        ...readPipesFile(),
        {
          name: pipeName,
          sql: `SELECT page, count() as views FROM default.${T.clicks} GROUP BY page ORDER BY views DESC LIMIT {{limit:10}}`,
          description: "E2E test pipe",
          allowed_roles: ["admin", "viewer"],
        },
      ]);

      const result = await wh.pipes.get(pipeName);
      expect(result.error).toBeNull();
      expect(result.data).toMatchObject({ name: pipeName, description: "E2E test pipe" });
    });

    it("lists pipes and finds the adopted one", async () => {
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
      await setPipes([
        ...readPipesFile(),
        {
          name: inPipe,
          sql: `SELECT page, count() AS views FROM default.${T.clicks} WHERE page IN {{pages}} GROUP BY page`,
          parameters: [{ name: "pages", type: "array", required: true }],
          description: "E2E test pipe (IN list)",
          allowed_roles: ["admin", "viewer"],
        },
      ]);

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

    it("drops a pipe removed from pipes.json", async () => {
      await setPipes(readPipesFile().filter((p) => p.name !== pipeName));

      const list = await wh.pipes.list();
      expect(list.error).toBeNull();
      const names = (list.data ?? []).map((p: any) => p.name);
      expect(names).not.toContain(pipeName);
      expect(names).toContain(inPipe);

      const result = await wh.pipes.get(pipeName);
      expect(result.error?.status).toBe(404);
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
});

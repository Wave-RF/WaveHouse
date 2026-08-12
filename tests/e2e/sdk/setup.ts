/**
 * Vitest globalSetup for the E2E suite.
 *
 * Lifecycle is owned by the Go orchestrator at scripts/orchestrator:
 * one ClickHouse testcontainer + one WaveHouse instance with auth
 * enabled. CLICKHOUSE_URL / WAVEHOUSE_URL come in via env. See
 * helpers.ts for client factories.
 *
 * Setup probes both URLs, creates the per-suite tables (see tables.ts —
 * each test file gets its own clicks_<suite>/events_<suite>/users_<suite>),
 * refreshes WH's schema until they all appear, and bootstraps a baseline
 * policy that covers every generated table.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { CH_URL, makeJWT, WH_URL } from "./helpers.js";
import { allTableSpecs, TABLE_DDL } from "./tables.js";

const setupAuth = () => `Bearer ${makeJWT({ sub: "e2e-setup", role: "admin", tenant_id: "acme" })}`;

async function probe(url: string, timeoutMs = 2000): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    return res.ok;
  } catch {
    return false;
  }
}

// Create one physical table per (suite, kind). DDL is templated in tables.ts
// (CREATE TABLE IF NOT EXISTS), so this is idempotent: re-runs against an
// existing stack are no-ops.
async function createTables(): Promise<void> {
  for (const { name, kind } of allTableSpecs()) {
    const res = await fetch(CH_URL, { method: "POST", body: TABLE_DDL[kind](name) });
    if (!res.ok) {
      const text = await res.text();
      // Belt-and-suspenders: IF NOT EXISTS already makes this idempotent, but
      // tolerate a racing "already exists" too.
      if (!text.includes("already exists")) {
        throw new Error(`Create table ${name} failed: ${text}`);
      }
    }
  }
}

// /v1/schema returns an array of { name, columns, ... } — wait until every
// generated table is present so we don't burn the full timeout on the happy
// path.
async function refreshSchema(): Promise<void> {
  const headers = { Authorization: setupAuth() };
  await fetch(`${WH_URL}/v1/schema/refresh`, { method: "POST", headers });

  const expected = allTableSpecs().map((t) => t.name);
  const start = Date.now();
  while (Date.now() - start < 30_000) {
    const res = await fetch(`${WH_URL}/v1/schema`, { headers });
    if (res.ok) {
      const schema = (await res.json()) as Array<{ name?: string }>;
      const present = new Set(schema.map((t) => t?.name).filter(Boolean));
      if (expected.every((t) => present.has(t))) return;
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error("schema not refreshed within 30s");
}

async function bootstrapTestPolicy(): Promise<void> {
  const rolePerms = (extra?: Record<string, unknown>) => ({
    "*": { allow_columns: ["*"], ...extra },
    viewer: { allow_columns: ["*"], ...extra },
    admin: { allow_columns: ["*"], ...extra },
  });
  // Permissive select+insert for every generated table. Tests that exercise
  // policy enforcement snapshot this baseline, mutate their own table's entry,
  // and restore it (the suite runs sequentially, so the snapshot/restore is
  // race-free — see tables.ts).
  const tables: Record<string, { select: unknown; insert: unknown }> = {};
  for (const { name } of allTableSpecs()) {
    tables[name] = { select: rolePerms(), insert: rolePerms() };
  }
  const policy = { tables };

  const res = await fetch(`${WH_URL}/v1/admin/policy`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      Authorization: setupAuth(),
    },
    body: JSON.stringify(policy),
  });
  if (!res.ok) {
    throw new Error(`Failed to bootstrap test policy: ${res.status} ${await res.text()}`);
  }
}

/**
 * Print the runtime this suite is actually running on, and say so loudly when
 * it isn't the one CI pins.
 *
 * Worth the four lines: a Node-version-specific transport bug (undici 8.8-8.9
 * stalling on idle pooled connections, nodejs/undici#5600) cost days of
 * investigation that started from "my machine must be broken", because nothing
 * in the output distinguished a local run from CI's. `.nvmrc` is read by CI's
 * setup-node but is inert locally unless you use a version manager, so the two
 * can drift silently. See #440.
 */
function reportRuntime(): void {
  const nvmrc = (() => {
    try {
      return readFileSync(join(import.meta.dirname, "../../../.nvmrc"), "utf8").trim();
    } catch {
      return "";
    }
  })();
  const undici = process.versions.undici ? ` (undici ${process.versions.undici})` : "";
  console.log(`  node ${process.version}${undici}`);

  const major = process.version.replace(/^v/, "").split(".")[0];
  if (nvmrc && major !== nvmrc.replace(/^v/, "").split(".")[0]) {
    console.log(`  ⚠ CI pins node ${nvmrc} (.nvmrc) — this run is on a different major.`);
    console.log(`    A failure here may not reproduce in CI, and vice versa.`);
  }
}

export async function setup(): Promise<void> {
  console.log(`\n🔍 E2E setup`);
  reportRuntime();
  console.log(`  CLICKHOUSE_URL=${CH_URL}`);
  console.log(`  WAVEHOUSE_URL=${WH_URL}`);

  if (!(await probe(`${CH_URL}/ping`))) {
    throw new Error(
      `ClickHouse not reachable at ${CH_URL}. Use the orchestrator (\`make test-e2e\`).`,
    );
  }
  if (!(await probe(`${WH_URL}/livez`))) {
    throw new Error(
      `WaveHouse not reachable at ${WH_URL}. Use the orchestrator (\`make test-e2e\`).`,
    );
  }

  console.log("  📦 Creating per-suite tables...");
  await createTables();

  console.log("  🔄 Refreshing schema...");
  await refreshSchema();
  console.log("  ✓ Schema refreshed");

  console.log("  🔑 Bootstrapping policy...");
  await bootstrapTestPolicy();
  console.log("  ✓ Policy bootstrapped\n");
}

export async function teardown(): Promise<void> {
  // Lifecycle is owned by the orchestrator. When running directly with
  // `pnpm test` against an externally-managed stack, teardown is a no-op
  // so the user's stack stays up between iterations.
}

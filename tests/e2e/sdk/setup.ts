/**
 * Vitest globalSetup for the E2E suite.
 *
 * Lifecycle is owned by the Go orchestrator at scripts/orchestrator:
 * one ClickHouse testcontainer + one WaveHouse instance with auth
 * enabled. CLICKHOUSE_URL / WAVEHOUSE_URL come in via env. See
 * helpers.ts for client factories.
 *
 * Setup probes both URLs, applies fixtures to CH, refreshes WH's
 * schema, and bootstraps a baseline policy.
 */

import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { CH_URL, makeJWT, WH_URL } from "./helpers.js";

const FIXTURES_DIR = path.resolve(__dirname, "../fixtures");
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

async function applyFixtures(): Promise<void> {
  const files = readdirSync(FIXTURES_DIR)
    .filter((f) => f.endsWith(".sql"))
    .sort();

  for (const file of files) {
    const sql = readFileSync(path.join(FIXTURES_DIR, file), "utf-8");
    const res = await fetch(CH_URL, { method: "POST", body: sql });
    if (!res.ok) {
      const text = await res.text();
      // Idempotent: tolerate "already exists" so re-runs against an
      // existing stack don't fail on the CREATE TABLE statements.
      if (!text.includes("already exists")) {
        throw new Error(`Fixture ${file} failed: ${text}`);
      }
    }
  }
}

// /v1/schema returns an array of { name, columns, ... } — wait until
// every expected table is present so we don't burn the full timeout
// on the happy path.
async function refreshSchema(): Promise<void> {
  const headers = { Authorization: setupAuth() };
  await fetch(`${WH_URL}/v1/schema/refresh`, { method: "POST", headers });

  const expected = ["clicks", "events", "users"];
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
  const policy = {
    tables: {
      clicks: { select: rolePerms(), insert: rolePerms() },
      events: { select: rolePerms(), insert: rolePerms() },
      users: { select: rolePerms(), insert: rolePerms() },
    },
  };

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

export async function setup(): Promise<void> {
  console.log(`\n🔍 E2E setup`);
  console.log(`  CLICKHOUSE_URL=${CH_URL}`);
  console.log(`  WAVEHOUSE_URL=${WH_URL}`);

  if (!(await probe(`${CH_URL}/ping`))) {
    throw new Error(
      `ClickHouse not reachable at ${CH_URL}. Use the orchestrator (\`make test-e2e\`).`,
    );
  }
  if (!(await probe(`${WH_URL}/health`))) {
    throw new Error(
      `WaveHouse not reachable at ${WH_URL}. Use the orchestrator (\`make test-e2e\`).`,
    );
  }

  console.log("  📦 Applying SQL fixtures...");
  await applyFixtures();

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

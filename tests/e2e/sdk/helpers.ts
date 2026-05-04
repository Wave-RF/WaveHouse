/**
 * Shared test helpers for E2E integration tests.
 */

import { createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import { createClient } from "@wavehouse/sdk";
import type { SetupState } from "./setup.js";

// ── Constants ─────────────────────────────────────────────────────────────────

export const WH_URL = process.env.WAVEHOUSE_URL ?? "http://localhost:8080";
export const CH_URL = process.env.CLICKHOUSE_URL ?? "http://localhost:8123";
export const JWT_SECRET = "sdk-dev-secret";

// ── Mode Detection ────────────────────────────────────────────────────────────

let _cachedMode: "dev" | "full" | undefined;

/**
 * Returns 'dev' if WaveHouse was already running (quick pass),
 * or 'full' if setup started it with real auth.
 */
export function getMode(): "dev" | "full" {
  if (_cachedMode) return _cachedMode;
  try {
    const state: SetupState = JSON.parse(
      readFileSync(path.resolve(__dirname, ".setup-state.json"), "utf-8"),
    );
    _cachedMode = state.mode;
  } catch {
    // If state file is missing, assume dev mode (safest — skip auth tests)
    _cachedMode = "dev";
  }
  return _cachedMode;
}

/** Returns true if running against an externally-started WaveHouse (auth may be off). */
export function isDevMode(): boolean {
  return getMode() === "dev";
}

/**
 * Returns a client appropriate for the current mode.
 * - Full mode: viewer (validates real JWT+policy flow)
 * - Dev mode: admin (auth may be disabled; admin works regardless)
 */
export function dataClient() {
  return isDevMode() ? adminClient() : viewerClient();
}

// ── JWT Builder ───────────────────────────────────────────────────────────────

export function makeJWT(
  claims: Record<string, unknown>,
  secret: string = JWT_SECRET,
): string {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    ...claims,
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const encode = (obj: unknown) =>
    Buffer.from(JSON.stringify(obj)).toString("base64url");

  const unsigned = `${encode(header)}.${encode(payload)}`;
  const sig = createHmac("sha256", secret).update(unsigned).digest("base64url");
  return `${unsigned}.${sig}`;
}

export function makeExpiredJWT(claims: Record<string, unknown>): string {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    ...claims,
    iat: Math.floor(Date.now() / 1000) - 7200,
    exp: Math.floor(Date.now() / 1000) - 3600, // expired 1h ago
  };
  const encode = (obj: unknown) =>
    Buffer.from(JSON.stringify(obj)).toString("base64url");

  const unsigned = `${encode(header)}.${encode(payload)}`;
  const sig = createHmac("sha256", JWT_SECRET)
    .update(unsigned)
    .digest("base64url");
  return `${unsigned}.${sig}`;
}

// ── Client Factories ──────────────────────────────────────────────────────────

/** Create an unauthenticated SDK client. */
export function publicClient() {
  return createClient({ baseURL: WH_URL });
}

/** Create an authenticated SDK client with a given role. */
export function authClient(
  role: string,
  extraClaims?: Record<string, unknown>,
) {
  return createClient({
    baseURL: WH_URL,
    auth: () =>
      makeJWT({ sub: `test-${role}`, role, tenant_id: "acme", ...extraClaims }),
  });
}

/** Create an admin SDK client. */
export function adminClient() {
  return authClient("admin");
}

/** Create an authenticated viewer SDK client. */
export function viewerClient() {
  return authClient("viewer");
}

// ── Wait Utilities ────────────────────────────────────────────────────────────

/**
 * Wait for the async ingest pipeline to flush to ClickHouse.
 * Default 4s covers Bento's batch window + overhead.
 */
export function waitForIngest(ms = 4000): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Poll a condition until it returns true, or timeout.
 */
export async function waitForCondition(
  fn: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
  intervalMs = 250,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`Condition not met after ${timeoutMs}ms`);
}

// ── Unique ID ─────────────────────────────────────────────────────────────────

/** Generate a unique test ID to isolate test data. */
export function testId(): string {
  return `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

// ── ClickHouse Direct Query ───────────────────────────────────────────────────

/** Query ClickHouse directly (bypassing WaveHouse) for test verification. */
export async function chQuery<T = Record<string, unknown>>(
  sql: string,
): Promise<T[]> {
  const res = await fetch(`${CH_URL}/?default_format=JSONEachRow`, {
    method: "POST",
    body: sql,
  });
  if (!res.ok) {
    throw new Error(`ClickHouse query failed: ${await res.text()}`);
  }
  const text = await res.text();
  if (!text.trim()) return [];
  return text
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
}

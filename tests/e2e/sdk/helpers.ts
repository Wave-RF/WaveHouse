/**
 * Shared test helpers for the E2E suite. The orchestrator runs one
 * WaveHouse instance with auth enabled; tests authenticate via JWTs
 * minted from the dev secret.
 */

import { createHmac } from "node:crypto";
import { createClient } from "@wavehouse/sdk";

// ── URLs ──────────────────────────────────────────────────────────────────────

export const WH_URL = process.env.WAVEHOUSE_URL ?? "http://localhost:8080";
export const CH_URL = process.env.CLICKHOUSE_URL ?? "http://localhost:8123";
export const JWT_SECRET = "sdk-dev-secret";

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
    exp: Math.floor(Date.now() / 1000) - 3600,
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

/** Unauthenticated SDK client — for negative-path / health-endpoint tests. */
export function publicClient() {
  return createClient({ baseURL: WH_URL });
}

/** Authenticated SDK client with a given role. */
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

export function adminClient() {
  return authClient("admin");
}

export function viewerClient() {
  return authClient("viewer");
}

/** Default client for data-plane tests — viewer role over auth. */
export function dataClient() {
  return viewerClient();
}

// ── Wait Utilities ────────────────────────────────────────────────────────────

/** Poll a condition until it returns true, or timeout. */
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

export function testId(): string {
  return `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

// ── ClickHouse Direct Query ───────────────────────────────────────────────────

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

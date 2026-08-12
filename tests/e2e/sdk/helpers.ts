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

export function makeJWT(claims: Record<string, unknown>, secret: string = JWT_SECRET): string {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    ...claims,
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const encode = (obj: unknown) => Buffer.from(JSON.stringify(obj)).toString("base64url");

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
  const encode = (obj: unknown) => Buffer.from(JSON.stringify(obj)).toString("base64url");

  const unsigned = `${encode(header)}.${encode(payload)}`;
  const sig = createHmac("sha256", JWT_SECRET).update(unsigned).digest("base64url");
  return `${unsigned}.${sig}`;
}

// ── Client Factories ──────────────────────────────────────────────────────────

/** Unauthenticated SDK client — for negative-path / health-endpoint tests. */
export function publicClient() {
  return createClient({ baseURL: WH_URL });
}

/** Authenticated SDK client with a given role. */
export function authClient(role: string, extraClaims?: Record<string, unknown>) {
  return createClient({
    baseURL: WH_URL,
    auth: () => makeJWT({ sub: `test-${role}`, role, tenant_id: "acme", ...extraClaims }),
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

/** Resolve after `ms`, or as soon as `signal` aborts — whichever comes first. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
  });
}

/**
 * Poll a condition until it returns true, or timeout.
 *
 * `timeoutMs` bounds the whole call, not just the gaps between polls. The
 * previous version checked the clock only on loop entry, so one slow `fn()`
 * overran the budget without bound — a 10s budget was measured running 28s,
 * past the caller's vitest `testTimeout`. Vitest then killed the test first
 * and reported `Test timed out in 20000ms`, naming neither the condition nor
 * how long the poll actually waited (#440).
 *
 * `fn` receives the budget's `AbortSignal`; pass it to anything cancellable
 * (e.g. `chQuery`) so an in-flight request is torn down at the deadline
 * instead of running on unobserved.
 */
export async function waitForCondition(
  fn: (signal: AbortSignal) => boolean | Promise<boolean>,
  timeoutMs = 10_000,
  intervalMs = 250,
): Promise<void> {
  const start = Date.now();
  const controller = new AbortController();
  const { signal } = controller;
  const budget = setTimeout(() => controller.abort(), timeoutMs);
  const expired = new Promise<false>((resolve) => {
    signal.addEventListener("abort", () => resolve(false), { once: true });
  });

  let polls = 0;
  let slowestPollMs = 0;

  try {
    while (!signal.aborted) {
      const pollStart = Date.now();
      polls += 1;
      const poll = (async () => Boolean(await fn(signal)))();
      // The race below stops awaiting `poll` at the deadline, but the call is
      // still in flight and may reject afterwards. Mark it handled so a late
      // rejection doesn't surface as an unhandled rejection in another test.
      poll.catch(() => {});

      // A rejecting `fn()` still propagates (it names its own failure, which
      // beats a generic timeout); only the deadline short-circuits the wait.
      if (await Promise.race([poll, expired])) return;
      slowestPollMs = Math.max(slowestPollMs, Date.now() - pollStart);
      if (signal.aborted) break;

      await sleep(intervalMs, signal);
    }
  } finally {
    clearTimeout(budget);
    controller.abort();
  }

  // Poll stats separate the two ways a wait can end: many fast polls means the
  // condition simply never became true (look upstream — the write never
  // landed); few slow ones means the polling itself was starved (look at the
  // machine or the query).
  throw new Error(
    `Condition not met after ${timeoutMs}ms ` +
      `(polled for ${Date.now() - start}ms; ${polls} poll(s), slowest ${slowestPollMs}ms)`,
  );
}

// ── Unique ID ─────────────────────────────────────────────────────────────────

export function testId(): string {
  return `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

// ── ClickHouse Direct Query ───────────────────────────────────────────────────

/**
 * Per-request ceiling for a direct ClickHouse call.
 *
 * Validated here rather than handed to `AbortSignal.timeout` raw: an unset-but-
 * exported `E2E_CH_QUERY_TIMEOUT_MS=` would become `0` and abort every query on
 * the next tick, and a typo'd value would become `NaN`, which throws a
 * `RangeError` naming neither the variable nor the value. Both are exactly the
 * kind of opaque failure this file exists to eliminate.
 */
const CH_QUERY_TIMEOUT_MS = ((): number => {
  const raw = process.env.E2E_CH_QUERY_TIMEOUT_MS;
  if (raw === undefined || raw.trim() === "") return 10_000;
  const parsed = Number(raw);
  // The upper bound is load-bearing, and not because the API rejects past it:
  // AbortSignal.timeout accepts any integer in [0, 4294967295], but anything
  // above 2^31-1 overflows setTimeout's int32 and Node silently clamps the
  // delay to 1ms (TimeoutOverflowWarning) — an instant abort on every query.
  // Reject those here, and 0 with them. Fractions the API does reject.
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 2_147_483_647) {
    throw new Error(
      `E2E_CH_QUERY_TIMEOUT_MS must be a whole number of milliseconds between 1 and ` +
        `2147483647; got ${JSON.stringify(raw)}`,
    );
  }
  return parsed;
})();

/** Single-line, length-capped SQL for error messages. */
const brief = (sql: string) => {
  const flat = sql.replace(/\s+/g, " ").trim();
  return flat.length > 120 ? `${flat.slice(0, 117)}...` : flat;
};

export async function chQuery<T = Record<string, unknown>>(
  sql: string,
  signal?: AbortSignal,
): Promise<T[]> {
  // Without a deadline a stalled request hangs until vitest kills the test,
  // reporting a timeout that names neither ClickHouse nor the query (#440).
  // `signal` (typically waitForCondition's budget) tears the request down
  // early when the caller has already given up.
  const ceiling = AbortSignal.timeout(CH_QUERY_TIMEOUT_MS);
  const deadline = signal ? AbortSignal.any([signal, ceiling]) : ceiling;

  try {
    const res = await fetch(`${CH_URL}/?default_format=JSONEachRow`, {
      method: "POST",
      body: sql,
      signal: deadline,
      // Don't reuse a pooled connection: undici 8.8.0-8.9.0 stalls for seconds
      // before writing a request onto a socket that has been idle a few
      // seconds. Upstream bug, not ours — nodejs/undici#5600, a scheduling
      // regression in scheduleIdleSocketValidation() (itself the fix for
      // GHSA-35p6-xmwp-9g52). Bisected on a fixed Node 22: 8.7.0 clean (22ms),
      // 8.8.0 broken (2708ms), 8.9.0 broken (7164ms), 8.10.0 clean (13ms).
      //
      // It reaches us because Node 26.x bundles 8.9.0 and this suite polls
      // seconds apart by construction (the 5s ingest linger sits between every
      // write and its first poll), so every visibility wait lands in the
      // triggering window — ~3 local runs in 5 failed, while ClickHouse itself
      // answered in ~1ms throughout (#440). CI is unaffected: .nvmrc pins
      // Node 22 (undici 6.28.0).
      //
      // DELETE THIS once the Node lines we run bundle undici >= 8.10.0; it
      // costs a connection per query (~1ms on loopback) and nothing else.
      headers: { connection: "close" },
    });
    // Read the body inside the try: aborting mid-response rejects here, not
    // at the fetch above.
    const text = await res.text();
    if (!res.ok) {
      throw new Error(`ClickHouse query failed: ${text}`);
    }
    if (!text.trim()) return [];
    return text
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
  } catch (err) {
    // Reclassify only genuine aborts. `ceiling` keeps running after fetch
    // settles, so a `!res.ok` throw or a JSON.parse failure on a slow-but-
    // successful response can reach here with `ceiling.aborted` already true —
    // rewriting those as a timeout would discard ClickHouse's own error text,
    // the opposite of what this is for. fetch rejects with TimeoutError for an
    // AbortSignal.timeout and AbortError for a controller abort.
    const isAbort =
      err instanceof Error && (err.name === "TimeoutError" || err.name === "AbortError");
    if (isAbort && ceiling.aborted) {
      throw new Error(`ClickHouse query timed out after ${CH_QUERY_TIMEOUT_MS}ms: ${brief(sql)}`);
    }
    if (isAbort && signal?.aborted) {
      throw new Error(`ClickHouse query aborted by caller: ${brief(sql)}`);
    }
    throw err;
  }
}

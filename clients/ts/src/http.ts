import { networkError, parseErrorResponse } from "./errors.js";
import type { HttpContext, WaveHouseError } from "./types.js";
import { resolveURL } from "./url.js";

interface RequestSpec {
  method: string;
  path: string;
  body?: unknown;
  /**
   * Pre-serialized request body, sent verbatim (no `JSON.stringify`) — e.g.
   * NDJSON text. Takes precedence over `body`. Must be re-sendable so retries
   * work, hence a string rather than a stream.
   */
  rawBody?: string;
  /** Override the request Content-Type (default `application/json`). */
  contentType?: string;
  params?: Record<string, string>;
  signal?: AbortSignal;
}

/** @internal */
export interface HttpResult<T> {
  data: T | null;
  error: WaveHouseError | null;
  headers: Headers;
}

/**
 * Merge configured headers underneath the SDK's own, matching names
 * case-insensitively so a caller's `authorization` can't sit alongside our
 * `Authorization` and let the server pick. Canonical casing wins, and values
 * replace rather than append — a header joined instead of replaced is a known
 * way to produce `Content-Type: application/json, image/png`.
 *
 * Shared with the SSE transport so both paths resolve `headers` the same way,
 * rather than growing a second precedence story.
 *
 * @internal
 */
export function mergeHeaders(
  base: Record<string, string>,
  extra: Record<string, string> | undefined,
): Record<string, string> {
  if (!extra) return base;
  const sdkNames = new Set(Object.keys(base).map((k) => k.toLowerCase()));
  const merged = { ...base };
  // Spelling each configured name was last stored under, so two entries
  // differing only in case replace each other here rather than surviving as
  // separate keys for `Headers` to comma-join at fetch time.
  const configured = new Map<string, string>();
  for (const [name, value] of Object.entries(extra)) {
    const lower = name.toLowerCase();
    // Skip rather than overwrite: `base` is the SDK's own, which outranks.
    if (sdkNames.has(lower)) continue;
    const prior = configured.get(lower);
    if (prior !== undefined) delete merged[prior];
    merged[name] = value;
    configured.set(lower, name);
  }
  return merged;
}

/** The documented shape for a cancelled request: a Result, never a throw. */
function abortedResult<T>(): HttpResult<T> {
  return {
    data: null,
    error: { status: 0, code: "ABORTED", message: "Request aborted", retryable: false },
    headers: new Headers(),
  };
}

/**
 * Internal fetch wrapper with auth injection, retry, backoff, and Retry-After.
 * @internal
 */
export async function request<T>(ctx: HttpContext, opts: RequestSpec): Promise<HttpResult<T>> {
  const url = resolveURL(ctx.baseURL, opts.path, opts.params).toString();
  const headers: Record<string, string> = {
    "Content-Type": opts.contentType ?? "application/json",
    Accept: "application/json",
  };

  // Serialize once so every retry attempt re-sends an identical body.
  const requestBody: string | undefined =
    opts.rawBody !== undefined
      ? opts.rawBody
      : opts.body !== undefined
        ? JSON.stringify(opts.body)
        : undefined;

  if (ctx.auth) {
    const token = await ctx.auth();
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
  }

  // After auth, so `auth` keeps ownership of Authorization, and after the
  // Content-Type/Accept this request needs.
  const finalHeaders = mergeHeaders(headers, ctx.options.headers);

  let lastError: WaveHouseError | null = null;
  const maxAttempts = ctx.options.maxRetries + 1;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      // Configured RequestInit first, so the fields the SDK controls overwrite
      // it — a supplied `method` or `body` would otherwise corrupt the request.
      const init: RequestInit = {
        ...ctx.options.fetchOptions,
        method: opts.method,
        headers: finalHeaders,
        body: requestBody,
        signal: opts.signal,
      };
      // Call the global directly when no override is configured, rather than
      // capturing it — a detached `fetch` reference is not universally safe to
      // invoke, and late binding is what lets tests swap `globalThis.fetch`.
      const doFetch = ctx.options.fetch;
      const res = doFetch ? await doFetch(url, init) : await fetch(url, init);

      if (res.ok) {
        const text = await res.text();
        const data = text ? (JSON.parse(text) as T) : (undefined as T);
        return { data, error: null, headers: res.headers };
      }

      const error = await parseErrorResponse(res);

      // 503 with Retry-After: wait the specified duration
      if (res.status === 503) {
        const retryAfter = res.headers.get("Retry-After");
        if (retryAfter && attempt < maxAttempts - 1) {
          const delay = parseInt(retryAfter, 10) * 1000 || 30_000;
          await sleep(delay, opts.signal);
          lastError = error;
          continue;
        }
      }

      // Retryable server errors (5xx)
      if (error.retryable && attempt < maxAttempts - 1) {
        await sleep(backoff(attempt), opts.signal);
        lastError = error;
        continue;
      }

      return { data: null, error, headers: res.headers };
    } catch (e) {
      // AbortError — return immediately, no retry
      if (e instanceof DOMException && e.name === "AbortError") {
        return abortedResult<T>();
      }

      lastError = networkError(e);
      if (attempt < maxAttempts - 1) {
        // This sleep is the one that runs inside the catch, so unlike the two
        // in the try above, an abort raised *by* the backoff would escape
        // `request()` as a raw DOMException instead of the documented ABORTED
        // result — and no caller wraps this, so it would reach the consumer as
        // an unhandled rejection.
        try {
          await sleep(backoff(attempt), opts.signal);
        } catch (abort) {
          if (abort instanceof DOMException && abort.name === "AbortError") {
            return abortedResult<T>();
          }
          throw abort;
        }
      }
    }
  }

  return { data: null, error: lastError!, headers: new Headers() };
}

function backoff(attempt: number): number {
  return Math.min(1000 * 2 ** attempt, 30_000);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

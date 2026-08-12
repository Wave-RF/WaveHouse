import { networkError, parseErrorResponse } from "./errors.js";
import type { HttpContext, WaveHouseError } from "./types.js";
import { resolveURL } from "./url.js";

interface RequestOptions {
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
 * Internal fetch wrapper with auth injection, retry, backoff, and Retry-After.
 * @internal
 */
export async function request<T>(ctx: HttpContext, opts: RequestOptions): Promise<HttpResult<T>> {
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

  let lastError: WaveHouseError | null = null;
  const maxAttempts = ctx.options.maxRetries + 1;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const init: RequestInit = {
        method: opts.method,
        headers,
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
        return {
          data: null,
          error: { status: 0, code: "ABORTED", message: "Request aborted", retryable: false },
          headers: new Headers(),
        };
      }

      lastError = networkError(e);
      if (attempt < maxAttempts - 1) {
        await sleep(backoff(attempt), opts.signal);
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

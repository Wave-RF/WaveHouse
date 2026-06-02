import type { Result, WaveHouseError } from "./types.js";

/** Create a WaveHouseError from an HTTP response. */
export async function parseErrorResponse(res: Response): Promise<WaveHouseError> {
  let body: Record<string, unknown> | undefined;
  try {
    body = (await res.json()) as Record<string, unknown>;
  } catch {
    body = undefined;
  }

  const message =
    typeof body?.error === "string"
      ? body.error
      : typeof body?.message === "string"
        ? body.message
        : res.statusText;

  const retryable = res.status === 503 || res.status >= 500;
  return {
    status: res.status,
    code: `HTTP_${res.status}`,
    message,
    details: body,
    retryable,
  };
}

/** Create a WaveHouseError from a network/fetch error. */
export function networkError(cause: unknown): WaveHouseError {
  const message = cause instanceof Error ? cause.message : String(cause);
  return {
    status: 0,
    code: "NETWORK_ERROR",
    message,
    retryable: true,
  };
}

/** Wrap a successful value in a Result. */
export function ok<T>(data: T): Result<T> {
  return { data, error: null };
}

/** Wrap a successful paginated value in a Result. */
export function okPage<T>(data: T, hasMore: boolean, next?: () => Promise<Result<T>>): Result<T> {
  return { data, error: null, hasMore, next };
}

/** Wrap an error in a Result. */
export function err<T = unknown>(error: WaveHouseError): Result<T> {
  return { data: null, error };
}

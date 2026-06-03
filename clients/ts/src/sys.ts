import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { Health, HttpContext, Ready, Result } from "./types.js";

/** Namespace for system health and readiness probes. */
export class SysNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /** Liveness probe — returns `{ status: "ok" }`. */
  async health(opts?: { signal?: AbortSignal }): Promise<Result<Health>> {
    const { data, error } = await request<Health>(this._ctx, {
      method: "GET",
      path: "/healthz",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Readiness probe — returns `{ status: "ready" }` or 503 with error details. */
  async ready(opts?: { signal?: AbortSignal }): Promise<Result<Ready>> {
    const { data, error } = await request<Ready>(this._ctx, {
      method: "GET",
      path: "/readyz",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }
}

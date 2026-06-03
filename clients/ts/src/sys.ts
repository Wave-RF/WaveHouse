import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { HttpContext, Ready, Result } from "./types.js";

/** Namespace for system health and readiness probes. */
export class SysNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /**
   * Liveness ping — resolves with no error when the server is reachable and
   * past boot. Hits the public, content-free `/v1/health` route (200/503, no
   * body), kept intentionally distinct from the `/livez` Kubernetes probe so
   * it stays reachable even in deployments that filter probe paths at the
   * reverse proxy. Use it to check a server is online before sending data, or
   * to pick among servers in a distributed setup.
   */
  async health(opts?: { signal?: AbortSignal }): Promise<Result<void>> {
    const { error } = await request<void>(this._ctx, {
      method: "GET",
      path: "/v1/health",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok<void>(undefined);
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

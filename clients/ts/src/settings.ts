import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { HttpContext, Result, SettingsReloadResult } from "./types.js";

/**
 * Namespace for the server's hot-reloadable settings directory. Requires the
 * admin role (the configured `admin_role`, `"admin"` by default).
 *
 * Policy (`policies.json`), roles (`roles.json`), and pipes (`pipes.json`)
 * are defined in that directory; files are the only write path. The server
 * re-adopts them on file change and on SIGHUP, and `reload()` triggers the
 * same path on demand.
 */
export class SettingsNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /**
   * Re-validate and adopt the settings directory (POST /v1/ops/settings/reload).
   * Resolves with `{ adopted: true, findings }` when adopted (warnings
   * included); a rejected directory is a 422 error whose `details` carries the
   * same `{ adopted: false, findings }` body, and the previous settings stay
   * in effect.
   */
  async reload(opts?: { signal?: AbortSignal }): Promise<Result<SettingsReloadResult>> {
    const { data, error } = await request<SettingsReloadResult>(this._ctx, {
      method: "POST",
      path: "/v1/ops/settings/reload",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }
}

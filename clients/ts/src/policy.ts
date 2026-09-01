import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { HttpContext, Policy, Result } from "./types.js";

/**
 * Namespace for reading the access control policy. Requires the admin role
 * (the configured `admin_role`, `"admin"` by default).
 *
 * The policy is defined in the server's settings directory (`policies.json`,
 * with every referenced role declared in `roles.json`); files are the only
 * write path. Edit the files and the server re-adopts them on change, on
 * SIGHUP, or on `wh.settings.reload()` (POST /v1/ops/settings/reload).
 * Validate a draft with the server's `wavehouse validate` command before
 * writing it to the directory.
 */
export class PolicyNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /** Get the current access control policy. */
  async get(opts?: { signal?: AbortSignal }): Promise<Result<Policy>> {
    const { data, error } = await request<Policy>(this._ctx, {
      method: "GET",
      path: "/v1/ops/policy",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }
}

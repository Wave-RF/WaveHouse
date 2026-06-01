import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { HttpContext, Policy, Result, ValidationResult } from "./types.js";

/** Namespace for access control policy management. Requires the admin role (the configured `admin_role`, `"admin"` by default). */
export class PolicyNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /** Get the current access control policy. */
  async get(opts?: { signal?: AbortSignal }): Promise<Result<Policy>> {
    const { data, error } = await request<Policy>(this._ctx, {
      method: "GET",
      path: "/v1/admin/policy",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Replace the entire access control policy. */
  async set(policy: Policy, opts?: { signal?: AbortSignal }): Promise<Result<void>> {
    const { error } = await request<{ ok: boolean }>(this._ctx, {
      method: "PUT",
      path: "/v1/admin/policy",
      body: policy,
      signal: opts?.signal,
    });
    if (error) return err<void>(error);
    return ok(undefined as undefined);
  }

  /** Validate a policy without applying it (dry run). */
  async validate(
    policy: Policy,
    opts?: { signal?: AbortSignal },
  ): Promise<Result<ValidationResult>> {
    const { data, error } = await request<ValidationResult>(this._ctx, {
      method: "POST",
      path: "/v1/admin/policy/validate",
      body: policy,
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }
}

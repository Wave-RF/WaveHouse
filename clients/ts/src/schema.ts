import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { HttpContext, Result, Schemas } from "./types.js";

/** Namespace for schema introspection. */
export class SchemaNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /** List all table schemas discovered from ClickHouse. */
  async list(opts?: { signal?: AbortSignal }): Promise<Result<Schemas>> {
    // The backend returns TableSchema[] — transform to Record<string, TableSchema>.
    const { data, error } = await request<unknown>(this._ctx, {
      method: "GET",
      path: "/v1/ops/schema",
      signal: opts?.signal,
    });
    if (error) return err(error);

    let schemas: Schemas;
    if (Array.isArray(data)) {
      schemas = {};
      for (const table of data) {
        if (table && typeof table === "object" && "name" in table) {
          schemas[(table as { name: string }).name] = table as Schemas[string];
        }
      }
    } else {
      schemas = data as Schemas;
    }
    return ok(schemas);
  }

  /** Force a schema refresh from ClickHouse system.columns. */
  async refresh(opts?: { signal?: AbortSignal }): Promise<Result<void>> {
    const { error } = await request<Schemas>(this._ctx, {
      method: "POST",
      path: "/v1/ops/schema/refresh",
      signal: opts?.signal,
    });
    if (error) return err<void>(error);
    return ok(undefined as undefined);
  }
}

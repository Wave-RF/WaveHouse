import { err, ok } from "./errors.js";
import { request, tenantParam } from "./http.js";
import type { StreamController } from "./stream/controller.js";
import type { DLQStats, HttpContext, OpsRequestOptions, Result, StreamOptions } from "./types.js";

type CreateStreamFn = (table: string, opts?: StreamOptions) => StreamController;

/** Namespace for Dead Letter Queue operations. */
export class DLQNamespace {
  private readonly _ctx: HttpContext;
  private readonly _createStream: CreateStreamFn;

  constructor(ctx: HttpContext, createStream: CreateStreamFn) {
    this._ctx = ctx;
    this._createStream = createStream;
  }

  /**
   * Get DLQ statistics (message counts per table) — of `opts.tenant`, the
   * default tenant without it. A tenant with no dead-letter queue is a `404`.
   */
  async list(opts?: OpsRequestOptions): Promise<Result<DLQStats>> {
    const { data, error } = await request<DLQStats>(this._ctx, {
      method: "GET",
      path: "/v1/ops/dlq/stats",
      params: tenantParam(opts),
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Get DLQ stats filtered by table name — of `opts.tenant`, the default tenant without it. */
  async table(name: string, opts?: OpsRequestOptions): Promise<Result<DLQStats>> {
    const { data, error } = await request<DLQStats>(this._ctx, {
      method: "GET",
      path: "/v1/ops/dlq/stats",
      params: { table: name, ...tenantParam(opts) },
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Subscribe to live DLQ events. */
  stream(opts?: StreamOptions): StreamController {
    return this._createStream("dlq", opts);
  }
}

import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { StreamController } from "./stream/controller.js";
import type { DLQStats, HttpContext, Result, StreamOptions } from "./types.js";

type CreateStreamFn = (table: string, opts?: StreamOptions) => StreamController;

/** Namespace for Dead Letter Queue operations. */
export class DLQNamespace {
  private readonly _ctx: HttpContext;
  private readonly _createStream: CreateStreamFn;

  constructor(ctx: HttpContext, createStream: CreateStreamFn) {
    this._ctx = ctx;
    this._createStream = createStream;
  }

  /** Get DLQ statistics (message counts per table). */
  async list(opts?: { signal?: AbortSignal }): Promise<Result<DLQStats>> {
    const { data, error } = await request<DLQStats>(this._ctx, {
      method: "GET",
      path: "/v1/ops/dlq/stats",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Get DLQ stats filtered by table name. */
  async table(name: string, opts?: { signal?: AbortSignal }): Promise<Result<DLQStats>> {
    const { data, error } = await request<DLQStats>(this._ctx, {
      method: "GET",
      path: "/v1/ops/dlq/stats",
      params: { table: name },
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

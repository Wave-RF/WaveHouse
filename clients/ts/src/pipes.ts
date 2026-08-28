import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { StreamController } from "./stream/controller.js";
import type { HttpContext, Pipe, PipeRequestOptions, Result, StreamOptions } from "./types.js";

type CreateStreamFn<Row> = (table: string, opts?: StreamOptions) => StreamController<Row>;

/** Reference to a named query pipe — PromiseLike for convenient `await`. */
export class PipeRef<Row = Record<string, unknown>> implements PromiseLike<Result<Row[]>> {
  private readonly _ctx: HttpContext;
  private readonly _name: string;
  private readonly _params?: Record<string, unknown>;
  private readonly _createStream: CreateStreamFn<Row>;

  constructor(
    ctx: HttpContext,
    name: string,
    params: Record<string, unknown> | undefined,
    createStream: CreateStreamFn<Row>,
  ) {
    this._ctx = ctx;
    this._name = name;
    this._params = params;
    this._createStream = createStream;
  }

  /**
   * Execute the pipe and return results.
   *
   * Takes only `signal` — deliberately narrower than the `RequestOptions` the
   * query builder accepts. The pipes endpoint binds the body as the pipe's
   * parameters, so there is no row cap to forward; give the pipe a `{{limit}}`
   * parameter in its SQL and pass it via `wh.pipe(name, { limit })`.
   */
  async fetch(opts?: PipeRequestOptions): Promise<Result<Row[]>> {
    const { data, error } = await request<Row[]>(this._ctx, {
      method: "POST",
      path: `/v1/pipes/${encodeURIComponent(this._name)}`,
      body: this._params ?? {},
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Subscribe to live events from the pipe's underlying query. */
  stream(opts?: StreamOptions): StreamController<Row> {
    return this._createStream(this._name, opts);
  }

  then<TResult1 = Result<Row[]>, TResult2 = never>(
    onfulfilled?: ((value: Result<Row[]>) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2> {
    return this.fetch().then(onfulfilled, onrejected);
  }
}

/**
 * Admin namespace for reading named query pipes.
 *
 * Pipes are defined in the server's settings directory (`pipes.json`, with
 * every `allowed_roles` entry declared in `roles.json`); files are the only
 * write path. Edit the files and the server re-adopts them on change, on
 * SIGHUP, or on `wh.settings.reload()` (POST /v1/ops/settings/reload).
 */
export class PipesNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /** List all registered pipes. */
  async list(opts?: { signal?: AbortSignal }): Promise<Result<Pipe[]>> {
    const { data, error } = await request<Pipe[]>(this._ctx, {
      method: "GET",
      path: "/v1/ops/pipes",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Get a single pipe definition by name. */
  async get(name: string, opts?: { signal?: AbortSignal }): Promise<Result<Pipe>> {
    const { data, error } = await request<Pipe>(this._ctx, {
      method: "GET",
      path: `/v1/ops/pipes/${encodeURIComponent(name)}`,
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }
}

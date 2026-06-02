import { err, ok } from "./errors.js";
import { request } from "./http.js";
import type { StreamController } from "./stream/controller.js";
import type { FetchOptions, HttpContext, Pipe, Result, StreamOptions } from "./types.js";

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

  /** Execute the pipe and return results. */
  async fetch(opts?: FetchOptions): Promise<Result<Row[]>> {
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

/** Admin namespace for managing named query pipes. */
export class PipesNamespace {
  private readonly _ctx: HttpContext;

  constructor(ctx: HttpContext) {
    this._ctx = ctx;
  }

  /** List all registered pipes. */
  async list(opts?: { signal?: AbortSignal }): Promise<Result<Pipe[]>> {
    const { data, error } = await request<Pipe[]>(this._ctx, {
      method: "GET",
      path: "/v1/admin/pipes",
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Get a single pipe definition by name. */
  async get(name: string, opts?: { signal?: AbortSignal }): Promise<Result<Pipe>> {
    const { data, error } = await request<Pipe>(this._ctx, {
      method: "GET",
      path: `/v1/admin/pipes/${encodeURIComponent(name)}`,
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Create or update a pipe. */
  async set(
    name: string,
    def: Omit<Pipe, "name">,
    opts?: { signal?: AbortSignal },
  ): Promise<Result<void>> {
    const { error } = await request<{ ok: boolean }>(this._ctx, {
      method: "PUT",
      path: `/v1/admin/pipes/${encodeURIComponent(name)}`,
      body: def,
      signal: opts?.signal,
    });
    if (error) return err<void>(error);
    return ok(undefined as undefined);
  }

  /** Delete a pipe by name. */
  async delete(name: string, opts?: { signal?: AbortSignal }): Promise<Result<void>> {
    const { error } = await request<{ ok: boolean }>(this._ctx, {
      method: "DELETE",
      path: `/v1/admin/pipes/${encodeURIComponent(name)}`,
      signal: opts?.signal,
    });
    if (error) return err<void>(error);
    return ok(undefined as undefined);
  }
}

import { err, ok } from "./errors.js";
import { request } from "./http.js";
import { QueryBuilder } from "./query-builder.js";
import type { StreamController } from "./stream/controller.js";
import type {
  FetchOptions,
  HttpContext,
  InsertResult,
  Result,
  StreamOptions,
  TableSchema,
} from "./types.js";

type CreateStreamFn<Row> = (table: string, opts?: StreamOptions) => StreamController<Row>;

/**
 * Reference to a table. NOT thenable — safe to pass around without triggering requests.
 * Use `.fetch()`, `.select()`, `.insert()`, `.schema()`, or `.stream()` to act on it.
 */
export class TableRef<Row = Record<string, unknown>> {
  private readonly _ctx: HttpContext;
  private readonly _table: string;
  private readonly _createStream: CreateStreamFn<Row>;

  constructor(ctx: HttpContext, table: string, createStream: CreateStreamFn<Row>) {
    this._ctx = ctx;
    this._table = table;
    this._createStream = createStream;
  }

  /** SELECT * shortcut — fetches rows with optional pagination. */
  async fetch(opts?: FetchOptions): Promise<Result<Row[]>> {
    return this.select()
      .limit(opts?.limit ?? 1000)
      .fetch(opts);
  }

  /** Start building a typed query. Returns an immutable, PromiseLike QueryBuilder. */
  select(...columns: string[]): QueryBuilder<Row> {
    return new QueryBuilder<Row>(
      this._ctx,
      {
        table: this._table,
        columns,
        aggregations: [],
        filters: [],
        groupBy: [],
        orderBy: [],
      },
      this._createStream,
    );
  }

  /** Insert one or more rows into this table. */
  async insert(
    data: Partial<Row> | Partial<Row>[],
    opts?: { signal?: AbortSignal },
  ): Promise<Result<InsertResult>> {
    if (Array.isArray(data)) {
      // TODO: Switch to NDJSON
      // FAST: Fire all HTTP requests concurrently instead of waiting for each one
      const promises = data.map((row) =>
        request<{ ok?: boolean; duplicate?: boolean }>(this._ctx, {
          method: "POST",
          path: `/v1/ingest?table=${encodeURIComponent(this._table)}`,
          body: row,
          signal: opts?.signal,
        }),
      );

      // TODO: with NDJSON/array version, need to rework how to show errors if only some/one fail
      const results = await Promise.all(promises);

      // Check if any of the concurrent requests failed
      for (const res of results) {
        if (res.error) return err(res.error);
      }
      return ok({ ok: true });
    }

    const { data: res, error } = await request<{ ok?: boolean; duplicate?: boolean }>(this._ctx, {
      method: "POST",
      path: `/v1/ingest?table=${encodeURIComponent(this._table)}`,
      body: data,
      signal: opts?.signal,
    });

    if (error) return err(error);
    const result: InsertResult = { ok: res?.ok ?? true };
    if (res?.duplicate != null) result.duplicate = res.duplicate;
    return ok(result);
  }

  /** Fetch the schema for this table. */
  async schema(opts?: { signal?: AbortSignal }): Promise<Result<TableSchema>> {
    const { data, error } = await request<TableSchema>(this._ctx, {
      method: "GET",
      path: `/v1/schema?table=${encodeURIComponent(this._table)}`,
      signal: opts?.signal,
    });
    if (error) return err(error);
    return ok(data!);
  }

  /** Subscribe to live events for this table. */
  stream(opts?: StreamOptions): StreamController<Row> {
    return this._createStream(this._table, opts);
  }
}

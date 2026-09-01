import { err, ok } from "./errors.js";
import { request } from "./http.js";
import { QueryBuilder } from "./query-builder.js";
import type { StreamController } from "./stream/controller.js";
import type {
  HttpContext,
  InsertRecordResult,
  InsertResult,
  RequestOptions,
  Result,
  StreamOptions,
  TableSchema,
} from "./types.js";

type CreateStreamFn<Row> = (table: string, opts?: StreamOptions) => StreamController<Row>;

/** Content-Type for the JSON single-object ingest path. */
const JSON_CONTENT_TYPE = "application/json";

/** Content-Type for the NDJSON batch ingest path. */
const NDJSON_CONTENT_TYPE = "application/x-ndjson";

/**
 * Accepted sources for {@link TableRef.insertNDJSON}: a string, raw bytes, a
 * Blob/File (browser file inputs), or a byte stream. Non-string sources are
 * read fully before sending.
 */
export type NDJSONSource = string | Uint8Array | Blob | ReadableStream<Uint8Array>;

/** Wire shape of the server's batch ingest response (JSON array or NDJSON). */
interface BatchIngestResponse {
  total: number;
  succeeded: number;
  failed: number;
  duplicates: number;
  results?: InsertRecordResult[];
}

/** Read an {@link NDJSONSource} fully into a UTF-8 string. */
async function ndjsonSourceToString(source: NDJSONSource): Promise<string> {
  if (typeof source === "string") return source;
  if (source instanceof Uint8Array) return new TextDecoder().decode(source);
  // Blob/File and ReadableStream<Uint8Array> are both valid fetch body inits,
  // so let Response drain them to text.
  return new Response(source).text();
}

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
  async fetch(opts?: RequestOptions): Promise<Result<Row[]>> {
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

  /**
   * Start a query that selects every column the caller's role is allowed to read
   * (the all-columns wildcard). Use instead of `.select(...)` for an explicit
   * full-row read; `.fetch()` does this implicitly.
   */
  selectAll(): QueryBuilder<Row> {
    return this.select().selectAll();
  }

  /**
   * Insert one or more rows into this table.
   *
   * A single object is sent as a JSON `POST /v1/ingest`. An **array** is
   * serialized to NDJSON (one record per line) and sent as a single
   * `application/x-ndjson` request: a bad record no longer fails or hides the
   * rest of the batch — per-record outcomes come back in the result
   * (`failed` / `results`), and `ok` is true only when every record succeeded.
   *
   * The array path sends one request regardless of size; bounded-concurrency
   * chunking of very large arrays is tracked separately (#196). For NDJSON you
   * already have (a file or stream), use {@link insertNDJSON}.
   */
  async insert(
    data: Partial<Row> | Partial<Row>[],
    opts?: { signal?: AbortSignal },
  ): Promise<Result<InsertResult>> {
    if (Array.isArray(data)) {
      if (data.length === 0) {
        // Nothing to send — succeed without a round trip.
        return ok({ ok: true, total: 0, succeeded: 0, failed: 0, duplicates: 0, results: [] });
      }
      const ndjson = data.map((row) => JSON.stringify(row)).join("\n");
      return this._sendNDJSON(ndjson, opts);
    }

    const { data: res, error } = await request<{ ok?: boolean; duplicate?: boolean }>(this._ctx, {
      method: "POST",
      path: `/v1/ingest?table=${encodeURIComponent(this._table)}`,
      body: data,
      // Ingest requires a declared format — an undeclared Content-Type is a
      // 415 — so state it here rather than leaning on the request default.
      contentType: JSON_CONTENT_TYPE,
      signal: opts?.signal,
    });

    if (error) return err(error);
    const result: InsertResult = { ok: res?.ok ?? true };
    if (res?.duplicate != null) result.duplicate = res.duplicate;
    return ok(result);
  }

  /**
   * Insert pre-formatted NDJSON (newline-delimited JSON, one record per line)
   * from a string, raw bytes, a Blob/File, or a byte stream — e.g. a `.ndjson`
   * file or a stream produced by another system. For in-memory rows, prefer
   * {@link insert}.
   *
   * Non-string sources are read fully into memory before sending, and the
   * server reads the whole body before parsing it, so the 16 MiB request cap
   * applies here exactly as it does to an array `insert` — split a larger
   * export across several calls. Returns the same per-record batch summary as
   * an array `insert`.
   */
  async insertNDJSON(
    source: NDJSONSource,
    opts?: { signal?: AbortSignal },
  ): Promise<Result<InsertResult>> {
    const ndjson = await ndjsonSourceToString(source);
    return this._sendNDJSON(ndjson, opts);
  }

  /**
   * @internal Send an NDJSON body and map the server's batch response onto an
   * {@link InsertResult}. A transport / whole-request failure surfaces as the
   * Result error arm; a processed batch (even with rejected records) surfaces
   * as the success arm with `ok = failed === 0`.
   */
  private async _sendNDJSON(
    ndjson: string,
    opts?: { signal?: AbortSignal },
  ): Promise<Result<InsertResult>> {
    const { data, error } = await request<BatchIngestResponse>(this._ctx, {
      method: "POST",
      path: `/v1/ingest?table=${encodeURIComponent(this._table)}`,
      rawBody: ndjson,
      contentType: NDJSON_CONTENT_TYPE,
      signal: opts?.signal,
    });

    if (error) return err(error);
    // A 200 with no/empty body (e.g. a record-stripping intermediary) must not
    // throw — mirror the single-object path's optimistic `?? true` and fall back
    // to a zeroed summary so the "never throws" Result contract holds.
    const r = data ?? { total: 0, succeeded: 0, failed: 0, duplicates: 0 };
    const result: InsertResult = {
      ok: r.failed === 0,
      total: r.total,
      succeeded: r.succeeded,
      failed: r.failed,
      duplicates: r.duplicates,
    };
    if (r.results && r.results.length > 0) result.results = r.results;
    return ok(result);
  }

  /** Fetch the schema for this table. */
  async schema(opts?: { signal?: AbortSignal }): Promise<Result<TableSchema>> {
    const { data, error } = await request<TableSchema>(this._ctx, {
      method: "GET",
      path: `/v1/ops/schema?table=${encodeURIComponent(this._table)}`,
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

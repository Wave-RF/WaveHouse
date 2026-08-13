import { DLQNamespace } from "./dlq.js";
import { PipeRef, PipesNamespace } from "./pipes.js";
import { PolicyNamespace } from "./policy.js";
import { SchemaNamespace } from "./schema.js";
import { sql } from "./sql.js";
import { StreamController } from "./stream/controller.js";
import { SSETransport } from "./stream/sse.js";
import { SysNamespace } from "./sys.js";
import { TableRef } from "./table.js";
import type { ClientConfig, Database, HttpContext, Result, StreamOptions } from "./types.js";

type TableName<DB> = DB extends Database ? Extract<keyof DB, string> : string;
type RowType<DB, T extends string> = DB extends Database
  ? T extends keyof DB
    ? DB[T]
    : Record<string, unknown>
  : Record<string, unknown>;

export class WaveHouseClient<DB extends Database = Database> {
  /** @internal */
  readonly _ctx: HttpContext;

  /** Schema introspection namespace. */
  readonly schema: SchemaNamespace;
  /** Access control policy namespace (admin). */
  readonly policy: PolicyNamespace;
  /** Dead Letter Queue namespace. */
  readonly dlq: DLQNamespace;
  /** System health/readiness namespace. */
  readonly sys: SysNamespace;
  /** Named query pipes admin namespace. */
  readonly pipes: PipesNamespace;

  constructor(config: ClientConfig<DB>) {
    this._ctx = {
      baseURL: config.baseURL.replace(/\/+$/, ""),
      auth: config.auth,
      options: {
        maxRetries: config.options?.maxRetries ?? 2,
        fetch: config.options?.fetch,
        headers: config.options?.headers,
        fetchOptions: config.options?.fetchOptions,
      },
    };

    this.schema = new SchemaNamespace(this._ctx);
    this.policy = new PolicyNamespace(this._ctx);
    this.dlq = new DLQNamespace(this._ctx, (table, opts) => this._createStream(table, opts));
    this.sys = new SysNamespace(this._ctx);
    this.pipes = new PipesNamespace(this._ctx);
  }

  /** Get a table reference for building queries, inserts, and streams. */
  from<T extends TableName<DB>>(table: T): TableRef<RowType<DB, T>> {
    return new TableRef<RowType<DB, T>>(this._ctx, table, (t, opts) =>
      this._createStream<RowType<DB, T>>(t, opts),
    );
  }

  /** Get a reference to a named query pipe. PromiseLike — `await` it to execute. */
  pipe<Row = Record<string, unknown>>(
    name: string,
    params?: Record<string, unknown>,
  ): PipeRef<Row> {
    return new PipeRef<Row>(this._ctx, name, params, (t, opts) => this._createStream<Row>(t, opts));
  }

  /**
   * Execute a raw SQL query against ClickHouse. Requires the admin role (the
   * configured `admin_role`, `"admin"` by default; there is no separate
   * `service` role). The endpoint proxies straight to ClickHouse's HTTP
   * interface so any ClickHouse-accepted SQL works; positional `?` param
   * binding is NOT supported — inline literals or use the structured query
   * builder for safe binding. See sql.ts for details.
   */
  sql<Row = Record<string, unknown>>(
    query: string,
    opts?: { signal?: AbortSignal },
  ): Promise<Result<Row[]>> {
    // Migration guard: the second argument used to be a positional-`?`
    // params array. TS callers get a compile-time error from the type
    // signature, but JS callers (or `any`-typed callsites) would silently
    // pass an array as `opts` and only discover the break via downstream
    // SQL errors. Throw a clear runtime error pointing at the migration.
    if (Array.isArray(opts)) {
      throw new Error(
        "[WaveHouse SDK] client.sql(sql, params) was removed. The /v1/admin/query endpoint does not accept positional `?` params. Inline literals into the SQL, or use the structured query builder (wh.from(table)…) for safe binding from user input.",
      );
    }
    return sql<Row>(this._ctx, query, opts);
  }

  /** @internal Create a stream for the given table. */
  private _createStream<T = Record<string, unknown>>(
    table: string,
    opts?: StreamOptions,
  ): StreamController<T> {
    const transport = new SSETransport<T>({
      baseURL: this._ctx.baseURL,
      table,
      since: opts?.since,
      auth: this._ctx.auth,
      fetch: this._ctx.options.fetch,
      headers: this._ctx.options.headers,
      fetchOptions: this._ctx.options.fetchOptions,
    });
    const controller = new StreamController<T>(transport);
    if (opts?.signal) controller.attachSignal(opts.signal);
    return controller;
  }
}

/** Create a new WaveHouse client instance. */
export function createClient<DB extends Database = Database>(
  config: ClientConfig<DB>,
): WaveHouseClient<DB> {
  return new WaveHouseClient(config);
}

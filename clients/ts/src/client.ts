import type { ClientConfig, Database, Result, HttpContext, StreamOptions } from './types.js';
import { TableRef } from './table.js';
import { PipeRef, PipesNamespace } from './pipes.js';
import { sql } from './sql.js';
import { SchemaNamespace } from './schema.js';
import { PolicyNamespace } from './policy.js';
import { DLQNamespace } from './dlq.js';
import { SysNamespace } from './sys.js';
import { StreamController } from './stream/controller.js';
import { SSETransport } from './stream/sse.js';
import { WSTransport } from './stream/ws.js';
import { SharedWSManager } from './stream/ws-manager.js';

type TableName<DB> = DB extends Database ? Extract<keyof DB, string> : string;
type RowType<DB, T extends string> = DB extends Database
  ? T extends keyof DB
    ? DB[T]
    : Record<string, unknown>
  : Record<string, unknown>;

export class WaveHouseClient<DB extends Database = Database> {
  /** @internal */
  readonly _ctx: HttpContext;
  private readonly _config: ClientConfig<DB>;
  /** @internal Shared WebSocket manager for multiplexed streams. */
  private _wsManager: SharedWSManager | null = null;

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
    this._config = config;
    this._ctx = {
      baseURL: config.baseURL.replace(/\/+$/, ''),
      auth: config.auth,
      options: {
        maxRetries: config.options?.maxRetries ?? 2,
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
    return new TableRef<RowType<DB, T>>(
      this._ctx,
      table,
      (t, opts) => this._createStream<RowType<DB, T>>(t, opts),
    );
  }

  /** Get a reference to a named query pipe. PromiseLike — `await` it to execute. */
  pipe<Row = Record<string, unknown>>(
    name: string,
    params?: Record<string, unknown>,
  ): PipeRef<Row> {
    return new PipeRef<Row>(this._ctx, name, params, (t, opts) =>
      this._createStream<Row>(t, opts),
    );
  }

  /** Execute a raw SQL query against ClickHouse. Requires admin/service role when policy is active. */
  sql<Row = Record<string, unknown>>(
    query: string,
    params?: unknown[],
    opts?: { signal?: AbortSignal },
  ): Promise<Result<Row[]>> {
    return sql<Row>(this._ctx, query, params, opts);
  }

  /** @internal Create a stream for the given table. */
  private _createStream<T = Record<string, unknown>>(
    table: string,
    opts?: StreamOptions,
  ): StreamController<T> {
    const transportType = opts?.transport ?? this._config.transport ?? 'auto';

    // The Smart 'auto' Logic
    let useWS = transportType === 'ws';

    if (transportType === 'auto') {
      if (this._ctx.auth ) {
        // Authenticated streams ALWAYS use WS for multiplexing
        useWS = true;
      } else if (typeof EventSource === 'undefined') {
        // Node.js environments lack native EventSource. Fallback to WS safely.
        useWS = true;
      } else {
        // Browsers/Deno/Bun have EventSource. Use SSE for public streams.
        useWS = false;
      }
    }

    if (useWS) {
      // SAFETY GUARD: Check if WebSocket actually exists before using it
      if (typeof WebSocket === 'undefined') {
        throw new Error(
          "[WaveHouse SDK] Native WebSocket is not available in this environment. " +
          "If you are using Node.js, please upgrade to Node.js 22+ or provide a global polyfill (e.g., `globalThis.WebSocket = require('ws')`)."
        );
      }

      // Use SharedWSManager for multiplexed WebSocket connections.
      if (!this._wsManager) {
        this._wsManager = new SharedWSManager(this._ctx.baseURL, this._ctx.auth);
      }
      const mgr = this._wsManager;
      const transport: import('./stream/controller.js').StreamTransport<T> = {
        onEvent: null,
        onStatus: null,
        onError: null,
        connect() {
          // Subscribe to the manager; forward events to the transport callbacks.
          const unsub = mgr.subscribe<T>(
            table,
            (event) => this.onEvent?.(event),
            (status) => this.onStatus?.(status),
            (error) => this.onError?.(error),
          );
          // Store unsubscribe so disconnect() can call it.
          (this as any)._unsub = unsub;
        },
        disconnect() {
          (this as any)._unsub?.();
        },
      };
      const controller = new StreamController<T>(transport);
      if (opts?.signal) controller.attachSignal(opts.signal);
      return controller;
    }

    // Since we know useWS is false, we must be using SSE.
    // Double-check EventSource just in case the user explicitly forced transport: 'sse' in Node.js
    if (typeof EventSource === 'undefined') {
      throw new Error(
        "[WaveHouse SDK] Native EventSource is not available in this environment. " +
        "Please use `transport: 'ws'` or provide a global polyfill (e.g., `globalThis.EventSource = require('eventsource')`)."
      );
    }

    const transport = new SSETransport<T>({
      baseURL: this._ctx.baseURL,
      table,
      since: opts?.since,
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

// Main WaveHouse client class.

import type {
  WaveHouseConfig,
  TableSchema,
  StructuredQuery,
  QueryResult,
  NamedPipe,
  LiveQueryOptions,
} from "./types.js";
import { request, queryRequest } from "./fetch.js";
import { subscribe, type SSEOptions, type SSESubscription } from "./sse.js";
import { QueryBuilder, query } from "./query-builder.js";
import { liveQuery, type LiveQueryHandle } from "./live.js";

/** WaveHouse client SDK. */
export class WaveHouseClient {
  private readonly config: WaveHouseConfig;

  constructor(config: WaveHouseConfig) {
    if (!config.baseUrl) throw new Error("baseUrl is required");
    // Strip trailing slash.
    this.config = {
      ...config,
      baseUrl: config.baseUrl.replace(/\/+$/, ""),
    };
  }

  // --- Schema Discovery ---

  /** List all known table schemas. */
  async schemas(): Promise<TableSchema[]> {
    const { data } = await request<TableSchema[]>(this.config, "/v1/schema");
    return data;
  }

  /** Get schema for a specific table. */
  async schema(table: string): Promise<TableSchema> {
    const { data } = await request<TableSchema>(
      this.config,
      `/v1/schema/${encodeURIComponent(table)}`
    );
    return data;
  }

  /** Force schema refresh. */
  async refreshSchema(): Promise<void> {
    await request(this.config, "/v1/schema/refresh", { method: "POST" });
  }

  // --- Ingestion ---

  /** Ingest a single record into a table. */
  async ingest<T extends Record<string, unknown>>(
    table: string,
    data: T
  ): Promise<void> {
    await request(this.config, `/v1/ingest/${encodeURIComponent(table)}`, {
      method: "POST",
      body: JSON.stringify(data),
    });
  }

  // --- Queries ---

  /** Execute a raw SQL query. */
  async rawQuery<T = Record<string, unknown>>(
    sql: string
  ): Promise<QueryResult<T>> {
    return queryRequest<T>(this.config, "/v1/query", { sql });
  }

  /** Execute a structured query against a table. */
  async query<T = Record<string, unknown>>(
    table: string,
    sq: StructuredQuery
  ): Promise<QueryResult<T>> {
    return queryRequest<T>(
      this.config,
      `/v1/tables/${encodeURIComponent(table)}/query`,
      sq
    );
  }

  /** Create a type-safe query builder for a table. */
  queryBuilder<TColumns extends string = string>(): QueryBuilder<TColumns> {
    return query<TColumns>();
  }

  /** Execute a query builder against a table. */
  async exec<T = Record<string, unknown>, TColumns extends string = string>(
    table: string,
    builder: QueryBuilder<TColumns>
  ): Promise<QueryResult<T>> {
    return this.query<T>(table, builder.build());
  }

  // --- Named Pipes ---

  /** Execute a named query pipe. */
  async pipe<T = Record<string, unknown>>(
    name: string,
    params?: Record<string, unknown>
  ): Promise<QueryResult<T>> {
    const qs = params
      ? "?" + new URLSearchParams(
          Object.entries(params).map(([k, v]) => [k, String(v)])
        ).toString()
      : "";
    return queryRequest<T>(
      this.config,
      `/v1/pipes/${encodeURIComponent(name)}${qs}`
    );
  }

  // --- Streaming ---

  /** Subscribe to real-time SSE events. */
  subscribe(options: SSEOptions): SSESubscription {
    return subscribe(this.config, options);
  }

  // --- Live Queries ---

  /**
   * Execute a structured query and keep it live via SSE.
   * Incrementable aggregations (count, sum, min, max) update in real-time.
   * Decomposable aggregations (avg) recompute from tracked state.
   * Non-incrementable aggregations (median, quantile) trigger periodic re-polling.
   */
  async live<T = Record<string, unknown>>(
    table: string,
    sq: StructuredQuery,
    onChange: (result: QueryResult<T>) => void,
    options?: LiveQueryOptions
  ): Promise<LiveQueryHandle<T>> {
    return liveQuery<T>(this.config, table, sq, onChange, options);
  }

  // --- Admin ---

  /** Get current access control policy. */
  async getPolicy(): Promise<unknown> {
    const { data } = await request(this.config, "/v1/admin/policy");
    return data;
  }

  /** Update access control policy. */
  async setPolicy(policy: unknown): Promise<void> {
    await request(this.config, "/v1/admin/policy", {
      method: "PUT",
      body: JSON.stringify(policy),
    });
  }

  /** List named pipes (admin). */
  async listPipes(): Promise<NamedPipe[]> {
    const { data } = await request<NamedPipe[]>(
      this.config,
      "/v1/admin/pipes"
    );
    return data;
  }

  /** Create or update a named pipe (admin). */
  async setPipe(name: string, pipe: Omit<NamedPipe, "name">): Promise<void> {
    await request(
      this.config,
      `/v1/admin/pipes/${encodeURIComponent(name)}`,
      { method: "PUT", body: JSON.stringify(pipe) }
    );
  }

  /** Delete a named pipe (admin). */
  async deletePipe(name: string): Promise<void> {
    await request(
      this.config,
      `/v1/admin/pipes/${encodeURIComponent(name)}`,
      { method: "DELETE" }
    );
  }

  // --- Health ---

  /** Check server health. */
  async health(): Promise<boolean> {
    try {
      await request(this.config, "/health");
      return true;
    } catch {
      return false;
    }
  }
}

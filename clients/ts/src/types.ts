// Core configuration and type definitions for the WaveHouse SDK.

/** Client configuration. */
export interface WaveHouseConfig {
  /** Base URL of the WaveHouse server (e.g. "http://localhost:8080"). */
  baseUrl: string;
  /** JWT token or function returning a token. */
  token?: string | (() => string | Promise<string>);
  /** Default cache TTL in seconds (overridable per-query). */
  defaultTTL?: number;
  /** Custom fetch implementation (defaults to global fetch). */
  fetch?: typeof fetch;
}

/** Column information from schema discovery. */
export interface ColumnInfo {
  name: string;
  type: string;
  is_nullable: boolean;
  has_default: boolean;
}

/** Table schema from the /v1/schema/{table} endpoint. */
export interface TableSchema {
  name: string;
  columns: ColumnInfo[];
}

/** Aggregation function call. */
export interface Aggregation {
  fn: AggregationFn;
  column: string;
  alias?: string;
}

/** Supported aggregation functions. */
export type AggregationFn =
  | "count"
  | "sum"
  | "avg"
  | "min"
  | "max"
  | "countDistinct"
  | "uniq"
  | "uniqExact"
  | "any"
  | "anyLast"
  | "argMin"
  | "argMax"
  | "groupArray"
  | "median"
  | "quantile"
  | "stddevPop"
  | "stddevSamp"
  | "varPop"
  | "varSamp";

/** Filter operator. */
export type FilterOp = "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "like";

/** Single WHERE condition. */
export interface Filter {
  column: string;
  op: FilterOp;
  value: unknown;
}

/** Sort direction. */
export interface OrderClause {
  column: string;
  dir: "asc" | "desc";
}

/** Time range constraint. */
export interface TimeRange {
  column: string;
  since: string;
  until?: string;
}

/** Structured query payload for POST /v1/tables/{table}/query. */
export interface StructuredQuery {
  columns?: string[];
  aggregations?: Aggregation[];
  filters?: Filter[];
  group_by?: string[];
  order_by?: OrderClause[];
  limit?: number;
  time_range?: TimeRange;
  cache_ttl?: number;
}

/** Query result returned by the server. */
export interface QueryResult<T = Record<string, unknown>> {
  data: T[];
  meta: {
    cached: boolean;
  };
}

/** SSE event from the stream. */
export interface StreamEvent<T = Record<string, unknown>> {
  table_name: string;
  received_timestamp: string;
  data: T;
}

/** Named query pipe definition. */
export interface NamedPipe {
  name: string;
  sql: string;
  parameters?: PipeParam[];
  description?: string;
  allowed_roles?: string[];
}

/** Named pipe parameter definition. */
export interface PipeParam {
  name: string;
  type: "string" | "number" | "boolean";
  required?: boolean;
  default?: unknown;
}

/** Aggregation classification for smart live queries. */
export type AggregationClass = "incrementable" | "decomposable" | "poll";

/** Live query options. */
export interface LiveQueryOptions {
  /** Polling interval in ms for non-incrementable aggregations. Default: 5000. */
  pollInterval?: number;
  /** SSE topic filter (e.g. "ingest.my_table"). */
  topic?: string;
}

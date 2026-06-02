// ============================================================================
// @wavehouse/sdk — Public Type Definitions
// ============================================================================

// --- Database type helper ---

/** User-provided database schema mapping table names to row types. */
export type Database = Record<string, Record<string, unknown>>;

// --- Result types ---

/** Discriminated union for all async SDK operations. Never throws. */
export type Result<T> =
  | { data: T; error: null; hasMore?: boolean; next?: () => Promise<Result<T>> }
  | { data: null; error: WaveHouseError; hasMore?: false; next?: undefined };

/** Structured error returned by all SDK operations. */
export interface WaveHouseError {
  status: number;
  code: string;
  message: string;
  details?: unknown;
  retryable: boolean;
}

// --- Filter operators ---

/** SDK filter operators (translated to backend equivalents). */
export type FilterOp = "=" | "!=" | ">" | ">=" | "<" | "<=" | "in" | "like" | "not_like";

// --- Streaming types ---

export type StreamStatus = "connecting" | "live" | "reconnecting" | "closed";

export interface StreamEvent<T = Record<string, unknown>> {
  table: string;
  timestamp: string;
  data: T;
}

export interface StreamSubscriber<T = Record<string, unknown>> {
  /** Called once with historical backfill data. */
  initial?: (result: Result<T[]>) => void;
  /** Called for each live event. */
  next: (event: StreamEvent<T>) => void;
  /** Called when stream connection status changes. */
  status?: (state: StreamStatus) => void;
  /** Called on stream errors. */
  error?: (err: WaveHouseError) => void;
}

// --- Client config ---

export interface ClientConfig<_DB extends Database = Database> {
  /** Base URL of the WaveHouse server (e.g. "http://localhost:8080"). */
  baseURL: string;
  /** Auth token provider. Omit for public/unauthenticated access. */
  auth?: () => Promise<string> | string;
  /** Additional client options. */
  options?: ClientOptions;
}

export interface ClientOptions {
  /** Maximum retry attempts for failed requests. Default: 2. */
  maxRetries?: number;
}

// --- Structured query AST (matches backend wire format) ---

export interface StructuredQuery {
  columns?: string[];
  aggregations?: Aggregation[];
  filters?: QueryFilter[];
  group_by?: string[];
  order_by?: OrderClause[];
  limit?: number;
  time_range?: TimeRange;
}

export interface Aggregation {
  fn: string;
  column: string;
  alias: string;
}

export interface QueryFilter {
  column: string;
  op: string;
  value: unknown;
}

export interface OrderClause {
  column: string;
  dir: "asc" | "desc";
}

export interface TimeRange {
  column: string;
  since: string;
  until?: string;
}

// --- Schema types ---

export interface Column {
  name: string;
  type: string;
  is_nullable: boolean;
  has_default: boolean;
}

export interface TableSchema {
  name: string;
  columns: Column[];
}

export type Schemas = Record<string, TableSchema>;

// --- Insert result ---

export interface InsertResult {
  ok: boolean;
  duplicate?: boolean;
}

// --- DLQ types ---

export interface DLQStats {
  tables: Record<string, number>;
  total: number;
}

// --- Health types ---

export interface Health {
  status: string;
}

export interface Ready {
  status: string;
  error?: string;
}

// --- Pipe types ---

export interface Pipe {
  name: string;
  sql: string;
  parameters?: ParamDef[];
  description?: string;
  allowed_roles?: string[];
}

export interface ParamDef {
  name: string;
  type: string;
  required?: boolean;
  default?: unknown;
}

// --- Policy types ---

export interface Policy {
  default_role?: string;
  tables: Record<string, TablePolicy>;
}

export interface TablePolicy {
  select?: Record<string, RolePermissions>;
  insert?: Record<string, RolePermissions>;
}

export interface RolePermissions {
  allow_columns?: string[];
  deny_columns?: string[];
  filter?: Record<string, PolicyFilter>;
  check?: Record<string, PolicyFilter>;
  allowed_aggregations?: string[];
  denied_aggregations?: string[];
  max_rows?: number;
  max_execution_time_ms?: number;
}

export interface PolicyFilter {
  _eq?: string;
  _neq?: string;
  _gt?: string;
  _lt?: string;
  _in?: string;
}

export interface ValidationResult {
  valid: boolean;
}

// --- Fetch options ---

export interface FetchOptions {
  signal?: AbortSignal;
  limit?: number;
}

// --- Stream options ---

export interface StreamOptions {
  since?: string;
  signal?: AbortSignal;
}

// --- Internal HTTP context ---

/** @internal */
export interface HttpContext {
  baseURL: string;
  auth?: () => Promise<string> | string;
  options: { maxRetries: number };
}

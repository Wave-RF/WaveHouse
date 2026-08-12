// ============================================================================
// @wavehouse/sdk — Public Type Definitions
// ============================================================================

// --- Database type helper ---

/** User-provided database schema mapping table names to row types. */
export type Database = Record<string, Record<string, unknown>>;

// --- Result types ---

/**
 * Discriminated union for all async SDK operations. Never throws for anything
 * the server returns — caller and environment errors (a non-absolute `baseURL`,
 * a missing `EventSource`, a rejecting `auth` callback) do throw.
 *
 * Discriminated on `ok`: `if (result.ok)` narrows to the success arm (and tells
 * the compiler `data` is present), while `error` is always available for
 * debugging on failure. `data`/`error` remain populated as before.
 */
export type Result<T> =
  | { ok: true; data: T; error: null; hasMore?: boolean; next?: () => Promise<Result<T>> }
  | { ok: false; data: null; error: WaveHouseError; hasMore?: false; next?: undefined };

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
  /**
   * Base URL of the WaveHouse server (e.g. "http://localhost:8080"). May include
   * a path prefix ("https://app.example.com/api/warehouse") for a WaveHouse
   * behind a backend-for-frontend (BFF), app-server route, or path-routed
   * ingress; request paths are appended to it. The proxy in front must strip
   * the prefix before forwarding.
   *
   * Must be **absolute** — scheme and host included. The transports report a
   * relative value such as "/api/warehouse" differently: a REST call rejects
   * with a `TypeError` instead of returning a `Result`, while a stream reports
   * `SSE_CONNECT_ERROR` to the subscriber's `error` callback. Build an absolute
   * one: `` `${location.origin}/api/warehouse` ``.
   */
  baseURL: string;
  /** Auth token provider. Omit for public/unauthenticated access. */
  auth?: () => Promise<string> | string;
  /** Additional client options. */
  options?: ClientOptions;
}

/**
 * A `fetch`-compatible function.
 *
 * Spelled out rather than written `typeof fetch`, which resolves differently
 * depending on whether the consumer's TypeScript `lib` includes DOM — the same
 * signature either way, but stable across configurations.
 *
 * In practice the SDK only ever calls it with a string URL, and reads `.ok` and
 * `.headers` always, `.text()` on success, and `.status`, `.statusText` plus
 * `.json()` when the response is not `ok`. A rejection becomes a
 * `NETWORK_ERROR` result and is retried with backoff — except a `DOMException`
 * named `AbortError` (what the platform `fetch` and undici throw on abort),
 * which becomes `ABORTED` and is not retried. Implementations that reject with
 * some other abort error, such as `node-fetch`, are retried instead.
 *
 * Implementations shipping their own request/response declarations (undici,
 * `node-fetch`) need a cast, since those types are separate from the ones
 * behind your global `fetch`; the narrow contract above is what makes it safe.
 */
export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface ClientOptions {
  /** Maximum retry attempts for failed requests. Default: 2. */
  maxRetries?: number;
  /**
   * HTTP implementation used for every REST request (SSE streams excluded).
   * Defaults to the global `fetch`.
   *
   * Provide one to route through a proxy, attach client certificates, add
   * middleware (logging, tracing, circuit breaking), or bypass a transport bug
   * in the runtime's bundled HTTP stack by routing through an implementation
   * you install yourself — e.g. an undici new enough to be free of
   * {@link https://github.com/nodejs/undici/issues/5600 | undici #5600}:
   *
   * ```ts
   * import { fetch as undiciFetch } from "undici"; // npm install undici — 8.10.0+
   * createClient({
   *   baseURL,
   *   options: {
   *     fetch: (url, init) => undiciFetch(url as string, init as never) as unknown as Promise<Response>,
   *   },
   * });
   * ```
   *
   * Only the SSE transport is exempt: the live connection behind `.stream()`
   * and `.liveQuery()` uses `EventSource`, which this does not replace.
   * `.liveQuery()`'s initial backfill is an ordinary request and does go
   * through your function.
   */
  fetch?: FetchLike;
  /**
   * Headers added to every REST request (SSE streams excluded) — for a
   * header-gated proxy in front of WaveHouse, such as a Cloudflare Access
   * service token.
   *
   * Matched case-insensitively, as HTTP headers are. Applied underneath the
   * SDK's own headers: `auth` still owns `Authorization`, and the
   * `Content-Type` and `Accept` a given request needs are not overridable
   * here — a global `Content-Type` that outranked the request's own is a
   * documented way to break uploads.
   */
  headers?: Record<string, string>;
  /**
   * Extra `RequestInit` fields merged into every REST request — `credentials`
   * for a cookie-authenticated origin, `mode`, `cache`, `keepalive`, or a
   * runtime-specific extension such as Next.js's `next: { tags }`.
   *
   * Fields the SDK controls (`method`, `headers`, `body`, `signal`) always
   * win, so this cannot corrupt the request itself. Non-standard fields may
   * need a cast, since `RequestInit` only declares the standard ones.
   */
  fetchOptions?: RequestInit;
}

// --- Structured query AST (matches backend wire format) ---

export interface StructuredQuery {
  /**
   * Explicit columns to project. A literal "*" is a column named "*", not a
   * wildcard. Omitting columns (with no aggregations and no select_all) selects
   * nothing — use select_all for a full-row read. Mutually exclusive with
   * select_all.
   */
  columns?: string[];
  /**
   * Request every column the caller's role may read (the all-columns wildcard,
   * expanded server-side to the allowed/denied set). Mutually exclusive with a
   * non-empty columns list.
   */
  select_all?: boolean;
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

/**
 * A per-record outcome from a batch (array / NDJSON) insert. Mirrors the
 * single-object response shape plus the record's position. Exactly one of
 * `ok` / `duplicate` / `error` is set.
 */
export interface InsertRecordResult {
  /** 1-based index of the record within the submitted batch. */
  index: number;
  /** Set when the record was validated and published. */
  ok?: boolean;
  /** Set when dedup skipped the record. */
  duplicate?: boolean;
  /** Set (with `ok`/`duplicate` absent) when the record was rejected. */
  error?: string;
}

export interface InsertResult {
  /** True for a fully successful insert: a single row, or a batch with no rejected records (`failed === 0`). */
  ok: boolean;
  /** Single insert only: set when dedup skipped the row. */
  duplicate?: boolean;
  /** Batch insert (array / NDJSON): number of records submitted. */
  total?: number;
  /** Batch insert: records validated and published. */
  succeeded?: number;
  /** Batch insert: records rejected — see `results`. */
  failed?: number;
  /** Batch insert: records skipped by dedup. */
  duplicates?: number;
  /** Batch insert: per-record outcomes, each `{index, ok|duplicate|error}` (may be truncated for very large batches; the counts stay authoritative). */
  results?: InsertRecordResult[];
}

// --- DLQ types ---

export interface DLQStats {
  tables: Record<string, number>;
  total: number;
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
  /** Caps the query result LIMIT. 0 = no limit. */
  max_rows?: number;
  /**
   * Max query execution time, enforced server-side by ClickHouse. Set it as a
   * duration string ("5s", "500ms") or a number of milliseconds; reads always
   * return the number of milliseconds.
   */
  max_execution_time?: number | string;
  /** Max rows scanned from storage, enforced server-side by ClickHouse. 0 = no limit. */
  max_rows_to_read?: number;
  /**
   * Max peak query memory, enforced server-side by ClickHouse. Set it as a size
   * string ("4GiB", "512MiB") or a number of bytes; reads always return the
   * number of bytes.
   */
  max_memory_usage?: number | string;
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

// --- Per-call request options ---

/**
 * Options for a single call, passed to `.fetch()`.
 *
 * Note that `await`ing a builder directly (`await wh.from("t").select("*")`)
 * takes no options — use the explicit `.fetch({ … })` form for those.
 */
export interface RequestOptions {
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
  // `fetch` stays optional rather than defaulted here, to keep the global late-bound.
  options: {
    maxRetries: number;
    fetch?: FetchLike;
    headers?: Record<string, string>;
    fetchOptions?: RequestInit;
  };
}

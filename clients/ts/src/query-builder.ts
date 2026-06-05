import { err, okPage } from "./errors.js";
import { request } from "./http.js";
import type { StreamTransport } from "./stream/controller.js";
import { StreamController } from "./stream/controller.js";
import { LiveQuery } from "./stream/live-query.js";
import type {
  Aggregation,
  FetchOptions,
  FilterOp,
  HttpContext,
  OrderClause,
  QueryFilter,
  Result,
  StreamOptions,
  StreamSubscriber,
  StructuredQuery,
  TimeRange,
} from "./types.js";

/** SDK operator → backend operator mapping. */
const OP_MAP: Record<FilterOp, string> = {
  "=": "eq",
  "!=": "neq",
  ">": "gt",
  ">=": "gte",
  "<": "lt",
  "<=": "lte",
  in: "in",
  like: "like",
  not_like: "not_like",
};

interface QueryState {
  table: string;
  columns: string[];
  aggregations: Aggregation[];
  filters: QueryFilter[];
  groupBy: string[];
  orderBy: OrderClause[];
  limit?: number;
  timeRange?: TimeRange;
  cacheTTL?: number;
}

type CreateStreamFn<Row> = (table: string, opts?: StreamOptions) => StreamController<Row>;

/**
 * Immutable, PromiseLike query builder.
 * Every chain method returns a new instance. `await builder` auto-executes `.fetch()`.
 */
export class QueryBuilder<Row = Record<string, unknown>> implements PromiseLike<Result<Row[]>> {
  /** @internal */
  private readonly _state: Readonly<QueryState>;
  /** @internal */
  private readonly _ctx: HttpContext;
  /** @internal */
  private readonly _createStream: CreateStreamFn<Row>;

  constructor(ctx: HttpContext, state: QueryState, createStream: CreateStreamFn<Row>) {
    this._ctx = ctx;
    this._state = Object.freeze({ ...state });
    this._createStream = createStream;
  }

  // --- Builder methods (each returns a new QueryBuilder) ---

  select(...columns: string[]): QueryBuilder<Row> {
    return this._clone({ columns: [...this._state.columns, ...columns] });
  }

  where(column: string, op: FilterOp, value: unknown): QueryBuilder<Row> {
    const filter: QueryFilter = { column, op: OP_MAP[op], value };
    return this._clone({ filters: [...this._state.filters, filter] });
  }

  count(column = "*", alias = "count"): QueryBuilder<Row> {
    return this._addAgg("count", column, alias);
  }

  sum(column: string, alias = `sum_${column}`): QueryBuilder<Row> {
    return this._addAgg("sum", column, alias);
  }

  avg(column: string, alias = `avg_${column}`): QueryBuilder<Row> {
    return this._addAgg("avg", column, alias);
  }

  min(column: string, alias = `min_${column}`): QueryBuilder<Row> {
    return this._addAgg("min", column, alias);
  }

  max(column: string, alias = `max_${column}`): QueryBuilder<Row> {
    return this._addAgg("max", column, alias);
  }

  countDistinct(column: string, alias = `count_distinct_${column}`): QueryBuilder<Row> {
    return this._addAgg("countDistinct", column, alias);
  }

  aggregate(fn: string, column: string, alias: string): QueryBuilder<Row> {
    return this._addAgg(fn, column, alias);
  }

  groupBy(...columns: string[]): QueryBuilder<Row> {
    return this._clone({ groupBy: [...this._state.groupBy, ...columns] });
  }

  orderBy(column: string, dir: "asc" | "desc" = "asc"): QueryBuilder<Row> {
    return this._clone({ orderBy: [...this._state.orderBy, { column, dir }] });
  }

  limit(n: number): QueryBuilder<Row> {
    return this._clone({ limit: n });
  }

  timeRange(column: string, since: string, until?: string): QueryBuilder<Row> {
    return this._clone({ timeRange: { column, since, until } });
  }

  cacheTTL(seconds: number): QueryBuilder<Row> {
    return this._clone({ cacheTTL: seconds });
  }

  // --- Execution ---

  /** Default row limit when none is specified. Matches backend DefaultMaxRows. */
  static readonly DEFAULT_LIMIT = 1000;

  async fetch(opts?: FetchOptions): Promise<Result<Row[]>> {
    const effectiveLimit = opts?.limit ?? this._state.limit ?? QueryBuilder.DEFAULT_LIMIT;
    const ast = this._buildAST(effectiveLimit);

    const { data, error } = await request<Row[]>(this._ctx, {
      method: "POST",
      path: `/v1/query?table=${encodeURIComponent(this._state.table)}`,
      body: ast,
      signal: opts?.signal,
    });

    if (error) return err(error);

    const rows = data!;
    const hasMore = effectiveLimit != null && rows.length >= effectiveLimit;

    // Cursor pagination needs an order column to walk. Offer `next()` only when
    // the caller set an explicit `.orderBy()`; otherwise still report `hasMore`
    // honestly from the row count, but with `next` undefined — there's no
    // deterministic cursor to build.
    if (hasMore && this._state.orderBy.length > 0) {
      const nextFn = () => this._fetchNext(rows, effectiveLimit!, opts);
      return okPage(rows, true, nextFn);
    }

    return okPage(rows, hasMore);
  }

  stream(opts?: StreamOptions): StreamController<Row> {
    const raw = this._createStream(this._state.table, opts);
    const filters = this._state.filters;
    const columns = this._state.columns;

    // If no filters or column projection, return the raw stream.
    if (filters.length === 0 && columns.length === 0) {
      return raw;
    }

    // Wrap with a filtering transport that applies AST predicates client-side.
    return new FilteredStreamController<Row>(raw, filters, columns);
  }

  /**
   * Start a live query: fetches historical data, then streams live updates.
   *
   * The subscriber's `initial()` is called once with the fetch result, then
   * `next()` fires for each live event. Events that arrived during the fetch
   * are deduplicated and flushed automatically.
   *
   * Returns a LiveQuery handle with a `.close()` method.
   */
  liveQuery(subscriber: StreamSubscriber<Row>, opts?: StreamOptions): LiveQuery<Row> {
    const stream = this.stream(opts);
    const fetchFn = () => this.fetch();
    return new LiveQuery<Row>(stream, fetchFn, subscriber, this._state.filters);
  }

  // --- PromiseLike implementation ---

  then<TResult1 = Result<Row[]>, TResult2 = never>(
    onfulfilled?: ((value: Result<Row[]>) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2> {
    return this.fetch().then(onfulfilled, onrejected);
  }

  // --- Private helpers ---

  private _clone(overrides: Partial<QueryState>): QueryBuilder<Row> {
    return new QueryBuilder(this._ctx, { ...this._state, ...overrides }, this._createStream);
  }

  private _addAgg(fn: string, column: string, alias: string): QueryBuilder<Row> {
    const agg: Aggregation = { fn, column, alias };
    return this._clone({ aggregations: [...this._state.aggregations, agg] });
  }

  private _buildAST(effectiveLimit?: number): StructuredQuery {
    const ast: StructuredQuery = {};
    if (this._state.columns.length > 0) ast.columns = this._state.columns;
    if (this._state.aggregations.length > 0) ast.aggregations = this._state.aggregations;
    if (this._state.filters.length > 0) ast.filters = this._state.filters;
    if (this._state.groupBy.length > 0) ast.group_by = this._state.groupBy;
    // No implicit default order: emit ORDER BY only when the caller set one via
    // `.orderBy()`. Sending a hardcoded column here would 500 on tables that
    // lack it (#270); pagination's `next()` is likewise gated on an explicit order.
    if (this._state.orderBy.length > 0) ast.order_by = this._state.orderBy;
    if (effectiveLimit != null) ast.limit = effectiveLimit;
    if (this._state.timeRange) ast.time_range = this._state.timeRange;
    return ast;
  }

  private async _fetchNext(
    prevRows: Row[],
    _limit: number,
    opts?: FetchOptions,
  ): Promise<Result<Row[]>> {
    // The keyset cursor walks the explicit order's first column. `fetch()` only
    // attaches `next()` when `.orderBy()` was set, so a missing cursor means
    // there's nothing to paginate by — return an empty terminal page rather than
    // guess a column the table may not have (#270).
    const cursor = this._state.orderBy[0];
    if (cursor == null) return okPage([] as unknown as Row[], false);
    const { column: orderCol, dir: orderDir } = cursor;

    const lastRow = prevRows[prevRows.length - 1] as Record<string, unknown>;
    const lastValue = lastRow?.[orderCol];

    if (lastValue === undefined) return okPage([] as unknown as Row[], false);

    const cursorOp = orderDir === "desc" ? "lt" : "gt";
    const cursorFilter: QueryFilter = { column: orderCol, op: cursorOp, value: lastValue };
    const nextBuilder = this._clone({
      filters: [...this._state.filters, cursorFilter],
      orderBy: this._state.orderBy,
    });

    return nextBuilder.fetch(opts);
  }
}

// ============================================================================
// Client-side stream filtering
// ============================================================================

/**
 * Wraps a StreamController, applying client-side filters and column projection
 * before delivering events to subscribers.
 *
 * Lifecycle: by the time we reach this constructor, `inner` has already been
 * built (and is opening its own SSE connection). The outer transport's
 * `connect()` doesn't open a new connection — it bridges `inner`'s events
 * through a filter into the outer subscriber set. On `disconnect()`, closing
 * `inner` is what actually tears down the SSE socket; the subscription we
 * register on `inner` is left to be garbage-collected with it, which is why
 * we don't bother saving the unsubscribe handle.
 * @internal
 */
class FilteredStreamController<T = Record<string, unknown>> extends StreamController<T> {
  constructor(inner: StreamController<T>, filters: QueryFilter[], columns: string[]) {
    const transport: StreamTransport<T> = {
      onEvent: null,
      onStatus: null,
      onError: null,
      connect() {
        inner.subscribe({
          next: (event) => {
            if (!matchesFilters(event.data as Record<string, unknown>, filters)) {
              return;
            }
            const projected =
              columns.length > 0
                ? {
                    ...event,
                    data: projectColumns(event.data as Record<string, unknown>, columns) as T,
                  }
                : event;
            this.onEvent?.(projected);
          },
          status: (s) => this.onStatus?.(s),
          error: (e) => this.onError?.(e),
        });
      },
      disconnect() {
        inner.close();
      },
    };
    super(transport);
  }
}

/** @internal Evaluate all filters against a data row. All must pass (AND). */
function matchesFilters(row: Record<string, unknown>, filters: QueryFilter[]): boolean {
  for (const f of filters) {
    const val = row[f.column];
    if (!evaluateFilter(val, f.op, f.value)) return false;
  }
  return true;
}

/** @internal Evaluate a single filter predicate. */
function evaluateFilter(actual: unknown, op: string, expected: unknown): boolean {
  switch (op) {
    case "eq":
      return actual === expected;
    case "neq":
      return actual !== expected;
    case "gt":
      return compareOrdered(actual, expected, (a, b) => a > b);
    case "gte":
      return compareOrdered(actual, expected, (a, b) => a >= b);
    case "lt":
      return compareOrdered(actual, expected, (a, b) => a < b);
    case "lte":
      return compareOrdered(actual, expected, (a, b) => a <= b);
    case "in":
      return Array.isArray(expected) && expected.includes(actual);
    case "like": {
      if (typeof actual !== "string" || typeof expected !== "string") return false;
      // Convert SQL LIKE pattern to regex: % → .*, _ → .
      const escaped = expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const pattern = escaped.replace(/%/g, ".*").replace(/_/g, ".");
      return new RegExp(`^${pattern}$`, "i").test(actual);
    }
    case "not_like": {
      if (typeof actual !== "string" || typeof expected !== "string") return false;
      const escaped = expected.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const pattern = escaped.replace(/%/g, ".*").replace(/_/g, ".");
      return !new RegExp(`^${pattern}$`, "i").test(actual);
    }
    default:
      return true; // unknown op — pass through
  }
}

/**
 * @internal Apply an ordered comparison only when both sides are the same
 * comparable primitive (number-vs-number or string-vs-string — strings are
 * lexicographic, which is correct for ISO-8601 timestamps). Mismatched or
 * unsupported types return false instead of relying on JS coercion.
 */
function compareOrdered(
  actual: unknown,
  expected: unknown,
  cmp: (a: number, b: number) => boolean,
): boolean {
  if (typeof actual === "number" && typeof expected === "number") {
    return cmp(actual, expected);
  }
  if (typeof actual === "string" && typeof expected === "string") {
    // `>`/`<` on strings is lexicographic; reuse the same comparator by
    // casting through `as unknown as number` — the runtime operator works
    // identically on strings.
    return cmp(actual as unknown as number, expected as unknown as number);
  }
  return false;
}

/** @internal Project a row to only the specified columns. */
function projectColumns(row: Record<string, unknown>, columns: string[]): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const col of columns) {
    if (col in row) result[col] = row[col];
  }
  return result;
}

// Type-safe query builder for structured queries.

import type {
  StructuredQuery,
  Aggregation,
  AggregationFn,
  Filter,
  FilterOp,
  OrderClause,
  TimeRange,
} from "./types.js";

/**
 * Fluent query builder that produces a StructuredQuery payload.
 * Supports both generated (type-safe) and generic usage.
 */
export class QueryBuilder<TColumns extends string = string> {
  private q: StructuredQuery = {};

  /** Select specific columns. */
  select(...columns: TColumns[]): this {
    this.q.columns = [...(this.q.columns ?? []), ...columns];
    return this;
  }

  /** Add an aggregation. */
  aggregate(fn: AggregationFn, column: TColumns | "*", alias?: string): this {
    const agg: Aggregation = { fn, column, alias: alias ?? `${fn}_${column}` };
    this.q.aggregations = [...(this.q.aggregations ?? []), agg];
    return this;
  }

  /** Shorthand aggregation methods. */
  count(column: TColumns | "*" = "*", alias?: string): this {
    return this.aggregate("count", column, alias ?? "count");
  }
  sum(column: TColumns, alias?: string): this {
    return this.aggregate("sum", column, alias);
  }
  avg(column: TColumns, alias?: string): this {
    return this.aggregate("avg", column, alias);
  }
  min(column: TColumns, alias?: string): this {
    return this.aggregate("min", column, alias);
  }
  max(column: TColumns, alias?: string): this {
    return this.aggregate("max", column, alias);
  }
  countDistinct(column: TColumns, alias?: string): this {
    return this.aggregate("countDistinct", column, alias);
  }

  /** Add a WHERE filter. */
  where(column: TColumns, op: FilterOp, value: unknown): this {
    const f: Filter = { column, op, value };
    this.q.filters = [...(this.q.filters ?? []), f];
    return this;
  }

  /** Shorthand: column = value. */
  eq(column: TColumns, value: unknown): this {
    return this.where(column, "eq", value);
  }
  neq(column: TColumns, value: unknown): this {
    return this.where(column, "neq", value);
  }
  gt(column: TColumns, value: unknown): this {
    return this.where(column, "gt", value);
  }
  gte(column: TColumns, value: unknown): this {
    return this.where(column, "gte", value);
  }
  lt(column: TColumns, value: unknown): this {
    return this.where(column, "lt", value);
  }
  lte(column: TColumns, value: unknown): this {
    return this.where(column, "lte", value);
  }
  in(column: TColumns, values: unknown[]): this {
    return this.where(column, "in", values);
  }
  like(column: TColumns, pattern: string): this {
    return this.where(column, "like", pattern);
  }

  /** Add GROUP BY columns. */
  groupBy(...columns: TColumns[]): this {
    this.q.group_by = [...(this.q.group_by ?? []), ...columns];
    return this;
  }

  /** Add ORDER BY clause. */
  orderBy(column: TColumns | string, dir: "asc" | "desc" = "asc"): this {
    const o: OrderClause = { column, dir };
    this.q.order_by = [...(this.q.order_by ?? []), o];
    return this;
  }

  /** Set result limit. */
  limit(n: number): this {
    this.q.limit = n;
    return this;
  }

  /** Set time range constraint. */
  timeRange(column: TColumns, since: string, until?: string): this {
    this.q.time_range = { column, since, until } as TimeRange;
    return this;
  }

  /** Override cache TTL for this query (seconds). */
  cacheTTL(seconds: number): this {
    this.q.cache_ttl = seconds;
    return this;
  }

  /** Build the structured query payload. */
  build(): StructuredQuery {
    return { ...this.q };
  }
}

/** Create a new query builder. Use the generic type parameter for column type safety. */
export function query<TColumns extends string = string>(): QueryBuilder<TColumns> {
  return new QueryBuilder<TColumns>();
}

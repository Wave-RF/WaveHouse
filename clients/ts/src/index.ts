// @wavehouse/sdk — TypeScript client for WaveHouse.
// Re-exports all public APIs.

export { WaveHouseClient } from "./client.js";
export { QueryBuilder, query } from "./query-builder.js";
export { subscribe } from "./sse.js";
export { liveQuery } from "./live.js";
export { WaveHouseError } from "./fetch.js";
export {
  classifyAggregation,
  updateAggregation,
  requiresPolling,
} from "./aggregations.js";

export type {
  WaveHouseConfig,
  TableSchema,
  ColumnInfo,
  StructuredQuery,
  Aggregation,
  AggregationFn,
  Filter,
  FilterOp,
  OrderClause,
  TimeRange,
  QueryResult,
  StreamEvent,
  NamedPipe,
  PipeParam,
  AggregationClass,
  LiveQueryOptions,
} from "./types.js";

export type { SSEOptions, SSESubscription } from "./sse.js";
export type { LiveQueryHandle } from "./live.js";

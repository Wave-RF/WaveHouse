// ============================================================================
// @wavehouse/sdk — Public API
// ============================================================================

// --- Main entry point ---
export { createClient, WaveHouseClient } from "./client.js";
export { DLQNamespace } from "./dlq.js";
export { PipeRef, PipesNamespace } from "./pipes.js";
export { PolicyNamespace } from "./policy.js";
export { QueryBuilder } from "./query-builder.js";
export { SchemaNamespace } from "./schema.js";
export { StreamController } from "./stream/controller.js";
export { LiveQuery } from "./stream/live-query.js";
export { SysNamespace } from "./sys.js";
// --- Core classes ---
export { TableRef } from "./table.js";

// --- Types ---
export type {
  Aggregation,
  // Config
  ClientConfig,
  ClientOptions,
  // Schema
  Column,
  // Database & result
  Database,
  // DLQ
  DLQStats,
  FetchOptions,
  // Query
  FilterOp,
  // Health
  Health,
  // Ingest
  InsertResult,
  OrderClause,
  ParamDef,
  // Pipes
  Pipe,
  // Policy
  Policy,
  PolicyFilter,
  QueryFilter,
  Ready,
  Result,
  RolePermissions,
  Schemas,
  StreamEvent,
  StreamOptions,
  // Streaming
  StreamStatus,
  StreamSubscriber,
  StructuredQuery,
  TablePolicy,
  TableSchema,
  TimeRange,
  ValidationResult,
  WaveHouseError,
} from "./types.js";

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
export { SettingsNamespace } from "./settings.js";
export { StreamController } from "./stream/controller.js";
export { LiveQuery } from "./stream/live-query.js";
export { SysNamespace } from "./sys.js";
export type { NDJSONSource } from "./table.js";
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
  // HTTP
  FetchLike,
  // Query
  FilterOp,
  InsertPermissions,
  // Ingest
  InsertRecordResult,
  InsertResult,
  OrderClause,
  ParamDef,
  // Pipes
  Pipe,
  PipeRequestOptions,
  // Policy
  Policy,
  PolicyFilter,
  QueryFilter,
  RequestOptions,
  Result,
  RolePermissions,
  Schemas,
  SelectPermissions,
  SettingsFinding,
  SettingsReloadResult,
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

// ============================================================================
// @wavehouse/sdk — Public API
// ============================================================================

// --- Main entry point ---
export { createClient, WaveHouseClient } from './client.js';

// --- Core classes ---
export { TableRef } from './table.js';
export { QueryBuilder } from './query-builder.js';
export { PipeRef, PipesNamespace } from './pipes.js';
export { SchemaNamespace } from './schema.js';
export { PolicyNamespace } from './policy.js';
export { DLQNamespace } from './dlq.js';
export { SysNamespace } from './sys.js';
export { StreamController } from './stream/controller.js';
export { SharedWSManager } from './stream/ws-manager.js';
export { LiveQuery } from './stream/live-query.js';

// --- Types ---
export type {
  // Database & result
  Database,
  Result,
  WaveHouseError,
  // Config
  ClientConfig,
  ClientOptions,
  // Query
  FilterOp,
  FetchOptions,
  StructuredQuery,
  Aggregation,
  QueryFilter,
  OrderClause,
  TimeRange,
  // Schema
  Column,
  TableSchema,
  Schemas,
  // Ingest
  InsertResult,
  // Streaming
  StreamStatus,
  StreamEvent,
  StreamSubscriber,
  StreamOptions,
  // DLQ
  DLQStats,
  // Health
  Health,
  Ready,
  // Pipes
  Pipe,
  ParamDef,
  // Policy
  Policy,
  TablePolicy,
  RolePermissions,
  PolicyFilter,
  ValidationResult,
} from './types.js';

# TypeScript SDK

`@wavehouse/sdk` is a zero-dependency TypeScript client for WaveHouse with type-safe queries, real-time streaming, and schema-generated types.

## Installation

```bash
npm install @wavehouse/sdk
```

## Quick Start

```typescript
import { WaveHouseClient } from '@wavehouse/sdk';

const wh = new WaveHouseClient({
  baseUrl: 'http://localhost:8080',
  token: 'your-jwt-token',
});

// Ingest data
await wh.ingest('clicks', { page: '/home', button: 'signup', score: 42.5 });

// Raw query
const result = await wh.rawQuery('SELECT page, count() FROM clicks GROUP BY page');

// Structured query (type-safe)
const data = await wh.query('clicks', {
  columns: ['page'],
  aggregations: [{ fn: 'count', column: '*', alias: 'total' }],
  group_by: ['page'],
  order_by: [{ column: 'total', dir: 'desc' }],
  limit: 10,
});
```

## Query Builder

Fluent, type-safe query builder with IDE autocompletion:

```typescript
const result = await wh.queryBuilder('clicks')
  .select('page', 'button')
  .count('*', 'views')
  .where('score', 'gt', 10)
  .groupBy('page', 'button')
  .orderBy('views', 'desc')
  .limit(50)
  .timeRange('received_timestamp', '1h')
  .exec();
```

## Named Pipes

Execute pre-defined server-side queries with parameters:

```typescript
const result = await wh.pipe('top_pages', {
  start_date: '2024-01-01',
  limit: 100,
});
```

## Real-Time SSE Streaming

Subscribe to live events from one or more tables:

```typescript
const sub = wh.subscribe({
  tables: ['clicks'],
  onEvent: (event) => console.log(event),
  onError: (err) => console.error(err),
  lastEventId: 'last-seen-id', // optional gap-fill
});

// Later: close the connection
sub.close();
```

## Live Queries

Combines a cached initial query with real-time SSE updates. Aggregations are updated intelligently:

- **Incrementable** (count, sum, min, max): Updated in real-time from each event.
- **Decomposable** (avg): Maintained via internal sum/count tracking.
- **Poll-required** (median, quantile, countDistinct): Re-fetched periodically.

```typescript
const live = wh.live('clicks', {
  columns: ['page'],
  aggregations: [
    { fn: 'count', column: '*', alias: 'views' },
    { fn: 'avg', column: 'score', alias: 'avg_score' },
  ],
  group_by: ['page'],
  timeRange: { column: 'received_timestamp', since: '1h' },
  onUpdate: (rows) => renderDashboard(rows),
  pollInterval: 30000, // for non-incrementable aggregations
});

// Stop live updates
live.stop();
```

## Codegen (Type Generation)

Generate TypeScript types from your ClickHouse schemas:

```bash
npx wavehouse codegen --url http://localhost:8080 --out src/wavehouse.gen.ts
```

This generates:

- `{Table}Row` interfaces with correct TS types for each ClickHouse column
- `{Table}Columns` string literal union types
- `WaveHouseTables` table name union
- `TableRowMap` and `TableColumnMap` mapped types
- `typedQuery()` factory with compile-time type checking

### Usage with Generated Types

```typescript
import { typedQuery } from './wavehouse.gen';

// Compile-time error if column name is wrong
const q = typedQuery('clicks')
  .select('page', 'button')
  .where('score', 'gt', 10)
  .build();
```

### ClickHouse → TypeScript Type Mapping

| ClickHouse | TypeScript |
| ---------- | ---------- |
| String, FixedString, UUID, IPv4, IPv6, Enum* | `string` |
| UInt*, Int*, Float*, Decimal* | `number` |
| Bool | `boolean` |
| Date, Date32, DateTime, DateTime64 | `string` |
| Array(*) | `T[]` |
| Nullable(*) | `T \| null` |
| Map(K, V), Tuple, JSON | `Record<string, unknown>` |

## Authentication

Provide a static token or a function that returns one (useful for token rotation):

```typescript
const wh = new WaveHouseClient({
  baseUrl: 'http://localhost:8080',
  token: async () => await getAccessToken(),
});
```

## Admin API

```typescript
// Access control policy
const policy = await wh.getPolicy();
await wh.setPolicy(updatedPolicy);

// Named pipes
const pipes = await wh.listPipes();
await wh.setPipe('top_pages', pipeDefinition);
await wh.deletePipe('old_pipe');
```

## API Reference

### `WaveHouseClient`

| Method | Description |
| ------ | ----------- |
| `schemas()` | List all table schemas |
| `schema(table)` | Get schema for a specific table |
| `refreshSchema()` | Trigger server-side schema refresh |
| `ingest(table, data)` | Ingest a row into a table |
| `rawQuery(sql)` | Execute raw SQL |
| `query(table, query)` | Structured query |
| `queryBuilder(table)` | Create a fluent query builder |
| `pipe(name, params?)` | Execute a named pipe |
| `subscribe(opts)` | SSE real-time subscription |
| `live(table, opts)` | Live query with smart aggregation updates |
| `getPolicy()` | Get access control policy |
| `setPolicy(policy)` | Update access control policy |
| `listPipes()` | List named pipes |
| `setPipe(name, pipe)` | Create/update a named pipe |
| `deletePipe(name)` | Delete a named pipe |
| `health()` | Health check |

# API Reference

## Authentication

Authentication is **optional** and controlled by `auth.enabled` (env: `WH_AUTH_ENABLED`). When disabled (default), all `/v1/*` endpoints are open. When enabled, every request to `/v1/*` must include a valid JWT Bearer token:

```text
Authorization: Bearer <token>
```

The JWT must use HMAC signing (HS256/HS384/HS512) or be validated via a JWKS endpoint (configured via `auth.jwks_url`).

For WebSocket and SSE connections where custom headers are not possible, you can pass the token as a query parameter:

```text
GET /v1/stream/ws?token=<jwt>
```

The `Authorization` header takes precedence when both are provided. The `token` query parameter is stripped from the URL after extraction.

When `auth.dev_mode` is enabled, all requests are treated as admin with no JWT validation — useful for development.

### Roles & Access Control

WaveHouse extracts the role from a configurable JWT claim path (`auth.role_claim`, default: `role`). Built-in role handling:

- **`admin`** / **`service`** — Full access to all tables, raw SQL, and admin endpoints.
- **Other roles** — Access determined by the access control policy (see Admin endpoints below).

Policies support Hasura-style row-level and column-level permissions with JWT claim templating (e.g., `{{ jwt.app_metadata.tenant_id }}`).

## Response Format

### Error Responses

Every error response from `/v1/*` endpoints carries a JSON body and the following headers:

```text
Content-Type: application/json
X-Content-Type-Options: nosniff
```

The body is always an object with a single `error` field describing the failure:

```json
{"error": "invalid json"}
```

This applies to validation errors (4xx), permission denials (403), not-found responses (404), and server errors (5xx). Strict clients can rely on the `Content-Type` to branch on response shape — historically some error paths defaulted to `text/plain` because they were emitted via `http.Error`; that has been corrected so every JSON body carries the matching media type.

The per-endpoint error tables below list the error bodies you can expect for each status code; the `Content-Type` and `X-Content-Type-Options` headers above apply uniformly and are not repeated.

## Endpoints

### `GET /health` — Liveness Probe

Returns `200 OK` if the process is running. No authentication required.

**Response:**

```json
{"status": "ok"}
```

---

### `GET /ready` — Readiness Probe

Returns `200 OK` if the process is running and ClickHouse is reachable. No authentication required.

**Response (ready):**

```json
{"status": "ready"}
```

**Response (not ready):**

```json
{"status": "not ready", "error": "connection refused"}
```

Status code: `503 Service Unavailable`

---

### `POST /v1/ingest/{table}` — Ingest Data

Accepts a flat JSON object, validates it against the ClickHouse schema for `{table}`, and publishes it to the message queue. Returns immediately — ClickHouse insertion happens asynchronously via the batch consumer.

The `{table}` URL parameter must match a table that exists in ClickHouse. WaveHouse discovers table schemas on startup and refreshes them periodically.

**Request:**

```json
{
  "url": "https://example.com/dashboard",
  "user_name": "Alice",
  "verified": true,
  "score": 42.5
}
```

The body is a **flat JSON object** whose keys must match column names in the target ClickHouse table. Values must be type-compatible (see schema validation below).

**Schema Validation:**

- Unknown fields (not in the ClickHouse schema) are rejected.
- Type mismatches are rejected (e.g., sending a string for a `Float64` column).
- Missing required columns (non-nullable without a default) are rejected.
- Null values for non-nullable columns are rejected.
- Type compatibility: `String`/`DateTime`/`UUID`/`Enum`/`IPv*` accept JSON strings; `Int*`/`Float*`/`Decimal` accept JSON numbers; `Bool` accepts JSON booleans or numbers; `Array` accepts JSON arrays; `Map`/`Tuple` accept JSON objects.
- `Nullable()` and `LowCardinality()` wrappers are handled transparently.

**Response (accepted):**

```json
{"ok": true}
```

**Response (duplicate):** *(only when dedup is enabled)*

```json
{"duplicate": true}
```

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"invalid json"}` | Malformed request body |
| 400 | `{"error":"unknown table: ..."}` | Table not found in ClickHouse schema |
| 400 | `{"error":"validation failed: ..."}` | Schema validation errors (unknown fields, type mismatches, missing required columns) |
| 500 | `{"error":"dedupe failed"}` | Deduplication backend error |
| 500 | `{"error":"publish failed"}` | Message queue error |
| 503 | `{"error":"service unavailable"}` | NATS JetStream stream full (backpressure). Response includes `Retry-After: 30` header. |

**curl example:**

```bash
curl -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
```

---

### `POST /v1/query` — Query ClickHouse

Executes a SQL query directly against ClickHouse. Results are cached using a two-tier cache (L1 in-memory + L2 Redis in clustered mode). UUID and DateTime columns are converted to string representations in the response.

**Request:**

```json
{
  "sql": "SELECT * FROM clicks LIMIT 10",
  "params": []
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `sql` | string | Yes | SQL query executed directly against ClickHouse. |
| `params` | array | No | Query parameters (bound positionally). |

**Response:**

```json
[
  {
    "page": "/home",
    "button": "signup",
    "score": 42.5,
    "received_timestamp": "2026-03-24T12:00:00.123Z"
  }
]
```

The response includes a cache header:

- `X-Cache: HIT` — served from cache
- `X-Cache: MISS` — fetched from ClickHouse

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"invalid json"}` | Malformed request body |
| 400 | `{"error":"missing sql"}` | Missing `sql` field |
| 500 | `{"error":"..."}` | ClickHouse query error |

**curl example:**

```bash
curl -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
```

---

### `POST /v1/tables/{table}/query` — Structured Query

Executes a type-safe structured query against a table. The query AST is validated against the schema and converted to parameterized SQL. Permissions from the access control policy are enforced (column filtering, row-level security, aggregation restrictions).

**Request:**

```json
{
  "columns": ["page", "button"],
  "aggregations": [
    {"fn": "count", "column": "*", "alias": "total"}
  ],
  "filters": [
    {"column": "score", "op": "gt", "value": 10}
  ],
  "group_by": ["page"],
  "order_by": [{"column": "total", "dir": "desc"}],
  "limit": 100,
  "time_range": {
    "column": "received_timestamp",
    "since": "1h",
    "until": ""
  },
  "cache_ttl": 60
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `columns` | string[] | No | Columns to SELECT. |
| `aggregations` | object[] | No | Aggregation functions (`fn`, `column`, `alias`). |
| `filters` | object[] | No | WHERE conditions (`column`, `op`, `value`). Ops: eq, neq, gt, gte, lt, lte, in, like. |
| `group_by` | string[] | No | GROUP BY columns. |
| `order_by` | object[] | No | ORDER BY clauses (`column`, `dir`). |
| `limit` | int | No | Max rows. |
| `time_range` | object | No | Time window (`column`, `since`, `until`). `since` can be relative ("1h", "30m") or RFC3339. |
| `cache_ttl` | int | No | Override default cache TTL (seconds). |

**Response:**

Same format as `/v1/query` with `X-Cache` header.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"..."}` | Schema validation error (unknown column, bad aggregation) |
| 403 | `{"error":"forbidden"}` | Role lacks select permission on table |
| 403 | `{"error":"column \"x\" not allowed"}` | Column denied by policy |
| 403 | `{"error":"aggregation \"x\" not allowed"}` | Aggregation fn denied by policy |
| 404 | `{"error":"unknown table: x"}` | Table not found |

---

### `GET/POST /v1/pipes/{name}` — Execute Named Pipe

Executes a pre-defined named query (pipe) with parameter binding. Parameters can be supplied via query string and/or JSON body. Results are cached.

**Query Parameters:** Any key matching a pipe parameter name.

**POST Body (optional):**

```json
{
  "start_date": "2024-01-01",
  "limit": 100
}
```

**Response:**

Same format as `/v1/query` with `X-Cache` header.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 404 | `{"error":"pipe not found"}` | Pipe name not registered |
| 403 | `{"error":"forbidden"}` | Role not in pipe's `allowed_roles` |
| 400 | `{"error":"missing required parameter: x"}` | Required parameter not supplied |

---

### `GET /v1/stream/sse` — Server-Sent Events Stream

Opens a persistent SSE connection for real-time event streaming. Supports historical gap-fill from NATS JetStream using `DeliverByStartTime`.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `topic` | string | `ingest.>` | NATS subject to subscribe to. Supports NATS wildcards: `*` matches one token, `>` matches one or more remaining tokens. |
| `since` | string | — | RFC 3339 timestamp. If provided, replays historical events from NATS before switching to live streaming. |
| `token` | string | — | JWT token (alternative to `Authorization` header, useful for `EventSource`). Stripped from URL after extraction. |

**Headers:**

| Header | Description |
| ------ | ----------- |
| `Last-Event-ID` | RFC 3339 timestamp of the last received event. If present, overrides the `since` query parameter for automatic reconnection (standard `EventSource` behavior). |

**Response:** SSE stream (`text/event-stream`). Each event includes an `id:` field set to the event's `received_timestamp`.

```text
id: 2026-03-24T12:00:00.123Z
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:00.123Z","data":{"page":"/home","button":"signup"}}

id: 2026-03-24T12:00:01.456Z
data: {"table_name":"page_views","received_timestamp":"2026-03-24T12:00:01.456Z","data":{"url":"/dashboard"}}
```

**Note:** When access control policies are active, streamed events are filtered per the caller's role — denied columns are removed and tables without select permission are skipped.

**curl example:**

```bash
# All tables
curl -N http://localhost:8080/v1/stream/sse

# Specific table
curl -N "http://localhost:8080/v1/stream/sse?topic=ingest.clicks"

# With gap-fill
curl -N "http://localhost:8080/v1/stream/sse?since=2026-03-24T11:00:00Z"
```

---

### `GET /v1/stream/ws` — WebSocket Stream

Opens a WebSocket connection for real-time event streaming. Supports in-band multiplexing — a single WebSocket can subscribe to multiple topics dynamically.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `topic` | string | — | Optional initial topic to subscribe to (backward compatible). If omitted, the client must send subscribe commands. |
| `since` | string | — | RFC 3339 timestamp for gap-fill on the initial `?topic=` subscription. |
| `token` | string | — | JWT token (alternative to `Authorization` header). Stripped from URL after extraction. |

**In-band commands (client → server):**

After connecting, send JSON commands to manage subscriptions:

```json
{"action": "subscribe", "topic": "ingest.clicks"}
{"action": "subscribe", "topic": "ingest.page_views"}
{"action": "unsubscribe", "topic": "ingest.clicks"}
```

**Outbound message format (server → client):**

Each message is wrapped in an envelope with the topic:

```json
{"topic": "ingest.clicks", "data": {"table_name": "clicks", "received_timestamp": "...", "data": {...}}}
```

**JavaScript example:**

```javascript
const ws = new WebSocket("ws://localhost:8080/v1/stream/ws?token=<jwt>");
ws.onopen = () => {
  ws.send(JSON.stringify({ action: "subscribe", topic: "ingest.clicks" }));
  ws.send(JSON.stringify({ action: "subscribe", topic: "ingest.page_views" }));
};
ws.onmessage = (event) => {
  const { topic, data } = JSON.parse(event.data);
  console.log(`[${topic}]`, data.table_name, data.data);
};
```

---

### `GET /v1/schema` — List All Table Schemas

Returns all discovered ClickHouse table schemas.

**Response:**

```json
{
  "clicks": {
    "columns": [
      {"name": "page", "type": "String", "is_nullable": false, "has_default": false},
      {"name": "button", "type": "String", "is_nullable": false, "has_default": false},
      {"name": "score", "type": "Float64", "is_nullable": true, "has_default": false}
    ]
  }
}
```

---

### `GET /v1/schema/{table}` — Get Table Schema

Returns the schema for a specific table.

**Response:**

```json
{
  "columns": [
    {"name": "page", "type": "String", "is_nullable": false, "has_default": false},
    {"name": "button", "type": "String", "is_nullable": false, "has_default": false}
  ]
}
```

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 404 | `{"error":"table not found"}` | Table not in discovered schemas |

---

### `POST /v1/schema/refresh` — Refresh Schemas

Triggers an immediate re-discovery of ClickHouse table schemas.

**Response:**

```json
{"ok": true}
```

---

### `GET /v1/dlq/stats` — DLQ Statistics

Returns per-table message counts in the Dead Letter Queue.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `table` | string | — | Filter stats to a specific table name (e.g., `?table=clicks` returns only `dlq.clicks`). |

**Response:**

```json
{
  "tables": {
    "dlq.clicks": 3,
    "dlq.page_views": 1
  },
  "total": 4
}
```

---

### Admin Endpoints

Admin endpoints require the `admin` or `service` role when auth is enabled.

#### `GET /v1/admin/policy` — Get Access Control Policy

Returns the current access control policy.

#### `PUT /v1/admin/policy` — Update Access Control Policy

Replaces the entire access control policy. Validated before saving.

**Request:**

```json
{
  "default_role": "viewer",
  "tables": {
    "clicks": {
      "select": {
        "viewer": {
          "allow_columns": ["page", "button", "timestamp"],
          "filter": {
            "tenant_id": {"_eq": "{{ jwt.app_metadata.tenant_id }}"}
          }
        },
        "admin": {
          "allow_columns": ["*"]
        }
      },
      "insert": {
        "viewer": {
          "allow_columns": ["page", "button", "user_id", "tenant_id"],
          "check": {
            "user_id": {"_eq": "{{ jwt.sub }}"},
            "tenant_id": {"_eq": "{{ jwt.app_metadata.tenant_id }}"}
          }
        }
      }
    }
  }
}
```

#### `POST /v1/admin/policy/validate` — Validate Policy (Dry Run)

Validates a policy without saving it. Returns `{"valid": true}` or an error.

#### `GET /v1/admin/pipes` — List Named Pipes

Returns all registered named query pipes.

#### `GET /v1/admin/pipes/{name}` — Get Named Pipe

Returns a specific named pipe definition.

#### `PUT /v1/admin/pipes/{name}` — Create/Update Named Pipe

```json
{
  "sql": "SELECT page, count() as views FROM clicks WHERE received_timestamp >= {{start_date}} GROUP BY page LIMIT {{limit}}",
  "parameters": [
    {"name": "start_date", "type": "string", "required": true},
    {"name": "limit", "type": "number", "required": false, "default": 100}
  ],
  "description": "Top pages by view count",
  "allowed_roles": ["viewer", "admin"]
}
```

#### `DELETE /v1/admin/pipes/{name}` — Delete Named Pipe

## Event Message Format

### Internal Wire Format (NATS)

The message format used on NATS JetStream between ingest and the batch consumer:

```json
{
  "table_name": "clicks",
  "received_timestamp": "2026-03-24T12:00:00.123456789Z",
  "data": {
    "page": "/home",
    "button": "signup",
    "score": 42.5
  }
}
```

| Field | Type | Description |
| ----- | ---- | ----------- |
| `table_name` | string | Target ClickHouse table (from URL). |
| `received_timestamp` | string | RFC 3339 nano timestamp when WaveHouse received the event. |
| `data` | object | The original flat JSON body. |

### Client-Facing Format (SSE/WebSocket)

Same as the wire format — events are passed through directly:

```json
{
  "table_name": "clicks",
  "received_timestamp": "2026-03-24T12:00:00.123456789Z",
  "data": {
    "page": "/home",
    "button": "signup",
    "score": 42.5
  }
}
```

## Dead Letter Queue (DLQ)

When batch inserts to ClickHouse fail (e.g., type errors, connection issues), the failed events are published to the DLQ NATS stream (`WAVEHOUSE_DLQ`) under subjects `dlq.{table}`. This prevents infinite retry loops — failed messages are ACKed from the main stream and moved to the DLQ for inspection.

Use `GET /v1/dlq/stats` to monitor DLQ depth.

## Generating a JWT for Testing

Only needed when `auth.enabled` is `true`. By default, authentication is disabled.

```bash
# Using jwt-cli (https://github.com/mike-engel/jwt-cli):
jwt encode --secret "change-me-in-production" '{"exp": 9999999999}'

# Export for use with curl:
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"exp": 9999999999}')
curl -X POST http://localhost:8080/v1/ingest/clicks \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home"}'
```

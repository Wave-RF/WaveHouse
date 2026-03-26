# API Reference

## Authentication

Authentication is **optional** and controlled by `auth.enabled` (env: `WH_AUTH_ENABLED`). When disabled (default), all `/v1/*` endpoints are open. When enabled, every request to `/v1/*` must include a valid JWT Bearer token:

```text
Authorization: Bearer <token>
```

The JWT must use HMAC signing (HS256/HS384/HS512).

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

### `GET /v1/stream/sse` — Server-Sent Events Stream

Opens a persistent SSE connection for real-time event streaming. Supports historical gap-fill from NATS JetStream using `DeliverByStartTime`.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `topic` | string | `ingest.>` | NATS subject to subscribe to. Use `ingest.{table}` to filter by table. |
| `since` | string | — | RFC 3339 timestamp. If provided, replays historical events from NATS before switching to live streaming. |

**Response:** SSE stream (`text/event-stream`).

```text
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:00.123Z","data":{"page":"/home","button":"signup"}}

data: {"table_name":"page_views","received_timestamp":"2026-03-24T12:00:01.456Z","data":{"url":"/dashboard"}}
```

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

Opens a WebSocket connection for real-time event streaming. Same semantics as SSE but over WebSocket.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `topic` | string | `ingest.>` | NATS subject to subscribe to. |
| `since` | string | — | RFC 3339 timestamp for gap-fill. |

**JavaScript example:**

```javascript
const ws = new WebSocket("ws://localhost:8080/v1/stream/ws?topic=ingest.clicks");
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log("Event:", data.table_name, data.data);
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

**Response:**

```json
{
  "subjects": {
    "dlq.clicks": 3,
    "dlq.page_views": 0
  }
}
```

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

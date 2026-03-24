# API Reference

All API endpoints (except health checks) require a valid JWT Bearer token with a `tenant_id` claim.

## Authentication

BeachHouse uses JWT Bearer tokens for authentication. Every request to `/v1/*` endpoints must include an `Authorization` header:

```text
Authorization: Bearer <token>
```

The JWT must:

- Use HMAC signing (HS256/HS384/HS512).
- Contain a `tenant_id` claim (string, **must be a valid UUID**). This is used for row-level security — tenants can only access their own data.

The `tenant_id` is **exclusively** sourced from the JWT. It is never read from request bodies, query parameters, or other headers. If the `tenant_id` claim is not a valid UUID, the request is rejected with `403 Forbidden`.

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

### `POST /v1/ingest` — Ingest Events

Accepts a JSON event, deduplicates it, flattens the data into typed maps, and publishes it to the message queue. Returns immediately — ClickHouse insertion happens asynchronously.

**Request:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-03-24T12:00:00Z",
  "type": "page_view",
  "data": {
    "url": "https://example.com/dashboard",
    "user": {
      "name": "Alice",
      "verified": true
    },
    "score": 42.5,
    "tags": ["analytics", "beta"]
  }
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `id` | string (UUID) | Yes | Unique event ID (must be a valid UUID). Used for deduplication (scoped to tenant). |
| `type` | string | Yes | Event type label. Acts as a pseudo-table in ClickHouse for partitioning and ordering. |
| `data` | object | Yes | Arbitrary nested JSON payload. Flattened into three typed maps (strings, numbers, booleans). |
| `timestamp` | string | No | RFC 3339 timestamp. Defaults to server receive time if omitted. |

**Flattening example:** The `data` field above is split into three typed maps:

*String data (`str_data`):*

| Key | Value |
| --- | ----- |
| `url` | `https://example.com/dashboard` |
| `user.name` | `Alice` |
| `tags.0` | `analytics` |
| `tags.1` | `beta` |

*Numeric data (`num_data`):*

| Key | Value |
| --- | ----- |
| `score` | `42.5` |

*Boolean data (`bool_data`):*

| Key | Value |
| --- | ----- |
| `user.verified` | `true` |

> **Note:** Null values in the JSON payload are skipped and not stored in any map.

**Response (accepted):**

```json
{"ok": true}
```

**Response (duplicate):**

```json
{"duplicate": true}
```

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"invalid json"}` | Malformed request body |
| 400 | `{"error":"missing id"}` | Missing `id` field |
| 400 | `{"error":"id must be a valid UUID"}` | `id` is not a valid UUID |
| 400 | `{"error":"missing type"}` | Missing `type` field |
| 400 | `{"error":"missing data"}` | Missing `data` field |
| 400 | `{"error":"flatten failed"}` | Invalid data structure |
| 403 | `{"error":"no tenant"}` | Missing tenant_id in JWT |
| 403 | `{"error":"tenant_id must be a valid UUID"}` | tenant_id claim is not a valid UUID |
| 500 | `{"error":"dedupe failed"}` | Deduplication backend error |
| 500 | `{"error":"publish failed"}` | Message queue error |
| 503 | `{"error":"service unavailable"}` | NATS JetStream stream full (backpressure). Response includes `Retry-After: 30` header. |

**curl example:**

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "type": "click",
    "data": {"button": "signup", "page": "/home"}
  }'
```

---

### `POST /v1/query` — Query ClickHouse

Executes a SQL query against ClickHouse with automatic row-level security (tenant isolation). The query is automatically wrapped in a CTE that pre-filters the `events` table by tenant — **you do not need to include a `WHERE tenant_id = ?` clause**. Results are cached using a two-tier cache (L1 in-memory + L2 Redis in clustered mode).

The response automatically:

- Unflattens typed map columns (`str_data`, `num_data`, `bool_data`) into a nested `data` JSON object.
- Strips the `tenant_id` column from output.
- Converts UUID and DateTime columns to string representations.

**Request:**

```json
{
  "sql": "SELECT event_id, type, timestamp, str_data, num_data, bool_data FROM events LIMIT 10",
  "params": []
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `sql` | string | Yes | SQL query. References to `events` table are automatically tenant-scoped via CTE injection. Do **not** include `WHERE tenant_id = ?`. |
| `params` | array | No | Additional query parameters (reserved for future use). |

**Response:**

```json
[
  {
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "type": "page_view",
    "timestamp": "2026-03-24T12:00:00Z",
    "data": {
      "url": "https://example.com/dashboard",
      "user": {"name": "Alice", "verified": true},
      "score": 42.5
    }
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
| 403 | `{"error":"no tenant"}` | Missing tenant_id in JWT |
| 500 | `{"error":"..."}` | ClickHouse query error |

**curl example:**

```bash
curl -X POST http://localhost:8080/v1/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM events LIMIT 10"}'
```

> **Note:** The query `SELECT * FROM events LIMIT 10` is automatically rewritten to:
>
> ```sql
> WITH __tenant_events AS (SELECT * FROM events WHERE tenant_id = toUUID(?)) SELECT * FROM __tenant_events LIMIT 10
> ```
>
> This ensures tenants can only ever access their own data.

---

### `GET /v1/stream/sse` — Server-Sent Events Stream

Opens a persistent SSE connection for real-time event streaming. Supports historical gap-fill from NATS JetStream using `DeliverByStartTime`.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `topic` | string | `ingest.events` | Topic to subscribe to. |
| `since` | string | — | RFC 3339 timestamp. If provided, creates an ephemeral NATS consumer starting at this time and sends historical events before switching to live streaming. The gap window is configurable via `BH_MQ_GAP_WINDOW_MINUTES`. |

**Response:** SSE stream (`text/event-stream`).

Events are automatically transformed to a client-friendly format: typed map columns are unflattened into a nested `data` object, and internal fields (`tenant_id`) are stripped.

```text
data: {"event_id":"550e8400-e29b-41d4-a716-446655440000","timestamp":"2026-03-24T12:00:00Z","received_timestamp":"2026-03-24T12:00:00.123Z","type":"click","data":{"button":"signup"}}

data: {"event_id":"660e8400-e29b-41d4-a716-446655440001","timestamp":"2026-03-24T12:00:01Z","received_timestamp":"2026-03-24T12:00:01.456Z","type":"page_view","data":{"url":"/home"}}
```

**curl example:**

```bash
curl -N http://localhost:8080/v1/stream/sse \
  -H "Authorization: Bearer $TOKEN"

# With gap-fill:
curl -N "http://localhost:8080/v1/stream/sse?since=2026-03-24T11:00:00Z" \
  -H "Authorization: Bearer $TOKEN"
```

---

### `GET /v1/stream/ws` — WebSocket Stream

Opens a WebSocket connection for real-time event streaming. Same semantics as SSE but over the WebSocket protocol.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `topic` | string | `ingest.events` | Topic to subscribe to. |
| `since` | string | — | RFC 3339 timestamp. Gap-fill via NATS JetStream `DeliverByStartTime` (same as SSE). |

**Messages:** Each message is a JSON-encoded event in the same client-friendly format as SSE `data:` payloads (unflattened `data`, no `tenant_id`).

**JavaScript example:**

```javascript
const ws = new WebSocket("ws://localhost:8080/v1/stream/ws?topic=ingest.events");
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log("Event:", data.event_id, data.type);
};
```

## Event Message Formats

### Internal Wire Format (MQ)

The internal message format used on NATS JetStream between ingest and buffer consumer:

```json
{
  "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_id": "660e8400-e29b-41d4-a716-446655440001",
  "received_timestamp": "2026-03-24T12:00:00.123456789Z",
  "timestamp": "2026-03-24T12:00:00Z",
  "type": "page_view",
  "str_data": {"url": "https://example.com", "user.name": "Alice"},
  "num_data": {"score": 42.5},
  "bool_data": {"user.verified": true}
}
```

| Field | Type | Description |
| ----- | ---- | ----------- |
| `tenant_id` | string (UUID) | Tenant identifier (from JWT). |
| `event_id` | string (UUID) | Unique event identifier (from client). |
| `received_timestamp` | string | RFC 3339 nano timestamp when BeachHouse received the event. |
| `timestamp` | string | RFC 3339 timestamp (client-supplied, or defaults to received time). |
| `type` | string | Event type label (acts as pseudo-table). |
| `str_data` | object | Flattened string values from `data`. |
| `num_data` | object | Flattened numeric values from `data`. |
| `bool_data` | object | Flattened boolean values from `data`. |

### Client-Facing Format (SSE/WebSocket)

Events sent to SSE and WebSocket clients are transformed: typed maps are unflattened into nested `data`, and `tenant_id` is stripped:

```json
{
  "event_id": "660e8400-e29b-41d4-a716-446655440001",
  "received_timestamp": "2026-03-24T12:00:00.123456789Z",
  "timestamp": "2026-03-24T12:00:00Z",
  "type": "page_view",
  "data": {
    "url": "https://example.com",
    "user": {"name": "Alice", "verified": true},
    "score": 42.5
  }
}
```

## Generating a JWT for Testing

For local development with the default config (`jwt_secret: change-me-in-production`), generate a test token:

The `tenant_id` claim **must be a valid UUID**.

```bash
# Using the jwt-cli tool (https://github.com/mike-engel/jwt-cli):
jwt encode --secret "change-me-in-production" '{"tenant_id": "550e8400-e29b-41d4-a716-446655440000", "exp": 9999999999}'

# Or using Python:
python3 -c "
import jwt, time
token = jwt.encode({'tenant_id': '550e8400-e29b-41d4-a716-446655440000', 'exp': int(time.time()) + 86400}, 'change-me-in-production', algorithm='HS256')
print(token)
"

# Export it for easy use with curl:
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"tenant_id": "550e8400-e29b-41d4-a716-446655440000", "exp": 9999999999}')
```

Then use it with any endpoint:

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "660e8400-e29b-41d4-a716-446655440001", "type": "click", "data": {"page": "/home"}}'
```

# API Reference

All API endpoints (except health checks) require a valid JWT Bearer token with a `tenant_id` claim.

## Authentication

BeachHouse uses JWT Bearer tokens for authentication. Every request to `/v1/*` endpoints must include an `Authorization` header:

```text
Authorization: Bearer <token>
```

The JWT must:

- Use HMAC signing (HS256/HS384/HS512).
- Contain a `tenant_id` claim (string). This is used for row-level security — tenants can only access their own data.

The `tenant_id` is **exclusively** sourced from the JWT. It is never read from request bodies, query parameters, or other headers.

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

Accepts a JSON event, deduplicates it, flattens the data to EAV format, and publishes it to the message queue. Returns immediately — ClickHouse insertion happens asynchronously.

**Request:**

```json
{
  "id": "evt-abc-123",
  "timestamp": "2026-03-24T12:00:00Z",
  "type": "page_view",
  "data": {
    "url": "https://example.com/dashboard",
    "user": {
      "name": "Alice",
      "plan": "enterprise"
    },
    "tags": ["analytics", "beta"]
  }
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `id` | string | Yes | Unique event ID. Used for deduplication (scoped to tenant). |
| `timestamp` | string | No | RFC 3339 timestamp. Defaults to server time if omitted. |
| `type` | string | No | Event type label for categorization. |
| `data` | object | No | Arbitrary nested JSON. Flattened to dot-notation EAV pairs. |

**Flattening example:** The `data` field above becomes:

| Key | Value |
| --- | ----- |
| `url` | `https://example.com/dashboard` |
| `user.name` | `Alice` |
| `user.plan` | `enterprise` |
| `tags.0` | `analytics` |
| `tags.1` | `beta` |

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
| 400 | `{"error":"flatten failed"}` | Invalid data structure |
| 403 | `{"error":"no tenant"}` | Missing tenant_id in JWT |
| 500 | `{"error":"dedupe failed"}` | Deduplication backend error |
| 500 | `{"error":"publish failed"}` | Message queue error |
| 503 | `{"error":"service unavailable"}` | NATS JetStream stream full (backpressure). Response includes `Retry-After: 30` header. |

**curl example:**

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt-001",
    "type": "click",
    "data": {"button": "signup", "page": "/home"}
  }'
```

---

### `POST /v1/query` — Query ClickHouse

Executes a SQL query against ClickHouse with automatic row-level security (tenant isolation). Results are cached using a two-tier cache (L1 in-memory + L2 Redis in clustered mode).

**Request:**

```json
{
  "sql": "SELECT type, count() FROM events WHERE tenant_id = ? GROUP BY type",
  "params": []
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `sql` | string | Yes | SQL query. The first `?` placeholder is automatically bound to the caller's tenant_id. |
| `params` | array | No | Additional query parameters (reserved for future use). |

**Response:**

```json
[
  {"type": "page_view", "count()": 1542},
  {"type": "click", "count()": 873}
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
  -d '{"sql": "SELECT * FROM events WHERE tenant_id = ? LIMIT 10"}'
```

---

### `GET /v1/stream/sse` — Server-Sent Events Stream

Opens a persistent SSE connection for real-time event streaming. Supports historical gap-fill from NATS JetStream using `DeliverByStartTime`.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `topic` | string | `ingest.events` | Topic to subscribe to. |
| `since` | string | — | RFC 3339 timestamp. If provided, creates an ephemeral NATS consumer starting at this time and sends historical events before switching to live streaming. The gap window is configurable via `BH_MQ_GAP_WINDOW_MINUTES`. |

**Response:** SSE stream (`text/event-stream`).

```text
data: {"tenant_id":"t1","event_id":"evt-001","timestamp":"2026-03-24T12:00:00Z","type":"click","map_keys":["button"],"map_values":["signup"]}

data: {"tenant_id":"t1","event_id":"evt-002","timestamp":"2026-03-24T12:00:01Z","type":"page_view","map_keys":["url"],"map_values":["/home"]}
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

**Messages:** Each message is a JSON-encoded event (same format as SSE `data:` payloads).

**JavaScript example:**

```javascript
const ws = new WebSocket("ws://localhost:8080/v1/stream/ws?topic=ingest.events");
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log("Event:", data.event_id, data.type);
};
```

## Event Message Format

All events flowing through the system (MQ, SSE, WebSocket) share this format:

```json
{
  "tenant_id": "tenant-abc",
  "event_id": "evt-001",
  "timestamp": "2026-03-24T12:00:00Z",
  "type": "page_view",
  "map_keys": ["url", "user.name", "user.plan"],
  "map_values": ["https://example.com", "Alice", "enterprise"]
}
```

| Field | Type | Description |
| ----- | ---- | ----------- |
| `tenant_id` | string | Tenant identifier (from JWT). |
| `event_id` | string | Unique event identifier (from client). |
| `timestamp` | string | RFC 3339 timestamp. |
| `type` | string | Event type label. |
| `map_keys` | string[] | Flattened dot-notation keys from `data`. |
| `map_values` | string[] | Corresponding string values. |

## Generating a JWT for Testing

For local development with the default config (`jwt_secret: change-me-in-production`), generate a test token:

```bash
# Using the jwt-cli tool (https://github.com/mike-engel/jwt-cli):
jwt encode --secret "change-me-in-production" '{"tenant_id": "test-tenant", "exp": 9999999999}'

# Or using Python:
python3 -c "
import jwt, time
token = jwt.encode({'tenant_id': 'test-tenant', 'exp': int(time.time()) + 86400}, 'change-me-in-production', algorithm='HS256')
print(token)
"

# Or use this pre-generated long-lived token for local dev (secret: change-me-in-production):
# Export it for easy use with curl:
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"tenant_id": "test-tenant", "exp": 9999999999}')
```

Then use it with any endpoint:

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id": "test-1", "type": "click", "data": {"page": "/home"}}'
```

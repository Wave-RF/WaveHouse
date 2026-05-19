---
title: "API Reference"
description: "All endpoints, authentication, request/response formats for the WaveHouse API."
sidebar:
  order: 5
---

Every HTTP endpoint WaveHouse exposes — ingest, query, streaming, schema introspection, and admin — with request/response formats, error codes, and examples. Authentication is optional and controlled by `auth.enabled`; see [Configuration](configuration.md#authentication) for the full auth config surface.

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

Error responses from WaveHouse carry a JSON body and the following headers:

```text
Content-Type: application/json
X-Content-Type-Options: nosniff
```

The body is always a JSON object that includes an `error` field describing the failure:

```json
{"error": "invalid json"}
```

Some endpoints (notably `/ready`) include additional fields like `status` alongside `error`; the guarantee is that an `error` field is always present and parseable.

This contract holds for:

- Handler-emitted errors — validation (4xx), permission denials (403), not-found (404), backend errors (5xx).
- Router-level **404 Not Found** when the URL does not match any registered route.
- Router-level **405 Method Not Allowed** when the URL matches a route but the method is not registered.
- Server-level **500 Internal Server Error** when a handler panics — recovered, logged with stack, and reported to the client as JSON **when the handler has not yet committed any response headers or body bytes**.

Historically some error paths defaulted to `text/plain` because they were emitted via `http.Error` or chi's default handlers; those paths now route through a shared `writeJSONError` helper so strict clients can branch on `Content-Type` consistently.

The per-endpoint error tables below list the bodies you can expect for each status code; the `Content-Type` and `X-Content-Type-Options` headers above apply uniformly and are not repeated.

> **Caveat — WebSocket upgrade failures.** A failed WebSocket upgrade on `GET /v1/stream/ws` (e.g., wrong method, missing `Upgrade` header, rejected `Origin`) is rejected at the HTTP/1.1 → WebSocket negotiation layer by the `coder/websocket` library, which writes a `text/plain` body. This sits below the application's error contract — clients negotiating a WebSocket should branch on the upgrade-handshake outcome rather than the response body.
>
> **Caveat — streaming / partial-write responses.** For SSE, streaming endpoints, WebSockets after upgrade, or any handler that has already started writing the response, a later panic is recovered and logged server-side but no JSON 500 body is written — once headers are flushed, replacing them would corrupt the stream. Clients consuming streams should treat connection termination or truncated output as the failure signal in those cases.

## Endpoints

### `GET /health` — Liveness Probe

Returns `200 OK` once the gateway has discovered ClickHouse table schemas at least once. Returns `503 Service Unavailable` with a diagnostic body while the boot-time schema discovery retry loop is still running (ClickHouse unreachable, target database missing, etc.). No authentication required.

**Response (ready):**

```json
{"status": "ok"}
```

**Response (boot-degraded):**

```json
{"status": "degraded", "error": "schema discovery: dial tcp 127.0.0.1:9000: connect: connection refused"}
```

Status code: `503 Service Unavailable`

The boot-degraded response lets an operator `curl /health` to learn why the gateway isn't ready to serve traffic yet, instead of grepping a restart-loop log. The binary is bound on `:8080` and serves diagnostics, but is not yet accepting ingest/query traffic. Schema discovery retries with exponential backoff (2s → 60s); once a Refresh succeeds, `/health` flips to `200` and stays there for the rest of the process lifetime — transient ClickHouse blips after that point are reflected in `/ready`, not `/health`.

---

### `GET /ready` — Readiness Probe

Returns `200 OK` if the process is fully booted (schema discovery complete) and ClickHouse is currently reachable. Returns `503 Service Unavailable` otherwise. No authentication required.

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

> **Insert-only.** The ingest pipeline accepts only inserts. All other mutations — `DELETE`, `UPDATE`, `TRUNCATE`, `DROP`, `ALTER`, `REPLACE`, etc. — must be issued through [`POST /v1/admin/query`](#post-v1adminquery--query-clickhouse), which is restricted to the `admin` / `service` role (the same gate as the rest of `/v1/admin/*`).
>
> The policy engine authorizes mutations by inspecting the columns being written. That works for inserts but not for predicate-driven mutations like `DELETE … WHERE` — there's no way to prove the predicate matches only rows the caller is allowed to touch. Routing those statements through the admin-gated raw-SQL surface keeps the policy contract honest.

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
| 400 | `{"error":"payload cannot contain reserved field 'received_timestamp'"}` | Payload contains a reserved field |
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

### `POST /v1/admin/query` — Query ClickHouse

Executes a SQL statement directly against ClickHouse. **WaveHouse proxies the SQL string verbatim to ClickHouse's HTTP interface** — any statement ClickHouse accepts works, including multi-statement input (`SELECT 1; TRUNCATE t`), arbitrary DDL/DML/SYSTEM verbs, and inline FORMAT directives. Read queries return a JSON array of result rows; mutations/DDL return HTTP 200 with `[]` on success. DateTime columns are ISO-8601 formatted via the upstream `date_time_output_format=iso` setting; other types are returned as ClickHouse renders them under `FORMAT JSON`.

This endpoint **does not cache, does not singleflight, and emits `Cache-Control: no-store`** — every request goes straight to ClickHouse, mutation or read, and downstream HTTP caches are explicitly told not to store the response. Raw SQL is an admin escape hatch with infrequent, ad-hoc traffic, so the L1/singleflight machinery would only add complexity without a real hit-rate win. Use [`POST /v1/tables/{table}/query`](#post-v1tablestablequery--structured-query) or [`GET/POST /v1/pipes/{name}`](#getpost-v1pipesname--execute-named-pipe) for the cached read paths (dashboards, high-QPS clients, etc.) — both share an in-process L1 (Ristretto) with singleflight coalescing.

> **Admin / service only.** The route is mounted under `/v1/admin/*`, which sits behind a `RequireRole("admin","service")` gate: callers whose JWT resolves to either role may use it (or any caller when `auth.enabled` is false, the dev/test posture). Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story — but the role set matches the rest of `/v1/admin/*` rather than carving out a separate tighter gate, because service tokens already hold admin-scoped powers across that whole tree (policy CRUD, pipes CRUD, log-level) and the inconsistency would be a footgun without a real authorization win. The normal surfaces for non-admin callers are `POST /v1/ingest/{table}` for writes, `POST /v1/tables/{table}/query` for structured reads, and `GET/POST /v1/pipes/{name}` for pre-defined queries — none of which expose raw SQL.

`/v1/admin/query` is the only sanctioned surface for non-insert mutations (the ingest pipeline is insert-only). Granting raw-SQL access to a non-admin role via the policy engine is no longer supported: authenticate as `admin` / `service`. (The endpoint moved here from `/v1/query` as part of the admin-lockdown change — the `policy.RolePermissions.raw_sql` field has been removed and `/v1/query` now returns 404.)

**Request:**

```json
{
  "sql": "SELECT * FROM clicks LIMIT 10"
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `sql` | string | Yes | SQL forwarded verbatim to ClickHouse's HTTP interface. |

> **No positional `?` param binding.** The earlier handler accepted a `params` array bound to `?` placeholders; the HTTP proxy doesn't, because ClickHouse's HTTP interface has a different (named) param model. Inline literals into the SQL, or use ClickHouse's native `{name:Type}` syntax (e.g. `WHERE id = {id:UInt32}`) — admins who need server-evaluated binding can extend the request with custom query-string params later, but for now the contract is "send the SQL, get rows back." For safe binding from user-supplied inputs, use the structured query endpoint (`POST /v1/tables/{table}/query`) — that's its job.

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

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"invalid json"}` | Malformed request body |
| 400 | `{"error":"missing sql"}` | Missing `sql` field |
| 401 | `{"error":"unauthorized"}` | `auth.enabled=true` and the request carries no role claim |
| 403 | `{"error":"forbidden"}` | Caller's role is not `admin` or `service` |
| 500 | `{"error":"<ClickHouse error message>"}` | ClickHouse rejected the statement. The body carries ClickHouse's own error text (e.g. `Code: 60. DB::Exception: Table x doesn't exist.`). ClickHouse's HTTP interface returns these with a 4xx status; the proxy maps them to 500 to keep response-shape uniform — distinguishing caller-fault from server-fault would require ClickHouse error-code parsing, out of scope for the escape hatch. |
| 502 | `{"error":"clickhouse request failed: ..."}` | Transport-level failure reaching ClickHouse (connection refused, timeout, etc.) |

**curl example:**

```bash
curl -X POST http://localhost:8080/v1/admin/query \
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

JSON array of result rows. The response carries an `X-Cache: HIT` or `X-Cache: MISS` header — this endpoint shares the in-process L1 (Ristretto) + singleflight machinery (unlike `/v1/admin/query`, which always hits ClickHouse).

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

Executes a pre-defined named query (pipe) with parameter binding. Parameters can be supplied via query string and/or JSON body. Results are cached in the shared L1 (Ristretto) with singleflight coalescing — same machinery as the structured query endpoint, and again, unlike `/v1/admin/query`.

**Query Parameters:** Any key matching a pipe parameter name.

**POST Body (optional):**

```json
{
  "start_date": "2024-01-01",
  "limit": 100
}
```

**Response:**

JSON array of result rows, with `X-Cache: HIT` or `X-Cache: MISS` indicating whether the row came from the in-process L1.

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
| `table` | string | (required) | Table name to subscribe to. Must match `^[a-zA-Z_][a-zA-Z0-9_]*$` (rejects NATS wildcards `*` / `>`). Returns 400 if missing or invalid. |
| `since` | string | — | RFC 3339 or RFC 3339 Nano timestamp. If provided, replays historical events from NATS before switching to live streaming. |
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
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:01.456Z","data":{"page":"/pricing"}}
```

Each SSE connection is bound to a single `?table=`; to consume multiple tables, open one connection per table or use the WebSocket endpoint with in-band multiplexing.

**Note:** When access control policies are active, streamed events are filtered per the caller's role — denied columns are removed and tables without select permission are skipped.

**curl example:**

```bash
# Subscribe to a specific table
curl -N "http://localhost:8080/v1/stream/sse?table=clicks"

# With gap-fill
curl -N "http://localhost:8080/v1/stream/sse?table=clicks&since=2026-03-24T11:00:00Z"
```

---

### `GET /v1/stream/ws` — WebSocket Stream

Opens a WebSocket connection for real-time event streaming. Supports in-band multiplexing — a single WebSocket can subscribe to multiple tables dynamically.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `table` | string | — | Optional initial table to subscribe to. If omitted, the client must send subscribe commands. When present, must match `^[a-zA-Z_][a-zA-Z0-9_]*$` — invalid values return 400 before the WebSocket upgrade. |
| `since` | string | — | RFC 3339 or RFC 3339 Nano timestamp for gap-fill on the initial `?table=` subscription. |
| `token` | string | — | JWT token (alternative to `Authorization` header). Stripped from URL after extraction. |

**In-band commands (client → server):**

After connecting, send JSON commands to manage subscriptions:

```json
{"action": "subscribe", "table": "clicks"}
{"action": "subscribe", "table": "page_views"}
{"action": "unsubscribe", "table": "clicks"}
```

**Outbound message format (server → client):**

Each message is wrapped in an envelope labelled with the table name:

```json
{"table": "clicks", "data": {"table_name": "clicks", "received_timestamp": "...", "data": {...}}}
```

**JavaScript example:**

```javascript
const ws = new WebSocket("ws://localhost:8080/v1/stream/ws?token=<jwt>");
ws.onopen = () => {
  ws.send(JSON.stringify({ action: "subscribe", table: "clicks" }));
  ws.send(JSON.stringify({ action: "subscribe", table: "page_views" }));
};
ws.onmessage = (event) => {
  const { table, data } = JSON.parse(event.data);
  console.log(`[${table}]`, data.table_name, data.data);
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

When batch inserts to ClickHouse fail (e.g., type errors, connection issues), the failed events are published to the DLQ NATS stream (`WAVEHOUSE_DLQ`) under subjects `dlq.{table}`. This prevents infinite retry loops — failed messages are ACKed from the main stream and moved to the DLQ for inspection. The DLQ payload is the inner data object that failed to insert (`{"id":"abc","field":...}`).

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

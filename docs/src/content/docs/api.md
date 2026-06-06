---
title: "API Reference"
description: "All endpoints, authentication, request/response formats for the WaveHouse API."
sidebar:
  order: 7
---

Every HTTP endpoint WaveHouse exposes — ingest, query, streaming, schema introspection, and admin — with request/response formats, error codes, and examples. The JWT middleware always runs; what a caller can do is driven by the policy; see [Configuration](/configuration#authentication) for the full auth config surface.

## Authentication

**There is no auth on/off switch** — the JWT middleware always runs. A request to `/v1/*` may include a JWT Bearer token:

```text
Authorization: Bearer <token>
```

The JWT must use HMAC signing (HS256/HS384/HS512) or be validated via a JWKS endpoint (configured via `auth.jwks_url`). The accepted signing algorithm is pinned to the active verifier and checked *before* any key is consulted: an HMAC deployment accepts only `HS256`/`HS384`/`HS512`, and a JWKS deployment accepts only the asymmetric family (`RS256/384/512`, `ES256/384/512`, `PS256/384/512`, `EdDSA`). Tokens using `alg: none`, or an algorithm from the other family (e.g. an `HS256` token sent to a JWKS deployment), are rejected outright.

For SSE connections where custom headers are not possible, you can pass the token as a query parameter:

```text
GET /v1/stream?token=<jwt>
```

The `Authorization` header takes precedence when both are provided: the `?token=` query parameter is only a fallback for clients that can't set headers (browser `EventSource`), so a token in the more log-leakable URL never overrides an explicit header credential. The `token` query parameter is stripped from the URL after extraction so it can't leak into logs.

**Authentication is decoupled from authorization.** A request with **no token**, or an **invalid/expired/malformed** one, is *not* rejected outright — it falls back to an empty role that resolves to the policy `default_role`, and authorization is decided downstream. Because the bad-token reason is remembered, a request that is then denied for lacking permission fails loud (`401` "invalid/expired token") instead of a bare `403`. Elevated access requires a valid token whose role is granted (or equals the `admin_role`). A `403` body has two forms: a request that resolves to **no role at all** (no token and no `default_role` configured) returns `{"error":"forbidden: request has no role and no public default_role is configured"}`, while a request carrying a concrete-but-unauthorized role returns the bare `{"error":"forbidden"}` shown in the tables below.

**Public (unauthenticated) access is driven by the policy.** Define a usable `default_role` and no-token requests are evaluated as that role (see [Roles & Access Control](#roles--access-control)); remove it and roleless requests are denied. Setting `default_role` equal to the `admin_role` is allowed — it makes every unauthenticated request admin (including `/v1/admin/*`), handy for local/dev — but it is logged loudly on every node that loads such a policy and must not be used in production. `/v1/admin/*` **and** the schema/DLQ endpoints are admin-only, and a pipe with **no `allowed_roles` authorizes nobody but the admin role** — but a pipe *can* be reached by the public when its `allowed_roles` lists the role the `default_role` resolves to (pipe access is plain allowlist membership, the same as any other role).

### Roles & Access Control

WaveHouse extracts the role from a configurable JWT claim path (`auth.role_claim`, default: `role`). Role handling:

- **`admin_role`** (policy field, `"admin"` by default, exact case-sensitive match) — Full access to all tables, raw SQL, and admin endpoints. There is no separate `service` role.
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

Some endpoints attach extra fields alongside `error` on their **failure** responses — e.g. a failing `/readyz` returns `{"status":"not ready","error":"…"}`. The guarantee is scoped to failures: whenever a response signals an error (any 4xx/5xx), an `error` field is present and parseable. Success responses carry each endpoint's own shape and need **not** include `error` — a healthy `/readyz` returns just `{"status":"ready"}`.

This contract holds for:

- Handler-emitted errors — validation (4xx), permission denials (403), not-found (404), backend errors (5xx).
- Router-level **404 Not Found** when the URL does not match any registered route.
- Router-level **405 Method Not Allowed** when the URL matches a route but the method is not registered.
- Server-level **500 Internal Server Error** when a handler panics — recovered, logged with stack, and reported to the client as JSON **when the handler has not yet committed any response headers or body bytes**.

Historically some error paths defaulted to `text/plain` because they were emitted via `http.Error` or chi's default handlers; those paths now route through a shared `writeJSONError` helper so strict clients can branch on `Content-Type` consistently.

The per-endpoint error tables below list the bodies you can expect for each status code; the `Content-Type` and `X-Content-Type-Options` headers above apply uniformly and are not repeated.

> **Caveat — streaming / partial-write responses.** For SSE, streaming endpoints, or any handler that has already started writing the response, a later panic is recovered and logged server-side but no JSON 500 body is written — once headers are flushed, replacing them would corrupt the stream. Clients consuming streams should treat connection termination or truncated output as the failure signal in those cases.

## Endpoints

### `GET /livez` — Liveness Probe

> Canonical name (current Kubernetes convention — the kube-apiserver split that replaced the older conflated `/healthz`). Also served at **`/healthz`** (a permanent alias — the most widely-recognized name) and **`/health`** (a deprecated alias, scheduled for removal in v0.2.0).

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

The boot-degraded response lets an operator `curl /livez` to learn why the gateway isn't ready to serve traffic yet, instead of grepping a restart-loop log. The binary is bound on `:8080` and serves diagnostics, but is not yet accepting ingest/query traffic. Schema discovery retries with exponential backoff (2s → 60s); once a Refresh succeeds, `/livez` flips to `200` and stays there for the rest of the process lifetime — transient ClickHouse blips after that point are reflected in `/readyz`, not `/livez`.

---

### `GET /readyz` — Readiness Probe

> Canonical name (current Kubernetes convention). Also served at **`/ready`** — a deprecated alias kept for v0.1.x and scheduled for removal in v0.2.0.

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

### Liveness vs readiness — behavior matrix

`/livez` (liveness) and `/readyz` (readiness) answer different questions, so they diverge once the process has booted. `/livez` is **sticky**: after the first successful schema discovery it stays `200` for the rest of the process lifetime, even if ClickHouse later becomes unreachable — liveness asks "is the process alive and past boot," not "is its backend up right now." `/readyz` stays **conditional**: it pings ClickHouse on every call and drops back to `503` whenever ClickHouse is unreachable.

| State                      | `/livez` | `/readyz` |
|----------------------------|:--------:|:---------:|
| Booting, ClickHouse down   | 503      | 503       |
| ClickHouse up after retry  | 200      | 200       |
| Post-boot, ClickHouse dies | 200 ★    | 503       |
| Post-boot, ClickHouse back | 200      | 200       |

★ Once boot completes, `/livez` no longer tracks ClickHouse state — a runtime ClickHouse outage surfaces in `/readyz` only. This is what keeps a Kubernetes `livenessProbe` from restart-looping the pod during a transient backend blip (see [Deployment → Boot-time degraded mode](/deployment#boot-time-degraded-mode)).

---

### `GET /v1/health` — Liveness ping (public, content-free)

Returns **`200 OK` with an empty body** once the gateway is past boot, or **`503 Service Unavailable`** (also empty) while boot-time schema discovery is still failing. No authentication required and no response body — the caller only branches on the status code, so there's nothing to JSON-encode or cache per request.

This is what the SDK's `wh.sys.health()` calls, and the endpoint to use when choosing among multiple servers in a distributed setup. It mirrors `/livez` under the hood but is intentionally a `/v1` API route rather than a Kubernetes probe path: an operator may filter the bare probe paths (`/livez`, `/readyz`, `/healthz`) out at the reverse proxy since they're internal probes, so the SDK relies on `/v1/health`, which is documented public API surface meant to stay reachable. It does **not** ping ClickHouse — readiness-based load balancing is the proxy/LB's job (via `/readyz`), not the client's.

---

### `GET /version` — Build Info

Returns the build metadata embedded in the running binary — the `version`, `git_commit`, and `build_time` injected at compile time via `-ldflags`, plus the `go_version` read from the runtime. No authentication required: these are the same values logged at startup, so the endpoint discloses nothing the logs don't already. Useful for confirming exactly which build is deployed when troubleshooting.

**Response:**

```json
{
  "version": "v1.2.3",
  "git_commit": "a1b2c3d",
  "build_time": "2026-06-02T12:00:00Z",
  "go_version": "go1.26.3"
}
```

A binary built without the `-ldflags` injection (e.g. a bare `go build` rather than `make build`) reports the fallback values `"dev"` / `"unknown"` for `version` / `git_commit`.

---

### `POST /v1/ingest?table={table}` — Ingest Data

Accepts a single flat JSON object, a JSON array of objects, or a newline-delimited JSON (NDJSON) batch, validates each record against the ClickHouse schema for `{table}`, and publishes it to the message queue. Returns immediately — ClickHouse insertion happens asynchronously via the batch consumer.

**The format is auto-detected from the body — `Content-Type` is only a hint.** The first non-whitespace byte decides: `[` selects a JSON array, anything else a single JSON object. An explicit `Content-Type: application/x-ndjson` selects NDJSON line-framing *unless* the body starts with `[` (the array wins), so a batch works whether or not the header matches.

| Body | Typical `Content-Type` | Response |
| ---- | ---------------------- | -------- |
| one flat JSON object | `application/json` *(default)* | `{"ok":true}` (or `{"duplicate":true}`) |
| a JSON array of objects (any length, even 1) | `application/json` | per-record summary — see [Batch Ingest](#batch-ingest) |
| one JSON object per line (NDJSON) | `application/x-ndjson` | per-record summary — see [Batch Ingest](#batch-ingest) |

The inbound request body is capped at 16 MiB; a body over the cap is rejected with `413` (matching [`POST /v1/admin/query`](#post-v1adminquery--query-clickhouse)).

The `{table}` URL query must match a table that exists in ClickHouse. WaveHouse discovers table schemas on startup and refreshes them periodically.

> **Insert-only.** The ingest pipeline accepts only inserts. All other mutations — `DELETE`, `UPDATE`, `TRUNCATE`, `DROP`, `ALTER`, `REPLACE`, etc. — must be issued through [`POST /v1/admin/query`](#post-v1adminquery--query-clickhouse), which is restricted to the admin role (`admin_role`, the same gate as the rest of `/v1/admin/*`).
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
| 400 | `{"error":"validation failed: ..."}` | Schema validation errors (unknown fields, type mismatches, missing required columns) |
| 400 | `{"error":"unknown column ... for table ..."}` (also: `missing required column ...`, `type mismatch for column ...`, `null value for non-nullable column ...`) | Schema validation failure |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason rather than silently falling back to `default_role`) |
| 403 | `{"error":"forbidden"}` (empty-role variant: `forbidden: request has no role and no public default_role is configured`) | The resolved role lacks `insert` on the table |
| 404 | `{"error":"unknown table: ..."}` | Table not found in ClickHouse schema |
| 413 | `{"error":"request body exceeded 16777216 bytes"}` | Request body over the 16 MiB cap |
| 500 | `{"error":"dedupe failed"}` | Deduplication backend error |
| 500 | `{"error":"publish failed"}` | Message queue error |
| 503 | `{"error":"service unavailable"}` | NATS JetStream stream full (backpressure). Response includes `Retry-After: 30` header. |

**curl example:**

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
```

#### Batch Ingest

A **JSON array** of objects (`[{…}, {…}]`) or an **NDJSON** body (`Content-Type: application/x-ndjson`, one JSON object per line) ingests a batch in a single request. Each record is validated, authorized, deduplicated, and published independently, so **one malformed or rejected record never blocks the rest of the batch**. (The SDK's `insert([...])` array helper uses the NDJSON form automatically; both forms return the same response.)

- **JSON array** — the most convenient form from most HTTP clients. A structural JSON syntax error fails the whole request (`400`), but a wrong-typed element (a non-object) is reported per-record like any other rejection. An explicit empty array (`[]`) is a valid, record-less batch (`200`, `total: 0`).
- **NDJSON** — the streaming-friendly form for very large uploads. Blank lines are skipped, and a single malformed *line* is reported and skipped (the newline reframes the next record).

**Request (JSON array):**

```http
POST /v1/ingest?table=clicks
Content-Type: application/json

[{"page": "/home", "score": 42.5}, {"page": "/about"}, {"page": "/pricing", "score": 7}]
```

**Request (NDJSON):**

```http
POST /v1/ingest?table=clicks
Content-Type: application/x-ndjson

{"page": "/home", "score": 42.5}
{"page": "/about"}
{"page": "/pricing", "score": 7}
```

**Response (`200`):** a per-record summary. Each `results` entry mirrors the single-object response (`ok` / `duplicate` / `error`) plus its 1-based `index`.

```json
{
  "total": 3,
  "succeeded": 2,
  "failed": 1,
  "duplicates": 0,
  "results": [
    { "index": 1, "ok": true },
    { "index": 2, "ok": true },
    { "index": 3, "error": "validation failed: ..." }
  ]
}
```

| Field | Meaning |
| ----- | ------- |
| `total` | records read from the body |
| `succeeded` | records validated and published |
| `failed` | records rejected — see `results` |
| `duplicates` | records skipped by dedup (when enabled) |
| `results` | per-record outcomes, each `{ index, ok\|duplicate\|error }` with `index` the 1-based record position. Truncated to the first 10,000 entries for very large batches (the counts stay authoritative). |

A `200` is returned whenever the body was read and the records were processed — **even if every record failed**, so branch on `failed`/`results`, not the status code. Per-record problems (a malformed NDJSON line, a non-object array element, schema validation, column/check permission failures) are reported in `results` and the batch continues. Whole-request conditions abort with a non-`200` instead:

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"empty body"}` / `{"error":"empty ndjson body"}` | The body has no records |
| 400 | `{"error":"invalid json: ..."}` | A structural JSON syntax error, or a truncated/unterminated JSON array (e.g. a cut-off upload — the whole request fails rather than reporting a partial success), or an oversized NDJSON line |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (same auth gate as the single-object path; surfaces the token reason) |
| 403 | `{"error":"forbidden"}` (empty-role variant: `forbidden: request has no role and no public default_role is configured`) | The resolved role lacks `insert` on the table (checked once, before any record) |
| 413 | `{"error":"request body exceeded 16777216 bytes"}` | Request body over the 16 MiB cap |
| 500 | `{"error":"publish failed"}` / `{"error":"dedupe failed"}` | Message-queue or dedup-backend failure mid-batch |
| 503 | `{"error":"service unavailable"}` | NATS JetStream full (backpressure) mid-batch; includes `Retry-After: 30` |

> **At-least-once on retry.** A batch aborted partway (a `503`/`500`, or a JSON-array syntax error, after some leading records were already published) re-publishes those leading records when the whole batch is retried. Enable deduplication if duplicate suppression matters — this is the same at-least-once property the single-object path already has (the SDK retries both on `503`).

**curl example (JSON array):**

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '[{"page":"/home"},{"page":"/about"}]'
```

**curl example (NDJSON):**

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary $'{"page":"/home"}\n{"page":"/about"}\n'
```

---

### `POST /v1/admin/query` — Query ClickHouse

Executes a SQL statement directly against ClickHouse. **WaveHouse proxies the SQL string verbatim to ClickHouse's HTTP interface** — any statement ClickHouse accepts works, including arbitrary DDL/DML/SYSTEM verbs and inline FORMAT directives. Multi-statement input (`SELECT 1; TRUNCATE t`) also works on recent ClickHouse versions where multi-query is enabled by default; older or restrictively-configured servers may reject the second statement with a clear error. Read queries return a JSON array of result rows; mutations/DDL return HTTP 200 with `[]` on success. DateTime columns are ISO-8601 formatted via the upstream `date_time_output_format=iso` setting; other types are returned as ClickHouse renders them under `FORMAT JSON`.

> **Inline `FORMAT` overrides the JSON envelope.** ClickHouse's inline `FORMAT` clause (e.g. `SELECT 1 FORMAT CSV` or `… FORMAT Pretty`) takes precedence over the URL-level `default_format=JSON` setting. When the SQL contains an explicit `FORMAT`, the proxy forwards ClickHouse's raw response body (CSV, Pretty, TSV, …) and passes through the upstream `Content-Type` header — `text/csv`, `text/tab-separated-values`, etc. — so consumers see the right MIME type. The "extract the `data` array" behavior only applies when ClickHouse returned the `FORMAT JSON` envelope, which is the default.
>
> **64 MiB response cap.** The proxy buffers the upstream response in memory before forwarding (no row-streaming yet), so a `SELECT *` from a large table can pin RAM on the API server. To avoid an admin OOMing themselves, responses larger than 64 MiB return 502 with a `clickhouse response exceeded N bytes` error. Narrow the query with `LIMIT`, or use a streaming client outside WaveHouse that talks to ClickHouse directly (the standard escape hatch — the same admin credentials work).

This endpoint **does not cache, does not singleflight, and emits `Cache-Control: no-store`** — every request goes straight to ClickHouse, mutation or read, and downstream HTTP caches are explicitly told not to store the response. Raw SQL is an admin escape hatch with infrequent, ad-hoc traffic, so the L1/singleflight machinery would only add complexity without a real hit-rate win. Use [`POST /v1/query?table={table}`](#post-v1querytabletable--structured-query) or [`GET/POST /v1/pipes/{name}`](#getpost-v1pipesname--execute-named-pipe) for the cached read paths (dashboards, high-QPS clients, etc.) — both share an in-process L1 (Ristretto) with singleflight coalescing.

> **Admin only.** The route is mounted under `/v1/admin/*`, behind the `RequireAdmin` gate: only a caller whose JWT role equals the policy `admin_role` (`"admin"` by default) may use it. A request with no/invalid token resolves to the `default_role` (not the admin role unless `default_role` is deliberately set to it — a loudly-warned dev-only setting) and is rejected. Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story, shared with the rest of `/v1/admin/*` (policy CRUD, pipes CRUD). The normal surfaces for non-admin callers are `POST /v1/ingest?table={table}` for writes, `POST /v1/query?table={table}` for structured reads, and `GET/POST /v1/pipes/{name}` for pre-defined queries — none of which expose raw SQL.

`/v1/admin/query` is the only sanctioned surface for non-insert mutations (the ingest pipeline is insert-only). Granting raw-SQL access to a non-admin role via the policy engine is no longer supported: authenticate with the admin role (`admin_role`).

**Request:**

```json
{
  "sql": "SELECT * FROM clicks LIMIT 10"
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `sql` | string | Yes | SQL forwarded verbatim to ClickHouse's HTTP interface. |

> **No parameter binding on this endpoint (yet).** The earlier handler accepted a `params` array bound to `?` placeholders; the HTTP proxy doesn't. ClickHouse's native named-param syntax (`WHERE id = {id:UInt32}` with `param_id=42` on the URL query string) is *not* forwarded today either — the proxy only sets `default_format`, `date_time_output_format`, and `database` on the upstream URL, and the request body is `{"sql": "..."}` with no escape hatch for query-string params. The current contract is "send raw SQL, get rows back": inline literals into the SQL for now. For safe binding from user-supplied input, use the structured query endpoint (`POST /v1/query?table={table}`) — that's its job.

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
| 400 | `{"error":"<ClickHouse error message>"}` | ClickHouse rejected the statement with a 4xx (bad SQL, missing table, type error, …). The body carries ClickHouse's own error text verbatim, e.g. `Code: 60. DB::Exception: Table default.x doesn't exist.`. The proxy maps any ClickHouse 4xx to HTTP 400 — caller-fault, the request itself is what's wrong. |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | The request carried a present-but-invalid/expired token and was denied for lacking permission (the gate surfaces the token reason) |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 502 | `{"error":"<ClickHouse error message>"}` | ClickHouse returned a 5xx (internal error, overloaded, etc.). The proxy maps any ClickHouse 5xx to HTTP 502 — gateway-fault, the upstream service had a problem. Same body convention: ClickHouse's text is forwarded as-is. |
| 502 | `{"error":"clickhouse request failed: ..."}` | Transport-level failure reaching ClickHouse (connection refused, timeout, the upstream went away mid-request) |
| 502 | `{"error":"clickhouse response exceeded N bytes; ..."}` | Response body exceeded the 64 MiB memory-safety cap. Narrow the query, add a `LIMIT`, or use `FORMAT JSONEachRow` with a streaming client outside WaveHouse. |

**curl example:**

```bash
curl -X POST http://localhost:8080/v1/admin/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
```

---

### `POST /v1/query?table={table}` — Structured Query

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
  }
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
| 403 | `{"error":"forbidden"}` | Role not in pipe's `allowed_roles` (and not the admin role). Fails closed: a request with no role (no token, or a JWT missing `auth.role_claim`) is denied unless a `default_role` resolves it into the list; a pipe with no `allowed_roles` denies everyone but the admin role. |
| 400 | `{"error":"missing required parameter: x"}` | Required parameter not supplied |

---

### `GET /v1/stream` — Server-Sent Events Stream

Opens a persistent SSE connection for real-time event streaming. Supports historical gap-fill from NATS JetStream using `DeliverByStartTime`.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `table` | string | (required) | Table name to subscribe to. Returns `400` only if missing/empty; other values aren't rejected — the name is encoded into a NATS-safe subject token (wildcards `*` / `>` are percent-encoded), so a nonexistent or odd name simply matches no events. |
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

Each SSE connection is bound to a single `?table=`; to consume multiple tables, open one connection per table.

**Note:** When access control policies are active, streamed events are filtered per the caller's role — denied columns are removed and tables without select permission are skipped.

**CORS:** `/v1/stream` honors the `server.cors_allowed_origins` allowlist like every endpoint, so a browser `EventSource` from an allowed origin connects normally. `Last-Event-ID` is allow-listed in the CORS preflight so fetch-based clients can resume cross-origin.

**curl example:**

```bash
# Subscribe to a specific table
curl -N "http://localhost:8080/v1/stream?table=clicks"

# With gap-fill
curl -N "http://localhost:8080/v1/stream?table=clicks&since=2026-03-24T11:00:00Z"
```

---

### `GET /v1/schema` — List All Table Schemas

Returns all discovered ClickHouse table schemas.

**Response:**

```json
[
  {
    "name": "clicks",
    "columns": [
      {"name": "page", "type": "String", "is_nullable": false, "has_default": false},
      {"name": "button", "type": "String", "is_nullable": false, "has_default": false},
      {"name": "score", "type": "Float64", "is_nullable": true, "has_default": false}
    ]
  }
]
```

---

### `GET /v1/schema?table={table}` — Get Table Schema

Returns the schema for a specific table.

**Response:**

```json
{
  "name": "clicks",
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

Triggers an immediate re-discovery of ClickHouse table schemas, then returns the refreshed schema list (same array shape as `GET /v1/schema`).

**Response:**

```json
[
  {
    "name": "clicks",
    "columns": [
      {"name": "page", "type": "String", "is_nullable": false, "has_default": false}
    ]
  }
]
```

---

### `GET /v1/dlq/stats` — DLQ Statistics

Returns per-table message counts in the Dead Letter Queue.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `table` | string | — | Filter stats to a specific table name (e.g., `?table=clicks` returns only the `clicks` count). |

**Response:**

```json
{
  "tables": {
    "clicks": 3,
    "page_views": 1
  },
  "total": 4
}
```

---

### Admin Endpoints

Admin endpoints require the policy `admin_role` (`"admin"` by default, exact case-sensitive match). There is no separate `service` role. The JWT middleware always runs — a request with no/invalid token resolves to the `default_role` (not the admin role unless `default_role` is deliberately set to it — a loudly-warned dev-only setting) and is denied.

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

The `default_role` field (optional) is the role assigned to any request that reaches the policy engine **without** a role — a valid token carrying no role claim, a request with no token at all, or one whose token was invalid/expired. **Setting it enables unauthenticated access:** roleless requests are evaluated as that role and receive exactly its permissions (or are denied if it grants none on the table/operation). If `default_role` is unset, a roleless request is denied. Setting it equal to the `admin_role` is allowed — every roleless request then becomes admin (including `/v1/admin/*`), which is handy for local/dev — but each node that loads such a policy logs a loud warning, and it must not be used in production.

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

**`allowed_roles`** restricts execution: the caller's role (a tokenless or roleless request is first resolved to the policy `default_role`) must appear in the list. The admin role (`admin_role`) always passes. Matching is exact — there is no `"*"` wildcard — and empty-string entries are ignored. An empty or omitted list authorizes **nobody but the admin role**, and a request whose role is absent or unlisted is denied (fails closed).

#### `DELETE /v1/admin/pipes/{name}` — Delete Named Pipe

## Event Message Format

### Internal Wire Format (NATS)

The message format used on NATS JetStream between ingest and the batch consumer:

```json
{
  "table_name": "clicks",
  "scope": "scope",
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
| `scope` | string | Optional scope namespace used for subject routing/cache invalidation context. |
| `received_timestamp` | string | RFC 3339 nano timestamp when WaveHouse received the event. |
| `data` | object | The original flat JSON body. |

### Client-Facing Format (SSE)

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

Needed whenever a caller must present a role — e.g. to reach an admin endpoint (role == `admin_role`) or any role beyond the policy `default_role`. The token must be signed with the configured `jwt_secret` (or a key the `jwks_url` serves) and must carry the role in its role claim (`auth.role_claim`, default `role`) — a token without the claim resolves to the policy `default_role`.

```bash
# Using jwt-cli (https://github.com/mike-engel/jwt-cli):
jwt encode --secret "change-me-in-production" '{"role": "admin", "exp": 9999999999}'

# Export for use with curl:
export TOKEN=$(jwt encode --secret "change-me-in-production" '{"role": "admin", "exp": 9999999999}')
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home"}'
```

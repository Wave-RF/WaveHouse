---
title: "API Reference"
description: "All endpoints, authentication, request/response formats for the WaveHouse API."
sidebar:
  order: 7
---

Every HTTP endpoint WaveHouse exposes — ingest, query, streaming, schema introspection, and admin — with request/response formats, error codes, and examples. The JWT middleware always runs; what a caller can do is driven by the policy; see [Configuration](/configuration#authentication) for the full auth config surface.

## Authentication

**The JWT middleware always runs.** Requests to `/v1/*` may include a Bearer token:

```text
Authorization: Bearer <token>
```

JWTs must use HMAC signing (HS256/HS384/HS512) or be validated via a JWKS endpoint (`auth.jwks_url`). The algorithm is pinned to the verifier and checked before key consultation: HMAC deployments accept only `HS256`/`HS384`/`HS512`; JWKS deployments accept only asymmetric families (`RS256/384/512`, `ES256/384/512`, `PS256/384/512`, `EdDSA`). Tokens using `alg: none` or the wrong family are rejected.

For SSE connections, tokens can be passed as a query parameter:

```text
GET /v1/stream?token=<jwt>
```

The `Authorization` header takes precedence; `?token=` is a fallback for clients like browser `EventSource`. To prevent log leaks, WaveHouse strips `?token=` from the URL after extraction, though you should still redact query strings at your proxy or CDN.

**Authentication is decoupled from authorization.** Requests with no token, or an invalid/expired/malformed one, fall back to an empty role resolving to the `default_role` policy. If a request is later denied for lacking permission, it fails with `401` ("invalid/expired token") if the token was bad, rather than a bare `403`. Elevated access requires a valid token granted the required role (or the `admin_role`). A `403` body returns `{"error":"forbidden: request has no role and no public default_role is configured"}` if there is no role/default role, otherwise it returns `{"error":"forbidden"}`.

**Public access is policy-driven.** Define a `default_role` to allow unauthenticated requests (see [Roles & Access Control](#roles--access-control)); without it, roleless requests are denied. Setting `default_role` to `admin_role` grants all unauthenticated requests admin access (including `/v1/admin/*`); this is for local development only and must not be used in production. `/v1/admin/*` and schema/DLQ endpoints are admin-only. A pipe with no `allowed_roles` authorizes only the admin role, but public access is possible if `allowed_roles` includes the `default_role`.

**Operator key (break-glass).** The `auth.operator_key` provides full-access platform operator privileges to the data plane and `/v1/admin/*` without a JWT. Use the `Operator` scheme or the `X-Operator-Key` alias:

```text
Authorization: Operator <operator-key>
# or, equivalently:
X-Operator-Key: <operator-key>
```

Checked before Bearer tokens using constant-time comparison, this key is honored even if the policy is `nil`/deleted, allowing restoration of wiped policies via HTTP. It is disabled by default (empty). See [Configuration — Authentication](/configuration#authentication) and [Access Control — Operator key](/access-control#operator-key).

### Roles & Access Control

WaveHouse extracts roles from a configurable JWT claim path (`auth.role_claim`, default: `role`).

- **`admin_role`** (default `"admin"`, case-sensitive): Full access to all tables, raw SQL, and admin endpoints.
- **Other roles**: Access determined by the access control policy.

Policies support Hasura-style row- and column-level permissions with JWT claim templating (e.g., `{{ jwt.app_metadata.tenant_id }}`).

## Response Format

### Error Responses

WaveHouse error responses (4xx/5xx) include these headers:

```text
Content-Type: application/json
X-Content-Type-Options: nosniff
```

The body is always a JSON object containing an `error` field:

```json
{"error": "invalid json"}
```

Some endpoints add extra fields; for example, a failing `/readyz` returns `{"status":"not ready","error":"…"}`. Success responses follow endpoint-specific shapes and may omit the `error` field (e.g., healthy `/readyz` returns `{"status":"ready"}`).

This contract applies to:

- Handler errors: validation (4xx), permission denials (403), not-found (404), and backend errors (5xx).
- Router-level **404 Not Found** (unmatched URL).
- Router-level **405 Method Not Allowed** (unsupported method).
- Server-level **500 Internal Server Error** for recovered handler panics, provided no headers or body bytes were previously committed.

Paths that once defaulted to `text/plain` via `http.Error` or chi's defaults now route through a shared `writeJSONError` helper, so `Content-Type` is consistent. Per-endpoint tables below detail expected bodies per status code; the global headers apply uniformly.

:::caution[Streaming / partial-write responses]
For SSE, streaming endpoints, or handlers that have already started writing, later panics are logged server-side but no JSON 500 body is sent to avoid corrupting the stream. Clients should treat connection termination or truncated output as failure signals.
:::

## Endpoints

### `GET /livez` — Liveness Probe

> Canonical name (Kubernetes convention). Also served at **`/healthz`** (permanent alias) and **`/health`** (deprecated; removed in v0.2.0).

Returns `200 OK` after the gateway discovers ClickHouse table schemas once. Returns `503 Service Unavailable` with a diagnostic body while the boot-time schema discovery retry loop runs (e.g., ClickHouse unreachable, missing database). No authentication required.

**Response (ready):**

```json
{"status": "ok"}
```

**Response (boot-degraded):**

```json
{
  "status": "degraded",
  "error": "schema discovery: dial tcp 127.0.0.1:9000: connect: connection refused"
}
```

Status code: `503 Service Unavailable`

The boot-degraded response allows operators to `curl /livez` for failure reasons instead of grepping logs. The binary binds on `:8080` for diagnostics before accepting traffic. Schema discovery retries with exponential backoff (2s → 60s); once successful, `/livez` stays `200`. Subsequent ClickHouse blips affect `/readyz`, not `/livez`.

### `GET /readyz` — Readiness Probe

Canonical name. **`/ready`** is a deprecated v0.1.x alias, removable in v0.2.0.

Returns `200 OK` if the process booted (schema discovery complete) and ClickHouse is reachable; otherwise `503 Service Unavailable`. No authentication required.

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

`/livez` and `/readyz` diverge after boot. `/livez` is **sticky**: after initial schema discovery, it remains `200` regardless of ClickHouse availability; it only confirms the process is alive and past boot. `/readyz` is **conditional**, pinging ClickHouse every call and returning `503` if unreachable.

| State                      | `/livez` | `/readyz` |
|----------------------------|:--------:|:---------:|
| Booting, ClickHouse down   | 503      | 503       |
| ClickHouse up after retry  | 200      | 200       |
| Post-boot, ClickHouse dies | 200 ★    | 503       |
| Post-boot, ClickHouse back | 200      | 200       |

★ After boot, `/livez` ignores ClickHouse state; outages surface only in `/readyz`. This prevents Kubernetes `livenessProbe` restart-loops during transient backend blips (see [Deployment → Boot-time degraded mode](/deployment#boot-time-degraded-mode)).

### `GET /v1/health` — Liveness ping (public, content-free)

Returns **`200 OK`** when the gateway has booted, or **`503 Service Unavailable`** if boot-time schema discovery is failing. Both responses have empty bodies. No authentication required.

The SDK's `wh.sys.health()` uses this endpoint for server selection in distributed setups. It mirrors `/livez` but exists as a `/v1` route because internal probes (`/livez`, `/readyz`, `/healthz`) may be filtered at the reverse proxy. This public API surface ensures reachability. It does **not** ping ClickHouse; readiness-based load balancing is handled by the proxy/LB via `/readyz`.

### `GET /version` — Build Info

Returns binary metadata: `version`, `git_commit`, and `build_time` (via `-ldflags`), plus the runtime `go_version`. No authentication required; these values are already logged at startup. Use this to confirm deployed builds during troubleshooting.

**Response:**

```json
{
  "version": "v1.2.3",
  "git_commit": "a1b2c3d",
  "build_time": "2026-06-02T12:00:00Z",
  "go_version": "go1.26.3"
}
```

Binaries built without `-ldflags` (e.g., `go build` instead of `make build`) report `"dev"` and `"unknown"` for `version` and `git_commit`.

### `POST /v1/ingest?table={table}` — Ingest Data

Accepts a flat JSON object, JSON array of objects, or newline-delimited JSON (NDJSON) batch. Validates records against the ClickHouse schema for `{table}` and publishes them to a message queue. Returns immediately; insertion is asynchronous via the batch consumer.

Format is auto-detected: `[` selects a JSON array; otherwise, it's a single JSON object. `Content-Type: application/x-ndjson` selects NDJSON unless the body starts with `[`.

| Body | Typical `Content-Type` | Response |
| ---- | ---------------------- | -------- |
| one flat JSON object | `application/json` *(default)* | `{"ok":true}` (or `{"duplicate":true}`) |
| a JSON array of objects | `application/json` | per-record summary — see [Batch Ingest](#batch-ingest) |
| NDJSON (one object per line) | `application/x-ndjson` | per-record summary — see [Batch Ingest](#batch-ingest) |

Request bodies are capped at 16 MiB; overflows return `413` (see [`POST /v1/admin/query`](#post-v1adminquery--query-clickhouse)). For larger uploads, use streaming NDJSON and configure the [reverse proxy](/reverse-proxy#request-body-size-limits).

The `{table}` query must match an existing ClickHouse table. WaveHouse discovers schemas on startup and refreshes them periodically.

:::note[Insert-only]
Only inserts are accepted here. All other mutations — `DELETE`, `UPDATE`, `TRUNCATE`, `DROP`, `ALTER`, `REPLACE`, etc. — must use [`POST /v1/admin/query`](#post-v1adminquery--query-clickhouse), restricted to the admin role (`admin_role`).

The policy engine authorizes inserts by inspecting columns. Predicate-driven mutations (e.g., `DELETE … WHERE`) are routed through the admin-gated raw-SQL surface because predicates cannot be proven to match only authorized rows.
:::

**Request:**

```json
{
  "url": "https://example.com/dashboard",
  "user_name": "Alice",
  "verified": true,
  "score": 42.5
}
```

The body must be a **flat JSON object** with keys matching ClickHouse column names and type-compatible values.

**Schema Validation:**

- Rejected: Unknown fields, type mismatches, missing required columns (non-nullable without default), or nulls in non-nullable columns without defaults.
- Type compatibility:
  - `String`: JSON strings, numbers, booleans (coerced by ClickHouse).
  - `FixedString`/`UUID`: Same as above at validation; non-strings are rejected by ClickHouse $\rightarrow$ DLQ.
  - `DateTime`/`Date`/`Enum`: JSON strings or numbers.
  - `IPv*`: JSON strings (numbers pass validation but fail in ClickHouse $\rightarrow$ DLQ).
  - `Int*`/`Float*`/`Decimal`: JSON numbers or strings (strings prevent JS 64-bit precision loss; non-numeric strings $\rightarrow$ DLQ).
  - `Bool`: JSON booleans, `0`, or `1` (others/strings pass validation but fail in ClickHouse $\rightarrow$ DLQ).
  - `Array`: JSON arrays.
  - `Map`: JSON objects.
  - `Tuple`: JSON arrays (unnamed) or objects (named); opposite shapes $\rightarrow$ DLQ.
  - Others (`JSON`, `Variant`, `Dynamic`, geo, …): any JSON value; failures surface in the DLQ.
- `Nullable()` and `LowCardinality()` are handled transparently.
- Top-level `DateTime`/`DateTime64` values use [Timestamp canonicalization](#timestamp-canonicalization).

**Response (accepted):**

```json
{"ok": true}
```

**Response (duplicate):** *(dedup enabled)*

```json
{"duplicate": true}
```

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"invalid json"}` | Malformed body |
| 400 | `{"error":"..."}` | Schema failure (unknown column, missing required column, type mismatch, or null in non-nullable). Body is the validator's verbatim message. |
| 400 | `{"error":"missing dedupe id field \"event_id\""}` | Dedupe enabled with `dedupe.require_id: true` and row lacks `id_field`. If `false`, row is published un-deduped. Logged at `WARN`; counted by `wavehouse_ingest_dedupe_missing_id_total`. Per-record failure in batches. |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | Invalid/expired token supplied. |
| 403 | `{"error":"forbidden"}` (or empty-role variant) | Role lacks `insert` permission on table. |
| 404 | `{"error":"unknown table: ..."}` | Table not found in schema. |
| 413 | `{"error":"request body exceeded 16777216 bytes"}` | Body over 16 MiB cap. |
| 500 | `{"error":"dedupe failed"}` | Deduplication backend error. |
| 500 | `{"error":"publish failed"}` | Message queue error. |
| 503 | `{"error":"service unavailable"}` | NATS JetStream stream full; includes `Retry-After: 30`. |

**curl example:**

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
```

#### Timestamp canonicalization

**Send the canonical form—RFC 3339 UTC, fraction truncated to column precision and trailing zeros trimmed—and the value is republished byte-for-byte.** Other accepted forms are rewritten to this **one canonical wire form (RFC 3339 UTC)** (e.g., `2026-06-21T04:00:00Z`) before publishing. This ensures the stored instant remains unchanged while all consumers (ClickHouse insert, [SSE subscribers](#get-v1stream--server-sent-events-stream), DLQ, and `/v1/query`) see identical spelling.

Accepted input forms:

- RFC 3339 with any offset (`.` fractions only).
- `YYYY-MM-DD[ T]HH:MM:SS[.fff]` or `YYYY-MM-DD` (zone-less): interpreted in the column's time zone, then the server's.
- Unix-seconds string of exactly 9–10 digits (`.fff` fraction honored only for `DateTime64`).
- **Non-negative integer** JSON number: read as Unix **seconds** for `DateTime`, or raw **tick count** for `DateTime64` (e.g., `1750478400500` is millisecond epoch `2025-06-21T04:00:00.5Z`).

**Fail-open**: Values in other forms are published verbatim; ClickHouse's parser determines insertability. Rejected values surface via the DLQ. `Date`/`Date32` columns pass through untouched.

:::note[Pass-through edge cases]

- Digit-strings not 9–10 characters (e.g., `YYYYMMDD` or 13/16/19-digit epochs) pass through untouched.
- Bare numbers with fractions or exponents (`1750478400.5`) pass through; ClickHouse rejects these for timestamp columns as it parses bare numbers only as integers.
- Instants outside the column type's range pass through because ClickHouse saturates values spelling-dependently (e.g., `DateTime64(9)` rejects inserts past 2262-04-11).
- Unresolvable time zones cause pass-through: named zones resolve from the runtime database. If a column or server zone cannot resolve, canonicalization is skipped to avoid guessing UTC and shifting the instant. Remedy: install `tzdata` or set the `ZONEINFO` environment variable.
- Timestamps in composite columns (`Array`, `Map`, `Tuple`) pass through; only top-level `DateTime`/`DateTime64` (including `Nullable`/`LowCardinality`) are canonicalized.
- Grammar is differentially tested against live ClickHouse: raw and canonical spellings must insert identically or both fail.

:::

:::caution[Upgrading WaveHouse against a pre-26.5 ClickHouse]
WaveHouse pins `date_time_input_format=best_effort` (the default since 26.5). On older servers using `basic`, all-digit strings $\ge$ 5 digits were read as Unix seconds; under `best_effort`, `"20260711"` is a date, not 1970-08-23. `DateTime64` columns diverge similarly on calendar shapes and mismatched epoch units (e.g., 16-digit $\mu$s into `DateTime64(3)`). The pin ensures RFC 3339 values with `Z` suffixes—which `basic` rejects—are insertable regardless of server version.
:::

**The canonical form, precisely.** This strict spelling is used by `/v1/query`, `/v1/pipes/{name}`, the SSE stream, and future row-filter comparisons ([#381](https://github.com/Wave-RF/WaveHouse/issues/381)):

- `YYYY-MM-DDTHH:MM:SSZ` or `YYYY-MM-DDTHH:MM:SS.FZ`. Uppercase `T` and `Z`, always UTC, seconds always present.
- Fraction is **truncated** (not rounded) to column precision: `DateTime` has no fraction; `DateTime64(3)` has at most three digits.
- Trailing fractional zeros are trimmed and all-zero fractions dropped (Go's `time.RFC3339Nano`): `.120` $\to$ `.12Z`, `.000` $\to$ `Z`.
- Column time zones only affect *input* interpretation; output always ends in `Z`.

Examples for `DateTime64(3, 'America/New_York')`:

- `"2026-06-21 00:00:00.1239"` (zone-less) $\to$ `"2026-06-21T04:00:00.123Z"`
- `"1750478400.5"` (Unix-seconds string) $\to$ `"2025-06-21T04:00:00.5Z"`
- `1750478400500` (integer ticks) $\to$ `"2025-06-21T04:00:00.5Z"`

#### Batch Ingest

Ingest batches via a **JSON array** (`[{…}, {…}]`) or **NDJSON** body (`Content-Type: application/x-ndjson`, one object per line). Each record is validated, authorized, deduplicated, and published independently; one rejected record never blocks the batch. The SDK's `insert([...])` helper uses NDJSON automatically.

- **JSON array**: Convenient for most clients. Structural syntax errors fail the whole request (`400`), but wrong-typed elements (non-objects) are reported per-record. An empty array (`[]`) is a valid batch (`200`, `total: 0`).
- **NDJSON**: Streaming-friendly for large uploads. Blank lines are skipped; malformed lines are reported and skipped.

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

**Response (`200`):** A per-record summary where each `results` entry mirrors the single-object response (`ok` / `duplicate` / `error`) plus its 1-based `index`.

```json
{
  "total": 3,
  "succeeded": 2,
  "failed": 1,
  "duplicates": 0,
  "results": [
    { "index": 1, "ok": true },
    { "index": 2, "ok": true },
    { "index": 3, "error": "unknown column \"referrer\" for table \"clicks\"" }
  ]
}
```

| Field | Meaning |
| ----- | ------- |
| `total` | records read from the body |
| `succeeded` | records validated and published |
| `failed` | records rejected — see `results` |
| `duplicates` | records skipped by dedup (when enabled) |
| `results` | per-record outcomes `{ index, ok\|duplicate\|error }`. Truncated to 10,000 entries for large batches; counts remain authoritative. |

A `200` is returned if the body was read and processed—even if all records failed. Branch on `failed`/`results`, not status code. Per-record issues (malformed NDJSON lines, non-object array elements, schema/permission failures) are reported in `results`. Whole-request errors abort with non-`200`:

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"empty body"}` / `{"error":"empty ndjson body"}` | No records in body |
| 400 | `{"error":"invalid json: ..."}` | Structural JSON error, truncated array, or oversized NDJSON line |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | Invalid/expired token |
| 403 | `{"error":"forbidden"}` (empty-role variant: `forbidden: request has no role and no public default_role is configured`) | Role lacks `insert` on table |
| 413 | `{"error":"request body exceeded 16777216 bytes"}` | Body over 16 MiB cap |
| 500 | `{"error":"publish failed"}` / `{"error":"dedupe failed"}` | Queue or dedup-backend failure mid-batch |
| 503 | `{"error":"service unavailable"}` | NATS JetStream full; includes `Retry-After: 30` |

:::caution[At-least-once on retry]
Batches aborted partway (`503`/`500`, or JSON syntax errors after some records published) re-publish leading records upon retry. Enable deduplication if duplicate suppression is required; the SDK retries both single and batch paths on `503`.
:::

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

### `POST /v1/admin/query` — Query ClickHouse

Executes a SQL statement directly against ClickHouse. **WaveHouse proxies the SQL string verbatim to ClickHouse's HTTP interface**; any statement ClickHouse accepts works, including DDL/DML/SYSTEM verbs and inline FORMAT directives. Multi-statement input (`SELECT 1; TRUNCATE t`) works on recent ClickHouse versions where multi-query is enabled by default; older servers may reject the second statement. Read queries return a JSON array of rows; mutations/DDL return HTTP 200 with `[]`. DateTime columns use ISO-8601 via the upstream `date_time_output_format=iso` setting, preserving trailing fraction zeros (e.g., `DateTime64(3)` returns `.000Z` where `/v1/query` renders plain `Z`). Other types follow `FORMAT JSON`.

:::note[Inline `FORMAT` overrides the JSON envelope]
ClickHouse's inline `FORMAT` clause (e.g. `SELECT 1 FORMAT CSV` or `… FORMAT Pretty`) takes precedence over the URL-level `default_format=JSON`. The proxy forwards ClickHouse's raw body (CSV, Pretty, TSV, …) and the upstream `Content-Type` — `text/csv`, `text/tab-separated-values`, etc. The "extract the `data` array" behavior only applies when ClickHouse returns the default `FORMAT JSON` envelope.
:::

:::caution[64 MiB response cap]
The proxy buffers responses in memory; large `SELECT *` queries can pin RAM. Responses exceeding 64 MiB return 502 with a `clickhouse response exceeded N bytes` error. Use `LIMIT` or a streaming client talking to ClickHouse directly (using the same admin credentials).
:::

This endpoint **does not cache, does not singleflight, and emits `Cache-Control: no-store`**. Every request goes straight to ClickHouse. For cached read paths (dashboards, high-QPS clients), use [`POST /v1/query?table={table}`](#post-v1querytabletable--structured-query) or [`GET/POST /v1/pipes/{name}`](#getpost-v1pipesname--execute-named-pipe), which share an in-process L1 (Ristretto) with singleflight coalescing.

:::note[Admin only]
Mounted under `/v1/admin/*` behind the `RequireAdmin` gate: only callers with a JWT role matching the policy `admin_role` (`"admin"` by default) may use it. Requests with no/invalid tokens resolve to `default_role` and are rejected. Raw SQL has no per-statement scope check; the role gate is the sole authorization mechanism, shared with `/v1/admin/*` (policy/pipes CRUD). Non-admins should use `POST /v1/ingest?table={table}` for writes, `POST /v1/query?table={table}` for structured reads, or `GET/POST /v1/pipes/{name}` for pre-defined queries.
:::

`/v1/admin/query` is the only sanctioned surface for non-insert mutations. Granting raw-SQL access to non-admin roles via the policy engine is unsupported; authenticate with the `admin_role`.

**Request:**

```json
{
  "sql": "SELECT * FROM clicks LIMIT 10"
}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `sql` | string | Yes | SQL forwarded verbatim to ClickHouse's HTTP interface. |

:::note[No parameter binding on this endpoint (yet)]
The proxy does not support `params` arrays or ClickHouse native named-param syntax (`WHERE id = {id:UInt32}` with `param_id=42` on the query string). The proxy only sets `default_format`, `date_time_output_format`, and `database` on the upstream URL. Use inline literals for now, or use the structured query endpoint (`POST /v1/query?table={table}`) for safe binding from user input.
:::

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
| 400 | `{"error":"<ClickHouse error message>"}` | ClickHouse rejected the statement (4xx). Body carries ClickHouse's error text verbatim, e.g. `Code: 60. DB::Exception: Table default.x doesn't exist.` |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | Invalid/expired token |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` |
| 502 | `{"error":"<ClickHouse error message>"}` | ClickHouse returned a 5xx. Body contains verbatim ClickHouse error text. |
| 502 | `{"error":"clickhouse request failed: ..."}` | Transport-level failure reaching ClickHouse |
| 502 | `{"error":"clickhouse response exceeded N bytes; ..."}` | Response body exceeded the 64 MiB cap. Use `LIMIT` or `FORMAT JSONEachRow` via a direct streaming client. |

**curl example:**

```bash
# Requires an admin-role JWT — see "Generating a JWT for Testing" below.
curl -X POST http://localhost:8080/v1/admin/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
```

### `POST /v1/query?table={table}` — Structured Query

Executes a type-safe structured query against a table. The AST is validated against the schema and converted to parameterized SQL, enforcing access control policies (column filtering, row-level security, aggregation restrictions).

:::note[The column allowlist is a hard cap on every clause]
Every referenced column—in `columns`, aggregations, `filters`, `group_by`, `order_by`, or `time_range`—must be permitted by the role's `allow_columns`/`deny_columns` or the request returns `403 column "x" not allowed`. Use `"select_all": true` for a full-row read (expanded to permitted columns; never raw `SELECT *`). **Omitting `columns` returns nothing** to prevent hidden column leaks. See [Access control → Column permissions](/access-control#column-permissions).
:::

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
| `columns` | string \| string[] | No | Columns to SELECT. A literal `"*"` is a column *named* `*`, not a wildcard. Omit (or send `[]`/`""`) to select nothing; use `select_all` for full-row reads. Mutually exclusive with `select_all`. |
| `select_all` | bool | No | Selects all columns the role may read. Mutually exclusive with non-empty `columns` and `aggregations`. |
| `aggregations` | object[] | No | Aggregation functions (`fn`, `column`, `alias`). |
| `filters` | object[] | No | WHERE conditions (`column`, `op`, `value`). Ops: eq, neq, gt, gte, lt, lte, in, like. |
| `group_by` | string[] | No | GROUP BY columns. |
| `order_by` | object[] | No | ORDER BY clauses (`column`, `dir`). |
| `limit` | int | No | Max rows. Omitted or above `query.default_max_rows` (default 10,000) → silently capped; policy `max_rows` can lower this further ([Access Control](/access-control#resource-limits)). |
| `time_range` | object | No | Time window (`column`, `since`, `until`). `since`/`until` accept RFC3339 or Go-durations ("1h", "30m", "7d", "2w"). Window applies only if `column` and `since` are set; `until` without `since` is ignored. |

:::note[Identifier names]
Identifiers are backtick-quoted, allowing dots, spaces, unicode, and keywords. Names containing literal `?` are rejected with `400` (clickhouse-go limitation [#279](https://github.com/Wave-RF/WaveHouse/issues/279)).
:::

**Response:**

JSON array of result rows. Top-level `DateTime`/`DateTime64` values use RFC 3339 UTC (`2026-06-21T04:00:00.123Z`), including `Nullable` columns (SQL `NULL` as JSON `null`). Timestamps in `Array`/`Map`/`Tuple` use the column's or server's zone, identical to the [SSE stream](#get-v1stream--server-sent-events-stream) for values [canonicalized at ingest](#timestamp-canonicalization). Includes `X-Cache: HIT` or `X-Cache: MISS` headers via L1 (Ristretto) + singleflight machinery.

Request bodies are capped at 1 MiB (`413` if exceeded) to prevent memory exhaustion. Reverse proxy limits ([/reverse-proxy#request-body-size-limits](/reverse-proxy#request-body-size-limits)) can narrow but not raise this cap.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"..."}` | Schema validation error (unknown column, bad aggregation, or unparseable `time_range`) |
| 403 | `{"error":"forbidden"}` | Role lacks select permission on table |
| 403 | `{"error":"column \"x\" not allowed"}` | Column denied by policy |
| 403 | `{"error":"aggregation \"x\" not allowed"}` | Aggregation fn denied by policy |
| 404 | `{"error":"unknown table: x"}` | Table not found |
| 413 | `{"error":"request body exceeded 1048576 bytes"}` | Request body over 1 MiB cap |

### `GET/POST /v1/pipes/{name}` — Execute Named Pipe

Executes a pre-defined named query (pipe) with parameter binding via query string and/or JSON body. Results use shared L1 (Ristretto) caching with singleflight coalescing, unlike `/v1/admin/query`.

**Query Parameters:** Any key matching a pipe parameter name.

**POST Body (optional):**

```json
{
  "start_date": "2024-01-01",
  "limit": 100
}
```

**Response:**

JSON array of result rows; `X-Cache: HIT` or `X-Cache: MISS` indicates if the row came from L1.

POST bodies are capped at 1 MiB; exceeding this returns `413` (same as [`POST /v1/query`](#post-v1querytabletable--structured-query) — see [reverse proxy → body limits](/reverse-proxy#request-body-size-limits)). Malformed bodies within the cap are ignored, allowing parameters to come from the query string alone.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 404 | `{"error":"pipe not found"}` | Pipe name not registered |
| 403 | `{"error":"forbidden"}` | Role not in `allowed_roles` (and not admin). Fails closed: requests without roles are denied unless a `default_role` resolves them; pipes with no `allowed_roles` deny all but admins. |
| 400 | `{"error":"missing required parameter: x"}` | Required parameter missing |
| 400 | `{"error":"parameter \"x\": unsupported parameter type object"}` | Non-scalar value without SQL literal form (JSON object). JSON arrays are valid and render as `IN` lists. |
| 400 | `{"error":"parameter \"x\": array parameter must not be empty"}` | Empty array (renders as invalid `IN ()`) |
| 413 | `{"error":"request body exceeded 1048576 bytes"}` | POST body over 1 MiB cap |

### `GET /v1/stream` — Server-Sent Events Stream

Opens a persistent SSE connection for real-time event streaming. Supports historical gap-fill from NATS JetStream via `DeliverByStartTime`.

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `table` | string | (required) | Table name to subscribe to. Returns `400` if missing/empty. Names are encoded into NATS-safe subject tokens (wildcards `*` / `>` are percent-encoded); nonexistent names match no events. |
| `since` | string | — | RFC 3339 or RFC 3339 Nano timestamp. Replays historical NATS events before live streaming. |
| `token` | string | — | JWT token (alternative to `Authorization` header). Stripped from URL after extraction. |

**Headers:**

| Header | Description |
| ------ | ----------- |
| `Last-Event-ID` | RFC 3339 timestamp of the last received event. Overrides `since` for automatic reconnection (`EventSource` behavior). |

**Response:** SSE stream (`text/event-stream`). Each event includes an `id:` field set to the `received_timestamp`. The stream starts with a `: connected` comment and emits a `:` keepalive comment every 30 seconds by default to prevent proxy closure; both are standard SSE comments ignored by `EventSource` (raw consumers should skip `:`-prefixed lines).

```text
id: 2026-03-24T12:00:00.123Z
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:00.123Z","data":{"page":"/home","button":"signup"}}

id: 2026-03-24T12:00:01.456Z
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:01.456Z","data":{"page":"/pricing"}}
```

Each connection binds to one `?table=`; open separate connections for multiple tables.

Top-level `DateTime`/`DateTime64` columns in `data` use canonical RFC 3339 UTC form (see [timestamp canonicalization](#timestamp-canonicalization)). Live events and `/v1/query` reads of the same row are byte-identical regardless of declared time zone or `Nullable` wrapper; non-UTC zones stream as `Z`. Canonicalization is fail-open at ingest: values outside accepted forms stream in original spelling, breaking byte-identity. Spells ClickHouse accepts are stored and query back canonical; rejected ones land in the DLQ. Events ingested before this behavior replay in original spelling ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)).

**Note:** Streamed events are filtered by caller role—denied columns are removed and tables without select permission are skipped.

**CORS:** Honors `server.cors_allowed_origins`. `Last-Event-ID` is allow-listed in CORS preflight for cross-origin resumption.

:::caution[Behind a proxy: disable response buffering]
Disable response buffering or proxies will hold events until the buffer fills. The `:` keepalive prevents idle timeouts ([#226](https://github.com/Wave-RF/WaveHouse/issues/226)), making higher idle/read timeouts optional. `EventSource` auto-reconnects via `Last-Event-ID`. See [Behind a reverse proxy → Server-Sent Events](/reverse-proxy#server-sent-events-sse).
:::

**curl example:**

```bash
# Subscribe to a specific table
curl -N "http://localhost:8080/v1/stream?table=clicks"

# With gap-fill
curl -N "http://localhost:8080/v1/stream?table=clicks&since=2026-03-24T11:00:00Z"
```

### `GET /v1/schema` — List All Table Schemas

Returns all discovered ClickHouse table schemas.

:::note[Admin only]
Schema and DLQ endpoints require `admin_role` (e.g., [`/v1/admin/query`](#post-v1adminquery--query-clickhouse)); others receive 401 or 403 errors. The trial `public` role cannot call them.
:::

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

### `GET /v1/schema?table={table}` — Get Table Schema

Returns a specific table's schema.

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
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | Invalid or expired token supplied |
| 403 | `{"error":"forbidden"}` | Caller lacks `admin_role` (`"admin"` default) |
| 404 | `{"error":"table not found"}` | Table not in discovered schemas |

### `POST /v1/schema/refresh` — Refresh Schemas

Triggers immediate ClickHouse table schema re-discovery and returns the refreshed list (same array shape as `GET /v1/schema`). Admin-only.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 / 403 | as above | Not admin role |
| 500 | `{"error":"refresh failed"}` | ClickHouse discovery query failed |

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

### `GET /v1/dlq/stats` — DLQ Statistics

Returns per-table message counts in the Dead Letter Queue. Admin-only. If no failures occurred, returns `200` with `{"tables":{},"total":0}`.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | Invalid or expired token supplied |
| 403 | `{"error":"forbidden"}` | Role is not the policy `admin_role` (`"admin"` default) |
| 500 | `{"error":"stream info failed"}` | NATS JetStream stream-info lookup failed |

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `table` | string | — | Filter stats to a specific table name (e.g., `?table=clicks`) |

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

### Admin Endpoints

Admin endpoints require the `admin_role` policy (`"admin"` by default, case-sensitive). There is no separate `service` role. JWT middleware always runs; requests with no or invalid tokens resolve to the `default_role` and are denied unless that role has permissions.

Endpoints accepting request bodies—`PUT /v1/admin/policy`, `POST /v1/admin/policy/validate`, and `PUT /v1/admin/pipes/{name}`—cap input at 1 MiB. Over-cap bodies return `413 {"error":"request body exceeded 1048576 bytes"}`.

#### `GET /v1/admin/policy` — Get Access Control Policy

Returns the current access control policy.

#### `PUT /v1/admin/policy` — Update Access Control Policy

Replaces and validates the entire access control policy.

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

The optional `default_role` is assigned to requests without a role (no token, invalid/expired token, or no role claim). **Setting this enables unauthenticated access.** If unset, roleless requests are denied. Setting it to `admin_role` grants admin access to all requests (including `/v1/admin/*`); this triggers a loud node warning and is for local/dev use only—not production.

#### `POST /v1/admin/policy/validate` — Validate Policy (Dry Run)

Validates a policy without saving. Returns `{"valid": true}` or an error.

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

**`allowed_roles`** restricts execution: the caller's role (or `default_role` for roleless requests) must be in the list. The `admin_role` always passes. Matching is exact; no `"*"` wildcard exists, and empty strings are ignored. An empty or omitted list authorizes only the admin role; others are denied.

#### `DELETE /v1/admin/pipes/{name}` — Delete Named Pipe

## Event Message Format

### Internal Wire Format (NATS)

Message format between ingest and batch consumer on NATS JetStream:

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
| `received_timestamp` | string | RFC 3339 nano timestamp of WaveHouse receipt. |
| `data` | object | Flat JSON body; `DateTime`/`DateTime64` values rewritten to canonical RFC 3339 UTC ([timestamp canonicalization](#timestamp-canonicalization)); others as sent. |

### Client-Facing Format (SSE)

Events pass through directly using the wire format:

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

If batch inserts to ClickHouse fail (e.g., type errors, connection issues), the worker re-inserts rows individually. Successful rows are acked; failures are published to the `WAVEHOUSE_DLQ` NATS stream under subjects `dlq.{table}` and ACKed from the main stream to prevent infinite loops.

The DLQ message body is the `EventMessage` envelope (`{"table_name":…,"received_timestamp":…,"data":{…}}`). Failed rows are in the `data` key, with `DateTime`/`DateTime64` values canonicalized if WaveHouse parsed them, otherwise original (see [timestamp canonicalization](#timestamp-canonicalization)). Headers `X-DLQ-Table`, `X-DLQ-Error`, and `X-DLQ-Timestamp` contain failure details.

Monitor depth via `GET /v1/dlq/stats`.

## Generating a JWT for Testing

Required when callers need a specific role (e.g., `admin_role`) beyond the `default_role`. Tokens must be signed with the configured `jwt_secret` or a key from `jwks_url` and include the role in the claim specified by `auth.role_claim` (default: `role`). Tokens lacking this claim resolve to `default_role`.

`"change-me-in-production"` is the placeholder in `config.yaml` used by `make dev` or `./bin/wavehouse`. The compose quickstart sets no secret; set `WH_AUTH_JWT_SECRET` on the `wavehouse` service and sign with that value (see [Development — Validating tokens](/development#validating-tokens)).

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

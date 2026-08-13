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

The `Authorization` header takes precedence when both are provided: the `?token=` query parameter is only a fallback for clients that can't set headers (browser `EventSource`), so a token in the more log-leakable URL never overrides an explicit header credential. A `?token=` is stripped from the URL after extraction whichever credential wins, so it stays out of WaveHouse's own logs — but it has already crossed the wire in the request URI, so redact query strings at any proxy, CDN, or load balancer in front.

**Authentication is decoupled from authorization.** A request with **no token**, or an **invalid/expired/malformed** one, is *not* rejected outright — it falls back to an empty role that resolves to the policy `default_role`, and authorization is decided downstream. Because the bad-token reason is remembered, a request that is then denied for lacking permission fails loud (`401` "invalid/expired token") instead of a bare `403`. Elevated access requires a valid token whose role is granted (or equals the `admin_role`). A `403` body has two forms: a request that resolves to **no role at all** (no token and no `default_role` configured) returns `{"error":"forbidden: request has no role and no public default_role is configured"}`, while a request carrying a concrete-but-unauthorized role returns the bare `{"error":"forbidden"}` shown in the tables below.

**Public (unauthenticated) access is driven by the policy.** Define a usable `default_role` and no-token requests are evaluated as that role (see [Roles & Access Control](#roles--access-control)); remove it and roleless requests are denied. Setting `default_role` equal to the `admin_role` is allowed — it makes every unauthenticated request admin (including `/v1/admin/*`), handy for local/dev — but it is logged loudly on every node that loads such a policy and must not be used in production. `/v1/admin/*` **and** the schema/DLQ endpoints are admin-only, and a pipe with **no `allowed_roles` authorizes nobody but the admin role** — but a pipe *can* be reached by the public when its `allowed_roles` lists the role the `default_role` resolves to (pipe access is plain allowlist membership, the same as any other role).

**Operator key (non-JWT, break-glass).** A separate, role-free credential — `auth.operator_key` — authorizes a caller as a **full-access platform operator**: the entire data plane *and* the `/v1/admin/*` surface, without a JWT and independently of the token verifier. Present it in the standard `Authorization` header with the `Operator` scheme (forwarded verbatim by proxies, no collision with Bearer JWTs), or via the `X-Operator-Key` alias:

```text
Authorization: Operator <operator-key>
# or, equivalently:
X-Operator-Key: <operator-key>
```

It is checked *before* the Bearer token (so it wins when both are present), compared in constant time, and — unlike a JWT bearing the `admin_role` — is honored **even when the policy is `nil`/deleted**, making it the only credential that can restore a wiped policy over HTTP. This deliberately bends "authentication is decoupled from authorization": a matching key both authenticates and authorizes in one step. It is disabled when empty (the default). See [Configuration — Authentication](/configuration#authentication) and [Access Control — Operator key](/access-control#operator-key).

### Roles & Access Control

WaveHouse extracts the role from a configurable JWT claim path (`auth.role_claim`, default: `role`). Role handling:

- **`admin_role`** (policy field, `"admin"` by default, exact case-sensitive match) — Full access to all tables, raw SQL, and admin endpoints. There is no separate `service` role, though the non-JWT operator key (above) reaches the same surface without a token.
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

:::caution[Streaming / partial-write responses]
For SSE, streaming endpoints, or any handler that has already started writing the response, a later panic is recovered and logged server-side but no JSON 500 body is written — once headers are flushed, replacing them would corrupt the stream. Clients consuming streams should treat connection termination or truncated output as the failure signal in those cases.
:::

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
{
  "status": "degraded",
  "error": "schema discovery: dial tcp 127.0.0.1:9000: connect: connection refused"
}
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

The inbound request body is capped at 16 MiB; a body over the cap is rejected with `413` (matching [`POST /v1/admin/query`](#post-v1adminquery--query-clickhouse)). For uploads larger than that, use the streaming NDJSON form below rather than one big body, and set your own outer limit at the [reverse proxy](/reverse-proxy#request-body-size-limits).

The `{table}` URL query must match a table that exists in ClickHouse. WaveHouse discovers table schemas on startup and refreshes them periodically.

:::note[Insert-only]
The ingest pipeline accepts only inserts. All other mutations — `DELETE`, `UPDATE`, `TRUNCATE`, `DROP`, `ALTER`, `REPLACE`, etc. — must be issued through [`POST /v1/admin/query`](#post-v1adminquery--query-clickhouse), which is restricted to the admin role (`admin_role`, the same gate as the rest of `/v1/admin/*`).

The policy engine authorizes mutations by inspecting the columns being written. That works for inserts but not for predicate-driven mutations like `DELETE … WHERE` — there's no way to prove the predicate matches only rows the caller is allowed to touch. Routing those statements through the admin-gated raw-SQL surface keeps the policy contract honest.
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

The body is a **flat JSON object** whose keys must match column names in the target ClickHouse table. Values must be type-compatible (see schema validation below).

**Schema Validation:**

- Unknown fields (not in the ClickHouse schema) are rejected.
- Type mismatches are rejected (e.g., sending a boolean for a `Float64` column).
- Missing required columns (non-nullable without a default) are rejected.
- Null values for non-nullable columns without a default are rejected.
- Type compatibility: `String` accepts JSON strings, numbers, and booleans (ClickHouse coerces the non-strings); `FixedString`/`UUID` accept the same at validation, but ClickHouse rejects a non-string value there, so it surfaces in the DLQ; `DateTime`/`Date`/`Enum` accept JSON strings or numbers; `IPv*` accepts JSON strings (a number passes validation but ClickHouse rejects it → DLQ); `Int*`/`Float*`/`Decimal` accept JSON numbers or strings — a string lets JavaScript callers avoid 64-bit precision loss, and its contents are ClickHouse's to judge (a non-numeric string is accepted here and surfaces in the DLQ, not as a `400`); `Bool` accepts JSON booleans and the numbers `0`/`1` (any other number, and *any* string — including `"true"` — passes validation but is rejected by ClickHouse → DLQ); `Array` accepts JSON arrays; `Map` accepts JSON objects; `Tuple` accepts JSON arrays or objects at validation, but ClickHouse takes an array only for an *unnamed* tuple and an object only for a *named* one — the other shape surfaces in the DLQ; any other ClickHouse type (`JSON`, `Variant`, `Dynamic`, geo, …) accepts any JSON value — WaveHouse defers to ClickHouse, so a bad value surfaces in the DLQ rather than as a `400`.
- `Nullable()` and `LowCardinality()` wrappers are handled transparently.
- Top-level `DateTime`/`DateTime64` values are rewritten to a canonical wire form on ingest — see [Timestamp canonicalization](#timestamp-canonicalization).

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
| 400 | `{"error":"unknown column ... for table ..."}` (also: `missing required column ...`, `type mismatch for column ...`, `null value for non-nullable column ...`) | Schema validation failure (unknown fields, type mismatches, missing required columns, null in a non-nullable column with no default). The body is the validator's message verbatim — there is no `validation failed:` prefix. |
| 400 | `{"error":"missing dedupe id field \"event_id\""}` | Only when dedupe is enabled with `dedupe.require_id: true` and the row lacks the configured `id_field`. With `require_id: false` (the default) the row is instead published un-deduped. Either way — reject or publish — the row is logged at `WARN` and counted by `wavehouse_ingest_dedupe_missing_id_total`. In a batch this is a per-record failure, not a whole-request error. |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason rather than silently falling back to `default_role`) |
| 403 | `{"error":"forbidden"}` (empty-role variant: `forbidden: request has no role and no public default_role is configured`) | The resolved role lacks `insert` on the table |
| 403 | `{"error":"column \"x\" not allowed for insert"}` | The record names a column the role's `allow_columns`/`deny_columns` forbids ([Access control → Column permissions](/access-control#column-permissions)) |
| 403 | `{"error":"check failed for column \"x\""}` | The record's value for a checked column doesn't satisfy the policy `check` (`_eq`/`_in`), or an `_in`-checked column is omitted ([Access control → Insert checks](/access-control#insert-checks)). On the batch path both this and the column error above are per-record failures reported in `results`, not whole-request rejections |
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

#### Timestamp canonicalization

**Send the canonical form — RFC 3339 UTC with the fraction already truncated to the column's precision and trailing zeros trimmed (spelled out below) — and the value is republished byte-for-byte.** A `DateTime`/`DateTime64` column value sent in any other accepted form — including RFC 3339 UTC with extra or trailing-zero fraction digits — is rewritten to that **one canonical wire form — RFC 3339 UTC** (`2026-06-21T04:00:00Z`, fraction truncated to the column's precision) before publishing, so the stored instant never changes but every consumer (the ClickHouse insert, [SSE subscribers](#get-v1stream--server-sent-events-stream), the DLQ) sees the same spelling `/v1/query` renders. The accepted input forms:

- RFC 3339, any offset (`.`-fractions only — ClickHouse has no `,` separator).
- `YYYY-MM-DD[ T]HH:MM:SS[.fff]` or `YYYY-MM-DD`, zone-less — interpreted in the column's time zone, else the ClickHouse server's, exactly as ClickHouse itself would.
- A Unix-seconds string of exactly 9–10 digits (a `.fff` fraction is honored only for `DateTime64` columns, as ClickHouse does).
- A **non-negative integer** JSON number, unquoted — read the way ClickHouse reads bare numbers: Unix **seconds** for a `DateTime` column, but the column's raw **tick count** for `DateTime64` (a `DateTime64(3)` stores milliseconds, so `1750478400500` is the millisecond epoch `2025-06-21T04:00:00.5Z` — and `1750478400` is January 1970, not June 2025).

**Fail-open**: a value in none of those forms is published verbatim — ClickHouse's more liberal parser decides insertability, and a value it too rejects surfaces via the DLQ, as before. `Date`/`Date32` columns pass through untouched.

:::note[Pass-through edge cases]

- Digit-strings of lengths other than 9–10 are ClickHouse's own forms — calendar shapes like `YYYYMMDD`, or its 13/16/19-digit ms/µs/ns epochs — and pass through untouched.
- A bare number with a fraction or exponent (`1750478400.5`) is passed through un-rewritten, and ClickHouse then fails the row for any timestamp column: it parses bare numbers as integers only — its lenient timestamp parsing, which accepts `"1750478400.5"` for a `DateTime64` column (on a plain `DateTime` the leftover fraction still fails the row), applies solely to quoted strings.
- An instant outside the column type's range also passes through — ClickHouse *saturates* out-of-range values spelling-dependently (and a `DateTime64(9)` column rejects the insert outright past the Int64-nanosecond ceiling, 2262-04-11 — a bound WaveHouse conservatively applies to every `DateTime64` of precision ≥ 7 when deciding what it may rewrite), so no rewrite there is safe.
- A time zone that doesn't resolve at runtime also causes pass-through: the binary embeds no tzdata, so named zones resolve from the runtime's zone database (the bundled distroless images ship one; a stripped-down custom runtime, or a server zone newer than the image's snapshot, may not resolve). An unresolvable *column* zone skips canonicalization for that column entirely; an unresolvable *server* default skips only zone-less values of columns without a declared zone — warned at schema refresh either way, and never guessed as UTC, which could move the stored instant. Remedy: install `tzdata` in a custom image, or point Go at a zone database via the `ZONEINFO` environment variable.
- Timestamps nested inside a composite column (`Array(DateTime)`, `Map(K, DateTime64)`, `Tuple(…, DateTime)`) pass through untouched; only top-level `DateTime`/`DateTime64` columns (including `Nullable`/`LowCardinality` wrappers) are canonicalized.
- The accepted grammar is differentially tested against a live ClickHouse: raw and canonicalized spellings must insert identically, or both fail.

:::

:::caution[Upgrading WaveHouse against a pre-26.5 ClickHouse]
WaveHouse pins `date_time_input_format=best_effort` on its inserts — the ClickHouse server default since 26.5. On an older server whose default was `basic`, a plain `DateTime` column read an all-digit timestamp string of five or more digits as Unix seconds (shorter runs it rejected outright, where `best_effort` reads `"2026"` as a year); under `best_effort`, `"20260711"` stores 2026-07-11, not 1970-08-23, and some lengths (e.g. 12 digits) are rejected outright. (`DateTime64` columns diverge the same way on calendar-shaped runs — `"20260711"` is 1970-08-23 under `basic`, 2026-07-11 under `best_effort` — and additionally whenever an epoch run's unit doesn't match the column scale, e.g. a 16-digit microsecond epoch into a `DateTime64(3)`; an epoch run whose unit matches the column scale (a 13-digit millisecond epoch into a `DateTime64(3)`) reads identically too — only 9–10-digit Unix-seconds runs, with an optional fraction, agree at *every* scale.) The canonical form itself is what the pin rescues: under `basic` an RFC 3339 value's `Z` suffix is rejected outright (the row fails and lands in the DLQ), and the pin is what makes it insertable regardless of server version. Zone-less date-times and 9–10-digit Unix-seconds strings parse identically under both settings.
:::

**The canonical form, precisely.** This is the one strict timestamp spelling in WaveHouse — the same one `/v1/query` and `/v1/pipes/{name}` render for top-level timestamp columns and the SSE stream carries (the raw-SQL proxy `/v1/admin/query` instead renders server-side via `date_time_output_format=iso`, which keeps trailing fraction zeros):

- `YYYY-MM-DDTHH:MM:SSZ`, or `YYYY-MM-DDTHH:MM:SS.FZ` when there is a sub-second part: uppercase `T` separator, uppercase `Z` suffix, always UTC — never a numeric offset — and seconds always present.
- The fraction is **truncated** (never rounded) to the column's precision: a `DateTime` column (whole seconds) never carries a fraction; a `DateTime64(3)` column carries at most three digits.
- Trailing fractional zeros are trimmed and an all-zero fraction is dropped (Go's `time.RFC3339Nano` rendering): `.120` becomes `.12Z`, `.000` becomes plain `Z` — byte-for-byte what `/v1/query` returns for the same stored value.
- A column's declared time zone changes only how zone-less *inputs* are interpreted, never the output: every canonical value ends in `Z`.

Examples for a `DateTime64(3, 'America/New_York')` column: `"2026-06-21 00:00:00.1239"` (zone-less, read in New York) → `"2026-06-21T04:00:00.123Z"`; `"1750478400.5"` (Unix-seconds string) → `"2025-06-21T04:00:00.5Z"`; the integer number `1750478400500` (ticks at the column's millisecond scale) → `"2025-06-21T04:00:00.5Z"`.

**The stream row-filter doesn't require this spelling.** Row-level enforcement compares timestamp operands as **instants** under the same input grammar, so a filter constant in any accepted spelling — zone-less, RFC 3339, Unix seconds — matches the canonical payload denoting the same instant, and an operand the grammar can't read withholds the row. Instant comparison also needs the column's timestamp parser from schema discovery — with no usable schema, or a declared zone that can't be loaded at runtime, the column falls back to byte-equality, where only an exactly matching spelling admits. See [the enforcement caution](/access-control#where-each-rule-is-enforced) for per-type comparison rules and the spelling that also works in query-path SQL.

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

:::caution[At-least-once on retry]
A batch aborted partway (a `503`/`500`, or a JSON-array syntax error, after some leading records were already published) re-publishes those leading records when the whole batch is retried. Enable deduplication if duplicate suppression matters — this is the same at-least-once property the single-object path already has (the SDK retries both on `503`).
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

---

### `POST /v1/admin/query` — Query ClickHouse

Executes a SQL statement directly against ClickHouse. **WaveHouse proxies the SQL string verbatim to ClickHouse's HTTP interface** — any statement ClickHouse accepts works, including arbitrary DDL/DML/SYSTEM verbs and inline FORMAT directives. Multi-statement input (`SELECT 1; TRUNCATE t`) also works on recent ClickHouse versions where multi-query is enabled by default; older or restrictively-configured servers may reject the second statement with a clear error. Read queries return a JSON array of result rows; mutations/DDL return HTTP 200 with `[]` on success. DateTime columns are ISO-8601 formatted via the upstream `date_time_output_format=iso` setting — server-side rendering that keeps trailing fraction zeros, so a `DateTime64(3)` whole-second value returns `.000Z` here where `/v1/query` renders plain `Z`; other types are returned as ClickHouse renders them under `FORMAT JSON`.

:::note[Inline `FORMAT` overrides the JSON envelope]
ClickHouse's inline `FORMAT` clause (e.g. `SELECT 1 FORMAT CSV` or `… FORMAT Pretty`) takes precedence over the URL-level `default_format=JSON` setting. When the SQL contains an explicit `FORMAT`, the proxy forwards ClickHouse's raw response body (CSV, Pretty, TSV, …) and passes through the upstream `Content-Type` header — `text/csv`, `text/tab-separated-values`, etc. — so consumers see the right MIME type. The "extract the `data` array" behavior only applies when ClickHouse returned the `FORMAT JSON` envelope, which is the default.
:::

:::caution[64 MiB response cap]
The proxy buffers the upstream response in memory before forwarding (no row-streaming yet), so a `SELECT *` from a large table can pin RAM on the API server. To avoid an admin OOMing themselves, responses larger than 64 MiB return 502 with a `clickhouse response exceeded N bytes` error. Narrow the query with `LIMIT`, or use a streaming client outside WaveHouse that talks to ClickHouse directly (the standard escape hatch — the same admin credentials work).
:::

This endpoint **does not cache, does not singleflight, and emits `Cache-Control: no-store`** — every request goes straight to ClickHouse, mutation or read, and downstream HTTP caches are explicitly told not to store the response. Raw SQL is an admin escape hatch with infrequent, ad-hoc traffic, so the L1/singleflight machinery would only add complexity without a real hit-rate win. Use [`POST /v1/query?table={table}`](#post-v1querytabletable--structured-query) or [`GET/POST /v1/pipes/{name}`](#getpost-v1pipesname--execute-named-pipe) for the cached read paths (dashboards, high-QPS clients, etc.) — both share an in-process L1 (Ristretto) with singleflight coalescing.

:::note[Admin only]
The route is mounted under `/v1/admin/*`, behind the `RequireAdmin` gate: only a caller whose JWT role equals the policy `admin_role` (`"admin"` by default) may use it. A request with no/invalid token resolves to the `default_role` (not the admin role unless `default_role` is deliberately set to it — a loudly-warned dev-only setting) and is rejected. Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story, shared with the rest of `/v1/admin/*` (policy CRUD, pipes CRUD). The normal surfaces for non-admin callers are `POST /v1/ingest?table={table}` for writes, `POST /v1/query?table={table}` for structured reads, and `GET/POST /v1/pipes/{name}` for pre-defined queries — none of which expose raw SQL.
:::

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

:::note[No parameter binding on this endpoint (yet)]
The earlier handler accepted a `params` array bound to `?` placeholders; the HTTP proxy doesn't. ClickHouse's native named-param syntax (`WHERE id = {id:UInt32}` with `param_id=42` on the URL query string) is *not* forwarded today either — the proxy only sets `default_format`, `date_time_output_format`, and `database` on the upstream URL, and the request body is `{"sql": "..."}` with no escape hatch for query-string params. The current contract is "send raw SQL, get rows back": inline literals into the SQL for now. For safe binding from user-supplied input, use the structured query endpoint (`POST /v1/query?table={table}`) — that's its job.
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
| 400 | `{"error":"<ClickHouse error message>"}` | ClickHouse rejected the statement with a 4xx (bad SQL, missing table, type error, …). The body carries ClickHouse's own error text verbatim, e.g. `Code: 60. DB::Exception: Table default.x doesn't exist.`. The proxy maps any ClickHouse 4xx to HTTP 400 — caller-fault, the request itself is what's wrong. |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | The request carried a present-but-invalid/expired token and was denied for lacking permission (the gate surfaces the token reason) |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 502 | `{"error":"<ClickHouse error message>"}` | ClickHouse returned a 5xx (internal error, overloaded, etc.). The proxy maps any ClickHouse 5xx to HTTP 502 — gateway-fault, the upstream service had a problem. Same body convention: ClickHouse's text is forwarded as-is. |
| 502 | `{"error":"clickhouse request failed: ..."}` | Transport-level failure reaching ClickHouse (connection refused, timeout, the upstream went away mid-request) |
| 502 | `{"error":"clickhouse response exceeded N bytes; ..."}` | Response body exceeded the 64 MiB memory-safety cap. Narrow the query, add a `LIMIT`, or use `FORMAT JSONEachRow` with a streaming client outside WaveHouse. |

**curl example:**

```bash
# Requires an admin-role JWT — see "Generating a JWT for Testing" below.
curl -X POST http://localhost:8080/v1/admin/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM clicks LIMIT 10"}'
```

---

### `POST /v1/query?table={table}` — Structured Query

Executes a type-safe structured query against a table. The query AST is validated against the schema and converted to parameterized SQL. Permissions from the access control policy are enforced (column filtering, row-level security, aggregation restrictions).

:::note[The column allowlist is a hard cap on every clause]
Every column the query references — in `columns`, an aggregation argument, `filters`, `group_by`, `order_by`, or `time_range` — must be permitted by the role's `allow_columns`/`deny_columns`, or the request is rejected with `403 column "x" not allowed`. A full-row read is requested with `"select_all": true` (expanded to the columns the role may read — never a raw `SELECT *`); **omitting `columns` returns nothing**, so a hidden column never leaks by being left out, grouped on, or filtered on. See [Access control → Column permissions](/access-control#column-permissions).
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
| `columns` | string \| string[] | No | Columns to SELECT — an array, or a single string for one column. A literal `"*"` is the column *named* `*`, **not** a wildcard. Omit (or send `[]` / `""`) to select nothing; use `select_all` for a full-row read. Mutually exclusive with `select_all`. |
| `select_all` | bool | No | Select every column the role may read (the all-columns wildcard, expanded server-side to the allow/deny set). Mutually exclusive with a non-empty `columns`, and with `aggregations`. |
| `aggregations` | object[] | No | Aggregation functions (`fn`, `column`, `alias`). |
| `filters` | object[] | No | WHERE conditions (`column`, `op`, `value`). Ops: eq, neq, gt, gte, lt, lte, in, like. |
| `group_by` | string[] | No | GROUP BY columns. |
| `order_by` | object[] | No | ORDER BY clauses (`column`, `dir`). |
| `limit` | int | No | Max rows. Omitted or above the configured `query.default_max_rows` (default 10,000) → silently capped at that value; a policy `max_rows` can lower it further (see [Access Control](/access-control#resource-limits)). |
| `time_range` | object | No | Time window (`column`, `since`, `until`). `since`/`until` accept RFC3339 or Go-duration relative values ("1h", "30m", "7d", "2w" — day and week suffixes expand to hours). Relative values mean that long *ago*. The window applies only when `column` and `since` are set — an `until` without `since` is ignored. |

:::note[Identifier names]
Table, column, and alias names may contain any characters ClickHouse accepts — dots, spaces, unicode, reserved keywords — because every identifier is backtick-quoted automatically. The one exception is a name containing a literal `?`, which is rejected with `400` (a clickhouse-go positional-binder limitation tracked in [#279](https://github.com/Wave-RF/WaveHouse/issues/279)).
:::

**Response:**

JSON array of result rows. Top-level `DateTime`/`DateTime64` values are returned in canonical RFC 3339 UTC (`2026-06-21T04:00:00.123Z`) — `Nullable` timestamp columns included (a SQL `NULL` renders as JSON `null`), while timestamps nested inside `Array`/`Map`/`Tuple` columns are rendered in the column's declared zone, else the ClickHouse server's, as the driver returns them — byte-identical to the [SSE stream](#get-v1stream--server-sent-events-stream) for values [canonicalized at ingest](#timestamp-canonicalization) (a fail-open pass-through that ClickHouse accepted still comes back canonical here, though it streamed in the producer's spelling). The response carries an `X-Cache: HIT` or `X-Cache: MISS` header — this endpoint shares the in-process L1 (Ristretto) + singleflight machinery (unlike `/v1/admin/query`, which always hits ClickHouse).

The inbound request body is capped at 1 MiB; a body over the cap is rejected with `413`. A query AST is bounded by nature (far under 1 MiB even with a large `in`-list), and the cap blocks a single-request memory-exhaustion vector on this public endpoint. Set a tighter or higher outer limit at your [reverse proxy](/reverse-proxy#request-body-size-limits) — but it can only narrow the effective limit, not raise it past this cap.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"..."}` | Schema validation error (unknown column, bad aggregation, or an unparseable `time_range` `since`/`until` — neither a relative duration nor an RFC3339 timestamp) |
| 403 | `{"error":"forbidden"}` | Role lacks select permission on table |
| 403 | `{"error":"column \"x\" not allowed"}` | Column denied by policy |
| 403 | `{"error":"aggregation \"x\" not allowed"}` | Aggregation fn denied by policy |
| 404 | `{"error":"unknown table: x"}` | Table not found |
| 413 | `{"error":"request body exceeded 1048576 bytes"}` | Request body over the 1 MiB cap |

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

The POST parameter body is capped at 1 MiB; a body over the cap is rejected with `413` (the same control-plane cap as [`POST /v1/query`](#post-v1querytabletable--structured-query) — see [reverse proxy → body limits](/reverse-proxy#request-body-size-limits)). A malformed-but-within-cap body is ignored rather than rejected, since parameters may legitimately come from the query string alone.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 404 | `{"error":"pipe not found"}` | Pipe name not registered |
| 403 | `{"error":"forbidden"}` | Role not in pipe's `allowed_roles` (and not the admin role). Fails closed: a request with no role (no token, or a JWT missing `auth.role_claim`) is denied unless a `default_role` resolves it into the list; a pipe with no `allowed_roles` denies everyone but the admin role. |
| 400 | `{"error":"missing required parameter: x"}` | Required parameter not supplied |
| 400 | `{"error":"parameter \"x\": unsupported parameter type object"}` | A non-scalar value with no SQL literal form — a JSON object, whether supplied directly or nested as an array element. A JSON **array** is valid and renders as an `IN`-style `(…)` list. |
| 400 | `{"error":"parameter \"x\": array parameter must not be empty"}` | An empty array — it would render as the invalid `IN ()`. |
| 413 | `{"error":"request body exceeded 1048576 bytes"}` | POST body over the 1 MiB cap |

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

**Response:** SSE stream (`text/event-stream`). Each event includes an `id:` field set to the event's `received_timestamp`. The stream opens with a `: connected` comment and emits a minimal `:` keepalive comment periodically (every 30 seconds by default), which keeps a quiet connection from being closed by a proxy; both are standard SSE comments that `EventSource` ignores (raw consumers should skip `:`-prefixed lines).

```text
id: 2026-03-24T12:00:00.123Z
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:00.123Z","data":{"page":"/home","button":"signup"}}

id: 2026-03-24T12:00:01.456Z
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:01.456Z","data":{"page":"/pricing"}}
```

Each SSE connection is bound to a single `?table=`; to consume multiple tables, open one connection per table.

Row values of top-level `DateTime`/`DateTime64` columns inside `data` arrive in the canonical RFC 3339 UTC form (ingest rewrites them before publishing — see [timestamp canonicalization](#timestamp-canonicalization)), so a live event and a `/v1/query` read of the same row agree on the instant in zone-explicit form — a zone-less spelling no longer parses as local time in a browser ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)). The two renderings are byte-identical regardless of the declared time zone or a `Nullable` wrapper — a column declared with a non-UTC zone also streams as `Z`, and `/v1/query` normalizes it (nullable or not) to UTC before rendering. Canonicalization is fail-open at ingest, so a value outside the accepted input forms streams in whatever spelling the producer sent — and for exactly those events the byte-identity above does not hold: a spelling ClickHouse accepts anyway is stored and still queries back canonical, while one it too rejects lands in the DLQ and never becomes queryable at all. Events ingested before this behavior shipped likewise replay in their original spelling.

**Note:** When access control policies are active, streamed events are filtered per the caller's role: tables without `select` permission are skipped, denied columns are removed from each event, and the role's [row-level `filter`](/access-control#row-level-security) is evaluated per subscriber against the caller's JWT claims — supplied by the connection's token (the `Authorization` header, or the `?token=` fallback above), with replayed gap-fill events filtered the same way. For a filter constant the query path's SQL also accepts ([the enforcement caution](/access-control#where-each-rule-is-enforced) gives per-type guidance), a connection is never delivered a row the query path would hide for that role — every comparison the stream can't prove fails closed and withholds the row instead. The one exception is a numeric `_neq`/`_gt`/`_lt` filter over a column type that narrows the payload on insert (a `Decimal`'s scale, `Float32` width); the caution documents that edge. The connection's claims are captured once, when the stream is established — a policy change applies from the next event, but an expired token or changed claims take effect only when the client reconnects.

**CORS:** `/v1/stream` honors the `server.cors_allowed_origins` allowlist like every endpoint, so a browser `EventSource` from an allowed origin connects normally. `Last-Event-ID` is allow-listed in the CORS preflight so fetch-based clients can resume cross-origin.

:::caution[Behind a proxy: disable response buffering]
SSE needs one bit of proxy configuration: disable response buffering, or the proxy holds events until a buffer fills and clients receive nothing in real time. Idle timeouts are handled for you — the `:` keepalive comment above keeps a quiet stream alive under typical proxy/tunnel idle windows ([#226](https://github.com/Wave-RF/WaveHouse/issues/226)), so raising the idle/read timeout is now optional. Browser `EventSource` still auto-reconnects (resuming via `Last-Event-ID`) if a connection drops. See [Behind a reverse proxy → Server-Sent Events](/reverse-proxy#server-sent-events-sse) for nginx/Caddy/Cloudflare specifics.
:::

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

:::note[Admin only]
The schema and DLQ endpoints in this section require the `admin_role` (like [`/v1/admin/query`](#post-v1adminquery--query-clickhouse)); other callers get 401 (bad token) / 403. The quickstart's trial `public` role cannot call them.
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
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason) |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 404 | `{"error":"table not found"}` | Table not in discovered schemas |

---

### `POST /v1/schema/refresh` — Refresh Schemas

Triggers an immediate re-discovery of ClickHouse table schemas, then returns the refreshed schema list (same array shape as `GET /v1/schema`). Admin-only, like the rest of this section.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 / 403 | as above | Not the admin role |
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

---

### `GET /v1/dlq/stats` — DLQ Statistics

Returns per-table message counts in the Dead Letter Queue. Admin-only, like the rest of this section. Before any failure has ever occurred, the endpoint returns `200` with `{"tables":{},"total":0}`.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason) |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 500 | `{"error":"stream info failed"}` | NATS JetStream stream-info lookup failed |

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

The admin endpoints that accept a request body — `PUT /v1/admin/policy`, `POST /v1/admin/policy/validate`, and `PUT /v1/admin/pipes/{name}` — cap it at 1 MiB (the same control-plane backstop as the public read endpoints); an over-cap body is rejected with `413 {"error":"request body exceeded 1048576 bytes"}`. A policy document or pipe definition is bounded, so this never binds legitimate use.

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
| `data` | object | The flat JSON body, with parseable `DateTime`/`DateTime64` column values rewritten to canonical RFC 3339 UTC (see [timestamp canonicalization](#timestamp-canonicalization)); other values as originally sent. |

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

When a batch insert to ClickHouse fails (e.g., type errors, connection issues), the worker re-inserts the batch row by row: rows that succeed are acked, and only the rows that fail again are published to the DLQ NATS stream (`WAVEHOUSE_DLQ`) under subjects `dlq.{table}`. This prevents infinite retry loops — those messages are ACKed from the main stream and moved to the DLQ for inspection. The DLQ message body is the published `EventMessage` envelope (`{"table_name":…,"received_timestamp":…,"data":{…}}` — the failed row is under its `data` key, its `DateTime`/`DateTime64` values as published: canonicalized where WaveHouse could parse them, otherwise the producer's original spelling — see [timestamp canonicalization](#timestamp-canonicalization)); the failure reason, table, and time travel in the `X-DLQ-Table` / `X-DLQ-Error` / `X-DLQ-Timestamp` message headers.

Use `GET /v1/dlq/stats` to monitor DLQ depth.

## Generating a JWT for Testing

Needed whenever a caller must present a role — e.g. to reach an admin endpoint (role == `admin_role`) or any role beyond the policy `default_role`. The token must be signed with the configured `jwt_secret` (or a key the `jwks_url` serves) and must carry the role in its role claim (`auth.role_claim`, default `role`) — a token without the claim resolves to the policy `default_role`.

`"change-me-in-production"` below is the placeholder shipped in the repo's `config.yaml` (what `make dev` / `./bin/wavehouse` load). The compose quickstart sets **no** secret — set `WH_AUTH_JWT_SECRET` on the `wavehouse` service and sign with that value (see [Development — Validating tokens](/development#validating-tokens)).

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

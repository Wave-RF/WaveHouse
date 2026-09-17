---
title: "API Reference"
description: "All endpoints, authentication, request/response formats for the WaveHouse API."
sidebar:
  order: 7
---

Every HTTP endpoint WaveHouse exposes — ingest, query, streaming, and the admin-gated `/v1/ops/*` surface (raw SQL, schema introspection, DLQ stats, pipe inspection, settings reload) — with request/response formats, error codes, and examples. The JWT middleware always runs; what a caller can do is driven by the policy; see [Configuration](/configuration#authentication) for the full auth config surface.

## Authentication

**There is no auth on/off switch** — the JWT middleware always runs. A request to `/v1/*` may include a JWT Bearer token:

```text
Authorization: Bearer <token>
```

The JWT must use HMAC signing (HS256/HS384/HS512) or be validated via a JWKS endpoint (configured via `auth.jwks_url` in the [settings directory](/settings-directory#authentication)). The accepted signing algorithm is pinned to the active verifier and checked *before* any key is consulted: an HMAC deployment accepts only `HS256`/`HS384`/`HS512`, and a JWKS deployment accepts only the asymmetric family (`RS256/384/512`, `ES256/384/512`, `PS256/384/512`, `EdDSA`). Tokens using `alg: none`, or an algorithm from the other family (e.g. an `HS256` token sent to a JWKS deployment), are rejected outright.

For SSE connections where custom headers are not possible, you can pass the token as a query parameter:

```text
GET /v1/stream?token=<jwt>
```

The `Authorization` header takes precedence when both are provided: the `?token=` query parameter is only a fallback for clients that can't set headers — a hand-rolled browser `EventSource`, for instance — so a token in the more log-leakable URL never overrides an explicit header credential. A `?token=` is stripped from the URL after extraction whichever credential wins, so it stays out of WaveHouse's own logs — but it has already crossed the wire in the request URI, so redact query strings at any proxy, CDN, or load balancer in front.

Prefer the header wherever you can. The TypeScript SDK streams over `fetch` and always uses `Authorization`, on browsers and servers alike; the query parameter exists for clients that have no other option.

**Authentication is decoupled from authorization.** A request with **no token**, or an **invalid/expired/malformed** one, is *not* rejected outright — it falls back to an empty role that resolves to the policy `default_role`, and authorization is decided downstream. Because the bad-token reason is remembered, a request that is then denied for lacking permission fails loud (`401` "invalid/expired token") instead of a bare `403`. Elevated access requires a valid token whose role is granted (or equals the `admin_role`). A `403` body has two forms: a request that resolves to **no role at all** (no token and no `default_role` configured) returns `{"error":"forbidden: request has no role and no public default_role is configured"}`, while a request carrying a concrete-but-unauthorized role returns the bare `{"error":"forbidden"}` shown in the tables below.

**Public (unauthenticated) access is driven by the policy.** Define a usable `default_role` and no-token requests are evaluated as that role (see [Roles & Access Control](#roles--access-control)); remove it and roleless requests are denied. Setting `default_role` equal to the `admin_role` is allowed — it makes every unauthenticated request admin (including `/v1/ops/*`), handy for local/dev — but it is logged loudly on every node that loads such a policy and must not be used in production. `/v1/ops/*` (raw SQL, pipe inspection, settings reload, schema, DLQ) is admin-only, and a pipe with **no `allowed_roles` authorizes nobody but the admin role** — but a pipe *can* be reached by the public when its `allowed_roles` lists the role the `default_role` resolves to (pipe access is plain allowlist membership, the same as any other role).

**Operator key (non-JWT, break-glass).** A separate, role-free credential — `auth.operator_key` — authorizes a caller as a **full-access platform operator**: the entire data plane *and* the `/v1/ops/*` surface, without a JWT and independently of the token verifier. Present it in the standard `Authorization` header with the `Operator` scheme (forwarded verbatim by proxies, no collision with Bearer JWTs), or via the `X-Operator-Key` alias:

```text
Authorization: Operator <operator-key>
# or, equivalently:
X-Operator-Key: <operator-key>
```

It is checked *before* the Bearer token (so it wins when both are present), compared in constant time, and — unlike a JWT bearing the `admin_role` — is honored **even when no policy is adopted** (an empty `policies.json`), making it the only credential that can still trigger `POST /v1/ops/settings/reload` over HTTP after the file is fixed. This deliberately bends "authentication is decoupled from authorization": a matching key both authenticates and authorizes in one step. It is disabled when empty (the default). See [Configuration — Authentication](/configuration#authentication) and [Access Control — Operator key](/access-control#operator-key).

### Roles & Access Control

WaveHouse extracts the role from a configurable JWT claim path (`auth.role_claim`, default: `role`). Role handling:

- **`admin_role`** (policy field, `"admin"` by default, exact case-sensitive match) — Full access to all tables, raw SQL, and admin endpoints. There is no separate `service` role, though the non-JWT operator key (above) reaches the same surface without a token.
- **Other roles** — Access determined by the access control policy, the settings directory's [`policies.json`](/settings-directory#policiesjson).

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
{"error": "unknown table: clicks"}
```

Some endpoints attach extra fields alongside `error` on their **failure** responses — e.g. a failing `/readyz` returns `{"status":"not ready","error":"…"}`. The guarantee is scoped to failures: whenever a response signals an error (any 4xx/5xx), an `error` field is present and parseable. Success responses carry each endpoint's own shape and need **not** include `error` — a healthy `/readyz` returns just `{"status":"ready"}`.

The contract holds for handler-emitted errors (validation, permission denials, not-found, backend failures), for router-level `404`s and `405`s, and for the `500` a recovered handler panic produces — the last one only while no response headers or body bytes have been committed. Everything routes through one `writeJSONError` helper, so strict clients can branch on `Content-Type` consistently.

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

The boot-degraded response lets an operator `curl /livez` to learn why the gateway isn't serving yet instead of grepping a restart-loop log: the binary is bound on `:8080` and serves diagnostics, but is not yet accepting ingest/query traffic. Schema discovery retries with exponential backoff (2s → 60s).

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

`/livez` is **sticky**: after the first successful schema discovery it stays `200` for the rest of the process lifetime, even if ClickHouse later becomes unreachable — liveness asks "is the process alive and past boot," not "is its backend up right now." `/readyz` stays **conditional**: it pings ClickHouse on every call and drops back to `503` whenever ClickHouse is unreachable.

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

Returns the build metadata embedded in the running binary — `version`, `git_commit`, and `build_time`, plus the `go_version` read from the runtime. No authentication required: these are the same values logged at startup, so the endpoint discloses nothing the logs don't already. Useful for confirming exactly which build is deployed when troubleshooting.

**Response:**

```json
{
  "version": "1.2.3",
  "git_commit": "a1b2c3d",
  "build_time": "2026-06-02T12:00:00Z",
  "go_version": "go1.27.0"
}
```

Where those values come from depends on how the binary was built:

| Build | `version` | `git_commit` / `build_time` |
| --- | --- | --- |
| Release artifact or container image | The tag **without** its leading `v` — GoReleaser injects `{{ .Version }}`, so `v1.2.3` reports `1.2.3` | Injected |
| `make build` | Whatever `git describe` returns, which **keeps** the `v` (e.g. `v1.2.3-4-gdeadbee`, or a bare short SHA before the first tag) | Injected |
| `go build` inside a checkout | The module pseudo-version Go derives from the commit (e.g. `0.0.0-20260815021004-1064a4fe6a59`) | Read from the VCS stamps Go embeds |
| `go install …/cmd/wavehouse@vX.Y.Z` | The module version, so the released tag without its leading `v` | `"unknown"` — a module-cache build carries no VCS stamps |

`go install` passes no `-ldflags`, so without the build-info fallback a perfectly good tagged install would report itself as `"dev"`. Ldflags always win when present.

---

### `POST /v1/ingest?table={table}` — Ingest Data

Validates a body of records against the ClickHouse schema for `{table}` and publishes each accepted one to the message queue. Returns immediately — ClickHouse insertion happens asynchronously via the batch consumer. A single-object body answers `{"ok":true}` (or `{"duplicate":true}` when dedup is on); every other body answers the [batch summary](#batch-ingest).

**The body goes to ClickHouse's own parser as-is.** WaveHouse never decodes a record: validation, type coercion, `DEFAULT` substitution and timestamp parsing are ClickHouse's own, running in-process via [chtypes](/deployment#chtypes-artifacts) (`internal/typelayer`) — the exact code path a real `INSERT` runs. A rejection therefore carries ClickHouse's own integer `code` and message rather than a WaveHouse-authored sentence, and there is no separate coercion table to keep in sync with the server.

**`Content-Type` is required and authoritative**: it declares the format and the bytes never override it.

| `Content-Type` | Body |
| --- | --- |
| `application/json` | one flat object, **or** a top-level array of them |
| `application/x-ndjson`, `application/ndjson`, `application/jsonl`, `application/jsonlines` | one object per line; always a batch |
| `text/csv` | header-less, positional — see [Positional formats](#positional-formats-csv--tsv) |
| `text/tab-separated-values` | header-less, positional |
| anything else, or none | `415`, listing the accepted types |

The two JSON families are one format to ClickHouse; the declaration decides only how the body frames its records. The single thing the body still chooses is *arity within `application/json`*: the first non-whitespace byte picks an array (`[`) or a single object. Under a single-object body only the first object is read — concatenated objects after it are ignored, a `200` for one record; declare NDJSON for anything line-framed ([#561](https://github.com/Wave-RF/WaveHouse/issues/561)). The reverse now works: a JSON array declared `application/x-ndjson` ingests every element.

:::note[What counts as a valid declaration]
The header is parsed with Go's `mime.ParseMediaType` (RFC 9110 §8.3) and only the **media type** decides the format, so no malformed *parameter* costs the request — `application/json; charset`, `application/json;;`, a value left mid-quote, a name repeated with different values all read as `application/json`. Two things are refused instead. A malformed parameter on a line that **also contains a comma** is a `415`, because the comma may be a second declaration joined on and the error cannot tell that from a comma inside data ([#563](https://github.com/Wave-RF/WaveHouse/issues/563)) — so `application/json; profile="a,b"` is fine and `application/json; profile="a,b"; charset` is not. And `Content-Type` is a **singleton** field (§5.3 forbids repeating it), so repeated header *lines* are accepted only when they agree, while a comma-joined value is refused outright: §8.3 warns that picking a member of the resulting pseudo-list is itself an interoperability and security hazard.

The 415 body quotes what you declared, bounded: at most **four distinct** header lines, each capped at 128 bytes and marked `…(truncated)` when cut, then `"…and N more"` counting every line not quoted, duplicates included. When declarations conflict, the one that actually disagreed is always quoted.
:::

The inbound request body is capped at 16 MiB and the `413` is decided before any record is processed, so nothing is published. The cap applies to every body shape — the whole body is read before it is parsed, so a line-framed batch is bounded exactly as a JSON array is. Split a larger upload across several requests, and set your own outer limit at the [reverse proxy](/reverse-proxy#request-body-size-limits). The `{table}` query parameter must name a table WaveHouse has discovered in ClickHouse; schemas refresh periodically.

:::note[Insert-only]
The ingest pipeline accepts only inserts. Every other mutation — `DELETE`, `UPDATE`, `TRUNCATE`, `DROP`, `ALTER`, `REPLACE` — goes through [`POST /v1/ops/query`](#post-v1opsquery--query-clickhouse) under the admin role (`admin_role`, the same gate as the rest of `/v1/ops/*`). The policy engine authorizes a write by the columns it names, which works for an insert but not for a predicate-driven `DELETE … WHERE`: nothing can prove the predicate matches only rows the caller may touch.
:::

**What ClickHouse decides, and what WaveHouse decides.** Everything about a *value* is ClickHouse's:

- A field the role may not write is indistinguishable from one the table does not have: both are code **117**, `Unknown field found while parsing JSONEachRow format: x`. So are `MATERIALIZED`, `ALIAS` and `EPHEMERAL` columns — none of the three is ever part of a published row.
- An omitted column, or an explicit `null` on one (WaveHouse pins `input_format_null_as_default`), takes its `DEFAULT` expression — evaluated by ClickHouse, including a volatile one like `now()` — or the type's implicit zero where none is declared, exactly as an `INSERT` naming fewer columns does.
- A coercion ClickHouse would make it makes here (a numeric string into an `Int*`, `"true"` into a `Bool`, an out-of-range integer wrapping); anything it would refuse fails synchronously in the ingest response with its real code, rather than surfacing later in the DLQ. `Nullable()` and `LowCardinality()` wrappers are transparent.

WaveHouse decides only policy: whether the role may insert at all, and whether the record satisfies the role's [`check` clauses](/access-control#insert-checks) — evaluated by the same compiled-filter engine as row-level security, against the row ClickHouse produced, so a check sees stored values rather than the payload's spelling. A record chtypes cannot evaluate at all — as opposed to accepting or rejecting it — is **declined** (`422`), which is not a data verdict.

**Error responses.** Rows marked **per-record** are reported in `results` on a batch body (the request itself stays `200`) and become the response status on a single-object body; every other row fails the whole request.

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"code":<N>,"error":"<ClickHouse message>"}` | **Per-record.** ClickHouse's parser refused the record; `code` and message are its own. `117` is an unknown field — which now includes a column the role may not write, and any `MATERIALIZED`/`ALIAS`/`EPHEMERAL` column; `27`/`26` are unparseable input; `6` out of range |
| 400 | `{"error":"invalid request body"}` | The body could not be read at all — a malformed transfer encoding, or an upload cut off *in transit* |
| 400 | `{"error":"empty body"}` (declared variants: `empty ndjson body`, `empty csv body`, `empty tsv body`) | The body holds no records |
| 400 | `{"error":"invalid json: unterminated json array"}` | A body declared `application/json` opening with `[` whose brackets do not balance — truncated, or structurally broken. It cannot be salvaged per record, so the whole request fails |
| 400 | `{"error":"missing dedupe id field \"event_id\""}` | **Per-record.** Only with `dedupe.require_id: true`, when the row carries no value for the configured `id_field`. With `require_id: false` (the default) the row is published un-deduped instead. Either way it is logged at `WARN` and counted by `wavehouse_ingest_dedupe_missing_id_total` |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason rather than silently falling back to `default_role`) |
| 403 | `{"error":"forbidden"}` (empty-role variant: `forbidden: request has no role and no public default_role is configured`) | The resolved role lacks `insert` on the table — checked once, before any record |
| 403 | `{"error":"check failed for column \"x\""}` (several: `check failed for columns "x", "y"`) | **Per-record.** The row does not satisfy the role's insert [`check`](/access-control#insert-checks). The filter is AND-joined over every checked column, so with more than one it names the set that was tested rather than guessing an attribution |
| 403 | `{"error":"policy check references column \"x\", which table \"t\" does not have"}` (also `… which is materialized and cannot be inserted`, the same for `alias`, and `… which is ephemeral and is never stored`) | **Per-record.** A **policy misconfiguration**, not a bad request: the role's `check` names a column the table lacks, one ClickHouse computes, or an `EPHEMERAL` one. None can be enforced, so the check would have passed silently while enforcing nothing. It fires on every insert by that role until the policy or the table is corrected, and names every offending column. `wavehouse validate` cannot catch it — it never sees the ClickHouse schema |
| 404 | `{"error":"unknown table: ..."}` | Table not found in the discovered schema |
| 413 | `{"error":"request body exceeded 16777216 bytes"}` | Request body over the 16 MiB cap |
| 415 | `{"error":"no Content-Type: ingest requires one of application/json, application/x-ndjson, application/ndjson, application/jsonl, application/jsonlines, text/csv, text/tab-separated-values"}` (declared variant: `Content-Type "text/plain": ingest requires one of …`; conflicting variant: `conflicting Content-Type declarations "application/json", "application/x-ndjson": ingest reads one format per request, and requires one of …`) | No `Content-Type`, an unsupported or unparseable one, a comma-bearing value that does not parse as a single media type, or repeated lines that disagree. Checked before the body is read |
| 422 | `{"error":"validation engine declined: <message>"}` | **Per-record.** chtypes could not evaluate the record at all — the artifact declined the shape, rather than the data being wrong. A `check` clause that could not be evaluated lands here too (`validation engine declined: the insert check for column "x" could not be evaluated`) |
| 500 | `{"error":"dedupe failed"}` / `{"error":"publish failed"}` | Deduplication backend or message-queue error |
| 503 | `{"error":"service unavailable"}` | NATS JetStream stream full (backpressure). Carries `Retry-After: 30` |
| 503 | `{"error":"<cause>"}` | No chtypes artifact matches the connected ClickHouse server's minor version, or the server's reported timezone changed since WaveHouse started — checked once for the whole table, so it aborts before any record. Carries `Retry-After: 30`; logged at most once per table per minute. See [Deployment → chtypes artifacts](/deployment#chtypes-artifacts) |

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}
```

#### Timestamp rendering

WaveHouse rewrites timestamps in neither direction. **Inbound**, any spelling ClickHouse's own parser accepts under `date_time_input_format=best_effort` (the setting WaveHouse pins, both at chtypes' ingest compile and on the worker's `INSERT`) is accepted — RFC 3339 with any offset, a zone-less `YYYY-MM-DD[ T]HH:MM:SS[.fff]` read in the column's declared zone else the server's default, a Unix-seconds string, a bare integer at the column's tick scale, among the other forms its lenient parser reads. It is ClickHouse's grammar, not a reimplementation of it, so whatever a real `INSERT` into this table would accept, ingest accepts, with the same coercions and the same refusals.

**Outbound**, `DateTime`/`DateTime64` values in the NATS/SSE wire row and in `/v1/query` / `/v1/pipes/{name}` results are the exact bytes ClickHouse's writer produces, in the column's declared zone else the server's default: `"2026-06-21 04:00:00.123"`, space-separated, no `Z` suffix, never RFC 3339. Every consumer renders from the same stored value the same way, so SSE and `/v1/query` agree on spelling for a given row **by construction**, with no WaveHouse rewriting step to keep in sync ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)). The raw-SQL proxy `/v1/ops/query` is the exception: it sets `date_time_output_format=iso`, which keeps trailing fraction zeros and an ISO-8601 `Z`, and is not expected to match the other two byte-for-byte.

**Row-level security compares instants, not spellings.** A stream row filter on a `DateTime`/`DateTime64` column is compiled and evaluated by the same engine that validates ingest ([internal/typelayer](/access-control#where-each-rule-is-enforced)), so a filter constant in any spelling ClickHouse would accept in a `WHERE` clause matches the stored instant however the payload spelled it, and a predicate the engine cannot compile withholds every row for that role.

#### Positional formats (CSV / TSV)

`text/csv` and `text/tab-separated-values` are **header-less and positional**. The fields are the table's **wire columns** — declaration order minus every `MATERIALIZED`, `ALIAS` and `EPHEMERAL` column — and a producer must send **every one of them, in that order**. `GET /v1/ops/schema?table={table}` returns the columns in `position` order; drop the three computed kinds and that is the field order.

| Body | Outcome |
| --- | --- |
| every field, in order | accepted |
| an empty field (CSV) or `\N` (TSV) | that column takes its `DEFAULT` |
| too few fields | rejected, code **27** — `Cannot parse input: expected end of row after 4 values, found 2` |
| too many fields | rejected, code **117** |
| a header line | **not a header** — one record that fails to parse, code 27; the data rows after it still parse |

An empty **TSV** field is the empty string, not a default: `\N` is TSV's spelling for "take the default", and a `DateTime64` cannot read `""`. There is no `CSVWithNames`/`TSVWithNames` — a positional producer cannot self-describe, so a column-order change silently re-assigns values. Pin the producer to the schema and re-check it after any `ALTER`.

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: text/csv" \
  --data-binary $'"/home","signup",42.5,\n"/about","nav",3,\n'
# → {"total":2,"succeeded":2,"failed":0,"duplicates":0,"results":[{"index":1,"ok":true},{"index":2,"ok":true}]}
```

#### Batch Ingest

A JSON array, an NDJSON body, a CSV body or a TSV body ingests a batch in one request. Each record is validated, authorized, deduplicated and published independently, so **one malformed or rejected record never blocks the rest of the batch** — including inside a single-line (compact) JSON array, which WaveHouse re-frames in place before handing it over. An explicit empty array (`[]`) is a valid record-less batch (`200`, `total: 0`); blank lines in an NDJSON body are skipped. (The SDK's `insert([...])` array helper uses the NDJSON form automatically; every form returns the same response.)

The response counts records read, published, rejected and deduplicated, then lists per-record outcomes: each entry mirrors the single-object response (`ok` / `duplicate` / `error`) plus its 1-based `index`, and carries ClickHouse's integer `code` when the rejection was its parser's. `results` is truncated to the first 10,000 entries; the four counts stay authoritative.

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '[{"page":"/home","button":"signup","score":42.5},{"page":"/about","button":"nav","score":3},{"page":"/pricing","button":"cta","score":7,"referrer":"/home"}]'
```

```json
{
  "total": 3,
  "succeeded": 2,
  "failed": 1,
  "duplicates": 0,
  "results": [
    { "index": 1, "ok": true },
    { "index": 2, "ok": true },
    { "index": 3, "error": "Unknown field found while parsing JSONEachRow format: referrer", "code": 117 }
  ]
}
```

A `200` is returned whenever the body was read and the records were processed — **even if every record failed**, so branch on `failed`/`results`, not the status code.

:::caution[At-least-once on retry]
A batch aborted partway — a `503` or `500` after some leading records were already published — re-publishes those leading records when the whole batch is retried. Failures decided before any record is processed are not in this class: a `413`, a `415`, the `400 invalid request body` of an upload cut off in transit, and the unterminated-array `400` all publish nothing and are safe to retry as-is (once split, for a `413`). Enable deduplication if duplicate suppression matters — the single-object path has the same at-least-once property, and the SDK retries both on `503`.
:::

---

### `POST /v1/ops/query` — Query ClickHouse

Executes a SQL statement directly against ClickHouse. **WaveHouse proxies the SQL string verbatim to ClickHouse's HTTP interface** — any statement ClickHouse accepts works, including arbitrary DDL/DML/SYSTEM verbs and inline FORMAT directives. Multi-statement input (`SELECT 1; TRUNCATE t`) also works on recent ClickHouse versions where multi-query is enabled by default; older or restrictively-configured servers may reject the second statement with a clear error. Read queries return a JSON array of result rows; mutations/DDL return HTTP 200 with `[]` on success. DateTime columns are ISO-8601 formatted via the upstream `date_time_output_format=iso` setting — server-side rendering that keeps trailing fraction zeros, so a `DateTime64(3)` whole-second value returns `.000Z` here where `/v1/query` renders plain `Z`; other types are returned as ClickHouse renders them under `FORMAT JSON`.

:::note[Inline `FORMAT` overrides the JSON envelope]
ClickHouse's inline `FORMAT` clause (e.g. `SELECT 1 FORMAT CSV` or `… FORMAT Pretty`) takes precedence over the URL-level `default_format=JSON` setting. When the SQL contains an explicit `FORMAT`, the proxy forwards ClickHouse's raw response body (CSV, Pretty, TSV, …) and passes through the upstream `Content-Type` header — `text/csv`, `text/tab-separated-values`, etc. — so consumers see the right MIME type. The "extract the `data` array" behavior only applies when ClickHouse returned the `FORMAT JSON` envelope, which is the default.
:::

:::caution[64 MiB response cap]
The proxy buffers the upstream response in memory before forwarding (no row-streaming yet), so a `SELECT *` from a large table can pin RAM on the API server. To avoid an admin OOMing themselves, responses larger than 64 MiB return 502 with a `clickhouse response exceeded N bytes` error. Narrow the query with `LIMIT`, or use a streaming client outside WaveHouse that talks to ClickHouse directly (the standard escape hatch — the same admin credentials work).
:::

This endpoint **does not cache, does not singleflight, and emits `Cache-Control: no-store`** — every request goes straight to ClickHouse, mutation or read, and downstream HTTP caches are explicitly told not to store the response. Raw SQL is an admin escape hatch with infrequent, ad-hoc traffic, so the L1/singleflight machinery would only add complexity without a real hit-rate win. Use [`POST /v1/query?table={table}`](#post-v1querytabletable--structured-query) or [`GET/POST /v1/pipes/{name}`](#getpost-v1pipesname--execute-named-pipe) for the cached read paths (dashboards, high-QPS clients, etc.) — both share an in-process L1 (Ristretto) with singleflight coalescing.

:::note[Admin only]
The route is mounted under `/v1/ops/*`, behind the `RequireAdmin` gate: only a caller whose JWT role equals the policy `admin_role` (`"admin"` by default) — or who presents the non-JWT [operator key](#authentication) — may use it. A tokenless request (or a valid token without a role claim) resolves to the `default_role` (not the admin role unless `default_role` is deliberately set to it — a loudly-warned dev-only setting) and is rejected with `403`; a present-but-invalid token — expired, malformed, bad signature — keeps its stashed verification error and fails loud with `401` instead. Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story, shared with the rest of `/v1/ops/*` (see [Admin Endpoints](#admin-endpoints)). The normal surfaces for non-admin callers are `POST /v1/ingest?table={table}` for writes, `POST /v1/query?table={table}` for structured reads, and `GET/POST /v1/pipes/{name}` for pre-defined queries — none of which expose raw SQL.
:::

`/v1/ops/query` is the only sanctioned surface for non-insert mutations (the ingest pipeline is insert-only). Granting raw-SQL access to a non-admin role via the policy engine is no longer supported: authenticate with the admin role (`admin_role`).

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
curl -X POST http://localhost:8080/v1/ops/query \
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
| `filters` | object[] | No | WHERE conditions (`column`, `op`, `value`). Ops: eq, neq, gt, gte, lt, lte, in, like. A `null` value is a `400`. |
| `group_by` | string[] | No | GROUP BY columns. |
| `order_by` | object[] | No | ORDER BY clauses (`column`, `dir`). |
| `limit` | int | No | Max rows. Omitted or above the configured `query.default_max_rows` (default 10,000) → silently capped at that value; a policy `max_rows` can lower it further (see [Access Control](/access-control#resource-limits)). |
| `time_range` | object | No | Time window (`column`, `since`, `until`). `since`/`until` accept RFC3339 or Go-duration relative values ("1h", "30m", "7d", "2w" — day and week suffixes expand to hours). Relative values mean that long *ago*. The window applies only when `column` and `since` are set — an `until` without `since` is ignored. |

:::note[Filter values bind as strings]
Every bound value — a caller's filter, a policy row filter, an insert `check` — binds as a `{p:String}` parameter and is compared under the column's own type. One rule across all three surfaces, and the answer is the server's: send ClickHouse's own spelling for a value and it reads it. There is no RFC3339 sniff any more, so a `"2026-01-01T00:00:00Z"` filter on a `DateTime` column is handed over verbatim — fine on every ClickHouse this repo pins (≥ 26.5 reads it natively), a per-query `500` below that line.

On **this endpoint** an `in` list binds as one `Array(String)` parameter, and ClickHouse caps a query-string parameter at about 64 KiB of literal text — roughly 6,000 short elements. Past that the query fails with ClickHouse's own `500` rather than a clean `400`; the 1 MiB request-body cap alone would have allowed more. Stream row filters bind their `in` elements one parameter each and carry no such ceiling.
:::

:::note[Identifier names]
Table, column, and alias names may contain any characters ClickHouse accepts — dots, spaces, unicode, reserved keywords — because every identifier is backtick-quoted automatically. The one exception is a name containing a literal `?`, which is rejected with `400`: the builder assembles positional placeholders before rewriting them to ClickHouse's named parameters, and a `?` inside an identifier would desync that rewrite ([#279](https://github.com/Wave-RF/WaveHouse/issues/279)).
:::

**Response:**

JSON array of result rows, **rendered by ClickHouse**: the query runs over its HTTP interface with `FORMAT JSONEachRow`, and WaveHouse frames the lines into an array without re-encoding a value. So every type is spelled the way the connected server spells it, per version, with no WaveHouse conversion table in between.

| ClickHouse type | JSON |
| --- | --- |
| `DateTime`, `DateTime64` | `"2026-06-21 04:00:00.123"` — space-separated, no `Z`, in the column's declared zone else the server's; byte-identical to the [SSE stream](#get-v1stream--server-sent-events-stream) for the same row (see [Timestamp rendering](#timestamp-rendering)) |
| `Decimal*` | a JSON **number** (`12.5`), not a string |
| `Int64`/`UInt64` past 2^53 | an unquoted number — still lossy in a JavaScript `number`; read it as text if you need every digit |
| `FixedString(n)` | a string padded to `n` bytes with `\u0000` |
| `Enum*` | the name, not the ordinal |
| `Nullable(T)` | `null` for a SQL `NULL` |
| `Array`, `Map`, `Tuple` | ClickHouse's own JSON for the container |
| `NaN` / `Inf` | `null` (ClickHouse's default rendering) |

Keys come back in **SELECT order**, not alphabetical. The response carries `X-Cache: HIT` or `X-Cache: MISS` — this endpoint shares the in-process L1 (Ristretto) + singleflight machinery (unlike `/v1/ops/query`, which always hits ClickHouse) and the cache stores ClickHouse's own bytes.

The inbound request body is capped at 1 MiB; a body over the cap is rejected with `413`. A query AST is bounded by nature (far under 1 MiB even with a large `in`-list), and the cap blocks a single-request memory-exhaustion vector on this public endpoint. Set a tighter or higher outer limit at your [reverse proxy](/reverse-proxy#request-body-size-limits) — but it can only narrow the effective limit, not raise it past this cap.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"unknown column: x"}` | Schema validation error — an unknown column, a bad aggregation, or an unparseable `time_range` `since`/`until` (neither a relative duration nor an RFC3339 timestamp) |
| 400 | `{"error":"filter value must not be null"}` | A filter carries `"value": null`. It is refused rather than answered: `col = NULL` is never true, and an empty parameter would silently ask a different question |
| 500 | `{"error":"clickhouse query: Code: 158. DB::Exception: … (TOO_MANY_ROWS) …"}` | ClickHouse refused the query — a resource limit, a type mismatch, anything else its engine raises. The body carries ClickHouse's own wording |
| 403 | `{"error":"forbidden"}` | Role lacks select permission on table |
| 403 | `{"error":"column \"x\" not allowed"}` | Column denied by policy |
| 403 | `{"error":"aggregation \"x\" not allowed"}` | Aggregation fn denied by policy |
| 404 | `{"error":"unknown table: x"}` | Table not found |
| 413 | `{"error":"request body exceeded 1048576 bytes"}` | Request body over the 1 MiB cap |

---

### `GET/POST /v1/pipes/{name}` — Execute Named Pipe

Executes a pre-defined named query (pipe) with parameter binding. Parameters can be supplied via query string and/or JSON body. Results are cached in the shared L1 (Ristretto) with singleflight coalescing — same machinery as the structured query endpoint, and again, unlike `/v1/ops/query`.

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

The POST parameter body is capped at 1 MiB; a body over the cap is rejected with `413` (the same 1 MiB parameter/AST-body cap as [`POST /v1/query`](#post-v1querytabletable--structured-query) — see [reverse proxy → body limits](/reverse-proxy#request-body-size-limits)). A malformed-but-within-cap body is ignored rather than rejected, since parameters may legitimately come from the query string alone.

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

**Response:** SSE stream (`text/event-stream`). Data events include an `id:` field set to the event's `received_timestamp`. The stream opens with a `: connected` comment and emits a minimal `:` keepalive comment periodically (every 30 seconds by default), which keeps a quiet connection from being closed by a proxy; both are standard SSE comments that `EventSource` ignores (raw consumers should skip `:`-prefixed lines).

**Row values arrive positionally, and the column names are announced separately.** Before the first row, and again whenever the column list changes, the stream sends an `event: schema` frame naming the columns of the rows that follow — in order, already reduced to what the caller's role may read. That re-announcement is **not** guaranteed after a gap-fill across a column change; see the arity note below. Every data frame's `row` array then has exactly one value per announced column, in that order. `schema` is a **named** SSE event, so a browser `EventSource` must `addEventListener('schema', …)` — it never reaches `onmessage`. A schema frame carries **no** `id:` line, so it never moves the client's `Last-Event-ID`. In the example below the table has its own `received_timestamp` **column**, which collides by name with the frame's top-level `received_timestamp` **field** — they are different values: the field is when WaveHouse received the event (WaveHouse's own RFC 3339 timestamp), the row slot is that column as ClickHouse rendered it (a record that omitted it carries the evaluated `DEFAULT`, not `null` — see [Timestamp rendering](#timestamp-rendering)).

```text
event: schema
data: {"table_name":"clicks","columns":["page","button","score","received_timestamp"]}

id: 2026-03-24T12:00:00.123Z
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:00.123Z","row":["/home","signup",42.5,"2026-03-24 11:59:58.512"]}

id: 2026-03-24T12:00:01.456Z
data: {"table_name":"clicks","received_timestamp":"2026-03-24T12:00:01.456Z","row":["/pricing","cta",7,"2026-03-24 12:00:01.456"]}
```

A raw consumer must keep the most recent announced column list and zip each `row` against it; a value the record did not carry arrives as `null` in its slot rather than being omitted, so positions never shift. **Check arity before zipping:** drop a `row` whose length disagrees with the last announced list rather than zipping it, because the announcement is not guaranteed in one case — a connection that gap-fills across a column change may receive live rows with no fresh announcement until the columns next change or it reconnects ([#543](https://github.com/Wave-RF/WaveHouse/issues/543)). An arity check covers an added or removed column; a *same-length* change (a `RENAME COLUMN`, or a drop paired with an add) it cannot see, and reconnecting is what resynchronizes. Separately, a replay spanning a server upgrade across the v2 ingest envelope silently omits the pre-upgrade events — see [Upgrading across the v2 ingest envelope](/deployment#upgrading-across-the-v2-ingest-envelope). The TypeScript SDK does this for you and still yields row objects — `.stream()` and `.liveQuery()` are unchanged. The announcement is **per connection**, so a client that joins mid-stream is told the columns before it is sent a row, and a reconnect is told again.

Each SSE connection is bound to a single `?table=`; to consume multiple tables, open one connection per table.

Values of top-level `DateTime`/`DateTime64` columns inside `row` are ClickHouse's own rendering of the stored value — the exact bytes chtypes' `RowsExport` produced for that record (see [Timestamp rendering](#timestamp-rendering)), not a WaveHouse rewrite — so a live event and a `/v1/query` read of the same row agree on spelling **by construction**, with no separate canonicalization step to keep in sync ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)). A column declared with a non-UTC zone streams in that zone, not normalized to UTC; parse the timestamp with a zone-aware parser rather than assuming `Z`.

**Note:** When access control policies are active, streamed events are filtered per the caller's role: tables without `select` permission are skipped, denied columns are removed from each event, and the role's [row-level `filter`](/access-control#row-level-security) is compiled and evaluated per subscriber against the caller's JWT claims — supplied by the connection's token (the `Authorization` header, or the `?token=` fallback above), with replayed gap-fill events filtered the same way. This runs through the same in-process ClickHouse parser (chtypes) that validates ingest, so every column type compares exactly as it would in the query path's `WHERE` clause — a connection is never delivered a row the query path would hide for that role, and a predicate that can't compile or evaluate withholds the row instead of guessing (see [the enforcement caution](/access-control#where-each-rule-is-enforced) for the fail-closed reasons). The residual payload-vs-stored case is an event whose insert later fails outright at ClickHouse — a connectivity fault or batch error, not a data-shape problem chtypes would already have caught — which the caution documents. The connection's claims are captured once, when the stream is established — a policy change applies from the next live event (an in-flight gap-fill finishes under the policy snapshot taken when the stream opened), but an expired token or changed claims take effect only when the client reconnects.

**CORS:** `/v1/stream` honors the `cors.allowed_origins` allowlist (settings directory) like every endpoint. Note that a **header-authenticated stream preflights before it connects** — `Authorization` is not CORS-safelisted — where a bare `EventSource` never preflighted at all: its request is not a `fetch()`, so Fetch's unsafe-request flag is never set and `Last-Event-ID` rides on the plain `GET`. Both headers are allow-listed, so an allowed origin connects *and* resumes cross-origin.

:::caution[Behind a proxy: disable response buffering]
SSE needs one bit of proxy configuration: disable response buffering, or the proxy holds events until a buffer fills and clients receive nothing in real time. Idle timeouts are handled for you — the `:` keepalive comment above keeps a quiet stream alive under typical proxy/tunnel idle windows ([#226](https://github.com/Wave-RF/WaveHouse/issues/226)), so raising the idle/read timeout is now optional. The TypeScript SDK's stream transport and browser `EventSource` both auto-reconnect (resuming via `Last-Event-ID`) if a connection drops. See [Behind a reverse proxy → Server-Sent Events](/reverse-proxy#server-sent-events-sse) for nginx/Caddy/Cloudflare specifics.
:::

**curl example:**

```bash
# Subscribe to a specific table
curl -N "http://localhost:8080/v1/stream?table=clicks"

# With gap-fill
curl -N "http://localhost:8080/v1/stream?table=clicks&since=2026-03-24T11:00:00Z"
```

---

### Admin Endpoints

Every admin-gated surface lives under the `/v1/ops/*` prefix, behind a single `RequireAdmin` gate: schema discovery, DLQ stats, and the pipe and settings-reload endpoints below, plus the raw-SQL passthrough [`POST /v1/ops/query`](#post-v1opsquery--query-clickhouse) documented with the query endpoints above. They require the policy `admin_role` (`"admin"` by default, exact case-sensitive match) — or the non-JWT [operator key](#authentication), which reaches the same surface without a token; other callers get 401 (present-but-invalid token) / 403, and the quickstart's trial `public` role cannot call any of them. There is no separate `service` role. The JWT middleware always runs — a tokenless request (or a valid token without a role claim) resolves to the `default_role` (not the admin role unless `default_role` is deliberately set to it — a loudly-warned dev-only setting) and is denied `403`, while a present-but-invalid token keeps its stashed verification error and is denied `401`.

No admin endpoint in this section accepts a request body — they are reads and triggers; the settings directory's files are the only write path. The raw-SQL `POST /v1/ops/query` carries the 16 MiB bulk-payload cap documented with the query endpoints above.

#### `GET /v1/ops/schema` — List All Table Schemas

Returns all discovered ClickHouse table schemas.

**Response:**

```json
[
  {
    "name": "clicks",
    "columns": [
      {"name": "page", "type": "String", "is_nullable": false, "has_default": false, "position": 1},
      {"name": "button", "type": "String", "is_nullable": false, "has_default": false, "position": 2},
      {"name": "score", "type": "Float64", "is_nullable": false, "has_default": false, "position": 3},
      {"name": "received_timestamp", "type": "DateTime64(3, 'UTC')", "is_nullable": false, "has_default": true, "default_kind": "DEFAULT", "default_expression": "now64(3, 'UTC')", "position": 4}
    ]
  }
]
```

---

#### `GET /v1/ops/schema?table={table}` — Get Table Schema

Returns the schema for a specific table.

**Response:**

```json
{
  "name": "clicks",
  "columns": [
    {"name": "page", "type": "String", "is_nullable": false, "has_default": false, "position": 1},
    {"name": "button", "type": "String", "is_nullable": false, "has_default": false, "position": 2}
  ]
}
```

Per-column fields: `name`, `type` and `is_nullable` describe the column; `position` is its 1-based ordinal in the table's declaration order (always present, and the order `columns` itself is in); `has_default` says whether it declares any default at all, while `default_kind` (`DEFAULT`, `MATERIALIZED`, `ALIAS`, `EPHEMERAL`) and `default_expression` say which and what — both omitted when the column declares none. A `MATERIALIZED` or `ALIAS` column is computed and **not** insertable, so it never appears in an ingest envelope or an SSE `schema` frame, though it is still reported here and can still be selected by name. An `EPHEMERAL` column is the reverse: insertable — it exists to be written — but never stored and never selectable at all, so it can appear in a stream's column list while no query can return it. The table's `CREATE TABLE` statement is captured on the same refresh but is deliberately **not** exposed here — for a table backed by an external engine it renders that engine's wiring — endpoint, bucket/host, database, username, access key id. (ClickHouse masks the password as `[HIDDEN]` from ~23.9; the topology is what is withheld here.)

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason) |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 404 | `{"error":"table not found"}` | Table not in discovered schemas |

---

#### `POST /v1/ops/schema/refresh` — Refresh Schemas

Triggers an immediate re-discovery of ClickHouse table schemas, then returns the refreshed schema list (same array shape as `GET /v1/ops/schema`). Admin-only, like the rest of this section.

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
      {"name": "page", "type": "String", "is_nullable": false, "has_default": false, "position": 1}
    ]
  }
]
```

---

#### `GET /v1/ops/dlq/stats` — DLQ Statistics

Returns per-table message counts in the Dead Letter Queue. Admin-only, like the rest of this section. Whether a poison row lands here is the settings directory's [`dlq.enabled`](/settings-directory#dead-letter-queue) switch (global or per table); the stream and this endpoint always exist. Before any failure has ever occurred, the endpoint returns `200` with `{"tables":{},"total":0}`.

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

#### Access Control Policy — no HTTP surface

The policy has no endpoints: it is the settings directory's [`policies.json`](/settings-directory#policiesjson), read, edited, and validated where it lives. Check a draft with `wavehouse validate` on the edited directory — it enforces everything adoption enforces, including the cross-file role references against `roles.json` — then let the watcher adopt it or trigger [`POST /v1/ops/settings/reload`](#post-v1opssettingsreload--reload-settings-directory), whose findings report exactly why a rejected directory was refused. The document anatomy (`default_role`, `admin_role`, `tables`) is covered in [Access Control](/access-control#anatomy-of-a-policy).

#### `GET /v1/ops/pipes` — List Named Pipes

Returns every adopted named query pipe — the settings directory's [`pipes.json`](/settings-directory#pipesjson). Pipes have no write endpoints: edit the file and reload.

#### `GET /v1/ops/pipes/{name}` — Get Named Pipe

Returns a specific named pipe definition:

```json
{
  "name": "top_pages",
  "sql": "SELECT page, count() as views FROM clicks WHERE received_timestamp >= {{start_date}} GROUP BY page LIMIT {{limit}}",
  "parameters": [
    {"name": "start_date", "type": "string", "required": true},
    {"name": "limit", "type": "number", "required": false, "default": 100}
  ],
  "description": "Top pages by view count",
  "allowed_roles": ["viewer"]
}
```

**`allowed_roles`** restricts execution: the caller's role (a tokenless or roleless request is first resolved to the policy `default_role`) must appear in the list. The admin role (`admin_role`) always passes. Matching is exact — there is no `"*"` wildcard — and empty-string entries are ignored. An empty or omitted list authorizes **nobody but the admin role**, and a request whose role is absent or unlisted is denied (fails closed).

#### `POST /v1/ops/settings/reload` — Reload Settings Directory

Re-validates the [settings directory](/settings-directory) — `roles.json`, `policies.json`, `pipes.json`, and `config.json` — and adopts it as one snapshot when no finding is an error — the same serialized reload path the file watcher and `SIGHUP` use. This is how a policy or pipe edit is applied on demand.

```json
{
  "adopted": true,
  "findings": [
    { "severity": "warning", "file": "policies.json", "message": "empty document — no policy; every request will be denied (fail closed)" }
  ]
}
```

`200` when adopted (warnings allowed); `422` when validation rejected the directory — the previous settings stay in effect, and `findings` says why.

## Event Message Format

### Internal Wire Format (NATS)

The message format used on NATS JetStream between ingest and the batch consumer:

```json
{
  "table_name": "clicks",
  "scope": "",
  "received_timestamp": "2026-03-24T12:00:00.123456789Z",
  "format": "JSONCompactEachRow",
  "columns": ["page", "button", "score", "received_timestamp"],
  "row": ["/home", "signup", 42.5, "2026-03-24 12:00:00.123"]
}
```

The request that produced this envelope omitted `received_timestamp` (`DEFAULT now64(3, 'UTC')` on the table); chtypes evaluated the default before publish, so `row` carries the resulting timestamp — ClickHouse's own rendering, not `null` and not a WaveHouse rewrite.

| Field | Type | Description |
| ----- | ---- | ----------- |
| `table_name` | string | Target ClickHouse table (from URL). |
| `scope` | string | Reserved; currently always empty. |
| `received_timestamp` | string | RFC 3339 nano timestamp when WaveHouse received the event. |
| `format` | string | Row format. Always `JSONCompactEachRow` today; stated on the wire so a reader can tell an envelope it understands from one it doesn't. |
| `columns` | string[] | The table's **wire** column names, in declaration order — what each position in `row` means (`internal/typelayer.Table.WireColumns`: the insertable subset minus any `MATERIALIZED`, `ALIAS`, or `EPHEMERAL` column — none of the three can be named in an `INSERT`, or is ever part of a published row). |
| `row` | array | One `JSONCompactEachRow` line: one value per entry in `columns`, in that order — the exact bytes ClickHouse's own writer produced for this stored row (`internal/typelayer.Table.Ingest`, via chtypes). A column the request body omitted carries its evaluated `DEFAULT` (or the type's implicit zero value where none is declared), not `null` — the same as a native `INSERT` naming fewer columns than the table has. `DateTime`/`DateTime64` values are ClickHouse's own rendering (see [Timestamp rendering](#timestamp-rendering)), and an out-of-range integer is wrapped the way a real `INSERT` wraps it. |

`columns` and `row` are only meaningful together: a reader that cannot pair them — a length mismatch, an undecodable row, a `columns` list naming one column twice — has no way to map a value to a column. Both readers also refuse an envelope whose `format` they do not recognize, which is what a pre-v2 message looks like. Either way the SSE fan-out withholds such an envelope rather than guess, and the batch consumer parks it on the DLQ with `X-DLQ-*` headers — acking and dropping it only where the DLQ is switched off for that table, since it can never insert on retry. Both outcomes increment `wavehouse_ingest_poison_total`, separated by its `disposition` label (`parked` / `dropped`).

### Client-Facing Format (SSE)

The same positional row, in a **narrower** body: a data frame carries `table_name`, `received_timestamp` and `row`, and neither `scope` nor `format` — those are envelope fields and do not cross to the client. The column list travels separately, in its own `event: schema` frame sent before the first row and again whenever the list drifts — though not guaranteed to re-announce after a gap-fill across a change ([#543](https://github.com/Wave-RF/WaveHouse/issues/543)) — rather than repeated on every event — see [`GET /v1/stream`](#get-v1stream--server-sent-events-stream) for the frame sequence. The announced list is the caller's **projected** columns (the role's allow/deny rules applied), so both it and the `row` it describes can be narrower than the envelope's — the row carries exactly one value per announced column, not per envelope column.

```json
{
  "table_name": "clicks",
  "received_timestamp": "2026-03-24T12:00:00.123456789Z",
  "row": ["/home", "signup", 42.5]
}
```

Three values, where the envelope above has four: this is the frame a role restricted to `page`, `button` and `score` receives, and its `event: schema` frame announces exactly those three.

## Dead Letter Queue (DLQ)

When a batch insert to ClickHouse fails (e.g., type errors, connection issues), the worker re-inserts the batch row by row: rows that succeed are acked, and only the rows that fail again are published to the DLQ NATS stream (`WAVEHOUSE_DLQ`) under subjects `dlq.{table}`. This prevents infinite retry loops — those messages are ACKed from the main stream and moved to the DLQ for inspection. A second class lands here too: an envelope the worker cannot *read* at all — malformed JSON, an unknown **or absent** `format` (a pre-v2 message has no `format` field at all, which is how it presents here), or `columns` and `row` that do not pair — is parked without ever reaching a table batch, which is what an operator sees after upgrading across the wire change without draining first. **Two different body shapes land here, and a consumer must not assume one decoder.** A row that failed its INSERT is parked as the `EventMessage` envelope above. An envelope the worker could not *read* is parked as **its original bytes, verbatim** — `parkOnDLQ` republishes what arrived — so it is whatever the producer sent: a pre-v2 `data` object, malformed JSON, or a v2 envelope whose `columns` and `row` do not pair. Being undecodable as an `EventMessage` is precisely why it was parked, so decode defensively and fall back on the `X-DLQ-Error` header, which names the reason. For the first shape the body is the published `EventMessage` envelope (`{"table_name":…,"scope":"","received_timestamp":…,"format":…,"columns":[…],"row":[…]}` — the failed row is the `row` array, read against `columns`, its `DateTime`/`DateTime64` values exactly as published: ClickHouse's own rendering of the stored value, since chtypes already validated and coerced the record before it was ever published — see [Timestamp rendering](#timestamp-rendering)); the failure reason, table, and time travel in the `X-DLQ-Table` / `X-DLQ-Error` / `X-DLQ-Timestamp` message headers. Because chtypes catches the type/shape problems synchronously at ingest, a row that reaches this DLQ path failed for a reason chtypes couldn't have caught up front — a ClickHouse-side outage or a genuine insert-time fault — not a data mismatch.

Use `GET /v1/ops/dlq/stats` to monitor DLQ depth.

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
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
```

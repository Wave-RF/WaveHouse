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

The JWT must use HMAC signing (HS256/HS384/HS512) or be validated via a JWKS endpoint (configured via `auth.jwks_url` in the [settings directory](/settings-directory#authentication) — per tenant, over [a nested directory](/deployment#the-nested-settings-directory), so a JWKS-issued token verifies only under the tenants whose `jwks_url` names its provider's key set; tenants that leave `jwks_url` empty all verify against the shared boot `jwt_secret` and accept each other's tokens, or validate no token at all while it is unset). While a tenant's JWKS has not been fetched yet — at boot, or after a reload built or rebuilt its verifier (a new tenant folder, one adopted again after being rejected or removed, or a changed `jwks_url` or `role_claim`) — a request carrying a token is answered `503 {"error": "token verifier not ready: the tenant's JWKS has not been fetched yet"}` with `Retry-After: 30` rather than evaluated under the `default_role`; requests without a token are unaffected, and so is one authenticated by a valid operator key, which is checked first and never consults the verifier. The accepted signing algorithm is pinned to the active verifier and checked *before* any key is consulted: an HMAC deployment accepts only `HS256`/`HS384`/`HS512`, and a JWKS deployment accepts only the asymmetric family (`RS256/384/512`, `ES256/384/512`, `PS256/384/512`, `EdDSA`). Tokens using `alg: none`, or an algorithm from the other family (e.g. an `HS256` token sent to a JWKS deployment), are rejected outright.

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

### ClickHouse errors on the query paths

When ClickHouse fails a query on [`POST /v1/query`](#post-v1querytabletable--structured-query), [`/v1/pipes/{name}`](#getpost-v1pipesname--execute-named-pipe) or [`POST /v1/ops/query`](#post-v1opsquery--query-clickhouse), the status comes from **what kind of failure it was**, not from ClickHouse's HTTP status: ClickHouse answers a syntax error, a missing grant and an overloaded server alike with HTTP `500`. WaveHouse reads the ClickHouse exception code (the `X-ClickHouse-Exception-Code` header or the `Code: NNN.` in the message, or the native driver's exception) and answers with two extra fields alongside `error`:

```json
{"error": "Code: 62. DB::Exception: Syntax error: …", "code": "clickhouse.rejected", "retryable": false}
```

| Status | `code` | `retryable` | When |
| ------ | ------ | ----------- | ---- |
| 400 | `clickhouse.rejected` | `false` | ClickHouse read the statement and refused it: bad SQL, an unknown table, column or identifier, a type mismatch — any exception code not listed below; also a `413` with no exception code from a proxy in front of ClickHouse (the statement is too large for it). Sent again unchanged, it fails the same way |
| 400 | `clickhouse.limit_exceeded` | `false` | The query outran a limit it ran under: rows read or returned, bytes (`TOO_MANY_ROWS`, `TOO_MANY_BYTES`, `TOO_MANY_ROWS_OR_BYTES`), or, on `/v1/query`, the role's own `max_memory_usage` cap or a `max_execution_time` cap no longer than the tenant's `clickhouse.query_timeout` (`TIMEOUT_EXCEEDED`, `TOO_SLOW`, `MEMORY_LIMIT_EXCEEDED`). Narrow the query |
| 403 | `clickhouse.access_denied` | `false` | The ClickHouse user WaveHouse connects as lacks a grant the statement needs (`ACCESS_DENIED`). Grant it, or run something it may. Logged at `WARN` too |
| 502 | `clickhouse.misconfigured` | `false` | ClickHouse refused the credentials or database WaveHouse connects with: a wrong password, an unknown or expired user, a refused address, the database denied, a `401`/`403` from a proxy in front of it, or any other `3xx`/`4xx` with no exception code — a wrong path, a redirect WaveHouse does not follow, or a read the server refused as a write (`READONLY`, from a `readonly=1` profile or a statement the mutation classifier missed) (a codeless `408`/`429` is `503`, a `413` is `400 clickhouse.rejected`). Every query fails until the operator fixes the tenant's `clickhouse` settings or `WH_CH_PASSWORD`, so retrying does not help. Logged at `WARN` |
| 502 | `clickhouse.response_too_large` | `false` | The response outgrew the 64 MiB the reader buffers (`/v1/query`, pipes and `/v1/ops/query` alike). Narrow the query or add a `LIMIT` |
| 503 | `clickhouse.unavailable` | `true` | ClickHouse, or the way to it, could not take the query now: connection refused or dropped, a timeout, too many queries, memory pressure, lost replicas or Keeper, or a `502`/`503`/`504`/`429`/`408` from a proxy. `Retry-After: 5` |
| 500 (`/v1/query`, pipes) / 502 (`/v1/ops/query`) | `clickhouse.unknown` | `true` | A failure with no verdict: no exception code and no recognizable transport error |

A [pipe that writes](/pipes#pipes-that-write) answers with the same status and `code`, but always `retryable: false` and with no `Retry-After`: the statement may have run, so a retry could run it twice.

On `/v1/query`, when the role sets `max_execution_time`, ClickHouse enforces it and reports an overrun as `TIMEOUT_EXCEEDED`, answered `400 clickhouse.limit_exceeded`; WaveHouse then waits two seconds past the cap before giving up itself, and that give-up — like a wait for a pooled connection or a dial timeout — is `503 clickhouse.unavailable`. If the tenant's `clickhouse.query_timeout` is shorter than the cap, that timeout is what ClickHouse enforces, and its overrun is answered as for a role with no cap. When the role sets `max_memory_usage`, every `MEMORY_LIMIT_EXCEEDED` is taken as that cap and answered `400`, even one caused by the server's total memory. Without a role cap of that kind, and always on pipes and `/v1/ops/query`, a timeout or memory limit is `503 clickhouse.unavailable`: it can be the server's state as much as the query's ([#620](https://github.com/Wave-RF/WaveHouse/issues/620)). The classes are the ones the ingest worker uses to decide between retrying a batch and dead-lettering it ([ingest pipeline](/ingest-pipeline#when-clickhouse-cannot-take-an-insert)); the lists of exception codes live in `internal/chconn/errclass.go`.

**Why a missing grant is a `403`.** A query path runs as the ClickHouse user in the tenant's settings, not as the caller, so `ACCESS_DENIED` is in one sense WaveHouse's configuration. It is still a verdict on *this statement*: ClickHouse understood it and refused it, the same statement is refused every time, and other statements from the same caller succeed. That is a `403`, and it matters most on `/v1/ops/query`, where the admin wrote the statement — a `CREATE USER` through a user without the grant is the admin asking for something this deployment does not allow. A `5xx` would tell clients and monitors that ClickHouse is down and invite retries of a request that can never pass. Denials that refuse every query, not one statement — the credentials, the user, the database — are the operator's to fix, so they are `502 clickhouse.misconfigured`, still not retryable.

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

The boot-degraded response lets an operator `curl /livez` to learn why the gateway isn't ready to serve traffic yet, instead of grepping a restart-loop log. The binary is bound on `:8080` and serves diagnostics, but is not yet accepting ingest/query traffic. Schema discovery retries with jittered exponential backoff (each wait a random time below a bound that doubles from 2s to 60s); once a Refresh succeeds, `/livez` flips to `200` and stays there for the rest of the process lifetime — transient ClickHouse blips after that point are reflected in `/readyz`, not `/livez`.

Over a [nested settings directory](/deployment#the-nested-settings-directory) the probe reads every tenant together: `/livez` is `503` while **no** tenant has completed a first discovery — the diagnostic names the tenant whose attempt it reports (`schema discovery: tenant acme: …`), and reads `no tenant has completed a first discovery yet` before any attempt, when the directory serves no tenant, and once the tenant it named stops being served — and `200` from the first tenant's success on, for the rest of the process lifetime. A tenant whose ClickHouse is unreachable after that is a log line and the `wavehouse_schema_refresh_failures_total{tenant}` counter, never a probe failure. A tenant that has not completed its own first discovery answers `503` (`schema not loaded yet`) on its schema-aware routes until it does; one whose ClickHouse goes down after that answers query errors, as a single-tenant server does.

---

### `GET /readyz` — Readiness Probe

> Canonical name (current Kubernetes convention). Also served at **`/ready`** — a deprecated alias kept for v0.1.x and scheduled for removal in v0.2.0.

Returns `200 OK` if the process is fully booted (schema discovery complete) and ClickHouse is currently reachable. Returns `503 Service Unavailable` otherwise. No authentication required. Over a [nested settings directory](/deployment#the-nested-settings-directory) it pings every open ClickHouse pool at once and answers `200` at the first one that does, so a tenant whose ClickHouse does not answer does not make the process unready; the `503` names every pool that failed (one per line in `error`) when none answers — including when no pool is open at all, a directory serving no tenant.

**Response (ready):**

```json
{"status": "ready"}
```

**Response (not ready):**

```json
{"status": "not ready", "error": "localhost:9000 database default user default: dial tcp 127.0.0.1:9000: connect: connection refused"}
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

★ Once boot completes, `/livez` no longer tracks ClickHouse state — a runtime ClickHouse outage surfaces in `/readyz` only. This is what keeps a Kubernetes `livenessProbe` from restart-looping the pod during a transient backend blip (see [Deployment → Boot-time degraded mode](/deployment#boot-time-degraded-mode)). Over a nested directory the rows hold per process rather than per tenant: "ClickHouse up" means at least one tenant's pool answers, and "discovery complete" means one tenant's has.

---

### `GET /v1/health` — Liveness ping (public, content-free)

Returns **`200 OK` with an empty body** once the gateway is past boot, or **`503 Service Unavailable`** (also empty) while boot-time schema discovery is still failing. Like every `/v1` route outside `/v1/ops/*` it [resolves a tenant](/deployment#multi-tenant-deployments) first, so a malformed or unknown `X-Tenant-ID` answers `400`/`404` before the probe runs, and — over a [nested settings directory](/deployment#the-nested-settings-directory) — a tenant whose settings folder was rejected answers a `503` that carries the usual JSON error body rather than this route's empty one, as does a request carrying a token, with no valid operator key, while that tenant's [JWKS has not been fetched yet](#authentication) (`Retry-After: 30`; the SDK sends its token on this ping too). No authentication required and no response body — the caller only branches on the status code, so there's nothing to JSON-encode or cache per request. Resolving the tenant makes the ping an unauthenticated answer to whether a tenant is served, deliberately: every tenant route gives an unknown tenant the same `404` before authenticating, since authenticating takes that tenant's own verifier, so the ping reveals nothing the others don't. It also answers a served tenant's ping from that tenant's own CORS list, where a tenant-exempt route answers from tenant `0`'s, which a nested directory need not have.

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

### A process without the `api` role — the ops listener

A process whose [`roles`](/configuration#process-roles) leave out `api` (an ingest worker, possible with [`mq.backend: nats`](/deployment#external-nats)) serves only these routes on `server.port`:

| Route | Notes |
| ----- | ----- |
| `GET /livez` (and `/healthz`, `/health`) | `200` once booted. It does not wait for schema discovery, which only the API runs. |
| `GET /readyz` (and `/ready`) | In a process running `ingest`, `200` when a ClickHouse pool answers, as above. |
| `GET /version` | As above. |
| The metrics path | When `prometheus.port` is `0`. |
| `POST /v1/ops/settings/reload` | As [below](#post-v1opssettingsreload--reload-settings-directory), but it accepts only the [operator key](#authentication): no token verifier runs without the `api` role, so an admin token is `401`. |

Every other route answers `404`, including every tenant route. Under `/v1/ops`, the operator-key check comes first, so a request there gets `403` without a credential, and `401` for a bearer token.

---

### `POST /v1/ingest?table={table}` — Ingest Data

Validates a body of records against the ClickHouse schema for `{table}` and publishes each accepted one to the message queue. Returns immediately — ClickHouse insertion happens asynchronously via the batch consumer. A single-object body answers `{"ok":true}` (or `{"duplicate":true}` when dedup is on); every other body answers the [batch summary](#batch-ingest).

**The body goes to ClickHouse's own parser as-is.** WaveHouse never decodes a record: validation, type coercion, `DEFAULT` substitution and timestamp parsing are ClickHouse's own, running in-process via [chtypes](/deployment#chtypes-artifacts) (`internal/typelayer`) — the exact code path a real `INSERT` runs. A rejection therefore carries ClickHouse's own message and its numeric error number as `exception_code` rather than a WaveHouse-authored sentence, and there is no separate coercion table to keep in sync with the server. A rejected single record answers `{"error","exception_code"}` with no string `code`; only a refusal of the whole request (a `header=present` header naming a column the table lacks) carries both, `code: "clickhouse.rejected"` and `exception_code`. The SDK reports either as `HTTP_400`, with the number in `error.details`.

**`Content-Type` is required and authoritative**: it declares the format and the bytes never override it.

| `Content-Type` | Body |
| --- | --- |
| `application/json` | one flat object, **or** a top-level array of them |
| `application/x-ndjson`, `application/ndjson`, `application/jsonl`, `application/jsonlines` | one object per line; always a batch |
| `text/csv`, `text/tab-separated-values` | positional, with ClickHouse's own header auto-detection — see [Positional formats](#positional-formats-csv--tsv) |
| `text/csv; header=absent`, `text/tab-separated-values; header=absent` | strictly positional, no header detection |
| `text/csv; header=present`, `text/tab-separated-values; header=present` | a header line naming the columns, in any order — see [Header formats](#header-formats-headerpresent) |
| anything else, or none | `415`, listing the accepted types |

The two JSON families are one format to ClickHouse; the declaration decides only how the body frames its records. The single thing the body still chooses is *arity within `application/json`*: the first non-whitespace byte picks an array (`[`) or a single object. Under a single-object body only the first object is read — concatenated objects after it are ignored, a `200` for one record; declare NDJSON for anything line-framed ([#561](https://github.com/Wave-RF/WaveHouse/issues/561)). The reverse now works: a JSON array declared `application/x-ndjson` ingests every element.

:::note[What counts as a valid declaration]
The header is parsed with Go's `mime.ParseMediaType` (RFC 9110 §8.3) and the **media type** decides the format, so no malformed *parameter* costs the request — `application/json; charset`, `application/json;;`, a value left mid-quote, a name repeated with different values all read as `application/json`. The one parameter that also decides a format is `header`, on `text/csv` and `text/tab-separated-values` only: `present` selects the header format, `absent` the strictly positional one, no `header` at all ClickHouse's default reading, and any other value is a `415`. A line whose parameters did not parse and that mentions `header` is a `415` as well, because guessing at it could ingest a declared header line as data or drop a data row as a header. Two more things are refused. A malformed parameter on a line that **also contains a comma** is a `415`, because the comma may be a second declaration joined on and the error cannot tell that from a comma inside data ([#563](https://github.com/Wave-RF/WaveHouse/issues/563)) — so `application/json; profile="a,b"` is fine and `application/json; profile="a,b"; charset` is not. And `Content-Type` is a **singleton** field (§5.3 forbids repeating it), so repeated header *lines* are accepted only when they agree, while a comma-joined value is refused outright: §8.3 warns that picking a member of the resulting pseudo-list is itself an interoperability and security hazard.

The 415 body quotes what you declared, bounded: at most **four distinct** header lines, each capped at 128 bytes and marked `…(truncated)` when cut, then `"…and N more"` counting every line not quoted, duplicates included. When declarations conflict, the one that actually disagreed is always quoted.
:::

The inbound request body is capped at 16 MiB and the `413` is decided before any record is processed, so nothing is published. The cap applies to every body shape — the whole body is read before it is parsed, so a line-framed batch is bounded exactly as a JSON array is. Split a larger upload across several requests, and set your own outer limit at the [reverse proxy](/reverse-proxy#request-body-size-limits). The `{table}` query parameter must name a table WaveHouse has discovered in ClickHouse; schemas refresh periodically.

:::note[Insert-only]
The ingest pipeline accepts only inserts. All other mutations — `DELETE`, `UPDATE`, `TRUNCATE`, `DROP`, `ALTER`, `REPLACE`, etc. — must be issued through [`POST /v1/ops/query`](#post-v1opsquery--query-clickhouse), which is restricted to the admin role (`admin_role`, the same gate as the rest of `/v1/ops/*`), or through an operator-authored [pipe that writes](/pipes#pipes-that-write): an operator authors its statement in `pipes.json`, and the roles in its `allowed_roles` run it with parameter values only.

The policy engine authorizes mutations by inspecting the columns being written. That works for inserts but not for predicate-driven mutations like `DELETE … WHERE` — there's no way to prove the predicate matches only rows the caller is allowed to touch. Routing those statements through the admin-gated raw-SQL surface, or through a pipe whose predicate the operator wrote, keeps the policy contract honest.
:::

**What ClickHouse decides, and what WaveHouse decides.** Everything about a *value* is ClickHouse's:

- A field the role may not write is indistinguishable from one the table does not have: both are code **117**, `Unknown field found while parsing JSONEachRow format: x`. So are `MATERIALIZED` and `ALIAS` columns — neither is ever part of a published row. An `EPHEMERAL` column is accepted as input wherever the format names its columns (the JSON family and the `…WithNames` formats) and feeds the `DEFAULT` expressions that read it, but it is never stored, selected or published; a positional CSV or TSV body carries the wire columns only.
- An omitted column, or an explicit `null` on one (WaveHouse pins `input_format_null_as_default`), takes its `DEFAULT` expression — evaluated by ClickHouse, including a volatile one like `now()` — or the type's implicit zero where none is declared, exactly as an `INSERT` naming fewer columns does.
- A coercion ClickHouse would make it makes here (a numeric string into an `Int*`, `"true"` into a `Bool`, an out-of-range integer wrapping); anything it would refuse fails synchronously in the ingest response with its real code, rather than surfacing later in the DLQ. `Nullable()` and `LowCardinality()` wrappers are transparent.

WaveHouse decides only policy: whether the role may insert at all, and whether the record satisfies the role's [`check` clauses](/access-control#insert-checks) — evaluated by the same compiled-filter engine as row-level security, in the same parse that validates the record and against the row ClickHouse produced, so a check sees stored values rather than the payload's spelling. A record ClickHouse refuses reports that refusal, never a check result. A record chtypes cannot evaluate at all — as opposed to accepting or rejecting it — is **declined** (`422`), which is not a data verdict.

**Error responses.** Rows marked **per-record** are reported in `results` on a batch body (the request itself stays `200`) and become the response status on a single-object body; every other row fails the whole request.

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"<ClickHouse message>","exception_code":<N>}` | **Per-record.** ClickHouse's parser refused the record; `exception_code` and the message are its own. `117` is an unknown field — which now includes a column the role may not write and any `MATERIALIZED`/`ALIAS` column; `27`/`26` are unparseable input; `6` out of range |
| 400 | `{"error":"Unknown field found in format header: 'x' at position 1 …","code":"clickhouse.rejected","exception_code":117}` | A `header=present` body whose header names a column the table — or the role's writable set — does not have, or names one twice. ClickHouse refuses the body before reading any record, so the whole request fails and nothing is published |
| 400 | `{"error":"invalid request body"}` | The body could not be read at all — a malformed transfer encoding, or a truncated upload (a body cut off *in transit*). A body that arrived complete but ends mid-value is not this error: a JSON array cut short is `invalid json: unterminated json array` below, while a single object or NDJSON cut mid-value is a per-record ClickHouse rejection (code 26 or 27) |
| 400 | `{"error":"empty body"}` (declared variants: `empty ndjson body`, `empty csv body`, `empty tsv body`, `empty csvwithnames body`, `empty tsvwithnames body`) | The body holds no bytes. A `header=present` body holding only its header line is a valid record-less batch (`200`, `total: 0`) |
| 400 | `{"error":"invalid json: unterminated json array"}` | A body declared `application/json` opening with `[` whose brackets do not balance — truncated, or structurally broken. It cannot be salvaged per record, so the whole request fails |
| 400 | `{"error":"missing dedupe id field \"event_id\""}` | **Per-record.** Only with `dedupe.require_id: true`, when the row carries no value for the configured `id_field` (an absent column, a `null` cell or an empty string — the value an omitted `String` id column stores). With `require_id: false` (the default) the row is published un-deduped instead. Either way it is logged at `WARN` and counted by `wavehouse_ingest_dedupe_missing_id_total` |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason rather than silently falling back to `default_role`) |
| 403 | `{"error":"forbidden"}` (empty-role variant: `forbidden: request has no role and no public default_role is configured`) | The resolved role lacks `insert` on the table — checked once, before any record |
| 403 | `{"error":"insert permissions were not resolved for this request"}` | The grant that resolved for the request is not an insert grant (an internal wiring fault, logged at `ERROR`). It is true for every record or none, so it fails the whole request — including an empty array, which answers `403` rather than `200` |
| 403 | `{"error":"check failed for column \"x\""}` (several: `check failed for columns "x", "y"`) | **Per-record.** The row does not satisfy the role's insert [`check`](/access-control#insert-checks). The filter is AND-joined over every checked column, so with more than one it names the set that was tested rather than guessing an attribution |
| 403 | `{"error":"policy check references column \"x\", which table \"t\" does not have"}` (also `… which is materialized and cannot be inserted`, the same for `alias`, and `… which is ephemeral and is never stored`) | **Per-record.** A **policy misconfiguration**, not a bad request: the role's `check` names a column the table lacks, one ClickHouse computes, or an `EPHEMERAL` one. None can be enforced, so the check would have passed silently while enforcing nothing. It fires on every insert by that role until the policy or the table is corrected, and names every offending column. `wavehouse validate` cannot catch it — it never sees the ClickHouse schema |
| 404 | `{"error":"unknown table: ..."}` | Table not found in the tenant's discovered schema |
| 413 | `{"error":"request body exceeded 16777216 bytes"}` | Request body over the 16 MiB cap |
| 415 | `{"error":"no Content-Type: ingest requires one of application/json, application/x-ndjson, application/ndjson, application/jsonl, application/jsonlines, text/csv, text/csv; header=present, text/csv; header=absent, text/tab-separated-values, text/tab-separated-values; header=present, text/tab-separated-values; header=absent"}` (declared variant: `Content-Type "text/plain": ingest requires one of …`; conflicting variant: `conflicting Content-Type declarations "application/json", "application/x-ndjson": ingest reads one format per request, and requires one of …`) | No `Content-Type`, an unsupported or unparseable one, a `header` value other than `present`/`absent`, a comma-bearing value that does not parse as a single media type, or repeated lines that disagree. Checked before the body is read |
| 422 | `{"error":"validation engine declined: <message>"}` | **Per-record.** chtypes could not evaluate the record at all — the artifact declined the shape, rather than the data being wrong. A `check` clause that could not be evaluated lands here too (`validation engine declined: the insert check for column "x" could not be evaluated`) |
| 500 | `{"error":"validation failed"}` | The parse itself failed for a reason that is neither the record's fault nor an unavailable tenant; logged. Nothing was published |
| 500 | `{"error":"dedupe failed"}` | Deduplication backend error |
| 503 | `{"error":"dedupe store unavailable"}` | Dedupe is on and its store cannot answer now: it is not open (for example, it failed to open on a reload), or a DynamoDB table is throttling, timing out or unreachable; `Retry-After: 5`. Nothing was published, so the retry is safe |
| 503 | `{"error":"schema not loaded yet"}` | The tenant's first schema discovery has not succeeded yet (its ClickHouse unreachable, or [no pool for it](/settings-directory#clickhouse)), so whether the table exists is not known; `Retry-After: 5`. Decided before the body is read |
| 500 | `{"error":"publish failed"}` | Message queue error whose outcome is unknown, other than a full queue or an unreachable broker (below): the event may have been stored. With dedupe on, the record's id is left to lapse with the dedupe lease ([`dedupe.lease`](/configuration#dedupe), 30 seconds by default) rather than given back: a retry inside the lease answers the in-flight `503`, and one after it is published under the same idempotency key, which the queue drops if the first copy was stored. The queue's duplicate window (two minutes) covers up to ~2×lease plus a margin, not just the lease itself, so a retry timed off `Retry-After` anywhere in this flow stores no second copy; a much later one is stored again. |
| 503 | `{"error":"service unavailable"}` | The tenant's ingest queue is full (backpressure, for that tenant alone) or not open (see [Message Queue](/settings-directory#message-queue)). Under [`mq.backend: nats`](/deployment#external-nats), the table's partition stream is full, which refuses every table in it, or the tenant's table holds as many unwritten rows as the stream allows one subject. Response includes `Retry-After: 30` header. With dedupe on, the record's id is given back, so the retry is published rather than reported as a duplicate. |
| 503 | `{"error":"service unavailable"}` | The message queue could not be reached or did not answer in time (`mq.ErrUnavailable`, a transient broker failure, not a full queue). Only under [`mq.backend: nats`](/deployment#external-nats), including a partition stream the operator deleted; the embedded broker never reports this, and its publish failures are the `500` above. As for the `500`, the record's id is left to lapse rather than given back, so a retry cannot land as a second copy; `Retry-After` is that lease, rounded up to whole seconds, when dedupe was on for the record, else the flat `Retry-After: 5`. |
| 503 | `{"error":"a request with the same dedupe id is in flight"}` | Dedupe is on and another request carrying the same id is still being published — usually a client's timeout-retry racing its own original. Its outcome decides whether this record is a duplicate, so retry after the `Retry-After` header (the dedupe lease, [`dedupe.lease`](/configuration#dedupe), 30 seconds by default). |
| 503 | `{"error":"ingest validation is unavailable"}` | The tenant's schema is not bound yet (no completed schema refresh), a table could not be compiled, its ClickHouse line has no installed chtypes artifact for its server version, or its server time zone differs from the zone this process already opened that line with (one process serves one server time zone per ClickHouse line). The body is generic on purpose — the cause, with zone names and artifact search paths, goes to the server log only. Only that tenant is refused — every other tenant keeps working — and it is decided before the body is read, with `Retry-After: 5`. See [Deployment → chtypes artifacts](/deployment#chtypes-artifacts) |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while the tenant's JWKS has not been fetched yet; refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

**curl example:**

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: application/json" \
  -d '{"page": "/home", "button": "signup", "score": 42.5}'
# → {"ok":true}
```

#### Timestamp rendering

WaveHouse rewrites timestamps in neither direction. **Inbound**, any spelling ClickHouse's own parser accepts under `date_time_input_format=best_effort` (the setting WaveHouse pins, both at chtypes' ingest compile and on the worker's `INSERT`) is accepted — RFC 3339 with any offset, a zone-less `YYYY-MM-DD[ T]HH:MM:SS[.fff]` read in the column's declared zone else the server's default, a Unix-seconds string, a bare integer (ClickHouse 26.8 reads a bare number in a `DateTime64` column as epoch **seconds**, so an epoch-millisecond number clamps to `9999-12-31` — send milliseconds as a quoted string, or as a decimal number of seconds), among the other forms its lenient parser reads. It is ClickHouse's grammar, not a reimplementation of it, so whatever a real `INSERT` into this table would accept, ingest accepts, with the same coercions and the same refusals.

**Outbound**, `DateTime`/`DateTime64` values in the NATS/SSE wire row and in `/v1/query` / `/v1/pipes/{name}` results are the exact bytes ClickHouse's writer produces, as RFC 3339 in UTC, whatever zone the column declares (ClickHouse's `date_time_output_format=iso`): `"2026-06-21T04:00:00.123Z"` for a `DateTime64(3)`, `"2026-06-21T04:00:00Z"` for a `DateTime`, with the fraction at the column's own precision, trailing zeros kept. Every consumer renders from the same stored value the same way, so SSE, `/v1/query` and `/v1/pipes/{name}` agree on spelling for a given row **by construction**, with no WaveHouse rewriting step to keep in sync ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)), and the strings order the same way the instants do. The raw-SQL proxy `/v1/ops/query` sets the same `date_time_output_format=iso`, so it spells a `DateTime`/`DateTime64` the same way; its other types are returned as ClickHouse renders them under `FORMAT JSON`. A `Date` column is `"2026-06-21"` throughout.

**One server time zone per ClickHouse line, per process.** The in-process parser takes its zone once, when a process first opens a ClickHouse line, and keeps it for the process's lifetime. A tenant whose server reports a different zone than the one that line was opened with is refused on its own — ingest answers `503`, a stream whose role has a row `filter` withholds its rows with reason `unavailable` — while every other tenant keeps working. Run tenants whose servers use different zones in separate processes.

**Row-level security compares instants, not spellings.** A stream row filter on a `DateTime`/`DateTime64` column is compiled and evaluated by the same engine that validates ingest ([internal/typelayer](/access-control#where-each-rule-is-enforced)), so a filter constant in any spelling ClickHouse would accept in a `WHERE` clause matches the stored instant however the payload spelled it, and a predicate the engine cannot compile withholds every row for that role.

#### Positional formats (CSV / TSV)

`text/csv` and `text/tab-separated-values` are **positional**; `header` is [RFC 4180 §3](https://www.rfc-editor.org/rfc/rfc4180#section-3)'s optional parameter, and WaveHouse maps it onto ClickHouse's own behavior three ways:

| Content-Type | Reading |
| --- | --- |
| `text/csv; header=present` | `CSVWithNames`: a header line is required and columns are addressed by name — see [Header formats](#header-formats-headerpresent) |
| `text/csv; header=absent` | `CSV`, strictly positional: header detection is off (`input_format_csv_detect_header=0`), so every line is a record |
| `text/csv` (no `header` parameter) | ClickHouse's default `CSV`: header auto-detection stays on |

`text/tab-separated-values` maps the same way, with `input_format_tsv_detect_header`. The auto-detection is ClickHouse's own heuristic, not WaveHouse's: send `header=absent` when a data row could spell the column names or you need the first line always read as a record. With no parameter, the positional fields are the table's **wire columns** — declaration order minus every `MATERIALIZED`, `ALIAS` and `EPHEMERAL` column — and a producer must send **every one of them, in that order**. `GET /v1/ops/schema?table={table}` returns the columns in `position` order; drop the three kinds and that is the field order. Only `header=present` can name an `EPHEMERAL` column.

| Body | Outcome |
| --- | --- |
| every field, in order | accepted |
| an empty field (CSV) or `\N` (TSV) | that column takes its `DEFAULT` |
| too few fields | rejected, code **27** — ClickHouse's own message, e.g. `Cannot parse input: expected ',' before: …` |
| too many fields | rejected, code **117** — `Expected end of line` |
| a header line, no `header` parameter | ClickHouse detects it and **consumes** it as a header: `total` and every `index` count data rows only |
| a header line, `header=absent` | **not a header** — read as a data row, so it fails to parse (code 27) wherever a column cannot read its own name; the data rows after it still parse |

The messages are ClickHouse's own and differ between ClickHouse lines; branch on the `exception_code`. An empty **TSV** field is the empty string, not a default: `\N` is TSV's spelling for "take the default", and a `DateTime64` cannot read `""`.

A positional producer cannot self-describe, so a column-order change silently re-assigns values — pin it to the schema and re-check it after any `ALTER`, or send a header with `header=present`.

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: text/csv" \
  --data-binary $'"/home","signup",42.5,\n"/about","nav",3,\n'
# → {"total":2,"succeeded":2,"failed":0,"duplicates":0,"results":[{"index":1,"ok":true},{"index":2,"ok":true}]}
```

#### Header formats (`header=present`)

`text/csv; header=present` and `text/tab-separated-values; header=present` open with a header line naming the columns, and the fields are addressed by it rather than by position. IANA's `text/tab-separated-values` registration defines no parameters, so `header` on TSV is WaveHouse's mirror of the CSV one.

| Body | Outcome |
| --- | --- |
| a header naming the columns, in any order | the header is **not a record**: `total` and every `index` count data lines only |
| a column the header omits | takes its `DEFAULT`, exactly as an omitted JSON field does — including a check clause's injected value |
| a header name in a different case | matched case-insensitively, as ClickHouse does from 26.5 |
| a header naming a column the table, or the role's writable set, lacks — or a name given twice | the whole request is a `400` with code **117**; nothing is published |
| a bad data row | rejected per record with its code; the rows around it still ingest |
| only the header line | a valid record-less batch: `200`, `total: 0` |

```bash
curl -X POST "http://localhost:8080/v1/ingest?table=clicks" \
  -H "Content-Type: text/csv; header=present" \
  --data-binary $'button,page\nsignup,/home\nnav,/about\n'
# → {"total":2,"succeeded":2,"failed":0,"duplicates":0,"results":[{"index":1,"ok":true},{"index":2,"ok":true}]}
```

#### Batch Ingest

A JSON array, an NDJSON body, a CSV body or a TSV body ingests a batch in one request. Each record is validated, authorized, deduplicated and published independently, so **one malformed or rejected record never blocks the rest of the batch** — including inside a single-line (compact) JSON array, which WaveHouse re-frames in place before handing it over. An explicit empty array (`[]`) is a valid record-less batch (`200`, `total: 0`) for a role whose insert grant resolves; blank lines in an NDJSON body are skipped. (The SDK's `insert([...])` array helper uses the NDJSON form automatically; every form returns the same response.)

The response counts records read, published, rejected and deduplicated, then lists per-record outcomes: each entry mirrors the single-object response (`ok` / `duplicate` / `error`) plus its 1-based `index`, and carries ClickHouse's numeric `exception_code` when the rejection was its parser's. `results` is truncated to the first 10,000 entries; the four counts stay authoritative.

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
    { "index": 3, "error": "Unknown field found while parsing JSONEachRow format: referrer", "exception_code": 117 }
  ]
}
```

| Field | Meaning |
| ----- | ------- |
| `total` | records read from the body |
| `succeeded` | records validated and published |
| `failed` | records rejected — see `results` |
| `duplicates` | records skipped by dedup (when enabled) |
| `results` | per-record outcomes, each `{ index, ok\|duplicate\|error }` with `index` the 1-based record position, plus `exception_code` when the rejection was ClickHouse's parser's. Truncated to the first 10,000 entries for very large batches (the counts stay authoritative). |

A `200` is returned whenever the body was read and the records were processed — **even if every record failed**, so branch on `failed`/`results`, not the status code. Per-record problems (a malformed NDJSON line, a non-object array element, a ClickHouse parser rejection, a denied column or a failed `check`) are reported in `results` and the batch continues. Whole-request conditions abort with a non-`200` instead:

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"empty body"}` (declared variants: `empty ndjson body`, `empty csv body`, `empty tsv body`, `empty csvwithnames body`, `empty tsvwithnames body`) | The body holds no bytes. A `header=present` body holding only its header line is a valid record-less batch (`200`, `total: 0`) |
| 400 | `{"error":"invalid request body"}` | The body could not be read at all — a malformed transfer encoding, or a truncated upload (a body cut off *in transit*). A body that arrived complete but ends mid-value is not this error: a JSON array cut short is `invalid json: unterminated json array` below, while a single object or NDJSON cut mid-value is a per-record ClickHouse rejection (code 26 or 27) |
| 400 | `{"error":"invalid json: unterminated json array"}` | A body declared `application/json` opening with `[` whose brackets do not balance — truncated, or structurally broken. It cannot be salvaged per record, so the whole request fails |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (same auth gate as the single-object path; surfaces the token reason) |
| 403 | `{"error":"forbidden"}` (empty-role variant: `forbidden: request has no role and no public default_role is configured`) | The resolved role lacks `insert` on the table (checked once, before any record) |
| 403 | `{"error":"insert permissions were not resolved for this request"}` | The grant that resolved for the request is not an insert grant; see the [single-record table](#post-v1ingesttabletable--ingest-data). Fails the whole request, an empty array included |
| 413 | `{"error":"request body exceeded 16777216 bytes"}` | Request body over the 16 MiB cap |
| 415 | `{"error":"no Content-Type: ingest requires one of application/json, application/x-ndjson, application/ndjson, application/jsonl, application/jsonlines, text/csv, text/csv; header=present, text/csv; header=absent, text/tab-separated-values, text/tab-separated-values; header=present, text/tab-separated-values; header=absent"}` (declared variant: `Content-Type "text/plain": ingest requires one of …`; conflicting variant: `conflicting Content-Type declarations "application/json", "application/x-ndjson": ingest reads one format per request, and requires one of …`) | No `Content-Type`, an unsupported or unparseable one, a `header` value other than `present`/`absent`, a comma-bearing value that does not parse as a single media type, or repeated lines that disagree. Checked before the body is read |
| 500 | `{"error":"validation failed"}` | The parse itself failed for a reason that is neither a record's fault nor an unavailable tenant; logged. Nothing was published |
| 500 | `{"error":"publish failed"}` / `{"error":"dedupe failed"}` | Message-queue or dedup-backend failure mid-batch, other than a full queue or an unreachable broker (below). After a publish failure the records before it keep their ids, so a whole-batch retry reports those as duplicates; the failing record's id is left to lapse as on the single-object path, and the rest of its window's ids are given back |
| 503 | `{"error":"service unavailable"}` | The tenant's ingest queue is full (backpressure) or not open, mid-batch; includes `Retry-After: 30`. The records before the refused one keep their ids, and its id and the rest of its window's are given back |
| 503 | `{"error":"service unavailable"}` | The message queue could not be reached or did not answer in time (`mq.ErrUnavailable`), mid-batch. Only under [`mq.backend: nats`](/deployment#external-nats); the embedded broker never reports this, and its publish failures are the `500` above. As for the `500`, the failing record's id is left to lapse rather than given back — so `Retry-After` is that record's dedupe lease, rounded up to whole seconds, when it was deduped; a record published un-deduped has no lapsing claim to wait out, so `Retry-After: 5` |
| 503 | `{"error":"dedupe store unavailable"}` | Dedupe is on and its store cannot answer now; `Retry-After: 5`. Nothing in the window being reserved was published; the windows before it were, and keep their ids |
| 503 | `{"error":"a request with the same dedupe id is in flight"}` | A record's dedupe id is held by another request still being published; includes `Retry-After` (the dedupe lease, [`dedupe.lease`](/configuration#dedupe), 30 seconds by default). Nothing in that record's window was published; the windows before it were |
| 503 | `{"error":"ingest validation is unavailable"}` | The tenant's schema is not bound yet, or its ClickHouse line has no installed chtypes artifact, or its server time zone differs from the zone this process opened that line with — the cause is in the server log, not the body. Decided once, before the body is read, so nothing is published. `Retry-After: 5`. See the [single-record table](#post-v1ingesttabletable--ingest-data) |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while the tenant's JWKS has not been fetched yet; refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

:::caution[At-least-once on retry]
A batch aborted partway — a `503` or `500` after some leading records were already published — re-publishes those leading records when the whole batch is retried. Records are published in windows of 256, in order: a dedupe failure drops the open window unpublished, so what an aborted batch published is the windows before it, plus, after a publish failure, the records of its window before the failing one. Failures decided before any record is processed are not in this class: a `413`, a `415`, the `400 invalid request body` of an upload cut off in transit, and the unterminated-array `400` all publish nothing and are safe to retry as-is (once split, for a `413`). Enable deduplication if duplicate suppression matters — the single-object path has the same at-least-once property, and the SDK retries both on `503`.
:::

---

### `POST /v1/ops/query` — Query ClickHouse

Executes a SQL statement directly against ClickHouse. **WaveHouse proxies the SQL string verbatim to ClickHouse's HTTP interface** — any statement ClickHouse accepts works, including arbitrary DDL/DML/SYSTEM verbs and inline FORMAT directives. Multi-statement input (`SELECT 1; TRUNCATE t`) also works on recent ClickHouse versions where multi-query is enabled by default; older or restrictively-configured servers may reject the second statement with a clear error. Read queries return a JSON array of result rows; mutations/DDL return HTTP 200 with `[]` on success. DateTime columns are ISO-8601 formatted via the upstream `date_time_output_format=iso` setting — the same server-side rendering `/v1/query`, pipes and the stream use, so a `DateTime64(3)` whole-second value returns `.000Z` on all of them; other types are returned as ClickHouse renders them under `FORMAT JSON`.

:::note[Inline `FORMAT` overrides the JSON envelope]
ClickHouse's inline `FORMAT` clause (e.g. `SELECT 1 FORMAT CSV` or `… FORMAT Pretty`) takes precedence over the URL-level `default_format=JSON` setting. When the SQL contains an explicit `FORMAT`, the proxy forwards ClickHouse's raw response body (CSV, Pretty, TSV, …) and passes through the upstream `Content-Type` header — `text/csv`, `text/tab-separated-values`, etc. — so consumers see the right MIME type. The "extract the `data` array" behavior only applies when ClickHouse returned the `FORMAT JSON` envelope, which is the default.
:::

:::caution[64 MiB response cap]
The proxy buffers the upstream response in memory before forwarding (no row-streaming yet), so a `SELECT *` from a large table can pin RAM on the API server. To avoid an admin OOMing themselves, responses larger than 64 MiB return 502 with a `clickhouse response exceeded N bytes` error. Narrow the query with `LIMIT`, or use a streaming client outside WaveHouse that talks to ClickHouse directly (the standard escape hatch — the same admin credentials work).
:::

This endpoint **does not cache, does not singleflight, and emits `Cache-Control: no-store`** — every request goes straight to ClickHouse, mutation or read, and downstream HTTP caches are explicitly told not to store the response. Raw SQL is an admin escape hatch with infrequent, ad-hoc traffic, so the cache/singleflight machinery would only add complexity without a real hit-rate win. Use [`POST /v1/query?table={table}`](#post-v1querytabletable--structured-query) or [`GET/POST /v1/pipes/{name}`](#getpost-v1pipesname--execute-named-pipe) for the cached read paths (dashboards, high-QPS clients, etc.) — both go through the query cache ([`cache.backend`](/configuration#backends): in-process, or a Redis shared by every instance) with singleflight coalescing.

:::note[Admin only]
The route is mounted under `/v1/ops/*`, behind the `RequireAdmin` gate: only a caller whose JWT role equals the policy `admin_role` (`"admin"` by default) — or who presents the non-JWT [operator key](#authentication) — may use it. A tokenless request (or a valid token without a role claim) resolves to the `default_role` (not the admin role unless `default_role` is deliberately set to it — a loudly-warned dev-only setting) and is rejected with `403`; a present-but-invalid token — expired, malformed, bad signature — keeps its stashed verification error and fails loud with `401` instead. Raw SQL has no per-statement scope check (a full SQL parser would be needed to authorize predicates), so the role gate is the entire authorization story, shared with the rest of `/v1/ops/*` (see [Admin Endpoints](#admin-endpoints)). The normal surfaces for non-admin callers are `POST /v1/ingest?table={table}` for writes, `POST /v1/query?table={table}` for structured reads, and `GET/POST /v1/pipes/{name}` for pre-defined queries — none of which expose raw SQL.
:::

`/v1/ops/query` is the only surface for ad-hoc non-insert mutations (the ingest pipeline is insert-only; a [pipe that writes](/pipes#pipes-that-write) runs only the statement an operator authored). Granting raw-SQL access to a non-admin role via the policy engine is no longer supported: authenticate with the admin role (`admin_role`).

An optional `?tenant=<id>` names the [tenant](/deployment#the-nested-settings-directory) whose ClickHouse the SQL runs against — its own database, credentials and HTTP wiring; without it the SQL runs against tenant `0`'s, which is the whole settings directory unless it is nested. The parameter is parsed as strictly as on the [schema routes](#get-v1opsschema--list-all-table-schemas): `400` for a query string that does not parse or an empty, repeated or malformed id, `404` for an unknown tenant, `503` for one whose settings folder was rejected — all decided before the body is read. A tenant on no ClickHouse pool ([no pool could be opened for it](/settings-directory#clickhouse), such as one the connection ceiling refused) answers `503` `{"error":"no ClickHouse connection is open for this tenant"}` with `Retry-After: 30`.

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
| 400 | `{"error":"invalid ?tenant: …"}` / `{"error":"invalid query string: …"}` | The query string does not parse (`?tenant=acme;x=1`, a bad `%` escape), or `tenant` is empty, repeated, or not a tenant id — parsed as strictly as on the [pipe reads](#get-v1opspipes--list-named-pipes) |
| 404 | `{"error":"unknown tenant: <id>"}` | No such tenant |
| 503 | `{"error":"tenant settings are invalid"}` | The tenant's settings folder was rejected |
| 400 | `{"error":"invalid json"}` | Malformed request body |
| 400 | `{"error":"missing sql"}` | Missing `sql` field |
| 400 / 403 / 502 / 503 | `{"error":"<ClickHouse error message>","code":"clickhouse.…","retryable":…}` | ClickHouse failed the statement. The status and `code` come from the exception code, not ClickHouse's HTTP status — see [ClickHouse errors on the query paths](#clickhouse-errors-on-the-query-paths). The `error` is ClickHouse's own text verbatim, e.g. `Code: 60. DB::Exception: Table default.x does not exist. (UNKNOWN_TABLE)` |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | The request carried a present-but-invalid/expired token and was denied for lacking permission (the gate surfaces the token reason) |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 502 | `{"error":"<message>","code":"clickhouse.unknown","retryable":true}` | An answer with no ClickHouse exception code that is not an outage — a `500` from something in front of ClickHouse. A codeless redirect or `4xx` such as a `404` (a wrong path) is `502 clickhouse.misconfigured`, `retryable: false`; a codeless `408`/`429` is `503 clickhouse.unavailable`, a `413` is `400 clickhouse.rejected` |
| 503 | `{"error":"clickhouse request failed: ...","code":"clickhouse.unavailable","retryable":true}` | ClickHouse could not be reached, or the query timed out (connection refused, the upstream went away mid-request); `Retry-After: 5`. A TLS failure, such as an untrusted certificate, is `502 clickhouse.unknown` |
| 502 | `{"error":"clickhouse response exceeded N bytes; ...","code":"clickhouse.response_too_large","retryable":false}` | Response body exceeded the 64 MiB memory-safety cap. Narrow the query, add a `LIMIT`, or use `FORMAT JSONEachRow` with a streaming client outside WaveHouse. |
| 503 | `{"error":"no ClickHouse connection is open for this tenant"}` | The tenant is on no ClickHouse pool — [no pool could be opened for it](/settings-directory#clickhouse), such as one the connection ceiling refused — so the SQL cannot run; `Retry-After: 30`, a settings reload retries the pool |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while tenant `0`'s JWKS has not been fetched yet (the ops tree verifies as tenant `0`); refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

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
| `time_range` | object | No | Time window (`column`, `since`, `until`). `since`/`until` accept RFC3339 or Go-duration relative values ("1h", "30m", "7d", "2w" — day and week suffixes expand to hours). Relative values mean that long *ago*. Both bounds are instants: they reach ClickHouse as RFC 3339 in UTC, so the column's zone cannot shift them. The window applies only when `column` and `since` are set — an `until` without `since` is ignored. |

:::note[Filter values bind as strings]
Every bound value — a caller's filter, a policy row filter, an insert `check` — binds as a `{p:String}` parameter and is compared under the column's own type. One rule across all three surfaces, and the answer is the server's: send ClickHouse's own spelling for a value and it reads it. A policy claim on an integer column is compared through a strict cast on top, so a claim that is not the canonical spelling of a value the column can hold matches nothing instead of wrapping (see [Access Control](/access-control#jwt-claim-templating)); a caller's own filter keeps the plain form, since it can only narrow what the policy admits.

On **this endpoint**, a caller's filter on a `Date`, `DateTime` or `DateTime64` column (`Nullable` or `LowCardinality` too) is parsed by ClickHouse rather than compared as text, because compared directly ClickHouse refuses RFC 3339 on those columns. The value still reaches the server as you wrote it, inside `parseDateTime64BestEffort(…)` with the column's declared zone: an offset or `Z` is the exact instant, a zone-less `"2026-06-21 13:00:00"` is local time in the column's zone (else the server's), a Unix-seconds number works, and a fraction compares exactly, so `> "…04:00:00.5Z"` on a whole-second column excludes `04:00:00`. A `Date` column takes the date of that instant in the server's zone. A value ClickHouse cannot parse is `400 clickhouse.rejected`. `like` compares text and is never parsed. A policy row filter keeps the plain form, so the query path and the stream judge it alike.

An `in` list travels as a ClickHouse **external table**, not a query parameter, so no ClickHouse field limit applies to it: any list the 1 MiB request body can carry reaches the server. Each element is converted to the column's type (`accurateCastOrNull`, or the timestamp parse above), so `["12.5"]` matches a `Decimal` 12.50, and an element that is not a value of the column's type (`"256"` on a `UInt8`, `"1.5"` on an integer column) matches no row. Scalar values ride on the request line, where ClickHouse takes at most 128 KiB per value once URL-encoded and about 1 MiB for the whole line. With an `in` list the SQL itself travels in one 128 KiB form field. Past any of these the request is a `400` that names the limit, before anything is sent.
:::

:::note[Identifier names]
Table, column, and alias names may contain any characters ClickHouse accepts — dots, spaces, unicode, reserved keywords — because every identifier is backtick-quoted automatically. The one exception is a name containing a literal `?`, which is rejected with `400`: the builder assembles positional placeholders before rewriting them to ClickHouse's named parameters, and a `?` inside an identifier would desync that rewrite ([#279](https://github.com/Wave-RF/WaveHouse/issues/279)).
:::

**Response:**

JSON array of result rows, **rendered by ClickHouse**: the query runs over its HTTP interface with `FORMAT JSONEachRow`, and WaveHouse frames the lines into an array without re-encoding a value. So every type is spelled the way the connected server spells it, per version, with no WaveHouse conversion table in between.

| ClickHouse type | JSON |
| --- | --- |
| `DateTime`, `DateTime64` | `"2026-06-21T04:00:00.123Z"` — RFC 3339 in UTC whatever zone the column declares; byte-identical to the [SSE stream](#get-v1stream--server-sent-events-stream) for the same row (see [Timestamp rendering](#timestamp-rendering)) |
| `Decimal*` | a JSON **number** (`12.5`), not a string |
| `Int64`/`UInt64` past 2^53 | an unquoted number — still lossy in a JavaScript `number`; read it as text if you need every digit |
| `FixedString(n)` | a string padded to `n` bytes with `\u0000` |
| `Enum*` | the name, not the ordinal |
| `Nullable(T)` | `null` for a SQL `NULL` |
| `Array`, `Map`, `Tuple` | ClickHouse's own JSON for the container |
| `NaN` / `Inf` | `null` (ClickHouse's default rendering) |

Keys come back in **SELECT order**, not alphabetical. The query runs with `readonly=2` and a server-side `max_execution_time`, so a statement that writes cannot slip through a read path and a runaway query is stopped by ClickHouse rather than only abandoned by WaveHouse. A filter value of `null` is a `400`, and an `in` list travels as an external table with no size cap of its own. The response carries `X-Cache: HIT` or `X-Cache: MISS` — this endpoint shares the query cache + singleflight machinery (unlike `/v1/ops/query`, which always hits ClickHouse), keyed by [tenant](/deployment#multi-tenant-deployments): a request is never served from, or coalesced with, another tenant's. The cache stores ClickHouse's own bytes.

The inbound request body is capped at 1 MiB; a body over the cap is rejected with `413`. It is also the only bound on an `in` list's length, and it blocks a single-request memory-exhaustion vector on this public endpoint. Set a tighter or higher outer limit at your [reverse proxy](/reverse-proxy#request-body-size-limits) — but it can only narrow the effective limit, not raise it past this cap.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 400 | `{"error":"unknown column: x"}` | Schema validation error — an unknown column, a bad aggregation, or an unparseable `time_range` `since`/`until` (neither a relative duration nor an RFC3339 timestamp) |
| 400 | `{"error":"filter value must not be null"}` | A filter carries `"value": null`. It is refused rather than answered: `col = NULL` is never true, and an empty parameter would silently ask a different question |
| 400 | `{"error":"filter value too large: …"}` / `{"error":"query too large: …"}` | A scalar filter value over the 128 KiB ClickHouse's HTTP interface takes for one value once URL-encoded, all of them over the request line, or a query with an `in` list whose SQL is over the 128 KiB form field it travels in. An `in` list itself is never refused for size |
| 403 | `{"error":"forbidden"}` | Role lacks select permission on table |
| 403 | `{"error":"column \"x\" not allowed"}` | Column denied by policy |
| 403 | `{"error":"aggregation \"x\" not allowed"}` | Aggregation fn denied by policy |
| 404 | `{"error":"unknown table: x"}` | Table not found in the tenant's discovered schema |
| 413 | `{"error":"request body exceeded 1048576 bytes"}` | Request body over the 1 MiB cap |
| 400 / 403 / 502 / 503 | `{"error":"Code: 60. DB::Exception: …","code":"clickhouse.…","retryable":…}` | ClickHouse failed the query: a column dropped since the schema was discovered (`400 clickhouse.rejected`), the role's `max_rows_to_read`/`max_memory_usage` cap, or a `max_execution_time` no longer than `clickhouse.query_timeout` (`400 clickhouse.limit_exceeded`), a response over the 64 MiB read cap (`502 clickhouse.response_too_large`; earlier versions had no cap here), ClickHouse down (`503 clickhouse.unavailable`, `Retry-After: 5`), … — see [ClickHouse errors on the query paths](#clickhouse-errors-on-the-query-paths) |
| 500 | `{"error":"…","code":"clickhouse.unknown","retryable":true}` | A failure with no verdict |
| 503 | `{"error":"schema not loaded yet"}` | The tenant's first schema discovery has not succeeded yet, so whether the table exists is not known; `Retry-After: 5` |
| 503 | `{"error":"no ClickHouse connection is open for this tenant"}` | The tenant is on no ClickHouse pool — [no pool could be opened for it](/settings-directory#clickhouse), such as one the connection ceiling refused — so the query cannot run; decided before a cached result is served, so nothing cached before is served either; `Retry-After: 30`, a settings reload retries the pool |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while the tenant's JWKS has not been fetched yet; refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

---

### `GET/POST /v1/pipes/{name}` — Execute Named Pipe

Executes a pre-defined named query (pipe) with parameter binding. Parameters can be supplied via query string and/or JSON body. A read's results are cached in the query cache ([`cache.backend`](/configuration#backends): in-process, or a Redis shared by every instance) with singleflight coalescing — same machinery as the structured query endpoint, keyed by [tenant](/deployment#multi-tenant-deployments) like it, and again, unlike `/v1/ops/query`; a [pipe that writes](/pipes#pipes-that-write) is neither cached nor coalesced (see Response).

**Query Parameters:** Any key matching a pipe parameter name.

**POST Body (optional):**

```json
{
  "start_date": "2024-01-01",
  "limit": 100
}
```

**Response:**

JSON array of result rows, with `X-Cache: HIT` or `X-Cache: MISS` indicating whether the rows came from the query cache. A pipe whose SQL is a write (`INSERT`, `ALTER`, `WITH … INSERT`, …) bypasses the cache and singleflight: it executes on every call, identical calls in flight are not coalesced, and the response is `[]` with `X-Cache: BYPASS` and `Cache-Control: no-store` (so an HTTP cache in front of a `GET` cannot answer a repeat) — see [Pipes that write](/pipes#pipes-that-write).

The POST parameter body is capped at 1 MiB; a body over the cap is rejected with `413` (the same 1 MiB parameter/AST-body cap as [`POST /v1/query`](#post-v1querytabletable--structured-query) — see [reverse proxy → body limits](/reverse-proxy#request-body-size-limits)). A malformed-but-within-cap body is ignored rather than rejected, since parameters may legitimately come from the query string alone.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 404 | `{"error":"pipe not found"}` | Pipe name not registered |
| 503 | `{"error":"no ClickHouse connection is open for this tenant"}` | The tenant is on no ClickHouse pool — [no pool could be opened for it](/settings-directory#clickhouse), such as one the connection ceiling refused; decided before a cached result is served or a query runs; `Retry-After: 30` |
| 403 | `{"error":"forbidden"}` | Role not in pipe's `allowed_roles` (and not the admin role). Fails closed: a request with no role (no token, or a JWT missing `auth.role_claim`) is denied unless a `default_role` resolves it into the list; a pipe with no `allowed_roles` denies everyone but the admin role. |
| 400 | `{"error":"missing required parameter: x"}` | Required parameter not supplied |
| 400 | `{"error":"parameter \"x\": unsupported parameter type object"}` | A non-scalar value with no SQL literal form — a JSON object, whether supplied directly or nested as an array element. A JSON **array** is valid and renders as an `IN`-style `(…)` list. |
| 400 | `{"error":"parameter \"x\": array parameter must not be empty"}` | An empty array — it would render as the invalid `IN ()`. |
| 413 | `{"error":"request body exceeded 1048576 bytes"}` | POST body over the 1 MiB cap |
| 400 / 403 / 500 / 502 / 503 | `{"error":"Code: 60. DB::Exception: …","code":"clickhouse.…","retryable":…}` | ClickHouse failed the pipe's query — for instance a parameter value it cannot use (`400 clickhouse.rejected`), a response over the 64 MiB read cap (`502 clickhouse.response_too_large`; earlier versions had no cap here), or ClickHouse down (`503 clickhouse.unavailable`, `Retry-After: 5`); see [ClickHouse errors on the query paths](#clickhouse-errors-on-the-query-paths). A [pipe that writes](/pipes#pipes-that-write) answers `retryable: false` with no `Retry-After` |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while the tenant's JWKS has not been fetched yet; refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

---

### `GET /v1/stream` — Server-Sent Events Stream

Opens a persistent SSE connection for real-time event streaming. Supports historical gap-fill from NATS JetStream using `DeliverByStartTime`. A connection that carries a token, with no valid operator key, while the tenant's JWKS has not been fetched yet is refused with `503` + `Retry-After: 30` (see [Authentication](#authentication)); a browser `EventSource` treats that as fatal rather than reconnecting, so reopen it after the delay (the SDK's stream re-dials on its own).

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

**Response:** SSE stream (`text/event-stream`). Data events include an `id:` field set to the event's `received_timestamp`. The stream opens with a `: connected` comment and emits a minimal `:` keepalive comment periodically (every 30 seconds by default), which keeps a quiet connection from being closed by a proxy; both are standard SSE comments that `EventSource` ignores (raw consumers should skip `:`-prefixed lines). When the server stops (see [Stopping](/deployment#stopping)) it ends every open stream immediately rather than holding it for the drain; `EventSource` reconnects on its own and resumes from `Last-Event-ID`. A reload that stops serving the stream's tenant — its folder removed or rejected, over a [nested settings directory](/deployment#the-nested-settings-directory) — ends that tenant's open streams the same way, and the reconnect then gets its `404` (removed) or `503` (rejected): the SDK stops on the `404` and retries the `503`, resuming from `Last-Event-ID` once the folder is back, while a browser `EventSource` treats either as fatal. A browser going cross-origin reads either refusal only when it passes CORS: it is decorated from tenant `0`'s list ([multi-tenant deployments](/deployment#multi-tenant-deployments)), so where tenant `0` is not served or its list does not admit the page's origin, the SDK sees a network error instead and keeps re-dialing.

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

Values of top-level `DateTime`/`DateTime64` columns inside `row` are ClickHouse's own rendering of the stored value — the exact bytes chtypes' `RowsExport` produced for that record (see [Timestamp rendering](#timestamp-rendering)), not a WaveHouse rewrite — so a live event and a `/v1/query` read of the same row agree on spelling **by construction**, with no separate canonicalization step to keep in sync ([#372](https://github.com/Wave-RF/WaveHouse/issues/372)). A column declared with a non-UTC zone is still rendered in UTC (the `Z` form), so the strings compare as the instants do.

**Note:** When access control policies are active, streamed events are filtered per the caller's role: tables without `select` permission are skipped, denied columns are removed from each event, and the role's [row-level `filter`](/access-control#row-level-security) is compiled and evaluated per subscriber against the caller's JWT claims — supplied by the connection's token (the `Authorization` header, or the `?token=` fallback above), with replayed gap-fill events filtered the same way. This runs through the same in-process ClickHouse parser (chtypes) that validates ingest, so every column type compares exactly as it would in the query path's `WHERE` clause — a connection is never delivered a row the query path would hide for that role, and a predicate that can't compile or evaluate withholds the row instead of guessing (see [the enforcement caution](/access-control#where-each-rule-is-enforced) for the fail-closed reasons). A tenant whose ClickHouse line has no installed artifact is unavailable on its own: a stream whose role has a row `filter` withholds that tenant's rows with reason `unavailable`, while other tenants' streams (and roles with no row filter) are unaffected. A withheld row is counted by `wavehouse_sse_rows_withheld_total{table,role,reason}`; `reason` is `filter` (the predicate answered false), `error` (it failed to evaluate), `decline` (the engine cannot answer for the row), `unavailable` (the tenant's line is not served, above) or `drift` (the event names a column the table no longer has, as after a schema change). A role's row `filter` over a column the inserting role cannot write, or over a `MATERIALIZED` column, never streams to that reader: a published row carries only the columns its inserting role wrote, and a `DEFAULT` or `MATERIALIZED` value is computed again when ClickHouse stores the row, so the stream declines the row rather than guess — `/v1/query` still returns it. The residual payload-vs-stored case is an event whose insert ClickHouse later rejects and parks on the dead-letter queue — a data-shape problem chtypes did not catch, since an outage only delays a row and never drops it — which the caution documents. The connection's claims are captured once, when the stream is established — a policy change applies from the next event, replayed or live (a gap-fill re-reads the policy per event too), but an expired token or changed claims take effect only when the client reconnects.

**CORS:** `/v1/stream` honors the request's tenant's `cors.allowed_origins` allowlist (settings directory) like every endpoint — the preflight included, which a browser sends without `X-Tenant-ID`, so over [a nested settings directory](/deployment#multi-tenant-deployments) the fronting proxy has to set the header on the `OPTIONS` too. Note that a **header-authenticated stream preflights before it connects** — `Authorization` is not CORS-safelisted — where a bare `EventSource` never preflighted at all: its request is not a `fetch()`, so Fetch's unsafe-request flag is never set and `Last-Event-ID` rides on the plain `GET`. Both headers are allow-listed, so an allowed origin connects *and* resumes cross-origin.

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

Every admin-gated surface lives under the `/v1/ops/*` prefix, behind a single `RequireAdmin` gate: schema discovery, DLQ stats, and the pipe and settings-reload endpoints below, plus the raw-SQL passthrough [`POST /v1/ops/query`](#post-v1opsquery--query-clickhouse) documented with the query endpoints above. They require the policy `admin_role` (`"admin"` by default, exact case-sensitive match) — or the non-JWT [operator key](#authentication), which reaches the same surface without a token; other callers get 401 (present-but-invalid token) / 403, and the quickstart's trial `public` role cannot call any of them. Over a [nested settings directory](/deployment#the-nested-settings-directory) these routes reach every tenant, so the operator key alone opens them and an admin-role token gets `403`. There is no separate `service` role. The JWT middleware always runs — a tokenless request (or a valid token without a role claim) resolves to the `default_role` (not the admin role unless `default_role` is deliberately set to it — a loudly-warned dev-only setting) and is denied `403`, while a present-but-invalid token keeps its stashed verification error and is denied `401`.

No admin endpoint in this section accepts a request body — they are reads and triggers; the settings directory's files are the only write path. The raw-SQL `POST /v1/ops/query` carries the 16 MiB bulk-payload cap documented with the query endpoints above.

#### `GET /v1/ops/schema` — List All Table Schemas

Returns all discovered ClickHouse table schemas of one tenant. All three schema routes take an optional `?tenant=<id>` naming the [tenant](/deployment#the-nested-settings-directory) whose schema is read or refreshed; without it they address tenant `0`, which is the whole settings directory unless it is nested. The query string is parsed strictly, with the pipe reads' answers: `400` for a query that does not parse or an empty, repeated or malformed id, `404` for an unknown tenant, `503` for one whose settings folder was rejected. A tenant whose first discovery has not succeeded yet answers `503` `{"error":"schema not loaded yet"}` with `Retry-After: 5` rather than an empty list, which would read as "no tables".

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

Per-column fields: `name`, `type` and `is_nullable` describe the column; `position` is its 1-based ordinal in the table's declaration order (always present, and the order `columns` itself is in); `has_default` says whether it declares any default at all, while `default_kind` (`DEFAULT`, `MATERIALIZED`, `ALIAS`, `EPHEMERAL`) and `default_expression` say which and what — both omitted when the column declares none. A `MATERIALIZED` or `ALIAS` column is computed and **not** insertable, so it never appears in an ingest envelope or an SSE `schema` frame, though it is still reported here and can still be selected by name. An `EPHEMERAL` column is the reverse: ClickHouse accepts it in an `INSERT` — it exists to be written — and WaveHouse ingest accepts it wherever the format names columns (the JSON family and `…WithNames`), feeding the `DEFAULT` expressions that read it, but it is never stored, never selectable and never published, so it appears in no envelope, no stream column list and no query result. The table's `CREATE TABLE` statement is captured on the same refresh but is deliberately **not** exposed here — for a table backed by an external engine it renders that engine's wiring — endpoint, bucket/host, database, username, access key id. (ClickHouse masks the password as `[HIDDEN]` from ~23.9; the topology is what is withheld here.)

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason) |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 400 | `{"error":"invalid ?tenant: …"}` / `{"error":"invalid query string: …"}` | The query string does not parse (`?tenant=acme;x=1`, a bad `%` escape), or `tenant` is empty, repeated, or not a tenant id — parsed as strictly as on the [pipe reads](#get-v1opspipes--list-named-pipes) |
| 404 | `{"error":"unknown tenant: <id>"}` | No such tenant |
| 503 | `{"error":"tenant settings are invalid"}` | The tenant's settings folder was rejected |
| 404 | `{"error":"table not found"}` | Table not in the tenant's discovered schema |
| 503 | `{"error":"schema not loaded yet"}` | The tenant's first schema discovery has not succeeded yet (its ClickHouse unreachable, or no pool for it); `Retry-After: 5` |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while tenant `0`'s JWKS has not been fetched yet (the ops tree verifies as tenant `0`); refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

---

#### `POST /v1/ops/schema/refresh` — Refresh Schemas

Triggers an immediate re-discovery of the `?tenant=`'s ClickHouse table schemas (tenant `0`'s without it), then returns the refreshed schema list (same array shape as `GET /v1/ops/schema`). Admin-only, like the rest of this section.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 / 403 | as above | Not the admin role |
| 400 / 404 / 503 | as on `GET /v1/ops/schema` | The `?tenant=` could not be resolved |
| 503 | `{"error":"no ClickHouse connection is open for this tenant"}` | The tenant is on no ClickHouse pool — [no pool could be opened for it](/settings-directory#clickhouse), such as one the connection ceiling refused — so nothing can be discovered; `Retry-After: 30`, a settings reload retries the pool |
| 503 | `{"error":"refresh failed: clickhouse unavailable"}` | ClickHouse could not be reached (connection refused, a timeout, overload); `Retry-After: 5` |
| 500 | `{"error":"refresh failed"}` | ClickHouse discovery query failed any other way |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while tenant `0`'s JWKS has not been fetched yet (the ops tree verifies as tenant `0`); refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

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

Returns per-table message counts in one tenant's Dead Letter Queue: the [tenant](/deployment#the-nested-settings-directory) an optional `?tenant=<id>` names, the default tenant `0` without it, which is the whole settings directory unless it is nested. The tenant is looked up in the message queue, not the settings, so a tenant whose folder was rejected or removed is read like one being served, since its queue is kept (nothing deletes it). The query string is parsed strictly, as on the other admin reads. Admin-only, like the rest of this section. Whether a poison row lands here is the settings directory's [`dlq.enabled`](/settings-directory#dead-letter-queue) switch (global or per table); a tenant's dead-letter stream is opened when the tenant is first served, and this endpoint always exists. Until a row has been parked, the endpoint returns `200` with `{"tables":{},"total":0}`. Under [`mq.backend: nats`](/deployment#external-nats) every tenant's rows are counted on one shared dead-letter stream, so any tenant id reads `200`, with zeros when it has never parked a row, and the `404` below does not occur.

**Error responses:**

| Status | Body | Cause |
| ------ | ---- | ----- |
| 401 | `{"error":"invalid token"}` / `{"error":"token expired"}` | A present-but-invalid/expired token was supplied and denied (the gate surfaces the token reason) |
| 400 | `{"error":"invalid query string: …"}` / `{"error":"invalid ?tenant: …"}` | The query string does not parse (`?tenant=acme;x=1`, a bad `%` escape), or `tenant` is empty, repeated, or not a tenant id |
| 403 | `{"error":"forbidden"}` | Caller's role is not the policy `admin_role` (`"admin"` by default) |
| 404 | `{"error":"no dead-letter queue for tenant: <id>"}` | Embedded queue only. The tenant has no dead-letter queue: it has never been served on this data directory, its queue could not be opened (see [Message Queue](/settings-directory#message-queue)), or the id names no tenant |
| 500 | `{"error":"stream info failed"}` | NATS JetStream stream-info lookup failed |
| 503 | `{"error":"token verifier not ready: the tenant's JWKS has not been fetched yet"}` | A token was supplied, with no valid operator key, while tenant `0`'s JWKS has not been fetched yet (the ops tree verifies as tenant `0`); refused before any policy runs, with a `Retry-After: 30` header — see [Authentication](#authentication) |

**Query Parameters:**

| Param | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `tenant` | string | `0` | The tenant whose dead-letter queue is read. |
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

Both pipe reads take an optional `?tenant=<id>` naming the [tenant](/deployment#the-nested-settings-directory) whose pipes are read; without it they read the default tenant `0`, which is the whole settings directory unless it is nested. The query string is parsed strictly, so that a request is never answered for a tenant it did not name:

| Status | When |
| ------ | ---- |
| `400` | The query string does not parse (`?tenant=acme;x=1`, a bad `%` escape), or `tenant` is empty, repeated, or not a tenant id |
| `404` | No such tenant |
| `503` | The tenant's settings folder was rejected |
| `503` | A token was supplied, with no valid operator key, while tenant `0`'s JWKS has not been fetched yet (the ops tree verifies as tenant `0`), with a `Retry-After: 30` header — see [Authentication](#authentication) |

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

An optional `?tenant=<id>` reloads that tenant's folder of a [nested settings directory](/deployment#the-nested-settings-directory) and nothing else; it is parsed as strictly as on the [pipe reads](#get-v1opspipes--list-named-pipes) (`400`), and an unknown tenant is a `404`. A token sent with no valid operator key while tenant `0`'s JWKS has not been fetched yet (the ops tree verifies as tenant `0`) is refused with `503` + `Retry-After: 30`. Over a nested directory a rejected folder is not kept on its previous settings, and a `422` for the whole directory can mean adopted in part — see that section.

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
| `columns` | string[] | The table's **wire** column names, in declaration order — what each position in `row` means (`internal/typelayer.Table.WireColumns`: the table's columns minus any `MATERIALIZED`, `ALIAS`, or `EPHEMERAL` column and minus any the role may not write — none of the three kinds is ever part of a published row). |
| `row` | array | One `JSONCompactEachRow` line: one value per entry in `columns`, in that order — the exact bytes ClickHouse's own writer produced for this stored row (`internal/typelayer`'s `Table.IngestWith`, via chtypes). A column the request body omitted carries its evaluated `DEFAULT` (or the type's implicit zero value where none is declared), not `null` — the same as a native `INSERT` naming fewer columns than the table has. `DateTime`/`DateTime64` values are ClickHouse's own rendering (see [Timestamp rendering](#timestamp-rendering)), and an out-of-range integer is wrapped the way a real `INSERT` wraps it. |

`columns` and `row` are only meaningful together: a reader that cannot pair them — a length mismatch, an undecodable row, a `columns` list naming one column twice — has no way to map a value to a column. Both readers also refuse an envelope whose `format` they do not recognize. Either way the SSE fan-out withholds such an envelope rather than guess, and the batch consumer parks it on the DLQ with `X-DLQ-*` headers — acking and dropping it only where the DLQ is switched off for that table, since it can never insert on retry. Both outcomes increment `wavehouse_ingest_poison_total`, separated by its `disposition` label (`parked` / `dropped`).

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

When ClickHouse **rejects** a batch insert (a value it cannot parse, a type mismatch, a table or column it does not have), the worker re-inserts the batch row by row: rows that succeed are acked, and only the rows ClickHouse rejects again are published to the tenant's own DLQ NATS stream (`DLQ_{tenant}`) under subjects `dlq.{tenant}.{table}` (the tenant the row was ingested under; `0` for a settings directory that holds the four files). This prevents infinite retry loops — those messages are ACKed from the main stream and moved to the DLQ for inspection. A ClickHouse that **cannot take** the insert — down, unreachable, timing out, overloaded, read-only, or refusing WaveHouse's credentials — never sends a row here: the batch stays in the tenant's ingest queue and is retried with backoff until it inserts (see [Ingest Pipeline](/ingest-pipeline#when-clickhouse-cannot-take-an-insert)). A batch whose tenant has no ClickHouse connection — one no longer served, or one no pool could be opened for (such as by the connection ceiling) — skips the row-by-row retry, which no row of it could pass, and is parked whole; only a served tenant whose DLQ is off for the table leaves it for redelivery, since a tenant no longer served has no switch to read. A second class lands here too: an envelope the worker cannot *read* at all — malformed JSON, an unknown **or absent** `format`, or `columns` and `row` that do not pair — is parked without ever reaching a table batch. **Two different body shapes land here, and a consumer must not assume one decoder.** A row that failed its INSERT is parked as the `EventMessage` envelope above. An envelope the worker could not *read* is parked as **its original bytes, verbatim** — `parkOnDLQ` republishes what arrived — so it is whatever the producer sent: malformed JSON, an envelope of an unknown `format`, or a v2 envelope whose `columns` and `row` do not pair. Being undecodable as an `EventMessage` is precisely why it was parked, so decode defensively and fall back on the `X-DLQ-Error` header, which names the reason. For the first shape the body is the published `EventMessage` envelope (`{"table_name":…,"scope":"","received_timestamp":…,"format":…,"columns":[…],"row":[…]}` — the failed row is the `row` array, read against `columns`, its `DateTime`/`DateTime64` values exactly as published: ClickHouse's own rendering of the stored value, since chtypes already validated and coerced the record before it was ever published — see [Timestamp rendering](#timestamp-rendering)); the failure reason, table, and time travel in the `X-DLQ-Table` / `X-DLQ-Error` / `X-DLQ-Timestamp` message headers. Because chtypes catches the type and shape problems synchronously at ingest, a row that reaches this DLQ path is one ClickHouse later rejected for a reason chtypes couldn't have caught up front, not a data mismatch; an outage only delays a row and never parks it.

Under [`mq.backend: nats`](/deployment#external-nats) the parked rows of every tenant go to one shared dead-letter stream instead, under `<prefix>.dlq.{tenant}.{table}`; the bodies and headers are the same.

Use `GET /v1/ops/dlq/stats` to monitor DLQ depth, per tenant (`?tenant=`).

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

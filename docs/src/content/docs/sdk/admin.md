---
title: "SDK Admin & System"
description: "Schema introspection, settings reload, DLQ stats, and health checks in @wavehouse/sdk."
---

Operational surfaces of `@wavehouse/sdk`. With one exception, everything on this page sits behind the server's admin gate: the caller must resolve to the admin role (`policy.admin_role`) or present the non-JWT [operator key](/api#authentication) — the SDK has no first-class operator-key option, but [`options.headers`](/sdk#custom-headers) can carry the `X-Operator-Key` header. Over a [nested settings directory](/deployment#the-nested-settings-directory) the operator key alone opens them, and an admin-role token gets `403`. The exception is `wh.sys.health()`, which calls the public, content-free `/v1/health` route and needs no credentials. See [Access Control](/access-control) for how roles resolve. Examples import from `@wavehouse/sdk` or `https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

## Schema — `wh.schema`

Introspect ClickHouse table schemas.

```ts
// List all table schemas
const { data: schemas } = await wh.schema.list();
// schemas: { clicks: { name: 'clicks', columns: [...] }, users: { ... } }

// Force refresh from ClickHouse
await wh.schema.refresh();
```

Individual table schema is also available via `wh.from('clicks').schema()`.

Over [a nested settings directory](/deployment#the-nested-settings-directory), pass `tenant` to read or refresh that tenant's schema, authenticating with the [operator key](/api#authentication) (sent as `X-Operator-Key` via [`options.headers`](/sdk#custom-headers)): the nested `/v1/ops/*` routes admit it alone, and a token carrying an admin role gets `403`; without it the calls address tenant `0`. A `503` with `Retry-After` is a tenant whose first discovery has not succeeded yet (`Retry-After: 5`), or one on no ClickHouse pool — [no pool could be opened for it](/settings-directory#clickhouse), such as one the connection ceiling refused — on the refresh (`Retry-After: 30`); the SDK retries both, and `wh.sql()` takes the same option to run against that tenant's ClickHouse:

```ts
const { data } = await wh.schema.list({ tenant: 'acme' });
await wh.schema.refresh({ tenant: 'acme' });
const { data: rows } = await wh.sql('SELECT count() FROM clicks', { tenant: 'acme' });
```

> `wh.schema.list()`, `wh.schema.refresh()`, and `wh.from(t).schema()` hit `/v1/ops/schema*`, which are **admin-only** endpoints: the caller must pass the admin gate — resolve to the policy admin role (`admin_role`, `"admin"` by default) or present the non-JWT [operator key](/api#authentication). Unless the deployment deliberately sets `default_role` to the admin role (the loudly-warned dev-only setting), construct the client with an admin-role token — or send the operator key via [`options.headers`](/sdk#custom-headers) — or these calls return `403`.

---

## Settings — `wh.settings`

Trigger a reload of the server's [settings directory](/settings-directory) — `roles.json`, `policies.json`, `pipes.json`, `config.json` — the same path the file watcher and `SIGHUP` use. Requires the admin gate.

```ts
// Re-validate and adopt the settings directory
const { data, error } = await wh.settings.reload();
// data: { adopted: true, findings: [...] } — warnings are included on success
// error: a 422 when the directory was rejected; error.details carries
//        { adopted: false, findings } and the previous settings stay in effect
```

Over [a nested settings directory](/deployment#the-nested-settings-directory), pass `tenant` to reload that tenant's folder alone: a `422` then means the folder was rejected and the tenant is no longer served (its requests answer `503`), not that its previous settings stayed. Without `tenant` the whole directory is reloaded, and a `422` can mean adopted in part.

```ts
const { data, error } = await wh.settings.reload({ tenant: 'acme' });
```

---

## DLQ — `wh.dlq`

Dead Letter Queue operations. Requires the admin gate — the admin role (`policy.admin_role`) or the [operator key](/api#authentication).

```ts
// Get DLQ statistics
const { data } = await wh.dlq.list();
// data: { tables: { "clicks": 3, "users": 0 }, total: 3 }

// Stats for a specific table
const { data } = await wh.dlq.table('clicks');
```

Each tenant has a dead-letter queue of its own, and the calls read tenant `0`'s without `tenant`. Over [a nested settings directory](/deployment#the-nested-settings-directory), pass `tenant` to read another's — a tenant whose folder was rejected or removed included, since its queue is kept — with the [operator key](/api#authentication), as for the schema reads above. A tenant with no dead-letter queue is a `404`:

```ts
const { data } = await wh.dlq.list({ tenant: 'acme' });
const { data: clicks } = await wh.dlq.table('clicks', { tenant: 'acme' });
```

`wh.dlq.stream()` exists in the API but is **not yet functional**: there is no server-side DLQ stream today (the SSE bridge only carries `ingest.>` subjects), so it connects and receives no events rather than failing. Live DLQ streaming is tracked in [#197](https://github.com/Wave-RF/WaveHouse/issues/197).

---

## System — `wh.sys`

Content-free server-online check.

```ts
// health() hits the public, content-free /v1/health route — 200, or 503 while degraded.
// Use it to check a server is reachable before sending data.
const result = await wh.sys.health();
if (result.ok) {
  // server is up and past boot
}
// on failure, result.error carries the reason (network vs. server error)
```

With `auth` configured, the ping carries your token, so while the tenant's JWKS has not been fetched yet it fails with `HTTP_503` (`token verifier not ready`), after waiting out `Retry-After: 30` on each retry — the server is up, but cannot check the token yet. See [API → Authentication](/api#authentication).

> Readiness (`/readyz`) is intentionally **not** exposed through the SDK — it runs a ClickHouse query per call and is a load-balancer / reverse-proxy concern, not the client's. Probe `/readyz` directly from your orchestrator if you need it.

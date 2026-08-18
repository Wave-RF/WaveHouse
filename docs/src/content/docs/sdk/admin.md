---
title: "SDK Admin & System"
description: "Schema introspection, access-control policy, DLQ stats, and health checks in @wavehouse/sdk."
---

Operational surfaces of `@wavehouse/sdk`. With one exception, everything on this page sits behind the server's admin gate: the caller must resolve to the admin role (`policy.admin_role`) or present the non-JWT [operator key](/api#authentication) — the SDK has no first-class operator-key option, but [`options.headers`](/sdk#custom-headers) can carry the `X-Operator-Key` header. The exception is `wh.sys.health()`, which calls the public, content-free `/v1/health` route and needs no credentials. See [Access Control](/access-control) for how roles resolve. Examples import from `@wavehouse/sdk`; using the CDN instead, import from `https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

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

> `wh.schema.list()`, `wh.schema.refresh()`, and `wh.from(t).schema()` hit `/v1/ops/schema*`, which are **admin-only** endpoints: the caller must pass the admin gate — resolve to the policy admin role (`admin_role`, `"admin"` by default) or present the non-JWT [operator key](/api#authentication). Unless the deployment deliberately sets `default_role` to the admin role (the loudly-warned dev-only setting), construct the client with an admin-role token — or send the operator key via [`options.headers`](/sdk#custom-headers) — or these calls return `403`.

---

## Policy — `wh.policy`

Manage Hasura-style access control policies. Requires the admin gate — the admin role (`policy.admin_role`) or the [operator key](/api#authentication).

```ts
// Get current policy
const { data: policy } = await wh.policy.get();

// Update policy
await wh.policy.set({
  default_role: 'viewer',
  tables: {
    clicks: {
      select: {
        viewer: {
          allow_columns: ['page', 'button', 'received_timestamp'],
          filter: { tenant_id: { _eq: '{{ jwt.app_metadata.tenant_id }}' } },
        },
        admin: { allow_columns: ['*'] },
      },
    },
  },
});

// Validate without applying (dry run)
const { data } = await wh.policy.validate(policyDraft);
// data: { valid: true } or error with validation details
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

`wh.dlq.stream()` exists in the API but is **not yet functional**. Live DLQ streaming is tracked in [#197](https://github.com/Wave-RF/WaveHouse/issues/197).

---

## System — `wh.sys`

Content-free server-online check.

```ts
// health() hits the public, content-free /v1/health route — 200/503, no body.
// Use it to check a server is reachable before sending data.
const result = await wh.sys.health();
if (result.ok) {
  // server is up and past boot
}
// on failure, result.error carries the reason (network vs. server error)
```

> Readiness (`/readyz`) is intentionally **not** exposed through the SDK as it runs a ClickHouse query per call – it is a load-balancer / reverse-proxy concern, not the client's. Probe `/readyz` directly from your orchestrator if you need it.

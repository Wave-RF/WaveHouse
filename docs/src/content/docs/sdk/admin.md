---
title: "SDK Admin & System"
description: "Schema introspection, access-control policy, DLQ stats, and health checks in @wavehouse/sdk."
---

Operational surfaces of `@wavehouse/sdk`. Everything here except
`wh.sys.health()` requires the admin role (`policy.admin_role`) — see
[Access Control](/access-control) for how roles resolve.
Examples import from `@wavehouse/sdk`; using the CDN instead, import from
`https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

:::caution[Admin paths moved to `/v1/ops/*`]
Server-side, these endpoints moved from `/v1/admin/*` and the top-level
`/v1/schema`/`/v1/dlq/stats` to `/v1/ops/*` with no aliases — upgrade
`@wavehouse/sdk` alongside the server, or every admin call on this page
returns `404` against a renamed server. The ops-path client is unreleased —
see the [Installation note](/sdk#installation) for the `@dev`-tag build
(`npm i @wavehouse/sdk@dev`) that targets the new paths.
:::

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

> `wh.schema.list()`, `wh.schema.refresh()`, and `wh.from(t).schema()` hit `/v1/ops/schema*`, which are **admin-only** endpoints. Against any non-dev policy (anything but `default_role: admin`), construct the client with an admin-role token or these calls return `403`.

---

## Policy — `wh.policy`

Manage Hasura-style access control policies. Requires the admin role (`policy.admin_role`).

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

Dead Letter Queue operations. Requires the admin role (`policy.admin_role`).

```ts
// Get DLQ statistics
const { data } = await wh.dlq.list();
// data: { tables: { "clicks": 3, "users": 0 }, total: 3 }

// Stats for a specific table
const { data } = await wh.dlq.table('clicks');
```

`wh.dlq.stream()` exists in the API but is **not yet functional**: there is no server-side DLQ stream today (the SSE bridge only carries `ingest.>` subjects), so it connects and receives no events — live DLQ streaming is tracked in [#197](https://github.com/Wave-RF/WaveHouse/issues/197).

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

> Readiness (`/readyz`) is intentionally **not** exposed through the SDK — it runs a ClickHouse query per call and is a load-balancer / reverse-proxy concern, not the client's. Probe `/readyz` directly from your orchestrator if you need it.

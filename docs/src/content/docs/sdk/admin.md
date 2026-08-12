---
title: "SDK Admin & System"
description: "Schema introspection, access-control policy, DLQ stats, and health checks in @wavehouse/sdk."
---

Operational surfaces of `@wavehouse/sdk`. Except for `wh.sys.health()`, all require the admin role (`policy.admin_role`)—see [Access Control](/access-control). Examples import from `@wavehouse/sdk`; for CDN, use `https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

## Schema — `wh.schema`

Introspect ClickHouse table schemas.

```ts
// List all table schemas
const { data: schemas } = await wh.schema.list();
// schemas: { clicks: { name: 'clicks', columns: [...] }, users: { ... } }

// Force refresh from ClickHouse
await wh.schema.refresh();
```

Individual table schema is available via `wh.from('clicks').schema()`.

> `wh.schema.list()`, `wh.schema.refresh()`, and `wh.from(t).schema()` hit **admin-only** `/v1/schema*` endpoints. Use an admin-role token or these return `403` against non-dev policies.

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

`wh.dlq.stream()` is **not yet functional**; there is no server-side DLQ stream today (SSE only carries `ingest.>` subjects). Live streaming is tracked in [#197](https://github.com/Wave-RF/WaveHouse/issues/197).

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

> Readiness (`/readyz`) is **not** exposed via SDK as it runs a ClickHouse query per call; probe `/readyz` directly from your orchestrator.

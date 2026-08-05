---
title: "Go SDK Admin & System"
description: "Schema introspection, access-control policy, DLQ stats, and health checks in the WaveHouse Go SDK."
---

Operational surfaces of `github.com/Wave-RF/WaveHouse/clients/go`.
Everything here except `client.Sys.Health` requires the admin role
(`policy.admin_role`) — see [Access Control](/access-control) for how roles
resolve. Compare with the TypeScript SDK's [Admin & System](/sdk/admin)
page.

## Schema — `client.Schema`

Introspect ClickHouse table schemas.

```go
// List all table schemas.
schemas, err := wh.Schema.List(ctx)
// schemas is wavehouse.Schemas — map[string]TableSchema, keyed by table name

// Force refresh from ClickHouse.
err = wh.Schema.Refresh(ctx)
```

Individual table schema is also available via `wh.From("clicks").Schema(ctx)`.

> `wh.Schema.List`, `wh.Schema.Refresh`, and `wh.From(t).Schema` hit
> `/v1/schema*`, which are **admin-only** endpoints. Against any non-dev
> policy (anything but `default_role: admin`), construct the client with an
> admin-role token or these calls return a `*wavehouse.Error` with
> `Status: 403`.

---

## Policy — `client.Policy`

Manage Hasura-style access control policies. Requires the admin role
(`policy.admin_role`).

```go
// Get current policy.
policy, err := wh.Policy.Get(ctx)

// Update policy.
tenantFilter := "{{ jwt.app_metadata.tenant_id }}"
err = wh.Policy.Set(ctx, &wavehouse.Policy{
    DefaultRole: "viewer",
    Tables: map[string]wavehouse.TablePolicy{
        "clicks": {
            Select: map[string]wavehouse.RolePermissions{
                "viewer": {
                    AllowColumns: []string{"page", "button", "received_timestamp"},
                    Filter: map[string]wavehouse.PolicyFilter{
                        "tenant_id": {Eq: &tenantFilter},
                    },
                },
                "admin": {AllowColumns: []string{"*"}},
            },
        },
    },
})

// Validate without applying (dry run).
result, err := wh.Policy.Validate(ctx, policyDraft)
// result.Valid == true, or err wraps the validation failure details
```

`PolicyFilter`'s fields (`Eq`, `Neq`, `Gt`, `Lt`, `In`) are `*string`, not
`string` — an intentional empty-string comparison round-trips distinctly
from an absent operator. Take the address of a local variable (as above) or
write a small helper if you find yourself doing this often:

```go
func strPtr(s string) *string { return &s }
```

---

## DLQ — `client.DLQ`

Dead Letter Queue operations. Requires the admin role (`policy.admin_role`).

```go
// Get DLQ statistics.
stats, err := wh.DLQ.List(ctx)
// stats.Tables: map[string]int{"clicks": 3, "users": 0}
// stats.Total: 3

// Stats for a specific table.
stats, err = wh.DLQ.Table(ctx, "clicks")
```

`wh.DLQ.Stream(opts)` exists in the API but is **not yet functional**:
there is no server-side DLQ stream today (the SSE bridge only carries
`ingest.>` subjects), so it connects and receives no events — live DLQ
streaming is tracked in
[#197](https://github.com/Wave-RF/WaveHouse/issues/197).

---

## System — `client.Sys`

Content-free server-online check.

```go
// Health hits the public, content-free /v1/health route — 200 → nil error,
// any other status (including 503) → a non-nil *wavehouse.Error.
// Use it to check a server is reachable before sending data.
if err := wh.Sys.Health(ctx); err != nil {
    // server is unreachable or not yet past boot
    log.Println(err)
}
```

> Readiness (`/readyz`) is intentionally **not** exposed through the SDK —
> it runs a ClickHouse query per call and is a load-balancer / reverse-proxy
> concern, not the client's. Probe `/readyz` directly from your
> orchestrator if you need it.

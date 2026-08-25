---
title: "Go SDK Admin & System"
description: "Schema introspection, access-control policy, DLQ stats, and health checks in the WaveHouse Go SDK."
---

Operational surfaces of `github.com/Wave-RF/WaveHouse/clients/go`. Every namespace here except `client.Sys.Health` sits behind the server's admin gate on `/v1/ops/*`, which a caller clears either with a JWT resolving to the policy admin role (`admin_role`, `"admin"` by default) or with the server's non-JWT [operator key](/api#authentication) sent as `X-Operator-Key` via [`ClientOptions.Headers`](/sdk/go#clientoptions). Without one, these calls return a `*wavehouse.Error` with `Status: 403` — unless the deployment sets `default_role` to admin, which is dev-only. See [Access Control](/access-control), and the TypeScript SDK's [Admin & System](/sdk/admin) page.

## Schema — `client.Schema`

Introspect ClickHouse table schemas. `Schema.List`, `Schema.Refresh`, and `From(t).Schema` all hit `/v1/ops/schema*`.

```go
// List all table schemas.
schemas, err := wh.Schema.List(ctx)
// schemas is wavehouse.Schemas — map[string]TableSchema, keyed by table name

// Force refresh from ClickHouse.
err = wh.Schema.Refresh(ctx)

// One table: wh.From("clicks").Schema(ctx)
```

---

## Policy — `client.Policy`

Manage Hasura-style access control policies.

```go
// Get current policy.
policy, err := wh.Policy.Get(ctx)

// Update policy.
tenantFilter := "{{ jwt.app_metadata.tenant_id }}"
policyDraft := &wavehouse.Policy{
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
}
err = wh.Policy.Set(ctx, policyDraft)

// Validate without applying (dry run).
result, err := wh.Policy.Validate(ctx, policyDraft)
// result.Valid == true, or err wraps the validation failure details
```

`PolicyFilter` fields (`Eq`, `Neq`, `Gt`, `Lt`, `In`) are `*string`, so an empty string is distinguishable from an absent operator — hence the `tenantFilter` variable above, or a `func strPtr(s string) *string { return &s }` helper.

---

## DLQ — `client.DLQ`

Dead Letter Queue statistics.

```go
// Totals across tables.
stats, err := wh.DLQ.List(ctx)
// stats.Tables: map[string]int{"clicks": 3, "users": 0}
// stats.Total: 3

// Stats for a specific table.
stats, err = wh.DLQ.Table(ctx, "clicks")
```

`wh.DLQ.Stream(opts)` is **not yet functional**: no server-side DLQ stream exists (the SSE bridge carries only `ingest.>` subjects), so it connects and receives nothing. Tracked in [#197](https://github.com/Wave-RF/WaveHouse/issues/197).

---

## System — `client.Sys`

The one surface on this page that needs no credentials.

```go
// Health hits the public, content-free /v1/health route — 200 → nil error,
// any other status (including 503) → a non-nil *wavehouse.Error.
// Use it to check a server is reachable before sending data.
if err := wh.Sys.Health(ctx); err != nil {
    // server is unreachable or not yet past boot
    log.Println(err)
}
```

> Readiness (`/readyz`) is intentionally **not** exposed through the SDK — it runs a ClickHouse query per call and is a load-balancer / reverse-proxy concern. Probe it directly from your orchestrator.

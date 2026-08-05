---
title: "Go SDK Pipes"
description: "Execute and manage named query pipes with the WaveHouse Go SDK."
---

Named pipes are server-defined, parameterized queries — the
[Named Pipes guide](/pipes) covers defining them. The SDK executes pipes for
any allowed role and manages their definitions under the admin role.
Compare with the TypeScript SDK's [Pipes](/sdk/pipes) page.

## Named Pipes — `client.Pipe(name, params)`

Execute a pre-defined named query pipe. Returns a `*PipeRef`; unlike the
TypeScript SDK's `PipeRef` (which is `PromiseLike`), you always call
`.FetchUntyped(ctx)` or the package-level `wavehouse.Fetch[Row]` explicitly.

```go
rows, err := wavehouse.Fetch[map[string]any](ctx,
    wh.Pipe("top_pages", map[string]any{"start_date": "2026-01-01", "limit": 50}),
)
```

### `wavehouse.Fetch[Row](ctx, pipeRef)`

Execute and decode results into `[]Row`. Package-level generic function
(Go has no generic methods) — the same pattern as `FetchTyped` for queries
and `SQL` for raw SQL.

```go
type TopPage struct {
    Page  string `json:"page"`
    Views int    `json:"views"`
}

rows, err := wavehouse.Fetch[TopPage](ctx, wh.Pipe("top_pages", map[string]any{"limit": 50}))
```

### `.FetchUntyped(ctx)`

Execute and decode results into `[]map[string]any`. The ordinary
(non-generic) method form of `Fetch`.

```go
rows, err := wh.Pipe("top_pages", nil).FetchUntyped(ctx)
```

Pass `nil` for `params` when the pipe takes none, or the pipe requires only
parameters with server-side defaults.

### `.Stream(opts)`

Open a live stream from the pipe's underlying query. See
[Streaming](/sdk/go/streaming).

This streams by table name, using the pipe's own name as the table — it
only works when the pipe name is also a valid table name. This matches the
TypeScript SDK's `PipeRef.stream()`, which has the same limitation.

```go
stream := wh.Pipe("top_pages", nil).Stream(nil)
```

---

## Pipes Admin — `client.Pipes`

Manage named query pipes. Requires the admin role (`policy.admin_role`).

```go
// List all pipes.
pipes, err := wh.Pipes.List(ctx)

// Get a single pipe definition.
pipe, err := wh.Pipes.Get(ctx, "top_pages")

// Create or update.
err = wh.Pipes.Set(ctx, "top_pages", wavehouse.PipeDef{
    SQL: "SELECT page, count() as views FROM clicks GROUP BY page LIMIT {{limit}}",
    Parameters: []wavehouse.ParamDef{
        {Name: "limit", Type: "number", Required: false, Default: 100},
    },
    Description:  "Top pages by view count",
    AllowedRoles: []string{"viewer", "admin"},
})

// Delete.
err = wh.Pipes.Delete(ctx, "old_pipe")
```

`PipeDef` is `Pipe` minus the `Name` field — the name is already in the
`Set`/`Get`/`Delete` call's path argument:

```go
type PipeDef struct {
    SQL          string
    Parameters   []ParamDef
    Description  string
    AllowedRoles []string
}
```

---
title: "Go SDK Pipes"
description: "Execute and manage named query pipes with the WaveHouse Go SDK."
---

Named pipes are server-defined, parameterized queries ([Named Pipes guide](/pipes)). The SDK executes them for allowed roles and manages definitions under the admin role. Compare with the TypeScript SDK's [Pipes](/sdk/pipes) page.

## Named Pipes — `client.Pipe(name, params)`

Execute a pre-defined named query pipe. Returns a `*PipeRef`. Unlike the TypeScript SDK's `PromiseLike` `PipeRef`, you must explicitly call `.FetchUntyped(ctx)` or the package-level `wavehouse.Fetch[Row]`.

```go
rows, err := wavehouse.Fetch[map[string]any](ctx,
    wh.Pipe("top_pages", map[string]any{"start_date": "2026-01-01", "limit": 50}),
)
```

### `wavehouse.Fetch[Row](ctx, pipeRef)`

Execute and decode results into `[]Row`. Package-level generic function (Go has no generic methods) — same pattern as `FetchTyped` for queries and `SQL` for raw SQL.

```go
type TopPage struct {
    Page  string `json:"page"`
    Views int    `json:"views"`
}

rows, err := wavehouse.Fetch[TopPage](ctx, wh.Pipe("top_pages", map[string]any{"limit": 50}))
```

### `.FetchUntyped(ctx)`

Execute and decode results into `[]map[string]any`. The non-generic method form of `Fetch`.

```go
rows, err := wh.Pipe("top_pages", nil).FetchUntyped(ctx)
```

Pass `nil` for `params` if the pipe takes none or only requires server-side defaults.

### `.Stream(opts)`

Open a live stream from the pipe's underlying query; see [Streaming](/sdk/go/streaming). Streams by table name using the pipe's own name, so it works only when that name is a valid table name — the same limitation as the TypeScript SDK's `PipeRef.stream()`.

```go
stream := wh.Pipe("top_pages", nil).Stream(nil)
```

---

## Pipes Admin — `client.Pipes`

Manage named query pipes. These sit behind the admin gate on `/v1/ops/*`, which a caller clears one of two ways: a JWT resolving to the policy admin role (`policy.admin_role`), or the server's non-JWT [operator key](/api#authentication) sent as `X-Operator-Key` via [`ClientOptions.Headers`](/sdk/go#clientoptions).

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

`PipeDef` is `Pipe` minus `Name` — the name is the method's path argument:

```go
type PipeDef struct {
    SQL          string
    Parameters   []ParamDef
    Description  string
    AllowedRoles []string
}
```

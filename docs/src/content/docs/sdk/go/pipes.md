---
title: "Go SDK Pipes"
description: "Execute and manage named query pipes with the WaveHouse Go SDK."
---

Named pipes are server-defined, parameterized queries ([Named Pipes guide](/pipes)). The SDK executes them for allowed roles and manages their definitions under the admin role. Compare with the TypeScript SDK's [Pipes](/sdk/pipes) page.

## Named Pipes — `client.Pipe(name, params)`

Returns a `*PipeRef` for a pre-defined named query pipe. Unlike the TypeScript SDK's `PromiseLike` `PipeRef`, you execute it explicitly with `.FetchUntyped(ctx)` or the package-level `wavehouse.Fetch[Row]`. Pass `nil` for `params` if the pipe takes none, or only needs its server-side defaults.

### `wavehouse.Fetch[Row](ctx, pipeRef)`

Executes the pipe and decodes results into `[]Row`. A package-level generic function, since Go has no generic methods — the same pattern as `FetchTyped` for queries and `SQL` for raw SQL.

```go
type TopPage struct {
    Page  string `json:"page"`
    Views int    `json:"views"`
}

rows, err := wavehouse.Fetch[TopPage](ctx,
    wh.Pipe("top_pages", map[string]any{"start_date": "2026-01-01", "limit": 50}),
)
```

### `.FetchUntyped(ctx)`

The non-generic method form: decodes results into `[]map[string]any`.

```go
rows, err := wh.Pipe("top_pages", nil).FetchUntyped(ctx)
```

### `.Stream(opts)`

Subscribes to live events using the pipe's name as a table name; see [Streaming](/sdk/go/streaming). The pipe's SQL and params are **not** applied — where a table of that name exists you receive its raw events, and otherwise the stream stays silent rather than erroring. This mirrors the TypeScript SDK's `PipeRef.stream()`; both wait on a pipe-aware stream endpoint ([#445](https://github.com/Wave-RF/WaveHouse/issues/445)).

```go
stream := wh.Pipe("top_pages", nil).Stream(nil)
defer stream.Close()
```

---

## Pipes Admin — `client.Pipes`

Create, read, and delete pipe definitions. These sit behind the [admin gate](/sdk/go/admin) on `/v1/ops/*`.

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

`PipeDef` is `Pipe` minus `Name`, which the methods take as a path argument:

```go
type PipeDef struct {
    SQL          string
    Parameters   []ParamDef
    Description  string
    AllowedRoles []string
}
```

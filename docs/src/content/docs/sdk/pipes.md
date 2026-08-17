---
title: "SDK Pipes"
description: "Execute and manage named query pipes with @wavehouse/sdk."
---

Named pipes are server-defined, parameterized queries — the
[Named Pipes guide](/pipes) covers defining them. The SDK executes pipes for
any allowed role and manages their definitions under the admin role.
Examples import from `@wavehouse/sdk`; using the CDN instead, import from
`https://esm.sh/@wavehouse/sdk` (see [Imports & Runtimes](/sdk#imports--runtimes)).

## Named Pipes — `wh.pipe(name, params?)`

Execute a pre-defined named query pipe. Returns a `PipeRef` which is **PromiseLike**.

```ts
// These are equivalent (PipeRef is PromiseLike):
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 }).fetch();
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 });
```

### `.fetch(opts?)`

Execute and return results. Takes `PipeRequestOptions` — `{ signal }` only,
narrower than the `.fetch(opts?)` on a [query builder](/sdk/queries), which also
accepts `limit`. Passing a `limit` is a compile error rather than a silent
no-op.

`limit` is typed `never` rather than left out, so the rejection also catches a
value passed in a variable — leaving it out would only reject an inline object.
That cuts both ways: a value *declared* as `RequestOptions` is rejected whether
or not it actually carries a limit, since the type permits one. If you share one
options object across calls, type it as `PipeRequestOptions` — the table and
query-builder `.fetch()` accept that too — or inline `{ signal }` at the pipe
call.

There is no per-call row cap here: the endpoint binds your `params` as the
pipe's parameters, so a limit has to be declared in the pipe's SQL as
`{{limit}}` (see [Named Pipes](/pipes)) and passed as `wh.pipe(name, { limit })`,
as in the example above.

### `.stream(opts?)`

Open a live stream. See [Streaming](/sdk/streaming).

---

## Pipes Admin — `wh.pipes`

Manage named query pipes. Requires the admin role (`policy.admin_role`).
These methods hit `/v1/ops/pipes*`; the client build targeting `/v1/ops` is
unreleased — see the [Installation note](/sdk#installation) for the
`@dev`-tag build.

```ts
// List all pipes
const { data: pipes } = await wh.pipes.list();

// Get a single pipe definition
const { data: pipe } = await wh.pipes.get('top_pages');

// Create or update
await wh.pipes.set('top_pages', {
  sql: 'SELECT page, count() as views FROM clicks GROUP BY page LIMIT {{limit}}',
  parameters: [{ name: 'limit', type: 'number', required: false, default: 100 }],
  description: 'Top pages by view count',
  allowed_roles: ['viewer', 'admin'],
});

// Delete
await wh.pipes.delete('old_pipe');
```

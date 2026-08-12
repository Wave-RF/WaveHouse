---
title: "SDK Pipes"
description: "Execute and manage named query pipes with @wavehouse/sdk."
---

Named pipes are server-defined, parameterized queries (see [Named Pipes guide](/pipes)). The SDK executes them for allowed roles and manages definitions via the admin role. Import from `@wavehouse/sdk` or `https://esm.sh/@wavehouse/sdk` ([Imports & Runtimes](/sdk#imports--runtimes)).

## Named Pipes — `wh.pipe(name, params?)`

Execute a pre-defined named query pipe. Returns a **PromiseLike** `PipeRef`.

```ts
// These are equivalent (PipeRef is PromiseLike):
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 }).fetch();
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 });
```

### `.fetch(opts?)`

Execute and return results.

### `.stream(opts?)`

Open a live stream (see [Streaming](/sdk/streaming)).

---

## Pipes Admin — `wh.pipes`

Manage named query pipes. Requires the admin role (`policy.admin_role`).

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

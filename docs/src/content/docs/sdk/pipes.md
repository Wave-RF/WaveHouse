---
title: "SDK Pipes"
description: "Execute and manage named query pipes with @wavehouse/sdk."
---

Named pipes are server-defined, parameterized queries — the
[Named Pipes guide](/pipes) covers defining them. The SDK executes pipes for
any allowed role and manages their definitions under the admin role.

## Named Pipes — `wh.pipe(name, params?)`

Execute a pre-defined named query pipe. Returns a `PipeRef` which is **PromiseLike**.

```ts
// These are equivalent:
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 }).fetch();
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 }); // PromiseLike
```

### `.fetch(opts?)`

Execute and return results.

### `.stream(opts?)`

Open a live stream. See [Streaming](/sdk/streaming).

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

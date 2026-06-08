# @wavehouse/sdk

Zero-dependency TypeScript client for [WaveHouse](https://github.com/Wave-RF/WaveHouse) — schema-aware real-time API gateway for ClickHouse.

## Installation

```bash
npm install @wavehouse/sdk
```

This works in any framework that uses a bundler — React, Vue, Svelte, Angular, Astro, SolidJS, or plain Vite — with `import { createClient } from '@wavehouse/sdk'`.

## Use without a build step (CDN)

No bundler, no `npm`, no framework required — drop the SDK straight into an HTML file you deploy over FTP, object storage, or any static host.

**ES module (recommended).** Modern browsers run `type="module"` natively:

```html
<script type="module">
  import { createClient } from 'https://esm.sh/@wavehouse/sdk';

  const wh = createClient({ baseURL: 'https://your-wavehouse.example.com' });
  const { data, error } = await wh.from('clicks').select('page').limit(10);
  console.log(data ?? error);
</script>
```

Pin a version for production (`https://esm.sh/@wavehouse/sdk@0.1.0`). jsDelivr (`https://cdn.jsdelivr.net/npm/@wavehouse/sdk/+esm`) and unpkg (`https://unpkg.com/@wavehouse/sdk?module`) serve the same ES module.

**Classic global (`<script src>`).** For pages that can't use ES modules, the bundled IIFE build attaches everything to a `WaveHouse` global:

```html
<script src="https://cdn.jsdelivr.net/npm/@wavehouse/sdk"></script>
<script>
  const wh = WaveHouse.createClient({ baseURL: 'https://your-wavehouse.example.com' });
  wh.from('clicks').select('page').limit(10).then(({ data }) => console.log(data));
</script>
```

**Versioning.** A bare CDN URL serves the latest published **release**; pin for production (`@wavehouse/sdk@0.1.0`) or float on a range (`@0` for the newest 0.x, `@0.1` for 0.1.x). Builds from `main` are published under the `dev` tag — `@wavehouse/sdk@dev` — for trying unreleased changes.

Streaming (`.stream()`) uses the browser's native `EventSource`, so it works in both forms with no polyfill.

## Quick Start

```ts
import { createClient } from '@wavehouse/sdk';

const wh = createClient({
  baseURL: 'http://localhost:8080',
  auth: async () => getAccessToken(), // omit for public access
});
```

### Query Data

```ts
// Structured query with the builder
const { data, error } = await wh
  .from('clicks')
  .select('page', 'button')
  .where('score', '>', 10)
  .count('*', 'total')
  .groupBy('page', 'button')
  .orderBy('total', 'desc')
  .limit(50)
  .timeRange('received_timestamp', '1h');

// Raw SQL (requires admin role)
const { data } = await wh.sql('SELECT page, count() FROM clicks GROUP BY page LIMIT 10');

// Named pipe
const { data } = await wh.pipe('top_pages', { start_date: '2026-01-01', limit: 50 });
```

### Insert Data

```ts
const { error } = await wh.from('clicks').insert({ page: '/home', button: 'signup', score: 42.5 });
```

### Stream Real-Time Events

```ts
// Callback pattern
const stream = wh.from('clicks').stream();
const unsub = stream.subscribe({
  next: (event) => console.log(event.data),
  status: (state) => console.log(state), // 'connecting' | 'live' | 'reconnecting' | 'closed'
});

// Async iterator pattern
for await (const event of wh.from('clicks').stream()) {
  console.log(event.table, event.data);
}
```

### Pagination

```ts
let result = await wh.from('clicks').select().orderBy('received_timestamp', 'desc').limit(100);

while (result.hasMore && result.next) {
  result = await result.next();
}
```

## Type Safety

Pass a `Database` type for autocomplete on table names and row shapes:

```ts
interface Database {
  clicks: { page: string; button: string; score: number; received_timestamp: string };
  users: { id: string; name: string; email: string };
}

const wh = createClient<Database>({ baseURL: '...' });
const { data } = await wh.from('clicks').select('page').limit(10);
// data: Array<{ page: string; button: string; ... }> | null
```

## Error Handling

All operations return `{ data, error }` tuples — the SDK never throws.

```ts
const { data, error } = await wh.from('clicks').fetch();
if (error) {
  console.error(error.code, error.message, error.retryable);
  return;
}
// data is guaranteed non-null here
```

## Codegen

Generate TypeScript types from your live WaveHouse schema. The package ships a `wavehouse-codegen` bin, so after installing you can run it with `npx`:

```bash
npx wavehouse-codegen --url http://localhost:8080 --out ./src/db.d.ts
```

This introspects `/v1/schema`, maps ClickHouse column types to TypeScript, and outputs a `Database` interface you can pass to `createClient<Database>()`. `/v1/schema` is admin-only — pass `--auth <jwt>` against a secured server.

## Development & Testing

### Unit Tests

Unit tests mock all HTTP calls — no backend required.

```bash
npm test               # run unit tests (no backend needed)
npm run test:watch     # watch mode
npm run test:coverage  # coverage report
```

### E2E Integration Tests

E2E tests live in `tests/e2e/sdk/` (repo root) and exercise the full pipeline through the SDK. See `make test-e2e` in the [Development Guide](https://wavehouse.dev/development#e2e-tests-via-sdk).

## API Reference

See the full [SDK API Reference](https://wavehouse.dev/sdk) for detailed documentation of every method, type, and option.

## License

Apache-2.0 © Wave RF — see [LICENSE](./LICENSE).

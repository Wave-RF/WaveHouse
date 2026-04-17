/**
 * SDK Playground — Admin
 *
 * Demonstrates admin-level access: raw SQL, schema management, policy CRUD,
 * pipes management, DLQ inspection.
 * Run: npm run playground:admin
 *
 * Prerequisites:
 *   docker compose -f playground/compose.yaml up -d
 *   npx tsx playground/setup.ts
 */

import { createClient } from '../src/index.js';

async function makeJWT(claims: Record<string, unknown>): Promise<string> {
  const { createHmac } = await import('node:crypto');
  const header = { alg: 'HS256', typ: 'JWT' };
  const payload = { ...claims, iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 3600 };
  const encode = (obj: unknown) =>
    Buffer.from(JSON.stringify(obj)).toString('base64url');

  const unsigned = `${encode(header)}.${encode(payload)}`;
  const sig = createHmac('sha256', 'sdk-dev-secret').update(unsigned).digest('base64url');
  return `${unsigned}.${sig}`;
}

const wh = createClient({
  baseURL: 'http://localhost:8080',
  auth: () => makeJWT({ sub: 'admin-1', role: 'admin', tenant_id: 'acme' }),
});

async function main() {
  console.log('═══ WaveHouse SDK — Admin Playground ═══\n');

  // ── Schema operations ─────────────────────────────────────────────────────
  console.log('1. List all schemas');
  const schemas = await wh.schema.list();
  if (schemas.data) {
    for (const [name, schema] of Object.entries(schemas.data)) {
      console.log(`   ${name}: ${schema.columns.map((c: any) => c.name).join(', ')}`);
    }
  } else {
    console.log('   Error:', schemas.error);
  }

  console.log('\n2. Force schema refresh');
  const refresh = await wh.schema.refresh();
  console.log('   ', refresh.error ? `Error: ${refresh.error.message}` : '✓ Refreshed');

  // ── Raw SQL ───────────────────────────────────────────────────────────────
  console.log('\n3. Raw SQL: table row counts');
  for (const table of ['clicks', 'events', 'users']) {
    const result = await wh.sql(`SELECT count() as cnt FROM default.${table}`);
    if (result.data) {
      console.log(`   ${table}: ${(result.data[0] as any)?.cnt} rows`);
    }
  }

  // ── Policy management ─────────────────────────────────────────────────────
  console.log('\n4. Policy management');

  // Get current policy
  const currentPolicy = await wh.policy.get();
  console.log('   Current policy:', currentPolicy.data ?? currentPolicy.error);

  // Define and validate a policy
  const testPolicy = {
    tables: {
      clicks: {
        viewer: {
          columns: ['page', 'country', 'duration_ms', 'received_timestamp'],
          filter: { column: 'country', op: 'eq', value: '{{ jwt.country }}' },
        },
        admin: {
          columns: '*',
        },
      },
    },
  };

  console.log('\n   Validating test policy...');
  const validation = await wh.policy.validate(testPolicy);
  console.log('   Validation:', validation.data ?? validation.error);

  // Apply the policy
  console.log('\n   Setting test policy...');
  const setPol = await wh.policy.set(testPolicy);
  console.log('   ', setPol.error ? `Error: ${setPol.error.message}` : '✓ Policy set');

  // ── Pipes management ──────────────────────────────────────────────────────
  console.log('\n5. Pipes (named queries)');

  // Create a pipe
  const pipeDef = {
    sql: 'SELECT page, count() as views FROM default.clicks GROUP BY page ORDER BY views DESC LIMIT {{limit:10}}',
    description: 'Top pages by views',
    allowed_roles: ['admin', 'viewer'],
  };
  const setResult = await wh.pipes.set('top_pages', pipeDef);
  console.log('   Set "top_pages":', setResult.error ? setResult.error.message : '✓');

  // List pipes
  const pipeList = await wh.pipes.list();
  if (pipeList.data) {
    console.log('   Available pipes:', pipeList.data);
  }

  // Execute the pipe
  console.log('\n   Executing "top_pages" pipe:');
  const pipeResult = await wh.pipe('top_pages', { limit: 5 });
  if (pipeResult.data) {
    for (const row of pipeResult.data) {
      console.log(`     ${(row as any).page}: ${(row as any).views} views`);
    }
  } else {
    console.log('   Error:', pipeResult.error);
  }

  // ── DLQ inspection ────────────────────────────────────────────────────────
  console.log('\n6. DLQ stats');
  const dlqStats = await wh.dlq.list();
  console.log('   DLQ:', dlqStats.data ?? dlqStats.error);

  // ── Table-level schema introspection ──────────────────────────────────────
  console.log('\n7. Table schema for "clicks"');
  const clickSchema = await wh.from('clicks').schema();
  if (clickSchema.data) {
    for (const col of (clickSchema.data as any).columns ?? []) {
      console.log(`   ${col.name}: ${col.type}${col.is_nullable ? ' (nullable)' : ''}`);
    }
  } else {
    console.log('   Error:', clickSchema.error);
  }

  // ── Async iterator streaming ──────────────────────────────────────────────
  console.log('\n8. Async iterator stream (3s)');
  const stream = wh.from('clicks').stream();

  // Insert in background
  setTimeout(async () => {
    for (let i = 0; i < 3; i++) {
      await wh.from('clicks').insert({
        event_id: `admin-${Date.now()}-${i}`,
        page: '/admin-test',
        user_id: 'admin-1',
        session_id: 'admin-sess',
        country: 'US',
        duration_ms: i * 500,
      });
    }
  }, 500);

  // Read via async iterator with a timeout
  const timeout = setTimeout(() => stream.close(), 3000);
  for await (const event of stream) {
    console.log(`   Event: ${JSON.stringify(event.data)}`);
  }
  clearTimeout(timeout);
  console.log('   Stream ended.');

  // ── Cleanup ───────────────────────────────────────────────────────────────
  console.log('\n9. Cleanup: delete test pipe');
  const del = await wh.pipes.delete('top_pages');
  console.log('   ', del.error ? `Error: ${del.error.message}` : '✓ Deleted');

  console.log('\n═══ Done ═══');
  process.exit(0);
}

main().catch(console.error);

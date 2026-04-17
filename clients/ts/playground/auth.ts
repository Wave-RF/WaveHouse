/**
 * SDK Playground — Authenticated User
 *
 * Demonstrates JWT-authenticated access: queries, streaming via WebSocket.
 * Run: npm run playground:auth
 *
 * Prerequisites:
 *   docker compose -f playground/compose.yaml up -d
 *   npx tsx playground/setup.ts
 *
 * Note: The compose file enables auth with dev_mode=true,
 * so any JWT signed with "sdk-dev-secret" is accepted.
 */

import { createClient } from '../src/index.js';

// Simple JWT builder for dev (HS256). Uses Node's built-in crypto.
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
  auth: () => makeJWT({ sub: 'user-42', role: 'viewer', tenant_id: 'acme' }),
  // transport: 'auto' → will select WS since auth is set
});

async function main() {
  console.log('═══ WaveHouse SDK — Authenticated Playground ═══\n');

  // ── Verify auth is working ────────────────────────────────────────────────
  console.log('1. Health check (auth header sent)');
  const health = await wh.sys.health();
  console.log('   ', health.data ?? health.error);

  // ── Fetch with auth ───────────────────────────────────────────────────────
  console.log('\n2. Fetch clicks (authenticated)');
  const clicks = await wh.from('clicks').fetch({ limit: 5 });
  if (clicks.data) {
    console.log(`   Got ${clicks.data.length} rows`);
    if (clicks.data[0]) console.log('   Sample:', clicks.data[0]);
  } else {
    console.log('   Error:', clicks.error);
  }

  // ── Query builder with auth ───────────────────────────────────────────────
  console.log('\n3. Aggregation: events by type');
  const byType = await wh
    .from('events')
    .select('type')
    .count('type', 'event_count')
    .groupBy('type')
    .orderBy('event_count', 'desc');

  if (byType.data) {
    for (const row of byType.data) {
      console.log(`   ${(row as any).type}: ${(row as any).event_count}`);
    }
  } else {
    console.log('   Error:', byType.error);
  }

  // ── Insert with auth ──────────────────────────────────────────────────────
  console.log('\n4. Insert event (authenticated)');
  const ins = await wh.from('events').insert({
    event_id: `auth-${Date.now()}`,
    type: 'page_view',
    user_id: 'user-42',
    payload: JSON.stringify({ screen: '/dashboard' }),
    source: 'web',
  });
  console.log('   Result:', ins.data ?? ins.error);

  // ── WebSocket streaming (5 seconds) ───────────────────────────────────────
  console.log('\n5. WS stream (listening 5s for events)...');
  const stream = wh.from('events').stream();
  const unsub = stream.subscribe({
    next: (event) => console.log(`   Event: ${JSON.stringify(event.data)}`),
    status: (s) => console.log(`   Status: ${s}`),
    error: (e) => console.log(`   Error: ${e.message}`),
  });

  // Insert while streaming
  setTimeout(async () => {
    for (let i = 0; i < 3; i++) {
      await wh.from('events').insert({
        event_id: `ws-${Date.now()}-${i}`,
        type: 'click',
        user_id: 'user-42',
        payload: JSON.stringify({ button: `btn-${i}` }),
        source: 'web',
      });
    }
  }, 1000);

  await new Promise((r) => setTimeout(r, 5000));
  unsub();
  stream.close();
  console.log('   Stream closed.');

  // ── Raw SQL (if role allows) ──────────────────────────────────────────────
  console.log('\n6. Raw SQL query');
  const sql = await wh.sql('SELECT count() as cnt FROM default.clicks');
  if (sql.data) {
    console.log('   Total clicks:', (sql.data[0] as any)?.cnt);
  } else {
    console.log('   Error:', sql.error);
  }

  console.log('\n═══ Done ═══');
  process.exit(0);
}

main().catch(console.error);

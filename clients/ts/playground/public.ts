/**
 * SDK Playground — Public (No Auth)
 *
 * Demonstrates unauthenticated access: queries, table operations, SSE streaming.
 * Run: npm run playground:public
 *
 * Prerequisites:
 *   docker compose -f playground/compose.yaml up -d
 *   npx tsx playground/setup.ts
 */

import { createClient } from '../src/index.js';

const wh = createClient({ baseURL: 'http://localhost:8080' });

async function main() {
  console.log('═══ WaveHouse SDK — Public Playground ═══\n');

  // ── Health check ──────────────────────────────────────────────────────────
  console.log('1. Health check');
  const health = await wh.sys.health();
  console.log('   ', health.data ?? health.error);

  // ── List schemas ──────────────────────────────────────────────────────────
  console.log('\n2. List schemas');
  const schemas = await wh.schema.list();
  if (schemas.data) {
    const tableNames = Object.keys(schemas.data);
    console.log(`   Tables: ${tableNames.join(', ')}`);
  } else {
    console.log('   Error:', schemas.error);
  }

  // ── Simple fetch (all clicks, default limit) ─────────────────────────────
  console.log('\n3. Fetch clicks (default limit)');
  const clicks = await wh.from('clicks').fetch();
  if (clicks.data) {
    console.log(`   Got ${clicks.data.length} rows, hasMore: ${clicks.hasMore}`);
    if (clicks.data.length > 0) {
      console.log('   First row:', clicks.data[0]);
    }
  } else {
    console.log('   Error:', clicks.error);
  }

  // ── Query builder: select + where + limit ─────────────────────────────────
  console.log('\n4. Query builder: top 5 US clicks');
  const usClicks = await wh
    .from('clicks')
    .select('page', 'user_id', 'duration_ms')
    .where('country', '=', 'US')
    .orderBy('duration_ms', 'desc')
    .limit(5);

  if (usClicks.data) {
    console.log(`   Got ${usClicks.data.length} rows:`);
    for (const row of usClicks.data) {
      console.log(`     ${(row as any).page} — ${(row as any).duration_ms}ms`);
    }
  } else {
    console.log('   Error:', usClicks.error);
  }

  // ── Aggregation: count clicks per page ────────────────────────────────────
  console.log('\n5. Aggregation: count clicks by page');
  const perPage = await wh
    .from('clicks')
    .select('page')
    .count('page', 'click_count')
    .groupBy('page')
    .orderBy('click_count', 'desc')
    .limit(10);

  if (perPage.data) {
    for (const row of perPage.data) {
      console.log(`   ${(row as any).page}: ${(row as any).click_count}`);
    }
  } else {
    console.log('   Error:', perPage.error);
  }

  // ── Insert a row ──────────────────────────────────────────────────────────
  console.log('\n6. Insert a click');
  const insert = await wh.from('clicks').insert({
    event_id: `playground-${Date.now()}`,
    page: '/playground',
    user_id: 'playground-user',
    session_id: 'sess-001',
    country: 'US',
    duration_ms: 1234,
  });
  console.log('   Result:', insert.data ?? insert.error);

  // ── Pagination cursor ─────────────────────────────────────────────────────
  console.log('\n7. Pagination: first 3 events, then next page');
  const page1 = await wh.from('events').select().limit(3).fetch();
  if (page1.data) {
    console.log(`   Page 1: ${page1.data.length} rows, hasMore: ${page1.hasMore}`);
    if (page1.hasMore && page1.next) {
      const page2 = await page1.next();
      console.log(`   Page 2: ${page2.data?.length ?? 0} rows`);
    }
  }

  // ── SSE streaming (5 seconds) ─────────────────────────────────────────────
  console.log('\n8. Stream (listening 5s for clicks)...');
  const stream = wh.from('clicks').stream({ transport: 'ws' });
  const unsub = stream.subscribe({
    next: (event) => console.log('   Event:', JSON.stringify(event.data)),
    status: (s) => console.log(`   Status: ${s}`),
  });

  // Insert some data to see in the stream
  setTimeout(async () => {
    for (let i = 0; i < 3; i++) {
      await wh.from('clicks').insert({
        event_id: `stream-${Date.now()}-${i}`,
        page: '/live',
        user_id: 'streamer',
        session_id: 'sess-live',
        country: 'US',
        duration_ms: i * 100,
      });
    }
  }, 1000);

  // Clean up after 5 seconds
  await new Promise((r) => setTimeout(r, 5000));
  unsub();
  stream.close();
  console.log('   Stream closed.');

  console.log('\n═══ Done ═══');
  process.exit(0);
}

main().catch(console.error);

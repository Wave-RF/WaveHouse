/**
 * SDK Playground — Setup Script
 *
 * Creates ClickHouse tables and seeds them with sample data via WaveHouse.
 * Run: npx tsx playground/setup.ts
 */

import { createHmac } from 'node:crypto';

const WH_URL = process.env.WH_URL ?? 'http://localhost:8080';
const CH_URL = process.env.CH_URL ?? 'http://localhost:8123';

// Setup hits admin-only endpoints (schema refresh, policy, ingest seeding), so
// it signs an admin JWT with the dev secret the playground server validates.
function makeJWT(claims: Record<string, unknown>): string {
  const header = { alg: 'HS256', typ: 'JWT' };
  const payload = {
    ...claims,
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const encode = (obj: unknown) =>
    Buffer.from(JSON.stringify(obj)).toString('base64url');
  const unsigned = `${encode(header)}.${encode(payload)}`;
  const sig = createHmac('sha256', 'sdk-dev-secret').update(unsigned).digest('base64url');
  return `${unsigned}.${sig}`;
}
const ADMIN_AUTH = () =>
  `Bearer ${makeJWT({ sub: 'playground-setup', role: 'admin', tenant_id: 'acme' })}`;

// ── ClickHouse table creation (via HTTP interface) ──────────────────────────

const tables = [
  `CREATE TABLE IF NOT EXISTS default.clicks (
    event_id     String,
    page         String,
    user_id      String,
    session_id   String,
    referrer     String DEFAULT '',
    country      String DEFAULT 'US',
    duration_ms  UInt32 DEFAULT 0,
    received_timestamp DateTime64(3) DEFAULT now64(3)
  ) ENGINE = MergeTree()
  ORDER BY (page, received_timestamp)`,

  `CREATE TABLE IF NOT EXISTS default.events (
    event_id     String,
    type         String,
    user_id      String,
    payload      String DEFAULT '{}',
    source       String DEFAULT 'web',
    received_timestamp DateTime64(3) DEFAULT now64(3)
  ) ENGINE = MergeTree()
  ORDER BY (type, received_timestamp)`,

  `CREATE TABLE IF NOT EXISTS default.users (
    user_id      String,
    name         String,
    email        String,
    plan         String DEFAULT 'free',
    created_at   DateTime64(3) DEFAULT now64(3),
    received_timestamp DateTime64(3) DEFAULT now64(3)
  ) ENGINE = MergeTree()
  ORDER BY (user_id)`,
];

async function createTables() {
  console.log('Creating ClickHouse tables...');
  for (const ddl of tables) {
    const res = await fetch(CH_URL, { method: 'POST', body: ddl });
    if (!res.ok) {
      const text = await res.text();
      // Ignore "already exists" errors
      if (!text.includes('already exists')) {
        console.error(`  DDL failed: ${text.trim()}`);
        continue;
      }
    }
    const tableName = ddl.match(/default\.(\w+)/)?.[1];
    console.log(`  ✓ ${tableName}`);
  }
}

// ── Seed data via WaveHouse ingest ──────────────────────────────────────────

const pages = ['/', '/about', '/pricing', '/docs', '/blog', '/contact'];
const countries = ['US', 'GB', 'DE', 'FR', 'JP', 'BR', 'AU', 'CA'];
const eventTypes = ['page_view', 'click', 'signup', 'purchase', 'logout'];
const sources = ['web', 'mobile', 'api'];
const plans = ['free', 'pro', 'enterprise'];

function randomFrom<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomId(): string {
  return Math.random().toString(36).slice(2, 10);
}

async function ingest(table: string, data: Record<string, unknown>) {
  const res = await fetch(`${WH_URL}/v1/ingest?table=${table}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: ADMIN_AUTH() },
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const text = await res.text();
    console.error(`  Ingest to ${table} failed: ${text.trim()}`);
  }
}

async function seedData() {
  console.log('\nWaiting for WaveHouse schema discovery...');
  // Give WaveHouse time to discover the new tables
  await new Promise((r) => setTimeout(r, 6000));

  // Trigger schema refresh (admin-only)
  await fetch(`${WH_URL}/v1/schema/refresh`, {
    method: 'POST',
    headers: { Authorization: ADMIN_AUTH() },
  });
  await new Promise((r) => setTimeout(r, 2000));

  const userIds = Array.from({ length: 20 }, () => randomId());

  console.log('Seeding users...');
  for (const userId of userIds) {
    await ingest('users', {
      user_id: userId,
      name: `User ${userId}`,
      email: `${userId}@example.com`,
      plan: randomFrom(plans),
    });
  }

  console.log('Seeding clicks (50 rows)...');
  for (let i = 0; i < 50; i++) {
    await ingest('clicks', {
      event_id: randomId(),
      page: randomFrom(pages),
      user_id: randomFrom(userIds),
      session_id: randomId(),
      referrer: Math.random() > 0.5 ? 'https://google.com' : '',
      country: randomFrom(countries),
      duration_ms: Math.floor(Math.random() * 30000),
    });
  }

  console.log('Seeding events (50 rows)...');
  for (let i = 0; i < 50; i++) {
    await ingest('events', {
      event_id: randomId(),
      type: randomFrom(eventTypes),
      user_id: randomFrom(userIds),
      payload: JSON.stringify({ screen: randomFrom(pages) }),
      source: randomFrom(sources),
    });
  }

  // Give the async pipeline time to flush to ClickHouse
  console.log('\nWaiting for async ingest to flush...');
  await new Promise((r) => setTimeout(r, 3000));

  console.log('✓ Setup complete! Tables: clicks, events, users');
}

async function waitForService(url: string, label: string, maxAttempts = 30) {
  for (let i = 1; i <= maxAttempts; i++) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {
      // not ready yet
    }
    if (i === 1) process.stdout.write(`Waiting for ${label}...`);
    else process.stdout.write('.');
    await new Promise((r) => setTimeout(r, 1000));
  }
  console.error(`\n✗ ${label} not reachable after ${maxAttempts}s`);
  process.exit(1);
}

// Grant the three demo personas: `public` (the default_role public.ts relies
// on — no token), and `viewer` (auth.ts). `admin` (admin.ts) is the built-in
// admin_role and needs no policy entry.
async function bootstrapPolicy(): Promise<void> {
  const perms = () => ({
    public: { allow_columns: ['*'] },
    viewer: { allow_columns: ['*'] },
  });
  const policy = {
    default_role: 'public',
    tables: {
      clicks: { select: perms(), insert: perms() },
      events: { select: perms(), insert: perms() },
      users: { select: perms(), insert: perms() },
    },
  };
  const res = await fetch(`${WH_URL}/v1/admin/policy`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', Authorization: ADMIN_AUTH() },
    body: JSON.stringify(policy),
  });
  if (!res.ok) {
    throw new Error(`Failed to bootstrap policy: ${res.status} ${await res.text()}`);
  }
  console.log('  ✓ policy bootstrapped (default_role=public, viewer)');
}

async function main() {
  try {
    await waitForService(CH_URL, 'ClickHouse');
    await waitForService(`${WH_URL}/health`, 'WaveHouse');
    console.log(' ready');
    await createTables();
    await bootstrapPolicy();
    await seedData();
  } catch (e) {
    console.error('Setup failed:', e);
    process.exit(1);
  }
}

main();

/**
 * Vitest globalSetup — Smart Docker Compose lifecycle manager.
 *
 * Two modes:
 *   - **Dev mode**: WaveHouse detected on :8080 (e.g. `make dev` / air running).
 *     Auth tests are skipped, data tests use admin client. Quick sanity check.
 *   - **Full mode**: No WaveHouse found → starts from compose with real auth
 *     (dev_mode=false). All tests run including JWT validation and policy enforcement.
 */

import { execSync } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const CH_URL = 'http://localhost:8123';
const WH_URL = 'http://localhost:8080';
const COMPOSE_FILE = path.resolve(__dirname, '../compose.yaml');
const FIXTURES_DIR = path.resolve(__dirname, '../fixtures');
const STATE_FILE = path.resolve(__dirname, '.setup-state.json');

export interface SetupState {
  startedClickHouse: boolean;
  startedWaveHouse: boolean;
  /** 'dev' = existing WaveHouse detected; 'full' = compose-started with auth */
  mode: 'dev' | 'full';
}

async function probe(url: string, timeoutMs = 2000): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    return res.ok;
  } catch {
    return false;
  }
}

async function waitFor(url: string, label: string, maxSeconds = 60): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < maxSeconds * 1000) {
    if (await probe(url)) return;
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`${label} not reachable at ${url} after ${maxSeconds}s`);
}

function compose(args: string): void {
  execSync(`docker compose -f ${COMPOSE_FILE} ${args}`, {
    stdio: 'inherit',
    env: { ...process.env },
  });
}

async function applyFixtures(): Promise<void> {
  const files = readdirSync(FIXTURES_DIR)
    .filter((f) => f.endsWith('.sql'))
    .sort();

  for (const file of files) {
    const sql = readFileSync(path.join(FIXTURES_DIR, file), 'utf-8');
    const res = await fetch(CH_URL, { method: 'POST', body: sql });
    if (!res.ok) {
      const text = await res.text();
      if (!text.includes('already exists')) {
        throw new Error(`Fixture ${file} failed: ${text}`);
      }
    }
  }
}

/** Build an admin JWT for setup operations. */
function makeSetupJWT(): string {
  const header = { alg: 'HS256', typ: 'JWT' };
  const payload = {
    sub: 'e2e-setup',
    role: 'admin',
    tenant_id: 'acme',
    iat: Math.floor(Date.now() / 1000),
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const encode = (obj: unknown) =>
    Buffer.from(JSON.stringify(obj)).toString('base64url');
  const unsigned = `${encode(header)}.${encode(payload)}`;
  const sig = createHmac('sha256', 'sdk-dev-secret').update(unsigned).digest('base64url');
  return `${unsigned}.${sig}`;
}

/**
 * Bootstrap a permissive test policy for all test tables.
 *
 * Includes a wildcard "*" role so that requests with an empty role
 * (auth disabled in dev mode) are still allowed. Named roles (viewer,
 * admin) are included for full-mode tests that exercise real JWT auth.
 */
async function bootstrapTestPolicy(mode: 'dev' | 'full'): Promise<void> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (mode === 'full') {
    headers['Authorization'] = `Bearer ${makeSetupJWT()}`;
  }

  const rolePerms = (extra?: Record<string, unknown>) => ({
    '*': { allow_columns: ['*'], ...extra },
    viewer: { allow_columns: ['*'], ...extra },
    admin: { allow_columns: ['*'], raw_sql: true, ...extra },
  });
  const policy = {
    tables: {
      clicks: { select: rolePerms(), insert: rolePerms() },
      events: { select: rolePerms(), insert: rolePerms() },
      users: { select: rolePerms(), insert: rolePerms() },
    },
  };

  const res = await fetch(`${WH_URL}/v1/admin/policy`, {
    method: 'PUT',
    headers,
    body: JSON.stringify(policy),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to bootstrap test policy: ${res.status} ${text}`);
  }
}

export async function setup(): Promise<void> {
  console.log('\n🔍 Probing for running services...');

  const state: SetupState = {
    startedClickHouse: false,
    startedWaveHouse: false,
    mode: 'full',
  };

  const chAlive = await probe(`${CH_URL}/ping`);
  const whAlive = await probe(`${WH_URL}/health`);

  if (chAlive) {
    console.log('  ✓ ClickHouse already running on :8123');
  } else {
    console.log('  ⏳ Starting ClickHouse...');
    compose('up -d clickhouse');
    state.startedClickHouse = true;
    await waitFor(`${CH_URL}/ping`, 'ClickHouse');
    console.log('  ✓ ClickHouse ready');
  }

  if (whAlive) {
    console.log('  ✓ WaveHouse already running on :8080');
    state.mode = 'dev';
  } else {
    console.log('  ⏳ Starting WaveHouse (full auth mode)...');
    compose('--profile app up -d wavehouse');
    state.startedWaveHouse = true;
    await waitFor(`${WH_URL}/health`, 'WaveHouse');
    console.log('  ✓ WaveHouse ready');
  }

  // Apply SQL fixtures (idempotent)
  console.log('  📦 Applying SQL fixtures...');
  await applyFixtures();

  // Trigger schema refresh so WaveHouse discovers the tables.
  // In full mode, auth is enabled so we need a JWT.
  const refreshHeaders: Record<string, string> = {};
  if (state.mode === 'full') {
    refreshHeaders['Authorization'] = `Bearer ${makeSetupJWT()}`;
  }
  await fetch(`${WH_URL}/v1/schema/refresh`, {
    method: 'POST',
    headers: refreshHeaders,
  });

  // Poll schema endpoint until our test tables are discovered.
  const expectedTables = ['clicks', 'events', 'users'];
  const schemaStart = Date.now();
  while (Date.now() - schemaStart < 30_000) {
    const res = await fetch(`${WH_URL}/v1/schema`, { headers: refreshHeaders });
    if (res.ok) {
      const schema = await res.json();
      if (expectedTables.every((t) => t in schema)) break;
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  console.log('  ✓ Schema refreshed');

  // Bootstrap a permissive test policy so all roles (including empty role
  // in dev mode) can access test tables. Overwrites any stale policy from
  // prior runs. In full mode, uses JWT auth; in dev mode, auth is disabled
  // so the PUT goes through without a token.
  await bootstrapTestPolicy(state.mode);
  console.log('  ✓ Test policy bootstrapped');

  // Persist state so tests know the mode and teardown knows what to stop
  writeFileSync(STATE_FILE, JSON.stringify(state));

  // Print mode banner
  if (state.mode === 'dev') {
    console.log('\n  ⚡ DEV MODE — WaveHouse was already running');
    console.log('  Auth/policy tests will be SKIPPED (auth may be disabled).');
    console.log('  For a full E2E pass, stop the dev server and re-run.\n');
  } else {
    console.log('\n  🔒 FULL MODE — WaveHouse started with auth enabled');
    console.log('  All tests will run including JWT validation + policy enforcement.\n');
  }
}

export async function teardown(): Promise<void> {
  if (process.env.KEEP_RUNNING === 'true' || process.env.KEEP_RUNNING === '1') {
    console.log('\n⏭  KEEP_RUNNING set — leaving services up\n');
    return;
  }

  let state: SetupState;
  try {
    state = JSON.parse(readFileSync(STATE_FILE, 'utf-8'));
  } catch {
    // No state file means setup didn't start anything
    return;
  }

  if (state.startedWaveHouse || state.startedClickHouse) {
    console.log('\n🧹 Tearing down services...');
    compose('--profile app down -v');
    console.log('  ✓ Services stopped\n');
  }
}

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createClient, WaveHouseClient } from './client.js';
import { TableRef } from './table.js';
import { PipeRef, PipesNamespace } from './pipes.js';
import { SchemaNamespace } from './schema.js';
import { PolicyNamespace } from './policy.js';
import { DLQNamespace } from './dlq.js';
import { SysNamespace } from './sys.js';

let fetchSpy: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchSpy = vi.fn().mockResolvedValue(
    new Response(JSON.stringify([]), { status: 200 }),
  );
  vi.stubGlobal('fetch', fetchSpy);
});

afterEach(() => vi.restoreAllMocks());

describe('createClient', () => {
  it('returns a WaveHouseClient instance', () => {
    const client = createClient({ baseURL: 'http://localhost:8080' });
    expect(client).toBeInstanceOf(WaveHouseClient);
  });

  it('strips trailing slashes from baseURL', () => {
    const client = createClient({ baseURL: 'http://localhost:8080///' });
    expect(client._ctx.baseURL).toBe('http://localhost:8080');
  });

  it('defaults maxRetries to 2', () => {
    const client = createClient({ baseURL: 'http://localhost:8080' });
    expect(client._ctx.options.maxRetries).toBe(2);
  });

  it('respects custom maxRetries', () => {
    const client = createClient({
      baseURL: 'http://localhost:8080',
      options: { maxRetries: 5 },
    });
    expect(client._ctx.options.maxRetries).toBe(5);
  });

  it('passes auth function to context', () => {
    const authFn = () => Promise.resolve('token');
    const client = createClient({ baseURL: 'http://localhost:8080', auth: authFn });
    expect(client._ctx.auth).toBe(authFn);
  });
});

describe('WaveHouseClient namespaces', () => {
  const client = createClient({ baseURL: 'http://localhost:8080' });

  it('has schema namespace', () => {
    expect(client.schema).toBeInstanceOf(SchemaNamespace);
  });

  it('has policy namespace', () => {
    expect(client.policy).toBeInstanceOf(PolicyNamespace);
  });

  it('has dlq namespace', () => {
    expect(client.dlq).toBeInstanceOf(DLQNamespace);
  });

  it('has sys namespace', () => {
    expect(client.sys).toBeInstanceOf(SysNamespace);
  });

  it('has pipes namespace', () => {
    expect(client.pipes).toBeInstanceOf(PipesNamespace);
  });
});

describe('WaveHouseClient.from()', () => {
  it('returns a TableRef', () => {
    const client = createClient({ baseURL: 'http://localhost:8080' });
    const table = client.from('clicks');
    expect(table).toBeInstanceOf(TableRef);
  });

  it('passes the table name through', async () => {
    const client = createClient({ baseURL: 'http://localhost:8080' });
    client.from('events').fetch();

    // Wait for the fetch to be called
    await vi.waitFor(() => expect(fetchSpy).toHaveBeenCalledTimes(1));
    expect(fetchSpy.mock.calls[0][0]).toContain('/v1/tables/events');
  });
});

describe('WaveHouseClient.pipe()', () => {
  it('returns a PipeRef', () => {
    const client = createClient({ baseURL: 'http://localhost:8080' });
    const pipe = client.pipe('top_pages');
    expect(pipe).toBeInstanceOf(PipeRef);
  });

  it('PipeRef is PromiseLike', () => {
    const client = createClient({ baseURL: 'http://localhost:8080' });
    const pipe = client.pipe('top_pages', { limit: 10 });
    expect(typeof pipe.then).toBe('function');
  });
});

describe('WaveHouseClient.sql()', () => {
  it('delegates to sql() and POSTs to /v1/admin/query', async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify([{ count: 42 }]), { status: 200 }),
    );

    const client = createClient({ baseURL: 'http://localhost:8080' });
    const result = await client.sql('SELECT count() FROM clicks');

    expect(result.data).toEqual([{ count: 42 }]);
    expect(fetchSpy.mock.calls[0][0]).toContain('/v1/admin/query');
  });

  it('throws a migration-clear error when called with a legacy params array', () => {
    // The second argument used to be a positional-`?` params array.
    // TS callers get a compile-time error; JS callers (or `any`-typed
    // call sites) would silently pass the array as `opts` and get
    // confusing downstream SQL errors. The runtime guard catches
    // that case and points at the migration.
    const client = createClient({ baseURL: 'http://localhost:8080' });
    // Force-cast so the test compiles under strict TS.
    const callWithLegacyParams = () =>
      (client.sql as unknown as (q: string, p: unknown) => unknown)(
        'SELECT * FROM clicks WHERE id = ?',
        ['some-id'],
      );

    expect(callWithLegacyParams).toThrow(/client\.sql\(sql, params\) was removed/);
  });
});

describe('_createStream transport selection', () => {
  // We can't easily test the private _createStream directly,
  // but we test through .from().stream() behavior by verifying
  // which global constructors are accessed.

  // vitest 4 tightened `vi.fn().mockImplementation()` behaviour: the
  // implementation is no longer implicitly callable with `new`. Arrow
  // functions don't have [[Construct]], so `new esConstructor()` in
  // SSETransport.connect / WSTransport._doConnect now throws
  // `TypeError: () => ({`. The fix is to pass a regular function (or a
  // class) — both have [[Construct]]. Keeps the test's intent intact.

  it('uses SSE when no auth (auto mode)', () => {
    const esConstructor = vi.fn(function () {
      return {
        addEventListener: vi.fn(),
        close: vi.fn(),
        readyState: 0,
        onopen: null,
        onmessage: null,
        onerror: null,
      };
    });
    vi.stubGlobal('EventSource', esConstructor);

    const client = createClient({ baseURL: 'http://localhost:8080' });
    // Stream indirectly triggers transport creation
    const stream = client.from('clicks').stream();
    expect(stream).toBeDefined();
    expect(esConstructor).toHaveBeenCalled();

    vi.unstubAllGlobals();
  });

  it('uses WS when auth is set (auto mode)', async () => {
    const wsConstructor = vi.fn(function () {
      return {
        addEventListener: vi.fn(),
        close: vi.fn(),
        readyState: 0,
        onopen: null,
        onmessage: null,
        onerror: null,
        onclose: null,
      };
    });
    vi.stubGlobal('WebSocket', wsConstructor);

    const client = createClient({
      baseURL: 'http://localhost:8080',
      auth: () => 'token',
    });
    const stream = client.from('clicks').stream();
    expect(stream).toBeDefined();
    // WSTransport._doConnect() is async (awaits auth token) — wait for it
    await vi.waitFor(() => expect(wsConstructor).toHaveBeenCalled());

    vi.unstubAllGlobals();
  });

  it('forces SSE transport when explicitly set', () => {
    const esConstructor = vi.fn(function () {
      return {
        addEventListener: vi.fn(),
        close: vi.fn(),
        readyState: 0,
        onopen: null,
        onmessage: null,
        onerror: null,
      };
    });
    vi.stubGlobal('EventSource', esConstructor);

    const client = createClient({
      baseURL: 'http://localhost:8080',
      auth: () => 'token',
      transport: 'sse',
    });
    const stream = client.from('clicks').stream({ transport: 'sse' });
    expect(esConstructor).toHaveBeenCalled();

    vi.unstubAllGlobals();
  });
});

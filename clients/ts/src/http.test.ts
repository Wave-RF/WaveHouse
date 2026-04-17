import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { request } from './http.js';
import type { HttpContext } from './types.js';

function makeCtx(overrides?: Partial<HttpContext>): HttpContext {
  return {
    baseURL: 'http://localhost:8080',
    options: { maxRetries: 0 },
    ...overrides,
  };
}

describe('request', () => {
  let fetchSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchSpy = vi.fn();
    vi.stubGlobal('fetch', fetchSpy);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('makes a successful GET request', async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ status: 'ok' }), { status: 200 }));

    const result = await request<{ status: string }>(makeCtx(), {
      method: 'GET',
      path: '/health',
    });

    expect(result.data).toEqual({ status: 'ok' });
    expect(result.error).toBeNull();
    expect(fetchSpy).toHaveBeenCalledOnce();

    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain('/health');
    expect(init.method).toBe('GET');
  });

  it('makes a POST request with JSON body', async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    await request(makeCtx(), {
      method: 'POST',
      path: '/v1/ingest/clicks',
      body: { page: '/home' },
    });

    const [, init] = fetchSpy.mock.calls[0];
    expect(init.method).toBe('POST');
    expect(init.body).toBe(JSON.stringify({ page: '/home' }));
    expect(init.headers['Content-Type']).toBe('application/json');
  });

  it('injects auth token when auth function provided', async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx({ auth: async () => 'my-token' }), {
      method: 'GET',
      path: '/v1/schema',
    });

    const [, init] = fetchSpy.mock.calls[0];
    expect(init.headers['Authorization']).toBe('Bearer my-token');
  });

  it('does not inject auth header when auth returns empty', async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx({ auth: async () => '' }), {
      method: 'GET',
      path: '/v1/schema',
    });

    const [, init] = fetchSpy.mock.calls[0];
    expect(init.headers['Authorization']).toBeUndefined();
  });

  it('returns error for 4xx responses', async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: 'unknown table: foo' }), { status: 404 }),
    );

    const result = await request(makeCtx(), { method: 'GET', path: '/v1/schema/foo' });

    expect(result.data).toBeNull();
    expect(result.error?.status).toBe(404);
    expect(result.error?.message).toBe('unknown table: foo');
    expect(result.error?.retryable).toBe(false);
  });

  it('returns error for 500 without retry when maxRetries=0', async () => {
    fetchSpy.mockResolvedValue(
      new Response(JSON.stringify({ error: 'internal' }), { status: 500 }),
    );

    const result = await request(makeCtx({ options: { maxRetries: 0 } }), {
      method: 'GET',
      path: '/health',
    });

    expect(result.error?.status).toBe(500);
    expect(fetchSpy).toHaveBeenCalledOnce();
  });

  it('retries on 5xx up to maxRetries', async () => {
    fetchSpy
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: 'err' }), { status: 500 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: 'err' }), { status: 500 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const result = await request(makeCtx({ options: { maxRetries: 2 } }), {
      method: 'GET',
      path: '/health',
    });

    expect(result.data).toEqual({ ok: true });
    expect(fetchSpy).toHaveBeenCalledTimes(3);
  });

  it('retries on network error', async () => {
    fetchSpy
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const result = await request(makeCtx({ options: { maxRetries: 1 } }), {
      method: 'GET',
      path: '/health',
    });

    expect(result.data).toEqual({ ok: true });
    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it('returns abort error when signal is aborted', async () => {
    fetchSpy.mockRejectedValue(new DOMException('Aborted', 'AbortError'));

    const ac = new AbortController();
    ac.abort();

    const result = await request(makeCtx(), {
      method: 'GET',
      path: '/health',
      signal: ac.signal,
    });

    expect(result.error?.code).toBe('ABORTED');
    expect(result.error?.retryable).toBe(false);
  });

  it('appends query params to URL', async () => {
    fetchSpy.mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));

    await request(makeCtx(), {
      method: 'GET',
      path: '/v1/dlq/stats',
      params: { table: 'clicks' },
    });

    const [url] = fetchSpy.mock.calls[0];
    expect(url).toContain('table=clicks');
  });

  it('handles empty response body', async () => {
    fetchSpy.mockResolvedValue(new Response('', { status: 200 }));

    const result = await request(makeCtx(), {
      method: 'POST',
      path: '/v1/schema/refresh',
    });

    expect(result.data).toBeUndefined();
    expect(result.error).toBeNull();
  });
});

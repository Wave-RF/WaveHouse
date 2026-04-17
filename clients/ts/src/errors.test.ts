import { describe, it, expect, vi, beforeEach } from 'vitest';
import { parseErrorResponse, networkError, ok, okPage, err } from './errors.js';

describe('parseErrorResponse', () => {
  it('extracts error string from JSON body', async () => {
    const res = new Response(JSON.stringify({ error: 'unknown table: foo' }), {
      status: 404,
      statusText: 'Not Found',
    });
    const e = await parseErrorResponse(res);
    expect(e).toEqual({
      status: 404,
      code: 'HTTP_404',
      message: 'unknown table: foo',
      details: { error: 'unknown table: foo' },
      retryable: false,
    });
  });

  it('extracts message field from JSON body', async () => {
    const res = new Response(JSON.stringify({ message: 'bad request' }), {
      status: 400,
      statusText: 'Bad Request',
    });
    const e = await parseErrorResponse(res);
    expect(e.message).toBe('bad request');
  });

  it('falls back to statusText when body has no error/message', async () => {
    const res = new Response(JSON.stringify({ code: 123 }), {
      status: 500,
      statusText: 'Internal Server Error',
    });
    const e = await parseErrorResponse(res);
    expect(e.message).toBe('Internal Server Error');
  });

  it('handles non-JSON body gracefully', async () => {
    const res = new Response('plain text error', {
      status: 502,
      statusText: 'Bad Gateway',
    });
    const e = await parseErrorResponse(res);
    expect(e.message).toBe('Bad Gateway');
    expect(e.details).toBeUndefined();
  });

  it('marks 503 as retryable', async () => {
    const res = new Response(JSON.stringify({ error: 'service unavailable' }), {
      status: 503,
      statusText: 'Service Unavailable',
    });
    const e = await parseErrorResponse(res);
    expect(e.retryable).toBe(true);
  });

  it('marks 5xx as retryable', async () => {
    const res = new Response(JSON.stringify({ error: 'internal' }), {
      status: 500,
      statusText: 'Internal Server Error',
    });
    const e = await parseErrorResponse(res);
    expect(e.retryable).toBe(true);
  });

  it('marks 4xx as not retryable', async () => {
    const res = new Response(JSON.stringify({ error: 'forbidden' }), {
      status: 403,
      statusText: 'Forbidden',
    });
    const e = await parseErrorResponse(res);
    expect(e.retryable).toBe(false);
  });
});

describe('networkError', () => {
  it('wraps an Error instance', () => {
    const e = networkError(new TypeError('Failed to fetch'));
    expect(e).toEqual({
      status: 0,
      code: 'NETWORK_ERROR',
      message: 'Failed to fetch',
      retryable: true,
    });
  });

  it('wraps a string', () => {
    const e = networkError('timeout');
    expect(e.message).toBe('timeout');
    expect(e.retryable).toBe(true);
  });
});

describe('ok', () => {
  it('wraps data with null error', () => {
    const result = ok({ name: 'test' });
    expect(result.data).toEqual({ name: 'test' });
    expect(result.error).toBeNull();
  });
});

describe('okPage', () => {
  it('includes pagination fields', () => {
    const nextFn = vi.fn();
    const result = okPage([1, 2, 3], true, nextFn);
    expect(result.data).toEqual([1, 2, 3]);
    expect(result.error).toBeNull();
    expect(result.hasMore).toBe(true);
    expect(result.next).toBe(nextFn);
  });

  it('works without next function', () => {
    const result = okPage([1], false);
    expect(result.hasMore).toBe(false);
    expect(result.next).toBeUndefined();
  });
});

describe('err', () => {
  it('wraps error with null data', () => {
    const error = { status: 500, code: 'ERR', message: 'fail', retryable: true };
    const result = err(error);
    expect(result.data).toBeNull();
    expect(result.error).toBe(error);
  });
});

import type { Result, HttpContext } from './types.js';
import { request } from './http.js';
import { ok, err } from './errors.js';

/** Execute a raw SQL query against ClickHouse. */
export async function sql<Row = Record<string, unknown>>(
  ctx: HttpContext,
  query: string,
  params?: unknown[],
  opts?: { signal?: AbortSignal },
): Promise<Result<Row[]>> {
  const body: { sql: string; params?: unknown[] } = { sql: query };
  if (params && params.length > 0) body.params = params;

  const { data, error } = await request<Row[]>(ctx, {
    method: 'POST',
    path: '/v1/query',
    body,
    signal: opts?.signal,
  });
  if (error) return err(error);
  return ok(data!);
}

import type { Result, HttpContext } from './types.js';
import { request } from './http.js';
import { ok, err } from './errors.js';

/**
 * Execute a raw SQL query against ClickHouse.
 *
 * Backed by `POST /v1/admin/query`, which is gated on `admin` / `service` —
 * the same role set as the rest of `/v1/admin/*`. Callers using this helper
 * must hold a JWT with one of those roles, or run against a WaveHouse with
 * `auth.enabled=false` (the dev/test posture). Non-admin use cases should
 * use the structured query builder (`wh.from(table)...`) instead.
 */
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
    path: '/v1/admin/query',
    body,
    signal: opts?.signal,
  });
  if (error) return err(error);
  return ok(data!);
}

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
 *
 * The server proxies the SQL string verbatim to ClickHouse's HTTP interface,
 * so any ClickHouse-accepted statement works — including multi-statement
 * input (`SELECT 1; TRUNCATE t`) and arbitrary DDL/DML/SYSTEM verbs.
 *
 * **No parameter binding.** Positional `?` substitution is not supported
 * (ClickHouse's HTTP interface uses a different param style entirely).
 * Inline literals into the SQL string, or use ClickHouse's named-param
 * syntax (`WHERE id = {id:UInt32}`) and pass `param_id=42` via custom
 * query-string params if you need a server-evaluated substitution. The
 * structured query builder (`wh.from(...).select(...).where(...)`) is the
 * supported safe-binding path.
 */
export async function sql<Row = Record<string, unknown>>(
  ctx: HttpContext,
  query: string,
  opts?: { signal?: AbortSignal },
): Promise<Result<Row[]>> {
  const { data, error } = await request<Row[]>(ctx, {
    method: 'POST',
    path: '/v1/admin/query',
    body: { sql: query },
    signal: opts?.signal,
  });
  if (error) return err(error);
  return ok(data!);
}

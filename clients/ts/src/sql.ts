import type { Result, HttpContext } from './types.js';
import { request } from './http.js';
import { ok, err } from './errors.js';

/**
 * Execute a raw SQL query against ClickHouse.
 *
 * Backed by `POST /v1/admin/query`, which is gated on `admin` / `service` —
 * the same role set as the rest of `/v1/admin/*`. When authentication is
 * enabled, callers must hold a JWT with one of those roles. With
 * `auth.enabled=false` or `auth.dev_mode=true` (both dev/test postures)
 * the endpoint is open. Non-admin use cases should use the structured
 * query builder (`wh.from(table)...`) instead.
 *
 * The server proxies the SQL string verbatim to ClickHouse's HTTP interface,
 * so any ClickHouse-accepted statement works — including multi-statement
 * input (`SELECT 1; TRUNCATE t`) and arbitrary DDL/DML/SYSTEM verbs.
 *
 * **No parameter binding.** Positional `?` substitution is not supported.
 * The SDK has no way to forward ClickHouse-style named params
 * (`WHERE id = {id:UInt32}` with `param_id=42` on the query string) —
 * the proxy doesn't forward arbitrary query-string params to ClickHouse
 * and the SDK doesn't expose a hook to add them. Inline literals into
 * the SQL string, or — for safe binding from user-supplied input — use
 * the structured query builder (`wh.from(...).select(...).where(...)`).
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

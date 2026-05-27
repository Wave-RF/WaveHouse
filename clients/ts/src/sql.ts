import type { Result, HttpContext } from './types.js';
import { request } from './http.js';
import { ok, err } from './errors.js';

/**
 * Execute a raw SQL query against ClickHouse.
 *
 * Backed by `POST /v1/admin/query`, which requires the admin role — the same
 * gate as the rest of `/v1/admin/*`. Callers must hold a JWT whose role is the
 * configured `admin_role` (`"admin"` by default); there is no separate
 * `service` role. The JWT middleware always runs, so a request with no token
 * or an invalid one is denied, never granted. Non-admin use cases should use
 * the structured query builder (`wh.from(table)...`) instead.
 *
 * The server proxies the SQL string verbatim to ClickHouse's HTTP interface,
 * so any ClickHouse-accepted statement works — including multi-statement
 * input (`SELECT 1; TRUNCATE t`) and arbitrary DDL/DML/SYSTEM verbs.
 *
 * **JSON-row contract.** This helper returns `Result<Row[]>` and assumes the
 * response is the standard `FORMAT JSON` envelope (or an empty body for
 * no-result mutations — coerced to `[]`). Inline non-JSON `FORMAT` overrides
 * (`SELECT 1 FORMAT CSV` / `FORMAT TSV` / `FORMAT Pretty` / etc.) are NOT
 * compatible with `sql()` — the proxy passes the upstream Content-Type
 * through, and the SDK's JSON decoder throws on a non-JSON body, which the
 * retry layer surfaces as a `NETWORK_ERROR` result (not a structured
 * format-mismatch error). For CSV/TSV exports, hit `/v1/admin/query`
 * directly with `fetch()` and read the body as text.
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

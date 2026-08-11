/**
 * Resolve an SDK request path against the configured `baseURL`.
 *
 * The single URL-construction rule for every transport (REST in `http.ts`, SSE
 * in `stream/sse.ts`, the codegen CLI). Request paths are joined **onto** the
 * base rather than resolved as absolute ones, so a base carrying a path prefix
 * — `https://app.example.com/api/warehouse`, a WaveHouse behind a BFF,
 * app-server route, or path-routed ingress — keeps that prefix. Resolving
 * `/v1/query` against such a base would silently discard it (#428).
 *
 * @internal
 */
export function resolveURL(base: string, path: string, params?: Record<string, string>): URL {
  const root = new URL(base);
  // Normalize the base to a directory: a bare last segment (or a query/fragment
  // shadowing one) makes relative resolution *replace* the prefix, not extend it.
  root.search = "";
  root.hash = "";
  if (!root.pathname.endsWith("/")) root.pathname += "/";

  const url = new URL(path.replace(/^\/+/, ""), root);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, v);
    }
  }
  return url;
}

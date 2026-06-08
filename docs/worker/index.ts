// Cloudflare Worker entrypoint. The heavy lifting is still cloudflare-md-router:
// it serves the `.md` twin of any static page to known LLM crawlers (or any
// request that sends `Accept: text/markdown`), falling back to the HTML asset
// otherwise. This wrapper adds two small agent/crawler-discovery niceties on
// top of that handler:
//
//   1. `/sitemap.xml` → 301 to the real sitemap. `@astrojs/sitemap` emits
//      `sitemap-index.xml` (+ `sitemap-0.xml`) and robots.txt points there,
//      but crawlers/agents that probe the conventional `/sitemap.xml` path
//      instead hit the 404 page (a 32 KB HTML body). The redirect closes that
//      gap without standing up a second source of truth.
//   2. A `Link:` header (RFC 8288) on HTML page responses advertising that
//      page's markdown twin as an `alternate` representation. The twin is
//      already reachable via UA sniffing and `Accept` negotiation; the header
//      just makes it discoverable up front from a plain GET, no guessing.
import { createMdRouter } from "cloudflare-md-router";

interface Env {
  ASSETS: { fetch(request: Request): Promise<Response> };
}

// cloudflare-md-router types its handler against @cloudflare/workers-types
// (the Workers runtime lib). This file is type-checked in the docs project's
// DOM-lib context (`astro check`), where pulling those globals in collides with
// lib.dom (Request/Response/Headers are redefined). The two Response/Request
// shapes are runtime-compatible, so we bridge the universes once, here, and
// keep the rest of the file plain DOM-typed.
const mdRouter = createMdRouter() as unknown as {
  fetch(request: Request, env: Env): Promise<Response>;
};

// `/foo` → `/foo.md`, `/` → `/index.md`. Mirrors cloudflare-md-router's own
// default twin mapping (worker.ts `defaultMdPathFor`); kept in step with it so
// the advertised URL and the one the router actually serves never diverge.
function mdTwinFor(pathname: string): string {
  const trimmed = pathname.replace(/\/$/, "");
  return `${trimmed || "/index"}.md`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/sitemap.xml") {
      return Response.redirect(new URL("/sitemap-index.xml", url.origin).href, 301);
    }

    const response = await mdRouter.fetch(request, env);

    // Only page documents (GET/HEAD, extension-less path, 200 HTML) get the
    // twin advertisement — never assets, redirects, 404s, or the `.md` responses
    // themselves. HEAD is included so a header probe sees the same `Link` as the
    // GET (RFC 9110 §9.3.2). `new Response(body, response)` is the canonical way
    // to return an asset response with an extra header (asset headers are
    // otherwise immutable).
    const isPageDocument =
      (request.method === "GET" || request.method === "HEAD") &&
      response.status === 200 &&
      !/\.[a-zA-Z0-9]+$/.test(url.pathname) &&
      (response.headers.get("content-type") ?? "").includes("text/html");

    if (!isPageDocument) return response;

    const withTwinLink = new Response(response.body, response);
    withTwinLink.headers.append(
      "Link",
      `<${mdTwinFor(url.pathname)}>; rel="alternate"; type="text/markdown"`,
    );
    return withTwinLink;
  },
};

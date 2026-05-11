// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightImageZoom from "starlight-image-zoom";
import starlightGlossary from "starlight-glossary";
import starlightLlmTools from "starlight-llm-tools";
import rehypeMermaid from "rehype-mermaid";
import remarkMath from "remark-math";
import rehypeKatex from "rehype-katex";
import { sidebar } from "./src/config/sidebar.ts";

export default defineConfig({
  site: "https://wavehouse.dev",
  trailingSlash: "never",
  markdown: {
    syntaxHighlight: { excludeLangs: ["mermaid"] },
    remarkPlugins: [remarkMath],
    rehypePlugins: [[rehypeMermaid, { strategy: "inline-svg" }], rehypeKatex],
  },
  integrations: [
    starlight({
      title: "WaveHouse",
      description:
        "The open-source real-time API gateway for ClickHouse — schema-aware ingest, async batching, real-time streaming, and tiered query caching in a single binary.",
      head: [
        // PostHog
        {
          tag: "script",
          content: `!function(t,e){var o,n,p,r;e.__SV||(window.posthog=e,e._i=[],e.init=function(i,s,a){function g(t,e){var o=e.split(".");2==o.length&&(t=t[o[0]],e=o[1]),t[e]=function(){t.push([e].concat(Array.prototype.slice.call(arguments,0)))}}(p=t.createElement("script")).type="text/javascript",p.async=!0,p.src=s.api_host+"/static/array.js",(r=t.getElementsByTagName("script")[0]).parentNode.insertBefore(p,r);var u=e;for(void 0!==a?u=e[a]=[]:a="posthog",u.people=u.people||[],u.toString=function(t){var e="posthog";return"posthog"!==a&&(e+="."+a),t||(e+=" (stub)"),e},u.people.toString=function(){return u.toString(1)+".people (stub)"},o="init capture register register_once register_for_session unregister unregister_for_session getFeatureFlag getFeatureFlagPayload isFeatureEnabled reloadFeatureFlags updateEarlyAccessFeatureEnrollment getEarlyAccessFeatures on onFeatureFlags onSessionId getSurveys getActiveMatchingSurveys renderSurvey canRenderSurvey getNextSurveyStep identify setPersonProperties group resetGroups setPersonPropertiesForFlags resetPersonPropertiesForFlags setGroupPropertiesForFlags resetGroupPropertiesForFlags reset opt_in_capturing opt_out_capturing has_opted_in_capturing has_opted_out_capturing clear_opt_in_out_capturing debug".split(" "),n=0;n<o.length;n++)g(u,o[n]);e._i.push([i,s,a])},e.__SV=1)}(document,window.posthog||[]);
posthog.init('phc_xFG2NGQa7bFg4QjBp3MAn8kr8bAPJxM7GvKzfoNEwZwj',{api_host:'https://us.i.posthog.com'});`,
        },
      ],
      customCss: ["katex/dist/katex.min.css", "./src/styles/custom.css"],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/Wave-RF/WaveHouse",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/Wave-RF/WaveHouse/edit/main/docs/",
      },
      lastUpdated: true,
      expressiveCode: {
        themes: ["github-dark", "github-light"],
        // Alias ad-hoc fence languages used in our docs to real Shiki
        // grammars so they highlight instead of rendering as plain text.
        shiki: {
          langAlias: {
            env: "bash",
            dns: "ini",
          },
        },
      },
      sidebar,
      plugins: [
        starlightImageZoom(),
        starlightGlossary({
          // Always-on auto-discovery: each build, lint findings get
          // submitted to Wikipedia. Unambiguous hits land cleanly in
          // glossary.json with the article title + extract; everything is
          // logged loudly so we can git-diff every auto-add. Cache lives
          // at glossary-cache.json — clear it (or run `make
          // refresh-glossary`) to re-fetch every entry from Wikipedia.
          lint: {
            autoDiscover: true,
            // Acronyms whose Wikipedia search hits a disambiguation page
            // (or 404s) get retried with the expansion here as a hint.
            // If the expansion has no Wikipedia article, a stub entry
            // is created with the expansion as the term.
            acronymExpansions: {
              NATS: "NATS Messaging",
              JWT: "JSON Web Token",
              SDK: "Software development kit",
              SSE: "Server-sent events",
              KV: "Key-value database",
              CI: "Continuous integration",
              WS: "WebSocket",
              JWKS: "JSON Web Key Set",
              L1: "CPU cache",
              RFC: "Request for Comments",
              AST: "Abstract syntax tree",
              TTL: "Time to live",
              POST: "POST (HTTP)",
              UID: "User identifier",
              ID: "Identifier",
              BI: "Business intelligence",
            },
            // Suppress lint candidates that aren't real glossary terms
            // in this codebase, or that already have a glossary entry
            // under a different label (DLQ covers Dead Letter Queue).
            ignore: [
              "PR",                  // ambiguous (pull request, public relations, …)
              "E2E",                 // ambiguous (end-to-end testing/encryption)
              "Env Var",             // generic prose
              "Query Parameters",    // generic prose
              "Conventional Commits", // no Wikipedia article
              "Task Board",          // generic prose
              "Dead Letter Queue",   // covered by DLQ entry
            ],
          },
        }),
        starlightLlmTools(),
      ],
    }),
  ],
});

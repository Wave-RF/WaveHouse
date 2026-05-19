// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightImageZoom from "starlight-image-zoom";
import starlightLlmTools from "starlight-llm-tools";
import starlightLinksValidator from "starlight-links-validator";
import tailwindcss from "@tailwindcss/vite";
import rehypeMermaid from "rehype-mermaid";
import remarkMath from "remark-math";
import rehypeKatex from "rehype-katex";
import { sidebar } from "./src/config/sidebar.ts";

export default defineConfig({
  site: "https://wavehouse.dev",
  trailingSlash: "never",
  vite: {
    plugins: [tailwindcss()],
  },
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
posthog.init('phc_xFG2NGQa7bFg4QjBp3MAn8kr8bAPJxM7GvKzfoNEwZwj',{api_host:'https://us.i.posthog.com',defaults:'2026-01-30'});`,
        },
        // /favicon.ico legacy fallback handled by browsers auto-probing /.
        // Explicit 32×32 favicon entry so browsers don't downscale a too-large
        // SVG / pick the wrong size — the SVG itself stays as the higher-DPI
        // primary; this is the tabs-style fallback most Chromium builds use.
        { tag: "link", attrs: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg", sizes: "any" } },
        { tag: "link", attrs: { rel: "icon", type: "image/x-icon", sizes: "32x32", href: "/favicon.ico" } },
        { tag: "link", attrs: { rel: "apple-touch-icon", sizes: "180x180", href: "/apple-touch-icon.png" } },
        // Open Graph
        { tag: "meta", attrs: { property: "og:image", content: "https://wavehouse.dev/og.png" } },
        { tag: "meta", attrs: { property: "og:image:width", content: "1200" } },
        { tag: "meta", attrs: { property: "og:image:height", content: "630" } },
        { tag: "meta", attrs: { name: "twitter:image", content: "https://wavehouse.dev/og.png" } },
      ],
      logo: {
        light: "./src/assets/branding/wavehouse-mark-light.svg",
        dark: "./src/assets/branding/wavehouse-mark-dark.svg",
        alt: "WaveHouse",
      },
      customCss: ["./src/styles/global.css", "katex/dist/katex.min.css"],
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
        styleOverrides: {
          borderRadius: "0.5rem",
          codeFontFamily:
            "'JetBrains Mono Variable', ui-monospace, 'SF Mono', Menlo, monospace",
          uiFontFamily:
            "'Inter Variable', ui-sans-serif, system-ui, sans-serif",
          frames: {
            shadowColor: "rgba(0, 0, 0, 0.3)",
          },
        },
        shiki: {
          langAlias: {
            env: "bash",
            dns: "ini",
          },
        },
      },
      components: {
        Hero: "./src/components/Hero.astro",
        SiteTitle: "./src/components/SiteTitle.astro",
        Footer: "./src/components/Footer.astro",
        Head: "./src/components/Head.astro",
      },
      sidebar,
      plugins: [
        starlightImageZoom(),
        // @ts-expect-error — plugin types target an older Starlight; runtime is fine.
        starlightLlmTools(),
        starlightLinksValidator({
          errorOnInvalidHashes: true,
          errorOnRelativeLinks: true,
        }),
      ],
    }),
  ],
});

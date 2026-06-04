// @ts-check

import starlight from "@astrojs/starlight";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "astro/config";
import { themedMermaid } from "astro-themed-mermaid";
import rehypeKatex from "rehype-katex";
import rehypeMermaid from "rehype-mermaid";
import remarkMath from "remark-math";
import starlightImageZoom from "starlight-image-zoom";
import starlightLinksValidator from "starlight-links-validator";
import starlightLlmTools from "starlight-llm-tools";
import { mermaidTheme } from "./src/config/mermaid-theme.mjs";
import { sidebar } from "./src/config/sidebar.ts";

// Color-agnostic Mermaid plugin (astro-themed-mermaid pkg) + WaveHouse's palette
// (src/config/mermaid-theme). Diagram colors are defined once in global.css
// (--wh-mermaid-*, derived from --brand-*); the config maps them, the plugin
// holds none.
const mermaid = themedMermaid(mermaidTheme);

export default defineConfig({
  site: "https://wavehouse.dev",
  trailingSlash: "never",
  vite: {
    plugins: [tailwindcss()],
  },
  markdown: {
    syntaxHighlight: { excludeLangs: ["mermaid"] },
    remarkPlugins: [remarkMath, mermaid.remarkInjectClassdefs],
    rehypePlugins: [[rehypeMermaid, mermaid.rehypeMermaidOptions], rehypeKatex],
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
        // The primary SVG favicon is set via Starlight's `favicon` option below
        // (→ /branding/favicon.svg). Here we add the .ico fallback (legacy /
        // non-SVG browsers; also auto-probed at the site root) and the
        // apple-touch icon. The whole brand kit is generated into /branding/ by
        // docs/scripts/branding/generate.sh.
        {
          tag: "link",
          attrs: {
            rel: "icon",
            type: "image/x-icon",
            sizes: "16x16 32x32 48x48",
            href: "/favicon.ico",
          },
        },
        {
          tag: "link",
          attrs: {
            rel: "apple-touch-icon",
            sizes: "180x180",
            href: "/branding/apple-touch-icon.png",
          },
        },
        // Open Graph — Starlight already emits og:title, og:type, og:url,
        // og:locale, og:description, og:site_name, twitter:card. These are
        // the bits it doesn't, applied site-wide so every shareable page
        // carries them. og:image:alt doubles as accessibility text for
        // screen readers and the fallback string platforms render when the
        // image fails to load.
        {
          tag: "meta",
          attrs: { property: "og:image", content: "https://wavehouse.dev/branding/og.png" },
        },
        { tag: "meta", attrs: { property: "og:image:type", content: "image/png" } },
        { tag: "meta", attrs: { property: "og:image:width", content: "1200" } },
        { tag: "meta", attrs: { property: "og:image:height", content: "630" } },
        {
          tag: "meta",
          attrs: {
            property: "og:image:alt",
            content: "WaveHouse — open-source real-time API gateway for ClickHouse",
          },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:image", content: "https://wavehouse.dev/branding/og.png" },
        },
        {
          tag: "meta",
          attrs: {
            name: "twitter:image:alt",
            content: "WaveHouse — open-source real-time API gateway for ClickHouse",
          },
        },
      ],
      // No `logo` here on purpose: the SiteTitle override renders the brand
      // mark via the shared <WaveMark/> component (currentColor, theme-aware),
      // so Starlight's logo config would never render. The brand mark lives in
      // exactly one place — src/components/WaveMark.astro.
      // Primary favicon (the .ico fallback + apple-touch are added via head above).
      favicon: "/branding/favicon.svg",
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
          codeFontFamily: "'JetBrains Mono Variable', ui-monospace, 'SF Mono', Menlo, monospace",
          uiFontFamily: "'Inter Variable', ui-sans-serif, system-ui, sans-serif",
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
    mermaid.integration,
  ],
});

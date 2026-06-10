// @ts-check

import starlight from "@astrojs/starlight";
import tailwindcss from "@tailwindcss/vite";
import { themedMermaid } from "@wave-rf/astro-themed-mermaid";
import starlightLlmTools from "@wave-rf/starlight-llm-tools";
import { defineConfig } from "astro/config";
import rehypeKatex from "rehype-katex";
import remarkMath from "remark-math";
import starlightImageZoom from "starlight-image-zoom";
import starlightLinksValidator from "starlight-links-validator";
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
    // mermaid.rehypeMermaid = rehype-mermaid behind the package's per-diagram
    // render cache (node_modules/.cache/astro-themed-mermaid/) — rebuilds that
    // don't change a diagram skip Chromium entirely (~6.5s → ~3.7s per build).
    rehypePlugins: [mermaid.rehypeMermaid, rehypeKatex],
  },
  integrations: [
    starlight({
      title: "WaveHouse",
      description:
        "The open-source real-time API gateway for ClickHouse — schema-aware ingest, async batching, real-time streaming, and tiered query caching in a single binary.",
      head: [
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
            rel: "icon",
            type: "image/svg+xml",
            sizes: "any",
            href: "/branding/favicon.svg",
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
      // Same SVG as the head[] icon entry — see the icon-set comment there.
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
        // Long lines wrap with a hanging indent instead of growing a
        // horizontal scrollbar — in a 45rem column, scroll-to-read code was
        // the docs' biggest readability complaint. Authors can still opt a
        // block out with ```lang wrap=false where alignment matters.
        defaultProps: {
          wrap: true,
          preserveIndent: true,
        },
        styleOverrides: {
          // 0.75rem − 1px so the frame's OUTER corner lands at exactly 12px
          // (--radius-md, matching cards): EC adds the 1px border width to the
          // configured radius for the frame, and derives the inner header/tab/
          // <pre> corners from the same base, so frame and inner stay in sync
          // (a manual `.frame` radius in global.css used to desync them).
          borderRadius: "calc(0.75rem - 1px)",
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
        // The validator fails the whole build on a broken link. Right for CI
        // and `make build-docs`; wrong for the rebuild-on-save dev loop
        // (scripts/dev.mjs sets WAVEHOUSE_DOCS_WATCH=1), where a mid-edit
        // dangling link would block every rebuild. Output is identical.
        ...(process.env.WAVEHOUSE_DOCS_WATCH
          ? []
          : [
              starlightLinksValidator({
                errorOnInvalidHashes: true,
                errorOnRelativeLinks: true,
              }),
            ]),
      ],
    }),
    mermaid.integration,
  ],
});

import type { StarlightUserConfig } from "@astrojs/starlight/types";

// Single source of truth for both Starlight's sidebar rendering and the
// page order in the LLM-friendly outputs (llms.txt and friends), via
// the starlight-llm-tools plugin which reads this from the Starlight
// config at build time.

export const sidebar: StarlightUserConfig["sidebar"] = [
  { label: "Home", link: "/" },
  { label: "Getting Started", slug: "getting-started" },
  { label: "Why WaveHouse?", slug: "why-wavehouse" },
  {
    label: "Guides",
    items: [
      { label: "Architecture", slug: "architecture" },
      { label: "Ingest Pipeline", slug: "ingest-pipeline" },
      { label: "Access Control", slug: "access-control" },
      { label: "Named Pipes", slug: "pipes" },
    ],
  },
  {
    // Lookup material, not narrative — its own group so "I need the exact
    // endpoint/method" readers don't scan the conceptual guides for it.
    label: "Reference",
    items: [
      { label: "API Reference", slug: "api" },
      {
        // Topic-first SDK pages: the multi-language plan on record (PR #313)
        // was for a second language to grow <Tabs syncKey="lang"> on these
        // shared pages instead of a parallel tree. The Go SDK launched as
        // its own docs/src/content/docs/sdk/go/* tree instead — its API
        // shape (context.Context, (T, error), generics on package-level
        // funcs) diverges enough from the TS builder that shared prose read
        // worse than dedicated pages. Revisit folding these into tabs if a
        // third language lands and the duplication becomes a maintenance
        // cost.
        label: "TypeScript SDK",
        items: [
          { label: "Overview", slug: "sdk" },
          { label: "Queries", slug: "sdk/queries" },
          { label: "Streaming & Live Queries", slug: "sdk/streaming" },
          { label: "Pipes", slug: "sdk/pipes" },
          { label: "Admin & System", slug: "sdk/admin" },
          { label: "Reference & CLI", slug: "sdk/reference" },
        ],
      },
      {
        label: "Go SDK",
        items: [
          { label: "Overview", slug: "sdk/go" },
          { label: "Queries", slug: "sdk/go/queries" },
          { label: "Streaming & Live Queries", slug: "sdk/go/streaming" },
          { label: "Pipes", slug: "sdk/go/pipes" },
          { label: "Admin & System", slug: "sdk/go/admin" },
          { label: "Reference & CLI", slug: "sdk/go/reference" },
        ],
      },
    ],
  },
  {
    label: "Operations",
    items: [
      { label: "Configuration", slug: "configuration" },
      { label: "Deployment", slug: "deployment" },
      { label: "Durability & Storage", slug: "durability" },
      { label: "Behind a reverse proxy", slug: "reverse-proxy" },
    ],
  },
  {
    label: "Contributing",
    items: [
      { label: "Development", slug: "development" },
      { label: "Claude Code & AI agents", slug: "claude-code" },
      {
        label: "Contributing Guide",
        link: "https://github.com/Wave-RF/WaveHouse/blob/main/CONTRIBUTING.md",
        attrs: { target: "_blank", rel: "noopener" },
      },
      {
        label: "Discussions",
        link: "https://github.com/Wave-RF/WaveHouse/discussions",
        attrs: { target: "_blank", rel: "noopener" },
      },
      {
        label: "Support",
        link: "https://github.com/Wave-RF/WaveHouse/blob/main/SUPPORT.md",
        attrs: { target: "_blank", rel: "noopener" },
      },
      {
        label: "Security Policy",
        link: "https://github.com/Wave-RF/WaveHouse/blob/main/SECURITY.md",
        attrs: { target: "_blank", rel: "noopener" },
      },
      {
        label: "Changelog",
        link: "https://github.com/Wave-RF/WaveHouse/blob/main/CHANGELOG.md",
        attrs: { target: "_blank", rel: "noopener" },
      },
    ],
  },
];

// Top-level link(s) for the site header (src/components/Header.astro), colocated
// with the sidebar so header and sidebar share one file as the single source of
// truth for navigation. A single "Docs" entry into the doc tree: the splash
// homepage hides the sidebar, so this is the way in from there; every doc page
// already carries the full sidebar. Root-absolute href matches the internal link
// convention used across the docs (index.mdx hero, LinkCards). Unlike those
// Markdown-authored links, component-rendered hrefs are OUTSIDE
// starlight-links-validator's scope — nothing build-verifies these targets, so
// keep them in sync by hand when restructuring the docs.
export const headerNav: { label: string; href: string }[] = [
  { label: "Docs", href: "/getting-started" },
];

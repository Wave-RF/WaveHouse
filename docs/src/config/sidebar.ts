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
        // Topic-first SDK pages: when a second SDK language lands, these
        // shared usage pages grow <Tabs syncKey="lang"> code tabs and each
        // language gets its own setup/caveats page — the topic URLs never
        // churn (decision in PR #313).
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

// Curated top-level destinations for the site header (src/components/Header.astro).
// A deliberately small subset of the sidebar above — the sidebar still carries
// the full tree — colocated here so header and sidebar share one file as the
// single source of truth for navigation. Root-absolute hrefs match the internal
// link convention used across the docs (index.mdx hero, LinkCards).
export const headerNav: { label: string; href: string }[] = [
  { label: "Getting Started", href: "/getting-started" },
  { label: "Why WaveHouse?", href: "/why-wavehouse" },
  { label: "Architecture", href: "/architecture" },
  { label: "API", href: "/api" },
  { label: "SDK", href: "/sdk" },
];

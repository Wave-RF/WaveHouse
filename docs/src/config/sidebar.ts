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
      { label: "Access Control", slug: "access-control" },
      { label: "Named Pipes", slug: "pipes" },
      { label: "API Reference", slug: "api" },
      { label: "TypeScript SDK", slug: "sdk" },
    ],
  },
  {
    label: "Operations",
    items: [
      { label: "Configuration", slug: "configuration" },
      { label: "Deployment", slug: "deployment" },
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

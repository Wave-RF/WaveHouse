// WaveHouse's input to the themed-mermaid plugin (src/plugins/mermaid).
//
// Where the colors live:
//   - The DISPLAYED colors are CSS variables in src/styles/global.css
//     (--wh-mermaid-*, which derive from the --brand-* palette). That stylesheet
//     is the single source of truth — change a brand hue there and diagrams
//     follow, in both light and dark mode.
//   - This file maps Mermaid's BUILD-TIME output onto those variables:
//       * `themeVariables` / `classDefs` hand Mermaid concrete hex to bake into
//         the SVG at build time (Mermaid needs real colors to render geometry).
//       * `colorReplacements` then rewrites each baked hex → the matching
//         `var(--wh-mermaid-*)` so the runtime stylesheet drives the colors.
//   The build-time hex are essentially sentinels; only `colorReplacements`'
//   right-hand side (the var names) reaches the browser. Keep the hex on the
//   left in sync with what themeVariables/classDefs bake.

import { fileURLToPath } from "node:url";

export const mermaidTheme = {
  font: {
    family: '"Inter Variable", ui-sans-serif, system-ui, sans-serif',
    // Inlined into the build-time Chromium for correct text measurement.
    woff2: fileURLToPath(
      new URL(
        "../../node_modules/@fontsource-variable/inter/files/inter-latin-wght-normal.woff2",
        import.meta.url,
      ),
    ),
  },

  // Mermaid `base` theme. Build-time hex; the ones that should respond to
  // light/dark are rewritten by colorReplacements below (others stay fixed,
  // e.g. the brand-blue default node border).
  themeVariables: {
    fontSize: "14px",
    primaryColor: "#14171C",
    primaryBorderColor: "#06B0BF",
    primaryTextColor: "#F1F3F7",
    secondaryColor: "#1B1F26",
    secondaryBorderColor: "#2F343F",
    secondaryTextColor: "#F1F3F7",
    tertiaryColor: "#232830",
    tertiaryBorderColor: "#2F343F",
    tertiaryTextColor: "#F1F3F7",
    lineColor: "#6B7280",
    clusterBkg: "rgba(6, 176, 191, 0.04)",
    clusterBorder: "rgba(6, 176, 191, 0.30)",
    titleColor: "#F1F3F7",
    edgeLabelBackground: "#14171C",
    labelBoxBkgColor: "#14171C",
    labelBoxBorderColor: "#2F343F",
    labelTextColor: "#9CA3AF",
    noteBkgColor: "#1B1F26",
    noteBorderColor: "#2F343F",
    noteTextColor: "#F1F3F7",
    mainBkg: "#14171C",
    secondBkg: "#1B1F26",
    background: "transparent",
    textColor: "#F1F3F7",
  },

  // Standard semantic node classes, injected into every flowchart so source
  // diagrams can use `:::wh`, `:::fail`, etc. without restating the palette.
  classDefs: [
    "classDef client fill:#475569,stroke:#94a3b8,color:#fff,stroke-width:2px",
    "classDef store fill:#334155,stroke:#64748b,color:#fff,stroke-width:2px",
    "classDef fail fill:#7f1d1d,stroke:#dc2626,color:#fff,stroke-width:2px",
    "classDef pain fill:#b91c1c,stroke:#ef4444,color:#fff,stroke-width:2px",
    "classDef win fill:#15803d,stroke:#22c55e,color:#fff,stroke-width:2px",
    "classDef wh fill:#0e7f8f,stroke:#5bbfcf,color:#fff,stroke-width:3px",
    "classDef neutral fill:#475569,stroke:#94a3b8,color:#fff,stroke-width:2px",
    "classDef infra fill:#334155,stroke:#64748b,color:#fff,stroke-width:2px",
  ],

  // Baked hex (exactly as Mermaid serializes them) → runtime CSS variable.
  // LHS must match themeVariables/classDefs above; RHS are defined in global.css.
  colorReplacements: [
    // base theme tokens
    ["#14171C", "var(--wh-mermaid-surface)"],
    ["#F1F3F7", "var(--wh-mermaid-ink)"],
    ["#9CA3AF", "var(--wh-mermaid-ink-muted)"],
    ["#6B7280", "var(--wh-mermaid-line)"],
    ["#1B1F26", "var(--wh-mermaid-surface-2)"],
    ["#232830", "var(--wh-mermaid-surface-3)"],
    ["#2F343F", "var(--wh-mermaid-border)"],
    ["rgba(20, 23, 28, 0.5)", "var(--wh-mermaid-surface-fade)"],
    ["rgba(6, 176, 191, 0.04)", "var(--wh-mermaid-cluster-bg)"],
    ["rgba(6, 176, 191, 0.30)", "var(--wh-mermaid-cluster-border)"],
    // classDef colors → brand-derived semantic palette
    ["#0e7f8f", "var(--wh-mermaid-wh-bg)"],
    ["#5bbfcf", "var(--wh-mermaid-wh-border)"],
    ["#b91c1c", "var(--wh-mermaid-pain-bg)"],
    ["#ef4444", "var(--wh-mermaid-pain-border)"],
    ["#7f1d1d", "var(--wh-mermaid-fail-bg)"],
    ["#dc2626", "var(--wh-mermaid-fail-border)"],
    ["#15803d", "var(--wh-mermaid-win-bg)"],
    ["#22c55e", "var(--wh-mermaid-win-border)"],
    ["#475569", "var(--wh-mermaid-neutral-bg)"],
    ["#94a3b8", "var(--wh-mermaid-neutral-border)"],
    ["#334155", "var(--wh-mermaid-infra-bg)"],
    ["#64748b", "var(--wh-mermaid-infra-border)"],
  ],

  flowchart: {
    curve: "basis",
    padding: 20,
    nodeSpacing: 48,
    rankSpacing: 56,
    wrappingWidth: 480,
    useMaxWidth: true,
  },
  sequence: { useMaxWidth: true, wrap: false },
};

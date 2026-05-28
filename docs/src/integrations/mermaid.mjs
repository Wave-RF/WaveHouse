// Mermaid build-time plumbing for the WaveHouse docs site. Split out of
// astro.config.mjs so the config file stays a config file.
//
// Three exports:
//   • rehypeMermaidOptions — pass to the rehype-mermaid plugin
//   • fixMermaidOutput()   — Astro integration that rewrites emitted SVGs
//   • remarkInjectMermaidClassdefs() — remark plugin that injects the
//     standard classDef declarations into every mermaid block so the
//     source markdown can stay free of presentation metadata
//
// The polish CSS that used to be injected per-SVG now lives in
// src/styles/global.css under "SVG-element polish".

import { readFile, writeFile, readdir } from "node:fs/promises";
import { resolve as joinPath } from "node:path";
import { fileURLToPath } from "node:url";
import { visit } from "unist-util-visit";

// ---------------------------------------------------------------------------
// Build-time font inlining
//
// Inject Inter Variable into the build-time Chromium that mermaid-isomorphic
// uses for SSR. Without this, Mermaid measures node widths against Chromium's
// fallback (~Arial) but the SVG declares whatever font-family we set in
// mermaidConfig — so if runtime renders with Inter, labels overflow because
// Inter is ~6-8% wider per character than Arial. Inlining the woff2 as a
// data URL with `font-display: block` makes Chromium load Inter synchronously
// before mermaid runs and measures correctly. (`font-display: swap`, which
// fontsource uses by default, doesn't block, so the package's own index.css
// can't substitute for this.)
let buildTimeFontCssDataUrl;
let buildTimeFontFamily;
try {
  const interWoff2 = await readFile(
    new URL(
      "../../node_modules/@fontsource-variable/inter/files/inter-latin-wght-normal.woff2",
      import.meta.url
    )
  );
  const interBase64 = interWoff2.toString("base64");
  const inlineCss = [
    "@font-face{",
    "font-family:'Inter Variable';",
    `src:url(data:font/woff2;base64,${interBase64}) format('woff2-variations');`,
    "font-weight:100 900;",
    "font-style:normal;",
    "font-display:block;",
    "}",
  ].join("");
  buildTimeFontCssDataUrl =
    "data:text/css;base64," + Buffer.from(inlineCss).toString("base64");
  buildTimeFontFamily =
    '"Inter Variable", ui-sans-serif, system-ui, sans-serif';
} catch {
  // Fontsource not on disk (fresh clone, no `pnpm install` yet). Fall back
  // to Mermaid's default Arial so the build still works.
  buildTimeFontFamily = '"Arial", sans-serif';
}

// ---------------------------------------------------------------------------
// Mermaid theme — tracks the dark-mode palette in src/styles/global.css.
// SVGs are rendered (and baked) at build time, so colors here are fixed;
// the values are picked to read well against the dark site chrome that is
// the brand default. The build hook below swaps each literal hex for a
// CSS variable, so light-mode rendering still works at runtime.

const mermaidThemeVariables = {
  fontFamily: buildTimeFontFamily,
  fontSize: "14px",

  // Default nodes — surface fill, brand-teal border, ink text
  primaryColor: "#14171C",
  primaryBorderColor: "#06B0BF",
  primaryTextColor: "#F1F3F7",

  // Secondary / tertiary nodes (used by some diagram types for alt fills)
  secondaryColor: "#1B1F26",
  secondaryBorderColor: "#2F343F",
  secondaryTextColor: "#F1F3F7",
  tertiaryColor: "#232830",
  tertiaryBorderColor: "#2F343F",
  tertiaryTextColor: "#F1F3F7",

  // Edges / arrows
  lineColor: "#6B7280",

  // Subgraphs (clusters)
  clusterBkg: "rgba(6, 176, 191, 0.04)",
  clusterBorder: "rgba(6, 176, 191, 0.30)",
  titleColor: "#F1F3F7",

  // Edge labels
  edgeLabelBackground: "#14171C",
  labelBoxBkgColor: "#14171C",
  labelBoxBorderColor: "#2F343F",
  labelTextColor: "#9CA3AF",

  // Notes
  noteBkgColor: "#1B1F26",
  noteBorderColor: "#2F343F",
  noteTextColor: "#F1F3F7",

  // Generic
  mainBkg: "#14171C",
  secondBkg: "#1B1F26",
  background: "transparent",
  textColor: "#F1F3F7",
};

export const rehypeMermaidOptions = {
  strategy: "inline-svg",
  // The data URL contains an inline Inter Variable woff2 with
  // `font-display: block`, so Chromium uses Inter for getBBox()
  // measurement before mermaid renders.
  ...(buildTimeFontCssDataUrl ? { css: buildTimeFontCssDataUrl } : {}),
  mermaidConfig: {
    // `fontFamily` must live at the top of mermaidConfig — mermaid-isomorphic
    // hard-codes `arial,sans-serif` here if absent and ignores the same key
    // inside themeVariables.
    fontFamily: buildTimeFontFamily,
    theme: "base",
    themeVariables: mermaidThemeVariables,
    flowchart: {
      curve: "basis",
      padding: 20,
      nodeSpacing: 48,
      rankSpacing: 56,
      wrappingWidth: 480,
      useMaxWidth: true,
    },
    sequence: { useMaxWidth: true, wrap: false },
    securityLevel: "strict",
  },
};

// ---------------------------------------------------------------------------
// Standard WaveHouse classDef declarations injected into every mermaid block
// so source markdown can use `:::class` without restating the palette in
// every diagram. Hex values must match MERMAID_THEME_REPLACEMENTS below —
// they get swapped for CSS variables at build time, so light mode works.

const WAVEHOUSE_CLASSDEFS = [
  "classDef client fill:#475569,stroke:#94a3b8,color:#fff,stroke-width:2px",
  "classDef store fill:#334155,stroke:#64748b,color:#fff,stroke-width:2px",
  "classDef fail fill:#7f1d1d,stroke:#dc2626,color:#fff,stroke-width:2px",
  "classDef pain fill:#b91c1c,stroke:#ef4444,color:#fff,stroke-width:2px",
  "classDef win fill:#15803d,stroke:#22c55e,color:#fff,stroke-width:2px",
  "classDef wh fill:#0e7f8f,stroke:#5bbfcf,color:#fff,stroke-width:3px",
  "classDef neutral fill:#475569,stroke:#94a3b8,color:#fff,stroke-width:2px",
  "classDef infra fill:#334155,stroke:#64748b,color:#fff,stroke-width:2px",
];

// `classDef` is valid syntax in flowchart/graph diagrams only. Other
// diagram types (sequenceDiagram, classDiagram, erDiagram, stateDiagram,
// gantt, pie, journey, …) parse it as an error, so we only inject when
// the block opens with a recognized flowchart header.
const FLOWCHART_HEADER_RE = /^\s*(?:flowchart|graph)\b/;

export function remarkInjectMermaidClassdefs() {
  return (tree) => {
    visit(tree, "code", (node) => {
      if (node.lang !== "mermaid") return;
      const lines = node.value.split("\n");
      const headerIdx = lines.findIndex((l) => l.trim().length > 0);
      if (headerIdx < 0) return;
      if (!FLOWCHART_HEADER_RE.test(lines[headerIdx])) return;
      // Inject all eight classDefs after the diagram-type header.
      // Mermaid silently ignores unused declarations, and a per-diagram
      // `classDef` later in the block will override ours since later
      // declarations win in mermaid.
      const indent = lines[headerIdx].match(/^\s*/)[0];
      const injected = WAVEHOUSE_CLASSDEFS.map((l) => indent + "    " + l);
      lines.splice(headerIdx + 1, 0, ...injected);
      node.value = lines.join("\n");
    });
  };
}

// ---------------------------------------------------------------------------
// Build-time SVG patches
//
// Astro integration: rewrite Mermaid SVG output so it (a) survives
// Chromium's parser and (b) responds to the site's light/dark theme.
//
// 1. <br></br> → <br>. Mermaid v11.15 emits `<br></br>` for line breaks.
//    Per HTML5, an end tag for a void element is a parse error and the
//    parser inserts a duplicate void element, so Chrome reads it as two
//    <br> nodes — three lines of rendered height. Mermaid only sized
//    the <foreignObject> for two lines, so the third line overflows and
//    appears dropped. Patching to `<br>` aligns Chrome's parse with
//    Mermaid's measurement.
//
// 2. Hex colors in each SVG → CSS variables. Mermaid bakes our theme's
//    colors as literal hex inside the SVG's scoped CSS rules AND inside
//    inline style="fill:#…" attributes on each <rect>/<circle>/<path>.
//    Swapping them for `var(--wh-mermaid-*)` lets global.css drive the
//    colors per `[data-theme]`, so light mode actually renders as light.
//
// Two pools of hex colors get swapped:
//   1. Default theme tokens from `mermaidThemeVariables` above — these
//      drive the architecture diagram (and any unstyled node).
//   2. classDef colors from WAVEHOUSE_CLASSDEFS — let every diagram pull
//      from the same brand palette without editing the source mermaid
//      blocks.
//
// Hex values here are the LITERAL ones Mermaid serializes (preserving
// case). The two pools don't collide, so the replacement is safe to
// apply to the entire SVG body.
const MERMAID_THEME_REPLACEMENTS = [
  // --- default theme tokens (from mermaidThemeVariables) ---------------
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

  // --- classDef colors from WAVEHOUSE_CLASSDEFS, mapped to brand -------
  // wh (WaveHouse itself) → brand accent
  ["#0e7f8f", "var(--wh-mermaid-wh-bg)"],
  ["#5bbfcf", "var(--wh-mermaid-wh-border)"],
  // pain (problem state) → rose
  ["#b91c1c", "var(--wh-mermaid-pain-bg)"],
  ["#ef4444", "var(--wh-mermaid-pain-border)"],
  // fail (critical error) → deeper rose
  ["#7f1d1d", "var(--wh-mermaid-fail-bg)"],
  ["#dc2626", "var(--wh-mermaid-fail-border)"],
  // win (success outcome) → emerald
  ["#15803d", "var(--wh-mermaid-win-bg)"],
  ["#22c55e", "var(--wh-mermaid-win-border)"],
  // neutral / client (external user, dashboard) → slate
  ["#475569", "var(--wh-mermaid-neutral-bg)"],
  ["#94a3b8", "var(--wh-mermaid-neutral-border)"],
  // infra / store (databases, queues, persistent storage) → terracotta
  ["#334155", "var(--wh-mermaid-infra-bg)"],
  ["#64748b", "var(--wh-mermaid-infra-border)"],
];

const SVG_BLOCK_RE =
  /<svg\b[^>]*aria-roledescription="(?:flowchart|sequence|class|state|gantt|pie|er)[^"]*"[\s\S]*?<\/svg>/g;

const FLOWCHART_SVG_RE =
  /<svg\b[^>]*aria-roledescription="flowchart[^"]*"[\s\S]*?<\/svg>/g;

export function fixMermaidOutput() {
  async function walk(dir) {
    const out = [];
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      const full = joinPath(dir, entry.name);
      if (entry.isDirectory()) out.push(...(await walk(full)));
      else if (entry.name.endsWith(".html")) out.push(full);
    }
    return out;
  }

  // Replace hex colors with CSS variables across the whole SVG body —
  // catches both the `<style>` block at the top and inline
  // `style="fill:#…"` attributes on each shape element.
  function patchMermaidStyleBlocks(html) {
    return html.replace(SVG_BLOCK_RE, (svg) => {
      let patched = svg;
      for (const [from, to] of MERMAID_THEME_REPLACEMENTS) {
        patched = patched.replaceAll(from, to);
      }
      return patched;
    });
  }

  // Mermaid stamps every nodeLabel foreignObject with inline
  // `style="color: rgb(255, 255, 255) !important; …"` on the outer div
  // and `style="color:#fff !important"` on the inner span. Inline
  // !important beats our CSS-rule !important by source order, so in
  // light mode default-themed nodes render as white-on-white. We strip
  // the inline color specifically; the rest of the inline style stays
  // (display: table-cell etc.). classDef-themed nodes still get white
  // text via their own `.classname span` !important rule in the SVG's
  // scoped <style> block, which is what we want.
  function stripInlineForcedColors(html) {
    return html
      .replaceAll(/color:\s*rgb\(255,\s*255,\s*255\)\s*!important;?\s*/g, "")
      .replaceAll(/color:\s*#fff\s*!important;?\s*/g, "");
  }

  // Move each cluster-label group to the END of its SVG so it renders
  // on top of edges and nodes. Mermaid emits the cluster-label inside
  // its subgraph's <g class="root" transform="translate(X, Y)"> wrapper,
  // and any edge path crossing the cluster's top border is drawn later
  // in source order than the label (SVG paint order is source order; no
  // z-index). Pulling the cluster-labels to the end of the SVG gives
  // them the highest paint priority.
  //
  // Lifting also moves the label OUT of its subgraph wrapper, so the
  // local-coord transform that mermaid emitted becomes meaningless at
  // the SVG root. We compose: for each label, add the nearest preceding
  // subgraph-root translate to the label's translate. Mermaid emits
  // nested subgraph roots as SIBLINGS, not nested, so "nearest preceding
  // <g class=\"root\" transform=...> opener" is the correct parent.
  function liftClusterLabels(html) {
    return html.replace(FLOWCHART_SVG_RE, (svg) => {
      const rootOpens = [];
      const rootOpenRe =
        /<g class="root"\s+transform="translate\(([-\d.]+),\s*([-\d.]+)\)[^"]*"[^>]*>/g;
      let rm;
      while ((rm = rootOpenRe.exec(svg)) !== null) {
        rootOpens.push({
          end: rm.index + rm[0].length,
          tx: parseFloat(rm[1]),
          ty: parseFloat(rm[2]),
        });
      }

      const labels = [];
      const labelRe =
        /<g class="cluster-label"\s+transform="translate\(([-\d.]+),\s*([-\d.]+)\)([^"]*)"([\s\S]*?)<\/g>/g;
      const stripped = svg.replace(labelRe, (match, lx, ly, tail, body, off) => {
        let parent = { tx: 0, ty: 0 };
        for (const r of rootOpens) {
          if (r.end <= off) parent = r;
        }
        const absX = parseFloat(lx) + parent.tx;
        const absY = parseFloat(ly) + parent.ty;
        labels.push(
          `<g class="cluster-label" transform="translate(${absX}, ${absY})${tail}"${body}</g>`
        );
        return "";
      });
      if (labels.length === 0) return svg;
      // Append right before the root group's closing </g>. Mermaid
      // emits <defs> + <linearGradient> AFTER the root group close
      // but before </svg>, so we anchor on the </g> that is followed
      // by either <defs>, <linearGradient>, or </svg>.
      return stripped.replace(
        /(<\/g>)(<defs\b|<linearGradient\b|<\/svg>)/,
        labels.join("") + "$1$2"
      );
    });
  }

  // Expand each mermaid SVG's viewBox upward and reposition each
  // cluster-label so the pill chip (a) renders inside the canvas
  // instead of being clipped, and (b) is centered above the cluster
  // rect's top edge rather than left-aligned where Mermaid placed it.
  //
  // The pill is shifted up 17px in CSS to straddle the cluster border;
  // for that to be visible we need at least 18px of vertical room above
  // y=0. We add 22px of padding at the top of the viewBox.
  //
  // For horizontal centering: Mermaid positions the cluster-label <g>
  // at translate(rect.x + inset, rect.y), so foreignObject sits near the
  // left edge of the rect. We rewrite the translate to put the
  // foreignObject CENTER on the rect CENTER instead.
  function centerClusterTitles(html) {
    return html.replace(FLOWCHART_SVG_RE, (svg) => {
      let patched = svg;

      patched = patched.replace(
        /viewBox="([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)"/,
        (_m, vx, vy, vw, vh) => {
          const PAD_TOP = 22;
          const newY = parseFloat(vy) - PAD_TOP;
          const newH = parseFloat(vh) + PAD_TOP;
          return `viewBox="${vx} ${newY} ${vw} ${newH}"`;
        }
      );

      // The negative lookahead stops the pre-label scan at the next
      // `<g class="cluster`, so a cluster with no label (e.g. an untitled
      // subgraph) fails to match here instead of greedily swallowing the
      // following cluster and repositioning ITS label against the wrong
      // rect. All current diagrams title every subgraph, so this only
      // guards future ones.
      patched = patched.replace(
        /<g class="cluster[^"]*"[^>]*>((?:(?!<g class="cluster)[\s\S])*?<g class="cluster-label"[^>]*>[\s\S]*?<\/g>)/g,
        (match, body) => {
          const rectMatch = body.match(
            /<rect[^>]+x="([-\d.]+)"[^>]+y="([-\d.]+)"[^>]+width="([\d.]+)"/
          );
          if (!rectMatch) return match;
          const rectX = parseFloat(rectMatch[1]);
          const rectY = parseFloat(rectMatch[2]);
          const rectW = parseFloat(rectMatch[3]);

          const foMatch = body.match(
            /<g class="cluster-label"[\s\S]*?<foreignObject\s+width="([\d.]+)"/
          );
          if (!foMatch) return match;
          const foW = parseFloat(foMatch[1]);

          const newLabelX = rectX + rectW / 2 - foW / 2;
          const newLabelY = rectY;

          const fixed = body.replace(
            /(<g class="cluster-label"[^>]*\btransform=")translate\([^)]+\)/,
            `$1translate(${newLabelX}, ${newLabelY})`
          );
          return match.replace(body, fixed);
        }
      );

      return patched;
    });
  }

  return {
    name: "fix-mermaid-output",
    hooks: {
      "astro:build:done": async ({ dir, logger }) => {
        const root = fileURLToPath(dir);
        const files = await walk(root);
        let brFixes = 0;
        let themeFixes = 0;
        let centeredFiles = 0;
        for (const file of files) {
          let html = await readFile(file, "utf8");
          let changed = false;
          if (html.includes("<br></br>")) {
            html = html.replaceAll("<br></br>", "<br>");
            brFixes++;
            changed = true;
          }
          if (html.includes("#mermaid-")) {
            const styled = patchMermaidStyleBlocks(html);
            const stripped = stripInlineForcedColors(styled);
            const centered = centerClusterTitles(stripped);
            const lifted = liftClusterLabels(centered);
            if (lifted !== html) {
              html = lifted;
              themeFixes++;
              if (centered !== stripped) centeredFiles++;
              changed = true;
            }
          }
          if (changed) await writeFile(file, html);
        }
        logger.info(
          `patched ${brFixes} <br></br> + ${themeFixes} theme + ${centeredFiles} cluster-center`
        );
      },
    },
  };
}

/* Measurement audit of the rendered mermaid SVGs. Loads each diagram into
 * headless chromium with the actual site CSS, then probes precise pixel
 * positions and color values for:
 *   - Edge label centers vs edge path midpoints (centering on lines)
 *   - Cylinder shape labels vs the path's visual body center
 *   - ClassDef fill colors + white text contrast ratios (WCAG)
 *   - Subgraph title pill position vs cluster rect border
 *
 * Reports numbers, not opinions. Run with: node scripts/mermaid-audit.mjs */

import { chromium } from "playwright";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const ROOT = resolve(process.cwd());
const OUT = resolve(ROOT, "screenshots");
await mkdir(OUT, { recursive: true });

function extractMermaidSvgs(html) {
  const svgs = [];
  const re =
    /<svg\b[^>]*aria-roledescription="(?:flowchart|sequence|class|state|gantt|pie|er)-[^"]*"[\s\S]*?<\/svg>/g;
  let m;
  while ((m = re.exec(html))) svgs.push(m[0]);
  return svgs;
}

const sources = [
  { name: "architecture", file: "dist/architecture/index.html" },
  { name: "why-wavehouse", file: "dist/why-wavehouse/index.html" },
];

const all = [];
for (const s of sources) {
  const html = await readFile(resolve(ROOT, s.file), "utf8");
  const svgs = extractMermaidSvgs(html);
  all.push({ ...s, svgs });
}

const siteCss = await readFile(resolve(ROOT, "src/styles/global.css"), "utf8");
function preparedCss() {
  return siteCss
    .replace(/@import\s+[^;]+;/g, "")
    .replace(/:root\[data-theme="light"\]/g, '.pane[data-theme="light"]')
    .replace(/:root\b/g, ".pane");
}

function relLum(color) {
  // Inputs come from rgbToHex() (always 6-digit hex) today, so this is
  // really just defensive: parse 3/4/6/8-digit hex and rgb()/rgba() (comma
  // or space separated), defaulting to black, so a stray value can never
  // crash on a null match or silently poison a contrast number with NaN.
  let r = 0;
  let g = 0;
  let b = 0;
  const hex = color.match(/^#?([0-9a-f]{3,8})$/i);
  if (hex) {
    const h = hex[1];
    if (h.length === 3 || h.length === 4) {
      r = parseInt(h[0] + h[0], 16);
      g = parseInt(h[1] + h[1], 16);
      b = parseInt(h[2] + h[2], 16);
    } else {
      r = parseInt(h.slice(0, 2), 16);
      g = parseInt(h.slice(2, 4), 16);
      b = parseInt(h.slice(4, 6), 16);
    }
  } else {
    const rgb = color.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/i);
    if (rgb) [r, g, b] = [rgb[1], rgb[2], rgb[3]].map(Number);
  }
  const lin = [r, g, b].map((v) => {
    v /= 255;
    return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2];
}
function contrast(c1, c2) {
  const L1 = relLum(c1);
  const L2 = relLum(c2);
  const [hi, lo] = L1 > L2 ? [L1, L2] : [L2, L1];
  return (hi + 0.05) / (lo + 0.05);
}
function rgbToHex(rgb) {
  const m = rgb.match(/rgba?\(([\d.]+),\s*([\d.]+),\s*([\d.]+)/);
  if (!m) return rgb;
  const [, r, g, b] = m.map(Number);
  return (
    "#" +
    [r, g, b]
      .map((v) => Math.round(v).toString(16).padStart(2, "0"))
      .join("")
  );
}

function probePage(svg, css) {
  return `<!doctype html>
<html><head><meta charset="utf-8">
<style>
  @font-face {
    font-family: "Inter Variable";
    src: url("https://cdn.jsdelivr.net/fontsource/fonts/inter:vf@latest/latin-wght-normal.woff2") format("woff2-variations");
    font-weight: 100 900;
    font-display: block;
  }
  html, body { margin: 0; padding: 0; background: #000; }
  ${css}
  .pane { padding: 2.5rem; background: var(--wh-bg); color: var(--wh-ink); }
  .pane h1::after { content: none !important; }
  .frame { background: var(--wh-surface); padding: 2rem; }
  svg { max-width: 100%; }
</style></head>
<body>
  <div class="pane" data-theme="dark">
    <div class="frame">${svg}</div>
  </div>
  <div class="pane" data-theme="light">
    <div class="frame">${svg}</div>
  </div>
</body></html>`;
}

const browser = await chromium.launch();
const ctx = await browser.newContext({
  viewport: { width: 1400, height: 1600 },
  deviceScaleFactor: 1,
});
const page = await ctx.newPage();

const targets = [
  { name: "architecture", svg: all[0].svgs[0] },
  { name: "why-wavehouse-1 (10k clients)", svg: all[1].svgs[0] },
  { name: "why-wavehouse-3 (cache+singleflight)", svg: all[1].svgs[2] },
  { name: "why-wavehouse-5 (ingest path)", svg: all[1].svgs[4] },
];

const audit = [];
for (const t of targets) {
  await page.setContent(probePage(t.svg, preparedCss()), { waitUntil: "load" });
  await page.evaluate(() => document.fonts.ready);

  const result = await page.evaluate(() => {
    function svgCenter(svgEl) {
      // For SVG geometry (filter-free), use getBBox + element CTM to get
      // page coordinates of the geometric center.
      if (!svgEl || !svgEl.getBBox) return null;
      const b = svgEl.getBBox();
      const m = svgEl.getScreenCTM();
      if (!m) return null;
      // Convert (cx, cy) of bbox through CTM
      const cx_local = b.x + b.width / 2;
      const cy_local = b.y + b.height / 2;
      const cx = m.a * cx_local + m.c * cy_local + m.e;
      const cy = m.b * cx_local + m.d * cy_local + m.f;
      return { cx, cy, w: b.width, h: b.height };
    }
    function bbox(el) {
      if (!el) return null;
      const r = el.getBoundingClientRect();
      return { x: r.x, y: r.y, w: r.width, h: r.height, cx: r.x + r.width / 2, cy: r.y + r.height / 2 };
    }
    const out = {};
    for (const pane of document.querySelectorAll(".pane")) {
      const theme = pane.dataset.theme;
      // ---- Cylinder centering ----
      const cylinderNodes = pane.querySelectorAll(".node:has(> path.basic.label-container)");
      const cylinders = [];
      for (const n of cylinderNodes) {
        const path = n.querySelector("path.basic.label-container");
        const label = n.querySelector(".label foreignObject");
        const text = n.querySelector(".nodeLabel p")?.textContent?.trim() || "";
        if (!path || !label) continue;
        const pCenter = svgCenter(path); // filter-free geometric center
        const lCenter = svgCenter(label);
        const computedTransform = getComputedStyle(label).transform;
        cylinders.push({
          text,
          path_center_y: Math.round(pCenter.cy),
          label_center_y: Math.round(lCenter.cy),
          offset_y: Math.round(lCenter.cy - pCenter.cy),
          fo_transform: computedTransform,
          path_height: Math.round(pCenter.h),
        });
      }
      // ---- Edge label centering on lines ----
      const edgeLabels = pane.querySelectorAll(".edgeLabel");
      const edges = [];
      for (const el of edgeLabels) {
        // Edge label text lives in span.edgeLabel > p (not .nodeLabel)
        const p = el.querySelector("span.edgeLabel p, .nodeLabel p");
        const text = p?.textContent?.trim();
        if (!text) continue;
        const labelBox = bbox(p);
        // Find the nearest .flowchart-link path; we identify edge by proximity
        let nearest = null;
        let nearestDist = Infinity;
        for (const path of pane.querySelectorAll(".flowchart-link, .edgePath .path")) {
          const r = path.getBoundingClientRect();
          // distance from label center to path bbox center
          const dx = (r.x + r.width / 2) - labelBox.cx;
          const dy = (r.y + r.height / 2) - labelBox.cy;
          const d = Math.hypot(dx, dy);
          if (d < nearestDist) { nearestDist = d; nearest = r; }
        }
        if (!nearest) continue;
        edges.push({
          text,
          label_cx: Math.round(labelBox.cx),
          label_cy: Math.round(labelBox.cy),
          path_cx: Math.round(nearest.x + nearest.width / 2),
          path_cy: Math.round(nearest.y + nearest.height / 2),
          dx: Math.round(labelBox.cx - (nearest.x + nearest.width / 2)),
          dy: Math.round(labelBox.cy - (nearest.y + nearest.height / 2)),
        });
      }
      // ---- ClassDef fills + contrast ----
      const contrastChecks = [];
      for (const cls of ["wh", "win", "pain", "fail", "neutral", "client", "infra", "store"]) {
        // find the visible shape (rect or path) for the first node of this class
        const node = pane.querySelector(`.node.default.${cls}`);
        if (!node) continue;
        const shape =
          node.querySelector("rect.basic.label-container") ||
          node.querySelector("path.basic.label-container");
        const textEl = node.querySelector(".nodeLabel p");
        if (!shape || !textEl) continue;
        const fill = getComputedStyle(shape).fill;
        const color = getComputedStyle(textEl).color;
        contrastChecks.push({
          class: cls,
          fill,
          text: color,
        });
      }
      // ---- Subgraph title position ----
      const titles = [];
      for (const cluster of pane.querySelectorAll(".cluster")) {
        const rect = cluster.querySelector("rect");
        const labelP = cluster.querySelector(".cluster-label .nodeLabel p");
        if (!rect || !labelP) continue;
        const r = bbox(rect);
        const l = bbox(labelP);
        titles.push({
          text: labelP.textContent?.trim(),
          rect_top: Math.round(r.y),
          rect_bottom: Math.round(r.y + r.h),
          title_top: Math.round(l.y),
          title_bottom: Math.round(l.y + l.h),
          title_height: Math.round(l.h),
          straddles_top: l.y < r.y && l.y + l.h > r.y,
          gap_to_rect_top: Math.round(l.y + l.h - r.y),
        });
      }
      out[theme] = { cylinders, edges, contrastChecks, titles };
    }
    return out;
  });
  audit.push({ name: t.name, ...result });
}

await browser.close();

// ---- Post-process: compute contrast ratios in Node ----
for (const d of audit) {
  for (const theme of ["dark", "light"]) {
    if (!d[theme]?.contrastChecks) continue;
    for (const c of d[theme].contrastChecks) {
      c.fill_hex = rgbToHex(c.fill);
      c.text_hex = rgbToHex(c.text);
      c.ratio = +contrast(c.fill_hex, c.text_hex).toFixed(2);
      c.wcag = c.ratio >= 7 ? "AAA" : c.ratio >= 4.5 ? "AA" : c.ratio >= 3 ? "AA-large" : "FAIL";
    }
  }
}

const report = JSON.stringify(audit, null, 2);
await writeFile(resolve(OUT, "audit.json"), report);
console.log(report);

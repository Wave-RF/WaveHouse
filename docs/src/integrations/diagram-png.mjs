// Build-time PNG export for Mermaid diagrams.
//
// The docs render each diagram to inline, theme-reactive SVG (rehype-mermaid,
// via @wave-rf/astro-themed-mermaid). Inline SVG is great for reading — text
// stays selectable, screen readers see real structure, colors follow the
// light/dark toggle — but you can't right-click "Copy/Save image" on an inline
// <svg>, and an SVG isn't easy to drop into a slide deck. So we ALSO emit a
// PNG of every diagram, surfaced via the Copy/Download buttons in the zoom
// lightbox (src/components/MermaidZoom.astro).
//
// Why a post-build pass and not the mermaid plugin: the plugin is deliberately
// color-agnostic — its SVGs carry `var(--wh-mermaid-*)` placeholders resolved
// at runtime from global.css, so at plugin-render time there are no concrete
// colors and no light/dark split. A WYSIWYG light+dark PNG must be rasterized
// where global.css is applied and a theme is set, i.e. against the BUILT page
// in a real browser. We reuse the Chromium that rehype-mermaid already needs.
//
// Output: dist/diagrams/<slug>/<index>-<theme>[-transparent].png, where <slug>
// is the page path and <index> is the diagram's position among
// `.sl-markdown-content svg[aria-roledescription]` — the exact selector the
// lightbox uses, so both sides agree on which PNG belongs to which diagram.
// Each diagram is emitted twice per theme: the solid surface card (default) and
// a transparent-background variant for dropping onto a slide deck.

import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// Load Playwright through Node's own resolver, not a bare import: at
// `astro:build:done` Vite's SSR module runner is already closed, so a dynamic
// `import("playwright")` there throws "module runner has been closed". This
// keeps it lazy (only required when a diagram actually needs rendering) while
// resolving straight from node_modules.
const require = createRequire(import.meta.url);

// Docs package root (…/docs), derived from this file's location rather than
// process.cwd() so the on-disk cache lands in the same node_modules/.cache
// regardless of where `astro build` is invoked from.
const PKG_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

const THEMES = ["light", "dark"];
const SCALE = 2; // retina; crisp when dropped into a deck
const PAD = 28; // surface padding around the diagram (matches the zoom card feel)
const MAX_DIM = 2400; // cap a diagram's CSS-px long edge so files stay sane
// Same selector the lightbox indexes by — keep these in lockstep.
const DIAGRAM = ".sl-markdown-content svg[aria-roledescription]";
// Bump to invalidate every cached PNG after a change to the render routine.
const RENDER_VERSION = "4";

// Each diagram is rasterized once per variant: the default solid "surface card"
// and a transparent-background version (drops cleanly onto any slide backdrop).
// The transparent file carries a `-transparent` suffix; the lightbox toggle
// (src/components/MermaidZoom.astro) picks between the two.
const VARIANTS = [
  { suffix: "", transparent: false },
  { suffix: "-transparent", transparent: true },
];

const CONTENT_TYPES = {
  ".css": "text/css",
  ".js": "text/javascript",
  ".mjs": "text/javascript",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".otf": "font/otf",
};

/** @returns {import('astro').AstroIntegration} */
export function diagramPng() {
  return {
    name: "wh-diagram-png",
    hooks: {
      "astro:build:done": async ({ dir, pages, logger }) => {
        if (process.env.WH_SKIP_DIAGRAM_PNG === "1") {
          logger.info("skipped (WH_SKIP_DIAGRAM_PNG=1)");
          return;
        }
        try {
          await run({ dir, pages, logger });
        } catch (err) {
          // A browser hiccup must not fail the whole docs build — the site is
          // already written; we just don't get fresh PNGs this run.
          logger.warn(`diagram PNG export skipped: ${err?.stack || err}`);
        }
      },
    },
  };
}

async function run({ dir, pages, logger }) {
  const distDir = fileURLToPath(dir);

  // Resolve routes → HTML files, keep only the ones that actually have a
  // diagram. No browser launched for the rest.
  const diagramPages = [];
  for (const { pathname } of pages ?? []) {
    const slug = pathname.replace(/^\/+|\/+$/g, "") || "index";
    const file = findHtml(distDir, slug);
    if (!file) continue;
    const html = await readFile(file, "utf8");
    // Match the ATTRIBUTE form a real diagram emits (`aria-roledescription="…"`).
    // The bare token also appears as a selector string in the always-present
    // MermaidZoom script, so a substring check would flag every page.
    if (!html.includes('aria-roledescription="')) continue;
    diagramPages.push({ slug, file, html });
  }
  if (diagramPages.length === 0) return;

  const cacheDir = join(PKG_ROOT, "node_modules/.cache/wh-diagram-png");
  const manifestPath = join(cacheDir, "manifest.json");
  const manifest = readJson(manifestPath);

  // Split work into cache-hits (just copy bytes into the fresh dist) and
  // misses (need Chromium). dist is wiped each build, so even cached diagrams
  // must be re-placed — the cache only saves the screenshot, not the copy.
  const toCopy = [];
  const toRender = [];
  for (const page of diagramPages) {
    for (const theme of THEMES) {
      const hash = diagramHash(page.html, theme);
      const entry = manifest[`${page.slug}|${theme}`];
      const cached =
        entry &&
        entry.hash === hash &&
        Array.from({ length: entry.count }).every((_, i) =>
          VARIANTS.every((v) => existsSync(join(cacheDir, `${hash}-${i}-${theme}${v.suffix}.png`))),
        );
      if (cached) toCopy.push({ ...page, theme, hash, count: entry.count });
      else toRender.push({ ...page, theme, hash });
    }
  }

  for (const job of toCopy) {
    for (let i = 0; i < job.count; i++) {
      for (const v of VARIANTS) {
        await place(
          join(cacheDir, `${job.hash}-${i}-${job.theme}${v.suffix}.png`),
          join(distDir, "diagrams", job.slug, `${i}-${job.theme}${v.suffix}.png`),
        );
      }
    }
  }

  let made = 0;
  if (toRender.length > 0) {
    const { chromium } = require("playwright");
    const browser = await chromium.launch();
    try {
      for (const theme of THEMES) {
        const jobs = toRender.filter((j) => j.theme === theme);
        if (jobs.length === 0) continue;
        const ctx = await browser.newContext({
          viewport: { width: 1800, height: 1200 },
          deviceScaleFactor: SCALE,
          colorScheme: theme,
        });
        // Starlight reads the theme from localStorage + the [data-theme] attr;
        // set both before first paint so global.css's light/dark branch (and
        // thus the diagram colors) is correct.
        await ctx.addInitScript((t) => {
          try {
            localStorage.setItem("starlight-theme", t);
          } catch {
            /* private mode, etc. */
          }
          document.documentElement.setAttribute("data-theme", t);
        }, theme);
        const page = await ctx.newPage();
        // `load`, not `networkidle`: some pages hold a long-lived connection
        // (the home page's live-stats SSE) that never goes idle. Diagrams are
        // pre-rendered static SVG, so `load` + fonts.ready is all we need.
        page.setDefaultNavigationTimeout(20000);
        // Cap non-navigation actions too (screenshot/$$eval), so one wedged
        // capture fails fast into the per-job catch instead of stalling 30s.
        page.setDefaultTimeout(20000);
        await routeDistAssets(page, distDir);

        for (const job of jobs) {
          try {
            await page.goto(pathToFileURL(job.file).href, { waitUntil: "load" });
            await page.evaluate(() => document.fonts?.ready);
            const count = await page.$$eval(DIAGRAM, (els) => els.length);
            for (let i = 0; i < count; i++) {
              for (const v of VARIANTS) {
                const buf = await capture(page, i, { transparent: v.transparent });
                await write(
                  join(distDir, "diagrams", job.slug, `${i}-${theme}${v.suffix}.png`),
                  buf,
                );
                await write(join(cacheDir, `${job.hash}-${i}-${theme}${v.suffix}.png`), buf);
                made++;
              }
            }
            // In-memory only; the whole manifest is flushed to disk once after
            // every theme finishes (a mid-run kill just re-renders next build).
            manifest[`${job.slug}|${theme}`] = { hash: job.hash, count };
          } catch (err) {
            // One stubborn page shouldn't sink the rest of the export.
            logger.warn(`diagram PNGs: ${job.slug} [${theme}] skipped — ${err?.message || err}`);
          }
        }
        await ctx.close();
      }
    } finally {
      await browser.close();
    }
    await write(manifestPath, JSON.stringify(manifest, null, 2));
  }

  const reused = toCopy.reduce((n, j) => n + j.count * VARIANTS.length, 0);
  logger.info(
    `diagram PNGs: ${made} rendered, ${reused} cached → dist/diagrams/ (${THEMES.join(", ")} × solid+transparent)`,
  );
}

// Clone the index-th diagram into a padded "surface card" (--wh-mermaid-surface
// background by default, like the zoom stage; transparent for the export
// variant), sized to the diagram's intrinsic dimensions, then clip a page
// screenshot to that card. Re-id the clone (the SVG scopes its inline
// <style>/marker refs by root id) and render it via the live page (not an
// isolated SVG screenshot) so foreignObject label CSS + fonts apply. The card
// is appended to <body>, NOT the content column: a transformed/contained
// content ancestor would become a `position: fixed` containing block and offset
// the card off (0,0); the label CSS is svg-scoped, so moving the clone to
// <body> doesn't change its look.
async function capture(page, index, { transparent = false } = {}) {
  const box = await page.evaluate(
    ({ index, selector, PAD, MAX_DIM, transparent }) => {
      const svg = document.querySelectorAll(selector)[index];
      const id = svg.getAttribute("id");
      let markup = svg.outerHTML;
      if (id) markup = markup.split(id).join(`${id}-pngshot`);

      const vb = (svg.getAttribute("viewBox") || "").split(/\s+/).map(Number);
      const rect = svg.getBoundingClientRect();
      const natW = vb.length === 4 && vb[2] ? vb[2] : rect.width;
      const natH = vb.length === 4 && vb[3] ? vb[3] : rect.height;
      const long = Math.max(natW, natH);
      const scale = long > MAX_DIM ? MAX_DIM / long : 1;

      const card = document.createElement("div");
      card.id = "__wh_pngshot";
      card.style.cssText =
        `position:fixed;left:0;top:0;z-index:2147483647;padding:${PAD}px;` +
        (transparent ? "background:transparent;" : "background:var(--wh-mermaid-surface);") +
        "display:inline-block;box-sizing:content-box;line-height:0;margin:0;border:0;";
      card.innerHTML = markup;
      const clone = card.firstElementChild;
      clone.style.maxWidth = "none";
      clone.style.minWidth = "0";
      clone.style.display = "block";
      clone.style.width = `${Math.round(natW * scale)}px`;
      clone.style.height = `${Math.round(natH * scale)}px`;
      document.body.appendChild(card);
      // Transparent variant: the card sits at (0,0), under the page's own
      // chrome (sticky header, fixed sidebar) — which paint their own (often
      // translucent) backgrounds and text that would bleed through the
      // transparent card. Inject a scoped stylesheet that clears the
      // <html>/<body> background and removes every other top-level element from
      // the render (`body > *` except the card), so the clip captures only the
      // diagram clone. display:none — NOT visibility:hidden — because
      // Starlight's fixed `.sidebar-pane` re-asserts `visibility: visible` on
      // itself, overriding an inherited `hidden` and leaking the nav into the
      // corner; a display:none ancestor can't be undone by a descendant. Doing
      // it via one stylesheet (rather than mutating each child's inline style)
      // leaves the page's own inline styles untouched — cleanup just drops the
      // <style>. Paired with screenshot({ omitBackground }) the surround renders
      // fully clear; the diagram's fills/edges/labels are untouched (their
      // colors come from :root vars, not from siblings).
      if (transparent) {
        const style = document.createElement("style");
        style.id = "__wh_pngshot_style";
        style.textContent =
          "html,body{background:transparent !important}" +
          "body>*:not(#__wh_pngshot){display:none !important}";
        document.head.appendChild(style);
      }
      // Clip to the card's MEASURED rect (normally (0,0) but resilient to any
      // residual offset) rather than assuming top-left.
      const r = card.getBoundingClientRect();
      return {
        x: Math.max(0, Math.floor(r.left)),
        y: Math.max(0, Math.floor(r.top)),
        width: Math.ceil(r.width),
        height: Math.ceil(r.height),
      };
    },
    { index, selector: DIAGRAM, PAD, MAX_DIM, transparent },
  );

  // Grow the viewport if the card overflows it, else the clip is truncated.
  const vp = page.viewportSize();
  const needW = box.x + box.width + 4;
  const needH = box.y + box.height + 4;
  if (vp.width < needW || vp.height < needH) {
    await page.setViewportSize({
      width: Math.max(vp.width, needW),
      height: Math.max(vp.height, needH),
    });
  }
  const buf = await page.screenshot({ type: "png", clip: box, omitBackground: transparent });
  await page.evaluate(() => {
    document.getElementById("__wh_pngshot")?.remove();
    document.getElementById("__wh_pngshot_style")?.remove();
  });
  return buf;
}

// Pages load over file://, so their absolute asset hrefs (/_astro/*, /fonts/*,
// /branding/*) resolve to the filesystem root and 404. Route every absolute
// file:// request back into dist/ so CSS, fonts and images load — without them
// the diagram colors and label fonts would be wrong.
async function routeDistAssets(page, distDir) {
  await page.route("**/*", async (route, req) => {
    const u = new URL(req.url());
    if (u.protocol === "file:" && u.pathname.startsWith("/")) {
      const onDisk = resolve(distDir, `.${u.pathname}`);
      if (onDisk.startsWith(distDir) && existsSync(onDisk)) {
        const ext = onDisk.slice(onDisk.lastIndexOf("."));
        return route.fulfill({
          body: await readFile(onDisk),
          contentType: CONTENT_TYPES[ext] || "application/octet-stream",
        });
      }
    }
    return route.continue();
  });
}

// Hash only what changes the rendered PNG: the diagram markup and the CSS
// bundle identity (its /_astro hash encodes global.css's content), per theme +
// render version. Hashing the whole page would bust the cache on unrelated
// prose / last-updated edits.
function diagramHash(html, theme) {
  const svgs = html.match(/<svg\b[^>]*aria-roledescription[\s\S]*?<\/svg>/g) || [];
  const css = html.match(/\/_astro\/[^"']+\.css/g) || [];
  return createHash("sha1")
    .update(`${RENDER_VERSION}|${theme}|${css.join(",")}|${svgs.join(" ")}`)
    .digest("hex")
    .slice(0, 16);
}

function findHtml(distDir, slug) {
  for (const rel of slug === "index" ? ["index.html"] : [`${slug}/index.html`, `${slug}.html`]) {
    const p = join(distDir, rel);
    if (existsSync(p)) return p;
  }
  return null;
}

function readJson(p) {
  try {
    return JSON.parse(readFileSync(p, "utf8"));
  } catch {
    return {};
  }
}

async function write(p, data) {
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, data);
}

async function place(src, dst) {
  await mkdir(dirname(dst), { recursive: true });
  await copyFile(src, dst);
}

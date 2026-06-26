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
// How a diagram is captured: we navigate to the built page (so its stylesheet
// + fonts load and a theme is set), read each diagram's SVG markup, then render
// that SVG ALONE — we replace the page <body> with just the diagram on a padded
// card and screenshot it. Rendering into an emptied body (rather than over the
// live page) means no site chrome — header, sidebar — sits behind the card, so
// the transparent variant needs no per-element hiding and no knowledge of the
// host theme's DOM. The diagram keeps its look because its colors come from
// :root custom properties and its label fonts from document @font-face — both
// in <head>, which we leave intact.
//
// Output: dist/diagrams/<slug>/<index>-<theme>[-transparent].png, where <slug>
// is the page path and <index> is the diagram's position among the configured
// selector — the exact selector the lightbox uses, so both sides agree on which
// PNG belongs to which diagram. Each diagram is emitted once per variant (solid
// surface card by default + a transparent-background version for slide decks).
//
// Everything host-specific (the diagram selector, theme names + how the theme
// is applied, the surface CSS variable, sizing, variants) is a config option
// with WaveHouse defaults, so the integration can be lifted into a standalone
// package without code changes — only `diagramPng({...})` overrides.

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

// Bump to invalidate every cached PNG after a change to the render routine.
const RENDER_VERSION = "5";

// Defaults are WaveHouse/Starlight-specific; every one is overridable via
// `diagramPng({...})` so this integration can move to a standalone package.
const DEFAULTS = {
  // CSS selector that matches each rendered diagram on a built page. The
  // lightbox MUST index by the same selector + DOM order (see MermaidZoom.astro).
  selector: ".sl-markdown-content svg[aria-roledescription]",
  // Theme names to render, one PNG set each.
  themes: ["light", "dark"],
  // How the host site selects a theme before first paint: the attribute set on
  // <html> and the localStorage key it reads (Starlight uses both).
  themeAttr: "data-theme",
  themeStorageKey: "starlight-theme",
  // CSS custom property holding the solid card background (the "surface" look).
  surfaceVar: "--wh-mermaid-surface",
  scale: 2, // retina; crisp when dropped into a deck
  pad: 28, // surface padding around the diagram (matches the zoom card feel)
  maxDim: 2400, // cap a diagram's CSS-px long edge so files stay sane
  // Each diagram is rasterized once per variant. The transparent file carries a
  // `-transparent` suffix; the lightbox toggle picks between the two.
  variants: [
    { suffix: "", transparent: false },
    { suffix: "-transparent", transparent: true },
  ],
};

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

/**
 * @param {Partial<typeof DEFAULTS>} [options]
 * @returns {import('astro').AstroIntegration}
 */
export function diagramPng(options = {}) {
  const cfg = { ...DEFAULTS, ...options };
  return {
    name: "wh-diagram-png",
    hooks: {
      "astro:build:done": async ({ dir, pages, logger }) => {
        if (process.env.WH_SKIP_DIAGRAM_PNG === "1") {
          logger.info("skipped (WH_SKIP_DIAGRAM_PNG=1)");
          return;
        }
        try {
          await run({ dir, pages, logger, cfg });
        } catch (err) {
          // A browser hiccup must not fail the whole docs build — the site is
          // already written; we just don't get fresh PNGs this run.
          logger.warn(`diagram PNG export skipped: ${err?.stack || err}`);
        }
      },
    },
  };
}

async function run({ dir, pages, logger, cfg }) {
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
    for (const theme of cfg.themes) {
      const hash = diagramHash(page.html, theme);
      const entry = manifest[`${page.slug}|${theme}`];
      const cached =
        entry &&
        entry.hash === hash &&
        Array.from({ length: entry.count }).every((_, i) =>
          cfg.variants.every((v) =>
            existsSync(join(cacheDir, `${hash}-${i}-${theme}${v.suffix}.png`)),
          ),
        );
      if (cached) toCopy.push({ ...page, theme, hash, count: entry.count });
      else toRender.push({ ...page, theme, hash });
    }
  }

  for (const job of toCopy) {
    for (let i = 0; i < job.count; i++) {
      for (const v of cfg.variants) {
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
      for (const theme of cfg.themes) {
        const jobs = toRender.filter((j) => j.theme === theme);
        if (jobs.length === 0) continue;
        const ctx = await browser.newContext({
          viewport: { width: 1800, height: 1200 },
          deviceScaleFactor: cfg.scale,
          colorScheme: theme === "light" || theme === "dark" ? theme : undefined,
        });
        // Set the theme the way the host site does (localStorage + the [attr]
        // on <html>) before first paint, so global.css's per-theme branch — and
        // thus the diagram colors — is correct.
        await ctx.addInitScript(
          ({ t, attr, key }) => {
            try {
              localStorage.setItem(key, t);
            } catch {
              /* private mode, etc. */
            }
            document.documentElement.setAttribute(attr, t);
          },
          { t: theme, attr: cfg.themeAttr, key: cfg.themeStorageKey },
        );
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
            // Read every diagram's markup + intrinsic size NOW, before the first
            // render empties the body (rendering one diagram destroys the others
            // in the DOM, so we can't re-read per index).
            const diagrams = await extractDiagrams(page, cfg.selector);
            for (let i = 0; i < diagrams.length; i++) {
              for (const v of cfg.variants) {
                const buf = await renderPng(page, diagrams[i], v, cfg);
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
            manifest[`${job.slug}|${theme}`] = { hash: job.hash, count: diagrams.length };
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

  const reused = toCopy.reduce((n, j) => n + j.count * cfg.variants.length, 0);
  logger.info(
    `diagram PNGs: ${made} rendered, ${reused} cached → dist/diagrams/ (${cfg.themes.join(", ")} × solid+transparent)`,
  );
}

// Read each diagram's SVG outerHTML + intrinsic size from the live (still-intact)
// page. Must run before any renderPng() call, which empties the <body>.
function extractDiagrams(page, selector) {
  return page.evaluate((selector) => {
    return [...document.querySelectorAll(selector)].map((svg) => {
      const vb = (svg.getAttribute("viewBox") || "").split(/\s+/).map(Number);
      const rect = svg.getBoundingClientRect();
      return {
        markup: svg.outerHTML,
        natW: vb.length === 4 && vb[2] ? vb[2] : rect.width,
        natH: vb.length === 4 && vb[3] ? vb[3] : rect.height,
      };
    });
  }, selector);
}

// Render ONE diagram to PNG: drop its SVG onto a padded card (surface bg, or
// transparent for the export variant), replace the whole <body> with just that
// card, and screenshot it. Replacing the body — rather than overlaying the live
// page — means no site chrome sits behind the card, so the transparent variant
// is simply `omitBackground` with nothing to hide and no host-DOM assumptions.
// <head> is untouched, so the diagram's themed fills (:root custom properties)
// and label fonts (@font-face) still resolve; the SVG's own scoped <style>
// carries the rest, so it looks identical to the inline diagram.
async function renderPng(page, diagram, { transparent }, cfg) {
  const box = await page.evaluate(
    ({ markup, natW, natH, transparent, pad, maxDim, surfaceVar }) => {
      const long = Math.max(natW, natH);
      const scale = long > maxDim ? maxDim / long : 1;

      const card = document.createElement("div");
      card.id = "__wh_pngshot";
      card.style.cssText =
        `position:fixed;left:0;top:0;padding:${pad}px;` +
        (transparent ? "background:transparent;" : `background:var(${surfaceVar});`) +
        "display:inline-block;box-sizing:content-box;line-height:0;margin:0;border:0;";
      card.innerHTML = markup;
      const clone = card.firstElementChild;
      clone.style.maxWidth = "none";
      clone.style.minWidth = "0";
      clone.style.display = "block";
      clone.style.width = `${Math.round(natW * scale)}px`;
      clone.style.height = `${Math.round(natH * scale)}px`;

      // The card is now the ENTIRE body — no chrome behind it. Reset the
      // html/body background too: their opaque themed bg would otherwise show
      // through a transparent card (it's a no-op behind the opaque solid card).
      document.documentElement.style.background = "transparent";
      document.body.style.cssText = "margin:0;padding:0;background:transparent;";
      document.body.replaceChildren(card);

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
    {
      markup: diagram.markup,
      natW: diagram.natW,
      natH: diagram.natH,
      transparent,
      pad: cfg.pad,
      maxDim: cfg.maxDim,
      surfaceVar: cfg.surfaceVar,
    },
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
  return page.screenshot({ type: "png", clip: box, omitBackground: transparent });
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
    .update(`${RENDER_VERSION}|${theme}|${css.join(",")}|${svgs.join(" ")}`)
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

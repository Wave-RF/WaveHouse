/* Mermaid diagram screenshots for visual regression.
 *
 * Usage: node scripts/screenshot-diagrams.mjs
 * Prereq: `pnpm build` first — this script reads `dist/`.
 *
 * What it does: loads each built docs page directly via `file://`,
 * intercepts the `/_astro/*.css` absolute hrefs so they resolve to
 * `dist/_astro/*` on disk, then screenshots every flowchart SVG on
 * the page in both light and dark mode at retina dpr.
 *
 * Why this approach (vs. the prior `mermaid-preview.mjs` / `zoom-*.mjs`
 * pattern that loaded SVGs in a stub HTML page with hand-edited CSS):
 *
 *   The stub-page approach silently mis-renders polish styles — the
 *   cluster-title pill and edge-label pill backgrounds didn't appear
 *   because the rewritten `:root` → `.pane` substitution doesn't
 *   reproduce the exact cascade. That looked like a regression but
 *   wasn't. Loading the real dist HTML eliminates the doubt: what you
 *   see here is what `wavehouse.dev` will serve.
 *
 * Output: screenshots/<page>-{light|dark}-<i>.png, one per diagram.
 * `screenshots/` is gitignored. */
import { chromium } from "playwright";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { readFile, mkdir } from "node:fs/promises";

const ROOT = resolve(process.cwd());
const DIST = resolve(ROOT, "dist");
const OUT = resolve(ROOT, "screenshots");
await mkdir(OUT, { recursive: true });

// Pages whose mermaid output we care about. Add new ones here as the
// docs grow — anything not listed is skipped (no auto-discovery; cheap
// is good).
const PAGES = [
  { name: "architecture", file: "architecture/index.html" },
  { name: "why-wavehouse", file: "why-wavehouse/index.html" },
];

const browser = await chromium.launch();
try {
  for (const theme of ["light", "dark"]) {
    const ctx = await browser.newContext({
      viewport: { width: 2000, height: 1400 },
      deviceScaleFactor: 2,
      colorScheme: theme,
    });
    // Starlight reads theme from localStorage and the [data-theme] attr;
    // initScript ensures both are set before the first render so the
    // light/dark CSS branches in global.css apply correctly.
    await ctx.addInitScript((t) => {
      try {
        localStorage.setItem("starlight-theme", t);
      } catch {}
      document.documentElement.setAttribute("data-theme", t);
    }, theme);

    const page = await ctx.newPage();
    // Astro emits absolute hrefs like `/_astro/common.<hash>.css`. Under
    // file://, those resolve to the filesystem root and 404. Route them
    // back into `dist/_astro/` so the page picks up its real CSS.
    await page.route("**/*", async (route, req) => {
      const u = new URL(req.url());
      if (u.protocol === "file:" && u.pathname.startsWith("/_astro/")) {
        const path = resolve(DIST, u.pathname.replace(/^\//, ""));
        try {
          const body = await readFile(path);
          const type = path.endsWith(".css")
            ? "text/css"
            : path.endsWith(".js")
            ? "text/javascript"
            : "application/octet-stream";
          return route.fulfill({ body, contentType: type });
        } catch {
          return route.continue();
        }
      }
      return route.continue();
    });

    for (const p of PAGES) {
      const fileUrl = pathToFileURL(resolve(DIST, p.file)).href;
      await page.goto(fileUrl, { waitUntil: "networkidle" });
      await page.evaluate(() => document.fonts.ready);

      const count = await page.$$eval(
        'svg[aria-roledescription^="flowchart"]',
        (svgs) => svgs.length
      );
      for (let i = 0; i < count; i++) {
        // Scroll the i-th svg into view, then page.screenshot clipped to
        // its bbox. Screenshotting the SVG element directly drops doc
        // CSS for its foreignObject HTML content; clipping the page does
        // not.
        const box = await page.evaluate((idx) => {
          const svg = document.querySelectorAll(
            'svg[aria-roledescription^="flowchart"]'
          )[idx];
          svg.scrollIntoView({ block: "center" });
          const r = svg.getBoundingClientRect();
          return { x: r.x, y: r.y, w: r.width, h: r.height };
        }, i);
        const outName = `${p.name}-${theme}-${i + 1}.png`;
        await page.screenshot({
          path: resolve(OUT, outName),
          clip: {
            x: Math.max(0, box.x - 20),
            y: Math.max(0, box.y - 20),
            width: box.w + 40,
            height: box.h + 40,
          },
        });
        console.log(`✓ screenshots/${outName}`);
      }
    }
    await ctx.close();
  }
} finally {
  await browser.close();
}

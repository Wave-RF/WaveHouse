/* Visual regression / preview screenshots for the WaveHouse docs site.
 * Run with: pnpm exec node scripts/screenshot.mjs
 * Requires the dev server to be running on http://127.0.0.1:4321
 * (the Astro default, which is also what `make dev-docs` starts). */

import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";

const BASE = process.env.BASE_URL ?? "http://127.0.0.1:4321";
const OUT = resolve(process.cwd(), "screenshots");

const pages = [
  { name: "landing", url: "/", viewport: { width: 1440, height: 900 }, full: true },
  { name: "landing-mobile", url: "/", viewport: { width: 390, height: 844 }, full: true },
  { name: "getting-started", url: "/getting-started", viewport: { width: 1440, height: 900 }, full: false },
  { name: "api", url: "/api", viewport: { width: 1440, height: 900 }, full: false },
  { name: "architecture", url: "/architecture", viewport: { width: 1440, height: 900 }, full: false },
];

await mkdir(OUT, { recursive: true });

const browser = await chromium.launch();
try {
  for (const theme of ["dark", "light"]) {
    for (const p of pages) {
      const ctx = await browser.newContext({
        viewport: p.viewport,
        colorScheme: theme === "dark" ? "dark" : "light",
      });
      const page = await ctx.newPage();
      await page.goto(`${BASE}${p.url}`, { waitUntil: "networkidle" });
      await page.evaluate((t) => {
        document.documentElement.setAttribute("data-theme", t);
        try { localStorage.setItem("starlight-theme", t); } catch {}
      }, theme);
      // Ensure web fonts load + wait past entrance animations. The hero
      // terminal staggers nine lines at 0.42s each, so the last line lands
      // around 3.8s after mount — wait a bit past that so the capture is
      // a fully-rendered frame, not a half-typed terminal.
      await page.evaluate(() => document.fonts.ready);
      await page.waitForTimeout(4500);
      const filename = `${p.name}-${theme}.png`;
      await page.screenshot({
        path: resolve(OUT, filename),
        fullPage: p.full,
      });
      console.log(`✓ ${filename}`);
      await ctx.close();
    }
  }
} finally {
  await browser.close();
}

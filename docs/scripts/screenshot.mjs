/* Visual regression / preview screenshots for the WaveHouse docs site.
 * Run with: pnpm exec node scripts/screenshot.mjs
 * Requires the docs server to be running on http://127.0.0.1:4321
 * (what `make dev-docs` serves on; `pnpm run start` lands there too). */

import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { chromium } from "playwright";

const BASE = process.env.BASE_URL ?? "http://127.0.0.1:4321";
const OUT = resolve(process.cwd(), "screenshots");

const pages = [
  { name: "landing", url: "/", viewport: { width: 1440, height: 900 }, full: true },
  { name: "landing-mobile", url: "/", viewport: { width: 390, height: 844 }, full: true },
  {
    name: "getting-started",
    url: "/getting-started",
    viewport: { width: 1440, height: 900 },
    full: false,
  },
  { name: "api", url: "/api", viewport: { width: 1440, height: 900 }, full: false },
  {
    name: "architecture",
    url: "/architecture",
    viewport: { width: 1440, height: 900 },
    full: false,
  },
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
      // "load", not "networkidle": the landing page holds a live SSE stream
      // open to stats.wavehouse.dev, so the network never goes idle and a
      // networkidle wait would time the goto out.
      await page.goto(`${BASE}${p.url}`, { waitUntil: "load" });
      await page.evaluate((t) => {
        document.documentElement.setAttribute("data-theme", t);
        try {
          localStorage.setItem("starlight-theme", t);
        } catch {}
      }, theme);
      // Wait on web fonts + the hero's own ready signal — Hero.astro stamps
      // `[data-screenshot-ready]` on <html> via animationend on the visual
      // column's entrance (first mount) or synchronously when the replay
      // guard suppresses the entrance choreography (subsequent mounts).
      // Pages without a hero never get the attribute, so we cap the wait at
      // 5s and fall through to the screenshot regardless. Live-demo data may
      // or may not have arrived by then — the panel is layout-stable either
      // way; screenshots assert layout, not live numbers.
      await page.evaluate(() => document.fonts.ready);
      await page.waitForSelector("html[data-screenshot-ready]", { timeout: 5000 }).catch(() => {});
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

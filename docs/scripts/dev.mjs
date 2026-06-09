/* Production-faithful docs dev loop — what `make dev-docs` runs.
 *
 * `astro dev` skips everything the Cloudflare Worker adds in production:
 * cloudflare-md-router content negotiation (`.md` twins), the pagefind
 * search index, and the starlight-llm-tools outputs only exist in real
 * builds. So instead of the dev server, this loop runs a full `astro build`
 * on every save and serves the result through `wrangler dev --live-reload`
 * on :4321 — the same Worker + Static Assets pipeline as wavehouse.dev,
 * with the browser auto-refreshing when a build lands.
 *
 * Builds go to a .dev-dist/ staging dir and are synced into dist/ (plain
 * node fs — no rsync or any other external tool, so WSL/minimal images work)
 * only on success: a failed build, or the emptied-outDir window during one,
 * never takes down the served site — you keep the last good build until a
 * green one replaces it, and failures print a red banner + terminal bell.
 * Trade-off vs `astro dev`: no HMR and no browser error overlay; each save
 * costs a real build (mostly rehype-mermaid's Chromium), with errors landing
 * in this terminal instead of the page. The raw Astro dev server remains
 * available as `pnpm run start` when HMR matters more than fidelity.
 *
 * Knobs:
 *   DOCS_PORT=…           serve port (default 4321)
 *   DOCS_WATCH_STRICT=1   keep starlight-links-validator in watch builds —
 *                         a broken link then fails the build loudly here
 *                         instead of waiting for CI. Off by default because
 *                         mid-edit pages routinely link to pages that don't
 *                         exist yet, which would block previewing every save
 *                         (see the WAVEHOUSE_DOCS_WATCH gate in
 *                         astro.config.mjs; rendered output is identical). */

import { spawn } from "node:child_process";
import { existsSync, watch } from "node:fs";
import { cp, readdir, rm, stat } from "node:fs/promises";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..");
const BIN = join(ROOT, "node_modules", ".bin");
const STAGING = join(ROOT, ".dev-dist");
const DIST = join(ROOT, "dist");
const PORT = process.env.DOCS_PORT ?? "4321";
const DEBOUNCE_MS = 300;

const log = (msg) => console.log(`\x1b[36m[dev-docs]\x1b[0m ${msg}`);
// Loud-failure banner: bold red + terminal bell (most terminals flash/bounce).
const fail = (msg) => console.log(`\x1b[36m[dev-docs]\x1b[0m \x1b[1;31m${msg}\x1b[0m\x07`);
const STRICT = Boolean(process.env.DOCS_WATCH_STRICT);

let activeBuild = null;
function run(cmd, args, opts = {}) {
  return new Promise((done) => {
    const child = spawn(cmd, args, { cwd: ROOT, stdio: "inherit", ...opts });
    activeBuild = child;
    const finish = (code) => {
      activeBuild = null;
      done(code);
    };
    child.on("error", () => finish(127)); // e.g. binary not found
    child.on("close", (code) => finish(code ?? 1));
  });
}

/* Sync staging → dist in place (same directory inode, so wrangler's asset
 * watcher sees an incremental update rather than a directory swap): prune
 * entries the new build dropped, then copy everything over. prune runs
 * FIRST so a path that changed type between builds (file ↔ directory, e.g.
 * a page restructure) is removed before cp would trip over it. */
async function syncToDist() {
  if (shuttingDown) return;
  await prune(STAGING, DIST);
  await cp(STAGING, DIST, { recursive: true, force: true });
}

/* Remove dist entries that are absent from staging or changed type. */
async function prune(stagingDir, distDir) {
  let entries;
  try {
    entries = await readdir(distDir, { withFileTypes: true });
  } catch {
    return; // first build: dist/ doesn't exist yet — cp creates it
  }
  for (const entry of entries) {
    const s = join(stagingDir, entry.name);
    const d = join(distDir, entry.name);
    const sStat = await stat(s).catch(() => null);
    if (!sStat || sStat.isDirectory() !== entry.isDirectory()) {
      await rm(d, { recursive: true, force: true });
    } else if (entry.isDirectory()) {
      await prune(s, d);
    }
  }
}

let building = false;
let dirty = false;
let pendingReason = "startup";
let buildCount = 0;

async function rebuild() {
  if (shuttingDown) return; // a debounce callback can fire after the signal
  if (building) {
    dirty = true;
    return;
  }
  building = true;
  do {
    dirty = false;
    const n = ++buildCount;
    const t0 = Date.now();
    log(`build #${n} (${pendingReason})…`);
    const code = await run(join(BIN, "astro"), ["build", "--outDir", ".dev-dist"], {
      env: { ...process.env, ...(STRICT ? {} : { WAVEHOUSE_DOCS_WATCH: "1" }) },
    });
    if (shuttingDown) break;
    if (code === 0) {
      try {
        await syncToDist();
        log(
          `build #${n} live in ${((Date.now() - t0) / 1000).toFixed(1)}s — browser reloads itself`,
        );
      } catch (err) {
        // A failed sync degrades like a failed build — never crash the loop
        // (that would orphan wrangler on the port).
        fail(
          `build #${n} sync FAILED (${err?.code ?? err}) — dist/ may be partial; next green build re-syncs`,
        );
      }
    } else {
      fail(
        `build #${n} FAILED (exit ${code}) — error above; still serving the previous good build`,
      );
    }
  } while (dirty && !shuttingDown);
  building = false;
}

let timer;
function onChange(file) {
  pendingReason = file;
  clearTimeout(timer);
  timer = setTimeout(rebuild, DEBOUNCE_MS);
}

// Editor droppings: macOS Finder, vim swap/backup files, vim's fsync probe.
const IGNORED = /(^|\/)(\.DS_Store|4913|.*\.sw[px]|.*~)$/;
const watchers = [];
function startWatchers() {
  for (const dir of ["src", "public"]) {
    watchers.push(
      watch(join(ROOT, dir), { recursive: true }, (_event, file) => {
        if (!file || IGNORED.test(file)) return;
        onChange(join(dir, file));
      }),
    );
  }
  // Root-level build inputs: Astro/TS config, the dep manifest (a `pnpm add`
  // of a new plugin should rebuild), and .env* files (Astro loads them at
  // build time). A non-recursive watch on the directory survives editors that
  // replace files on save (watching a file itself would not). Deliberately
  // absent: worker/index.ts + wrangler.jsonc (wrangler dev watches those
  // itself) and pnpm-lock.yaml (changes on `git pull` before `pnpm install`
  // has run — rebuilding at that moment would fail spuriously).
  const rootTriggers = new Set(["astro.config.mjs", "tsconfig.json", "package.json"]);
  watchers.push(
    watch(ROOT, (_event, file) => {
      if (file && (rootTriggers.has(file) || file.startsWith(".env"))) onChange(file);
    }),
  );
}

let wrangler;
let shuttingDown = false;
function shutdown(code) {
  if (shuttingDown) return;
  shuttingDown = true;
  clearTimeout(timer);
  for (const w of watchers) w.close(); // open watchers would keep the event loop alive
  activeBuild?.kill("SIGINT");
  wrangler?.kill("SIGINT");
  process.exitCode = code;
}
process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));
// Backstop for anything the loop doesn't catch (incl. fs.watch 'error'
// events): tear wrangler down with us rather than orphaning it on the port.
for (const event of ["uncaughtException", "unhandledRejection"]) {
  process.on(event, (err) => {
    log(`fatal (${event}): ${err?.stack ?? err}`);
    shutdown(1);
  });
}

if (existsSync(join(DIST, "index.html"))) {
  log("serving the existing dist/ while a fresh build runs");
  void rebuild();
} else {
  log("no dist/ yet — first build must finish before serving starts");
  await rebuild();
  if (!existsSync(join(DIST, "index.html"))) {
    log("first build failed and there is no previous dist/ to serve — exiting");
    process.exit(1);
  }
}

// A signal during the cold-start build means we're done before serving starts.
if (shuttingDown) process.exit();

wrangler = spawn(join(BIN, "wrangler"), ["dev", "--live-reload", "--port", PORT], {
  cwd: ROOT,
  stdio: "inherit",
});
// wrangler owns the terminal UX; when it exits (its `x` hotkey, a crash),
// take the whole loop down with it.
wrangler.on("close", (code) => shutdown(code ?? 0));

startWatchers();
log(`watching src/, public/, and root config — wrangler serves http://localhost:${PORT}`);
log(
  STRICT
    ? "strict mode: links validator ON — a broken link fails the build here"
    : "links validator skipped in watch builds (CI still enforces; DOCS_WATCH_STRICT=1 to keep it on)",
);

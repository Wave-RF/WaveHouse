/* Production-faithful docs dev loop — what `make dev-docs` runs.
 *
 * `astro dev` skips everything the Cloudflare Worker adds in production:
 * cloudflare-md-router content negotiation (`.md` twins), the pagefind
 * search index, and the starlight-llm-tools outputs only exist in real
 * builds. So instead of the dev server, this loop runs a full `astro build`
 * on every save and serves the result through `wrangler dev --live-reload`
 * on :4321 (or the next free port) — the same Worker + Static Assets pipeline
 * as wavehouse.dev, with the browser auto-refreshing when a build lands.
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
 *   DOCS_PORT=…           first port to try (default 4321). If it's taken the
 *                         loop walks upward to the next free one and prints
 *                         where it landed — wrangler itself would just die,
 *                         see findFreePort() below.
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
import { createServer as createNetServer } from "node:net";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..");
const BIN = join(ROOT, "node_modules", ".bin");
const STAGING = join(ROOT, ".dev-dist");
const DIST = join(ROOT, "dist");
const DEFAULT_PORT = 4321;
const MAX_PORT = 65535;
const PORT_TRIES = 20;

/* DOCS_PORT has to be a real port before it reaches the scan: "" and "0" would
 * bind an ephemeral port and announce http://localhost:0, anything non-numeric
 * becomes NaN (the loop then runs zero times and reports "NaN–NaN"), and
 * anything out of range throws ERR_SOCKET_BAD_PORT from inside the probe. */
function parsePort(raw) {
  if (raw === undefined || raw === "") return DEFAULT_PORT;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > MAX_PORT) return null;
  return port;
}

const PORT_START = parsePort(process.env.DOCS_PORT);
if (PORT_START === null) {
  console.log(
    `\x1b[36m[dev-docs]\x1b[0m \x1b[1;31mDOCS_PORT must be an integer 1-${MAX_PORT}, got ${JSON.stringify(process.env.DOCS_PORT)}\x1b[0m\x07`,
  );
  process.exit(1);
}
const DEBOUNCE_MS = 300;

const log = (msg) => console.log(`\x1b[36m[dev-docs]\x1b[0m ${msg}`);
// Loud-failure banner: bold red + terminal bell (most terminals flash/bounce).
const fail = (msg) => console.log(`\x1b[36m[dev-docs]\x1b[0m \x1b[1;31m${msg}\x1b[0m\x07`);
const STRICT = Boolean(process.env.DOCS_WATCH_STRICT);

/* Pick the port BEFORE handing it to wrangler.
 *
 * `wrangler dev` hunts for a free port when you don't name one, but treats an
 * explicit `--port` as strict — it dies with a raw kj bind exception rather
 * than moving. We have to pass `--port` (the URL is logged below, and bare
 * wrangler would land somewhere we couldn't announce), so the hunting is ours
 * to do. `astro dev` never enters into it: this loop serves builds through the
 * Worker, so Vite's own port-hunting is not in the path.
 *
 * Ports are machine-wide, not per-worktree or per-repo, so the usual collision
 * is a dev server from an entirely different checkout.
 *
 * Both stacks get probed because wrangler binds 127.0.0.1 and [::1] as
 * separate sockets, and the common squatter — an `astro dev` elsewhere — holds
 * only [::1]. Probing IPv4 alone would call the port free and we would fail on
 * the v6 bind anyway, which is exactly the failure this replaces. */
function portFree(port, host) {
  return new Promise((resolvePort) => {
    const probe = createNetServer();
    // A host with IPv6 disabled answers EADDRNOTAVAIL for ::1, and that alone
    // must not veto the port. Every other error — EADDRINUSE, EACCES, a bad
    // host, EADDRNOTAVAIL on any other address — means we cannot claim it.
    probe.once("error", (err) => resolvePort(err.code === "EADDRNOTAVAIL" && host === "::1"));
    probe.listen({ port, host, exclusive: true }, () => probe.close(() => resolvePort(true)));
  });
}

/** Last port the scan will try — clamped, since start+tries can exceed the range. */
const lastPortFor = (start, tries) => Math.min(start + tries - 1, MAX_PORT);

async function findFreePort(start, tries) {
  for (let port = start, last = lastPortFor(start, tries); port <= last; port++) {
    if ((await portFree(port, "127.0.0.1")) && (await portFree(port, "::1"))) {
      return port;
    }
  }
  return null;
}

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

// Resolved here rather than at startup so the gap between "it was free" and
// "wrangler has it" stays as small as possible — a cold-start build is minutes
// of window during which someone else could take the port.
const PORT = await findFreePort(PORT_START, PORT_TRIES);
if (PORT === null) {
  fail(
    `no free port in ${PORT_START}–${lastPortFor(PORT_START, PORT_TRIES)}. ` +
      `Stop one of the servers holding them, or set DOCS_PORT to a clear range.`,
  );
  process.exit(1);
}
if (PORT !== PORT_START) {
  log(`port ${PORT_START} is busy (often a dev server from another checkout) — using ${PORT}`);
}

wrangler = spawn(join(BIN, "wrangler"), ["dev", "--live-reload", "--port", String(PORT)], {
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

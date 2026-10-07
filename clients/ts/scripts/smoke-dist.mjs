// Loads each built entry point the way a consumer does and checks they agree.
// Run it at the oldest Node the package supports (`make smoke-ts-dist`):
// a dependency that is ESM-only breaks `require()` there but not on a current Node.
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const pkg = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));

// `--min-node` prints the oldest Node that `engines.node` admits, as a full x.y.z.
if (process.argv.includes("--min-node")) {
  const m = /^>=\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?$/.exec(pkg.engines.node.trim());
  if (!m) throw new Error(`cannot derive a minimum from engines.node ${pkg.engines.node}`);
  console.log(`${m[1]}.${m[2] ?? 0}.${m[3] ?? 0}`);
  process.exit(0);
}

const failures = [];
const check = (label, fn) => {
  try {
    fn();
  } catch (err) {
    failures.push(`${label}: ${err?.stack ?? err}`);
  }
};
const names = (o) => Object.keys(o).sort();

const esm = await import(pathToFileURL(resolve(root, pkg.exports["."].import)).href);
// The ESM build is the reference surface; CJS and IIFE must match it exactly.
const expected = names(esm);
if (expected.length === 0 || typeof esm.createClient !== "function") {
  failures.push(`esm (${pkg.exports["."].import}): empty or missing createClient`);
}

const sameSurface = (label, mod) => {
  const got = names(mod);
  const missing = expected.filter((n) => !got.includes(n));
  const extra = got.filter((n) => !expected.includes(n));
  if (missing.length || extra.length) {
    throw new Error(`export surface differs from ESM (missing: [${missing}], extra: [${extra}])`);
  }
};

check(`cjs (${pkg.exports["."].require}) via require()`, () => {
  const mod = createRequire(import.meta.url)(resolve(root, pkg.exports["."].require));
  sameSurface("cjs", mod);
});

check(`iife (${pkg.unpkg}) as a classic script`, () => {
  const ctx = vm.createContext({ URL, URLSearchParams, TextDecoder, TextEncoder, AbortController });
  vm.runInContext(readFileSync(resolve(root, pkg.unpkg), "utf8"), ctx, { filename: pkg.unpkg });
  const g = ctx.WaveHouse;
  if (!g) throw new Error("script did not define the WaveHouse global");
  sameSurface("iife", g);
});

if (failures.length) {
  console.error(`dist smoke FAILED on node ${process.version}:\n${failures.join("\n\n")}`);
  process.exit(1);
}
console.log(`dist smoke ok on node ${process.version}: ${expected.length} exports across esm, cjs, iife`);

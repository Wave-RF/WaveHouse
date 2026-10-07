// Loads each built entry point the way a consumer does and checks they agree.
// Run it at the oldest Node the package supports (`make smoke-ts-dist`):
// a dependency that is ESM-only breaks `require()` there but not on a current Node.
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
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

// `--expect-node X.Y.Z` fails unless this process is that exact Node, so a
// fallback to whatever `node` is on PATH cannot pass for the oldest-Node run.
const expectIdx = process.argv.indexOf("--expect-node");
if (expectIdx !== -1 && process.version !== `v${process.argv[expectIdx + 1]}`) {
  console.error(
    `dist smoke: expected node v${process.argv[expectIdx + 1]}, running ${process.version}`,
  );
  process.exit(1);
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

const exp = pkg.exports["."];
check("package.json entry points agree", () => {
  if (pkg.main !== exp.require)
    throw new Error(`main ${pkg.main} != exports.require ${exp.require}`);
  if (pkg.module !== exp.import)
    throw new Error(`module ${pkg.module} != exports.import ${exp.import}`);
  if (pkg.jsdelivr !== pkg.unpkg) throw new Error(`jsdelivr ${pkg.jsdelivr} != unpkg ${pkg.unpkg}`);
});

// The ESM build is the reference surface; CJS and IIFE must match it exactly.
let expected = [];
try {
  const esm = await import(pathToFileURL(resolve(root, exp.import)).href);
  expected = names(esm);
  if (expected.length === 0 || typeof esm.createClient !== "function") {
    throw new Error("empty export surface or missing createClient");
  }
} catch (err) {
  failures.push(`esm (${exp.import}) via import(): ${err?.stack ?? err}`);
}

const sameSurface = (mod) => {
  const got = names(mod);
  const missing = expected.filter((n) => !got.includes(n));
  const extra = got.filter((n) => !expected.includes(n));
  if (missing.length || extra.length) {
    throw new Error(`export surface differs from ESM (missing: [${missing}], extra: [${extra}])`);
  }
};

check(`cjs (${exp.require}) via require()`, () => {
  sameSurface(createRequire(import.meta.url)(resolve(root, exp.require)));
});

check(`iife (${pkg.unpkg}) as a classic script`, () => {
  // A browser-like global: the host's web-standard globals minus Node-only ones.
  const nodeOnly = new Set(["process", "Buffer", "require", "global", "module", "exports"]);
  const sandbox = {};
  for (const k of Object.getOwnPropertyNames(globalThis)) {
    if (!nodeOnly.has(k) && /^[A-Z]/.test(k)) sandbox[k] = globalThis[k];
  }
  sandbox.fetch = globalThis.fetch;
  const ctx = vm.createContext(sandbox);
  vm.runInContext(readFileSync(resolve(root, pkg.unpkg), "utf8"), ctx, { filename: pkg.unpkg });
  const g = ctx.WaveHouse;
  if (!g) throw new Error("script did not define the WaveHouse global");
  sameSurface(g);
});

check(`bin (${pkg.bin["wavehouse-codegen"]}) --help`, () => {
  const r = spawnSync(process.execPath, [resolve(root, pkg.bin["wavehouse-codegen"]), "--help"], {
    encoding: "utf8",
  });
  if (r.status !== 0) throw new Error(`exit ${r.status}: ${r.stderr}`);
});

if (failures.length) {
  console.error(`dist smoke FAILED on node ${process.version}:\n${failures.join("\n\n")}`);
  process.exit(1);
}
console.log(
  `dist smoke ok on node ${process.version}: ${expected.length} exports across esm, cjs, iife`,
);

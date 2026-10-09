// Loads each built entry point the way a consumer does and checks they agree.
// Run it at the oldest Node the package supports (`make smoke-ts-dist`):
// a dependency that is ESM-only breaks `require()` there but not on a current Node.
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
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
  if (pkg.main !== exp.require.default)
    throw new Error(`main ${pkg.main} != exports.require.default ${exp.require.default}`);
  if (pkg.module !== exp.import.default)
    throw new Error(`module ${pkg.module} != exports.import.default ${exp.import.default}`);
  if (pkg.jsdelivr !== pkg.unpkg) throw new Error(`jsdelivr ${pkg.jsdelivr} != unpkg ${pkg.unpkg}`);
  if (pkg.types !== exp.import.types)
    throw new Error(`types ${pkg.types} != exports.import.types ${exp.import.types}`);
  for (const f of [exp.import.types, exp.require.types])
    if (!existsSync(resolve(root, f))) throw new Error(`${f} does not exist`);
});

// Entry points are loaded by package name, through the exports map, so a broken
// map fails here; the resolved file must also be the one the map names.
const requireFromPkg = createRequire(resolve(root, "package.json"));
check("exports map resolves to the declared files", () => {
  const same = (got, want, cond) => {
    if (resolve(got) !== resolve(root, want))
      throw new Error(`${cond} resolved to ${got}, want ${want}`);
  };
  same(fileURLToPath(import.meta.resolve(pkg.name)), exp.import.default, "import");
  same(requireFromPkg.resolve(pkg.name), exp.require.default, "require");
});

// The ESM build is the reference surface; CJS and IIFE must match it exactly.
let expected = [];
try {
  const esm = await import(pkg.name);
  expected = names(esm);
  if (expected.length === 0 || typeof esm.createClient !== "function") {
    throw new Error("empty export surface or missing createClient");
  }
} catch (err) {
  failures.push(`esm (${exp.import.default}) via import(): ${err?.stack ?? err}`);
}

const sameSurface = (mod) => {
  const got = names(mod);
  const missing = expected.filter((n) => !got.includes(n));
  const extra = got.filter((n) => !expected.includes(n));
  if (missing.length || extra.length) {
    throw new Error(`export surface differs from ESM (missing: [${missing}], extra: [${extra}])`);
  }
};

check(`cjs (${exp.require.default}) via require()`, () => {
  sameSurface(requireFromPkg(pkg.name));
});

check(`iife (${pkg.unpkg}) as a classic script`, () => {
  // A browser-like global: a fresh context plus the host's web-standard globals
  // (fetch, URL, TextDecoder, console, timers...), minus what only Node has.
  const ctx = vm.createContext({});
  const own = new Set(Object.getOwnPropertyNames(vm.runInContext("globalThis", ctx)));
  const nodeOnly = new Set(["process", "Buffer", "global", "setImmediate", "clearImmediate"]);
  for (const k of Object.getOwnPropertyNames(globalThis)) {
    if (!own.has(k) && !nodeOnly.has(k)) ctx[k] = globalThis[k];
  }
  ctx.window = ctx.self = ctx;
  vm.runInContext(readFileSync(resolve(root, pkg.unpkg), "utf8"), ctx, {
    filename: pkg.unpkg,
  });
  const g = ctx.WaveHouse;
  if (!g) throw new Error("script did not define the WaveHouse global");
  sameSurface(g);
});

// A TypeScript consumer picks the declarations by its own module format: under
// node16/nodenext a CommonJS project may not `import` an ESM-typed package
// (TS1479), though the runtime `require()` works. Compile one consumer of each
// format against the built package, offline, from a scratch project that
// resolves it by name through node_modules like an installed copy.
check("type declarations resolve for CommonJS and ESM consumers", () => {
  const tsc = resolve(root, "node_modules/typescript/bin/tsc");
  const dir = mkdtempSync(join(tmpdir(), "wavehouse-sdk-types-"));
  try {
    mkdirSync(join(dir, "node_modules/@wavehouse"), { recursive: true });
    symlinkSync(root, join(dir, "node_modules", pkg.name), "dir");
    const use = `import { createClient } from "${pkg.name}";\nexport const c: ReturnType<typeof createClient> = createClient({ baseURL: "http://localhost" });\n`;
    const bad = [];
    for (const [kind, type, file] of [
      ["CommonJS", "commonjs", "index.ts"],
      ["ESM", "module", "index.ts"],
    ]) {
      const proj = join(dir, kind);
      mkdirSync(proj);
      writeFileSync(join(proj, "package.json"), JSON.stringify({ type }));
      writeFileSync(join(proj, file), use);
      const r = spawnSync(
        process.execPath,
        [
          tsc,
          "--noEmit",
          "--strict",
          "--skipLibCheck",
          "--module",
          "node16",
          "--target",
          "es2022",
          join(proj, file),
        ],
        { encoding: "utf8", cwd: proj },
      );
      if (r.status !== 0) bad.push(`${kind} consumer:\n${r.stdout}${r.stderr}`);
    }
    if (bad.length) throw new Error(bad.join("\n"));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
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
  `dist smoke ok on node ${process.version}: ${expected.length} exports across esm, cjs, iife; exports map, type declarations (CommonJS and ESM consumers), bin and package.json fields ok`,
);

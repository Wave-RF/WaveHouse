import { defineConfig } from "tsup";

// Three build targets from one source. `dist/` is cleaned by the `build` npm
// script (not tsup's `clean`) so these configs can't race each other wiping
// one another's output:
//   1. ESM + CJS — what `npm install @wavehouse/sdk` resolves for bundlers
//      (Vite/webpack/Rollup/esbuild → React, Vue, Svelte, Astro, Angular) and
//      Node. Ships the type declarations.
//   2. IIFE global — `dist/index.global.js`, a self-contained minified bundle
//      that defines `window.WaveHouse` for a plain `<script src>` tag, i.e. a
//      no-build / FTP-deployed site. Served by CDNs via the `unpkg`/`jsdelivr`
//      package.json fields.
//   3. codegen CLI — `dist/cli/codegen.js`, shipped as the `wavehouse-codegen`
//      bin (Node only; keeps its `#!/usr/bin/env node` shebang).
export default defineConfig([
  {
    entry: { index: "src/index.ts" },
    format: ["esm", "cjs"],
    dts: true,
    sourcemap: true,
    splitting: false,
    target: "es2022",
  },
  {
    entry: { index: "src/index.ts" },
    format: ["iife"],
    globalName: "WaveHouse",
    platform: "browser",
    minify: true,
    sourcemap: true,
    splitting: false,
    target: "es2022",
  },
  {
    entry: { "cli/codegen": "src/cli/codegen.ts" },
    format: ["esm"],
    sourcemap: false,
    splitting: false,
    target: "es2022",
  },
]);

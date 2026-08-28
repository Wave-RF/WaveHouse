import path from "node:path";
import { defineConfig } from "vitest/config";

// TS_E2E_COVERAGE_DIR is set by the root Makefile (`make test-e2e`) so the
// e2e suite's SDK coverage lands at tmp/coverage/ts-e2e/, ready for
// `cov ts-merge` (run via `make cov`) to combine with ts-unit.
// Standalone runs (no env var) fall back to <thisdir>/coverage.
const reportsDirectory =
  process.env.TS_E2E_COVERAGE_DIR ?? path.join(import.meta.dirname, "coverage");

// Root at the REPO ROOT, not this config's dir. The e2e tests live here
// (tests/e2e/sdk) but they exercise SDK source at clients/ts/src, which is
// OUTSIDE this dir. Vitest's v8 coverage only reports files located under
// `root`; with the default root (tests/e2e/sdk) every executed SDK file is
// silently dropped and coverage comes back empty (0/0). Rooting at the repo
// root puts clients/ts/src under root, so the SDK is instrumented and the
// resulting coverage-final.json keys are absolute paths identical to
// ts-unit's — exactly what `cov ts-merge` (nyc merge) needs to combine the
// two suites into ts-total.
const repoRoot = path.resolve(import.meta.dirname, "../../..");
const sdkSrc = path.join(repoRoot, "clients/ts/src");
const here = path.relative(repoRoot, import.meta.dirname); // "tests/e2e/sdk"

// Under COV_DEFER (set by `make ci`/`test-all`), drop the console reporter —
// `make cov` (scripts/cov report) prints ONE consolidated table at the end.
// Standalone, show a 4-line text-summary (per-file detail lives in HTML).
const quietConsole = !!process.env.COV_DEFER;

export default defineConfig({
  root: repoRoot,
  resolve: {
    alias: {
      // Resolve @wavehouse/sdk to the source so coverage instruments the SDK
      // directly (not the prebuilt dist/).
      "@wavehouse/sdk": path.join(sdkSrc, "index.ts"),
    },
  },
  test: {
    globals: true,
    environment: "node",
    // Root-relative now that root is the repo root. Scope the glob tightly
    // to this dir so the SDK's own *.test.ts unit files under clients/ts/src
    // are NOT pulled into the e2e run.
    include: [`${here}/*.test.ts`],
    // Absolute (via import.meta.dirname) so it resolves regardless of root/cwd.
    // No setupFiles: streaming runs on the SDK's fetch transport, so there is
    // no longer an EventSource global to polyfill.
    globalSetup: path.join(import.meta.dirname, "setup.ts"),
    testTimeout: 30_000,
    hookTimeout: 120_000,
    pool: "forks",
    // Run test files sequentially (one fork, shared module state). Tables are
    // now isolated per file (see tables.ts), so data no longer contaminates
    // across files — but parallel files are still blocked by the shared
    // mutable settings directory: there is one policies.json / roles.json /
    // pipes.json per run, several files read-modify-write the policy
    // (settings.ts) and reload it, and streaming.test.ts flips the global
    // default_role, so concurrent files would race those writes. Dropping
    // maxWorkers:1 is deferred to follow-up issue #214 (per-table policy
    // storage); see the "Deferred" section of
    // docs/src/content/docs/ingest-pipeline.md.
    // (Vitest 4 replaced poolOptions.forks.singleFork with these two
    // top-level keys — see https://vitest.dev/guide/migration#pool-rework)
    maxWorkers: 1,
    isolate: false,
    coverage: {
      // Coverage is only collected when --coverage is passed (the
      // orchestrator always does). The config block is always present so
      // it's wired correctly when enabled.
      provider: "v8",
      reportsDirectory,
      // html → browsable report; json-summary → pct read by `cov report`;
      // json → coverage-final.json that `cov ts-merge`/`report` combines
      // with ts-unit into the merged ts-total report.
      reporter: quietConsole
        ? ["html", "json-summary", "json"]
        : ["text-summary", "html", "json-summary", "json"],
      // Cover SDK source files (root-relative), not the e2e test files.
      include: ["clients/ts/src/**/*.ts"],
      exclude: ["**/*.test.ts"],
    },
  },
});

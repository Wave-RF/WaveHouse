import path from "node:path";
import { defineConfig } from "vitest/config";

// TS_E2E_COVERAGE_DIR is set by the root Makefile (`make ts-cov-e2e` and
// `make ts-cov`) so the e2e suite's SDK coverage lands at
// tmp/coverage/ts-e2e/, ready for `cov ts-merge` to combine with ts-unit.
// Standalone runs (no env var) fall back to ./coverage.
const reportsDirectory = process.env.TS_E2E_COVERAGE_DIR ?? "./coverage";

// Resolve @wavehouse/sdk to the source so coverage instruments the SDK
// directly (not the prebuilt dist/). The source path is relative to
// repo root, which is what coverage.include / cov ts-merge expects.
const sdkSrc = path.resolve(__dirname, "../../../clients/ts/src");

export default defineConfig({
  resolve: {
    alias: {
      "@wavehouse/sdk": path.join(sdkSrc, "index.ts"),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["./*.test.ts"],
    setupFiles: ["./polyfills.ts"],
    globalSetup: "./setup.ts",
    testTimeout: 30_000,
    hookTimeout: 120_000,
    pool: "forks",
    // Run test files sequentially to avoid port/stream conflicts.
    // (Vitest 4 replaced poolOptions.forks.singleFork with these two
    // top-level keys — see https://vitest.dev/guide/migration#pool-rework)
    maxWorkers: 1,
    isolate: false,
    coverage: {
      // Coverage is only collected when --coverage is passed (the
      // orchestrator does this when E2E_COVERAGE=1). The config block
      // is always present so it's wired correctly when enabled.
      provider: "v8",
      reportsDirectory,
      // text → console summary; html → browsable report; json-summary →
      // pct read by `cov merge` for the side-by-side breakdown; json →
      // coverage-final.json that `cov ts-merge` combines with ts-unit
      // to produce the merged ts-total report.
      reporter: ["text", "html", "json-summary", "json"],
      // Cover SDK source files, not the e2e test files themselves.
      include: [path.relative(__dirname, sdkSrc) + "/**/*.ts"],
      exclude: ["**/*.test.ts"],
    },
  },
});

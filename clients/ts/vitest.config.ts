import { defineConfig } from "vitest/config";

// TS_UNIT_COVERAGE_DIR is set by the root Makefile so the SDK's coverage
// reports land at tmp/coverage/ts-unit/ alongside the Go suites' covdata.
// Manual `pnpm test:coverage` (no env var) falls back to ./coverage, which
// keeps the SDK package self-contained for one-off local coverage.
const reportsDirectory = process.env.TS_UNIT_COVERAGE_DIR ?? "./coverage";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.test.ts"],
    coverage: {
      provider: "v8",
      reportsDirectory,
      // text → console summary; html → browsable report; json-summary →
      // machine-readable totals; json → coverage-final.json consumed by
      // `cov ts-merge` to merge SDK unit + e2e coverage into ts-total.
      reporter: ["text", "html", "json-summary", "json"],
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts"],
    },
  },
});

import { defineConfig } from "vitest/config";

// TS_UNIT_COVERAGE_DIR is set by the root Makefile so the SDK's coverage
// reports land at tmp/coverage/ts-unit/ alongside the Go suites' covdata.
// Manual `pnpm test:coverage` (no env var) falls back to ./coverage, which
// keeps the SDK package self-contained for one-off local coverage.
const reportsDirectory = process.env.TS_UNIT_COVERAGE_DIR ?? "./coverage";

// Under COV_DEFER (set by `make ci`/`test-all`), drop the console reporter
// entirely — `make cov` (scripts/cov report) prints ONE consolidated table at
// the end, so a per-file dump here would just be upstream noise. Standalone,
// show a 4-line text-summary (full per-file detail lives in the HTML report).
const quietConsole = !!process.env.COV_DEFER;

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.test.ts"],
    coverage: {
      provider: "v8",
      reportsDirectory,
      // html → browsable report; json-summary → machine-readable totals
      // (pct read by `cov report`); json → coverage-final.json consumed by
      // `cov ts-merge`/`report` to merge SDK unit + e2e into ts-total.
      reporter: quietConsole
        ? ["html", "json-summary", "json"]
        : ["text-summary", "html", "json-summary", "json"],
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts"],
    },
  },
});

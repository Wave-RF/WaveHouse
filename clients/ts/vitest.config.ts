import { defineConfig } from "vitest/config";

// SDK_COVERAGE_DIR is set by scripts/test-suite.sh so the SDK's coverage
// reports land at tmp/coverage/sdk/ alongside the Go suites' covdata. Manual
// `pnpm test:coverage` runs without the env var fall back to ./coverage,
// which keeps the SDK package self-contained for one-off local coverage.
const reportsDirectory = process.env.SDK_COVERAGE_DIR ?? "./coverage";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.test.ts"],
    coverage: {
      provider: "v8",
      reportsDirectory,
      // text → console summary; html → browsable report; json-summary →
      // machine-readable totals consumed by scripts/coverage.sh.
      reporter: ["text", "html", "json-summary"],
      include: ["src/**/*.ts"],
      exclude: [
        "src/**/*.test.ts",
      ],
    },
  },
});

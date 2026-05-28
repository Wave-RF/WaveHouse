import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      // Resolve @wavehouse/sdk directly to source — no build step needed
      "@wavehouse/sdk": path.resolve(
        __dirname,
        "../../../clients/ts/src/index.ts",
      ),
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
  },
});

import path from "node:path";
import { defineConfig } from "vitest/config";

// Roots at the repo root so `include` stays a repo-relative glob. The suite
// imports the PUBLISHED @wavehouse/sdk pinned in package.json (bumped
// deliberately), so there is no SDK source to alias or instrument.
const repoRoot = path.resolve(import.meta.dirname, "../../..");
const here = path.relative(repoRoot, import.meta.dirname); // "tests/e2e/sdk"

export default defineConfig({
  root: repoRoot,
  test: {
    globals: true,
    environment: "node",
    // Root-relative now that root is the repo root. Scope the glob tightly
    // to this dir.
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
  },
});

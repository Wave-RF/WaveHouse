/**
 * Settings-directory helpers for the E2E suite.
 *
 * Files are the only write path for policy and pipes: the server reads
 * `policies.json`, `roles.json`, and `pipes.json` from its settings directory
 * and re-adopts them on file change, SIGHUP, or POST /v1/ops/settings/reload.
 * The orchestrator copies tests/e2e/fixtures/settings to a per-run scratch
 * directory and exports it as WAVEHOUSE_SETTINGS_DIR; these helpers write the
 * files there and trigger the reload endpoint, so a test's read-modify-write
 * (`policy.get()` → mutate → `setPolicy()`) lands synchronously.
 *
 * `roles.json` is derived, never hand-written: every role a policy or pipe
 * references must be declared or validation rejects the directory, so each
 * write recomputes the registry from the policy and pipes on disk.
 */

import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { Pipe, Policy } from "@wavehouse/sdk";
import { adminClient } from "./helpers.js";

function settingsDir(): string {
  const dir = process.env.WAVEHOUSE_SETTINGS_DIR;
  if (!dir) {
    throw new Error(
      "WAVEHOUSE_SETTINGS_DIR is not set. Use the orchestrator (`make test-e2e`), " +
        "which exports the per-run settings directory the server reads.",
    );
  }
  return dir;
}

function readJSON<T>(name: string): T {
  return JSON.parse(readFileSync(join(settingsDir(), name), "utf8")) as T;
}

function writeJSON(name: string, doc: unknown): void {
  writeFileSync(join(settingsDir(), name), `${JSON.stringify(doc, null, 2)}\n`);
}

/** The policy currently on disk (`policies.json`). */
export function readPolicyFile(): Policy {
  return readJSON<Policy>("policies.json");
}

/** The pipes currently on disk (`pipes.json`). */
export function readPipesFile(): Pipe[] {
  return readJSON<{ pipes?: Pipe[] }>("pipes.json").pipes ?? [];
}

/** Every role name a policy or pipe list references, for `roles.json`. */
export function referencedRoles(policy: Policy, pipes: Pipe[]): string[] {
  const roles = new Set<string>();
  if (policy.default_role) roles.add(policy.default_role);
  if (policy.admin_role) roles.add(policy.admin_role);
  for (const table of Object.values(policy.tables ?? {})) {
    for (const role of Object.keys(table.select ?? {})) roles.add(role);
    for (const role of Object.keys(table.insert ?? {})) roles.add(role);
  }
  for (const pipe of pipes) {
    for (const role of pipe.allowed_roles ?? []) roles.add(role);
  }
  return [...roles].sort();
}

/**
 * Trigger POST /v1/ops/settings/reload and require adoption. A rejected
 * directory (422) throws with every finding so the failing file and path are
 * in the test output, not just "adopted: false".
 */
export async function reloadSettings(): Promise<void> {
  const result = await adminClient().settings.reload();
  if (result.error) {
    const details = result.error.details as { findings?: unknown[] } | undefined;
    const findings = details?.findings ?? [];
    throw new Error(
      `settings reload rejected (${result.error.status} ${result.error.message}): ` +
        JSON.stringify(findings, null, 2),
    );
  }
  if (!result.data?.adopted) {
    throw new Error(`settings reload not adopted: ${JSON.stringify(result.data, null, 2)}`);
  }
}

/**
 * Replace the whole policy (`policies.json`), regenerate `roles.json` from it
 * and the pipes on disk, and reload. Accepts the full document so callers keep
 * the read-modify-write pattern: `policy.get()` → mutate → `setPolicy()`.
 */
export async function setPolicy(policy: Policy): Promise<void> {
  writeJSON("roles.json", { roles: referencedRoles(policy, readPipesFile()) });
  writeJSON("policies.json", policy);
  await reloadSettings();
}

/**
 * Replace every pipe (`pipes.json`), regenerate `roles.json` from them and the
 * policy on disk, and reload.
 */
export async function setPipes(pipes: Pipe[]): Promise<void> {
  writeJSON("roles.json", { roles: referencedRoles(readPolicyFile(), pipes) });
  writeJSON("pipes.json", { pipes });
  await reloadSettings();
}

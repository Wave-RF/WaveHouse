/**
 * Per-suite table isolation for the E2E suite (issue #191, AC "e2e test
 * isolation").
 *
 * Each test FILE ("suite") gets its own physical ClickHouse tables —
 * `clicks_<suite>`, `events_<suite>`, `users_<suite>` — so no two files ever
 * read or write the same table. Cross-file data contamination (one file's rows
 * perturbing another's COUNT assertions, or tripping a shared table's batch
 * size trigger) becomes structurally impossible, on top of the per-table
 * batching fix in `internal/ingest/worker.go`.
 *
 * `setup.ts` (globalSetup) creates every suite's tables, refreshes WaveHouse's
 * schema until they all appear, and bootstraps a permissive policy covering
 * them. Each test file calls `suiteTables("<name>")` to get its own names.
 *
 * NOTE: test files still run SEQUENTIALLY (vitest `maxWorkers: 1`). Table
 * isolation is the prerequisite for running files in parallel, but parallelism
 * is *also* blocked by shared global policy state — several files do
 * read-modify-write on the single policy document, and streaming.test.ts flips
 * the global `default_role`. Concurrent files would race those writes. Dropping
 * `maxWorkers: 1` is therefore deferred to follow-up issue #214 (per-table
 * policy storage); see the comment in `vitest.config.ts` and the "Deferred"
 * section of `docs/src/content/docs/ingest-pipeline.md`.
 *
 * Adding a new `*.test.ts` file: add its suite name to `SUITES` below (by
 * convention, the suite name is the file stem).
 */

/** The base table "kinds". Each suite gets one physical copy of each. */
export type TableKind = "clicks" | "events" | "users";

export const KINDS: readonly TableKind[] = ["clicks", "events", "users"] as const;

/**
 * Column definitions per kind, templated on the physical table name so each
 * suite gets an independent copy. This is the single source of truth for the
 * e2e table schemas (it replaces the former tests/e2e/fixtures/00*_*.sql).
 */
export const TABLE_DDL: Record<TableKind, (name: string) => string> = {
  clicks: (name) => `CREATE TABLE IF NOT EXISTS default.\`${name}\` (
    event_id     String,
    page         String,
    user_id      String,
    session_id   String,
    referrer     String DEFAULT '',
    country      String DEFAULT 'US',
    duration_ms  UInt32 DEFAULT 0,
    received_timestamp DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (page, received_timestamp)`,
  events: (name) => `CREATE TABLE IF NOT EXISTS default.\`${name}\` (
    event_id     String,
    type         String,
    user_id      String,
    payload      String DEFAULT '{}',
    source       String DEFAULT 'web',
    received_timestamp DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (type, received_timestamp)`,
  users: (name) => `CREATE TABLE IF NOT EXISTS default.\`${name}\` (
    user_id      String,
    name         String,
    email        String,
    plan         String DEFAULT 'free',
    created_at   DateTime64(3) DEFAULT now64(3),
    received_timestamp DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (user_id)`,
};

/** One entry per `*.test.ts` file. Suite name == file stem by convention. */
export const SUITES = [
  "batching",
  "dlq",
  "streaming",
  "ingest",
  "query",
  "cache",
  "admin",
  "stress",
  "auth",
] as const;

export type Suite = (typeof SUITES)[number];
export type SuiteTables = Record<TableKind, string>;

/** Physical table name for a (suite, kind) pair, e.g. `clicks_batching`. */
export function tableName(suite: Suite, kind: TableKind): string {
  return `${kind}_${suite}`;
}

/** The set of tables owned by one suite. Call this at the top of a test file. */
export function suiteTables(suite: Suite): SuiteTables {
  return {
    clicks: tableName(suite, "clicks"),
    events: tableName(suite, "events"),
    users: tableName(suite, "users"),
  };
}

/**
 * Every physical table across all suites, with its kind — used by setup.ts to
 * create the tables, wait for schema discovery, and bootstrap the policy.
 */
export function allTableSpecs(): Array<{ name: string; kind: TableKind }> {
  return SUITES.flatMap((suite) => KINDS.map((kind) => ({ name: tableName(suite, kind), kind })));
}

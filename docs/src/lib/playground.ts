// Shared runtime for the docs query playground — the single source of truth
// behind both the landing "Query it like a database" tabs (HomeQueryDemo.astro)
// and the full /playground builder (QueryPlayground.astro). It owns:
//   - the curated public schema of the demo's `gh_events` table,
//   - a browser @wavehouse/sdk client bound to the public read-only role,
//   - a structured QuerySpec ⇄ SDK-chain / raw-AST / live-results bridge.
//
// Everything here runs in the visitor's browser against stats.wavehouse.dev
// (our own GitHub-activity dogfood, Wave-RF/WaveHouse-Stats) — the same public
// endpoint LiveDemo.astro reads. No eval: the builder UIs drive this typed
// spec, so the "code box" is generated, copy-pasteable SDK, never executed text.

import type { QueryBuilder, Result, StreamSubscriber, WaveHouseClient } from "@wavehouse/sdk";
import { createClient } from "@wavehouse/sdk";

/** Every demo row is an untyped record (the demo has no generated Database type). */
type Row = Record<string, unknown>;

/** Build-time overridable so a fork / staging docs build can point elsewhere. */
export const STATS_BASE_URL: string =
  import.meta.env.PUBLIC_WAVEHOUSE_STATS_URL || "https://stats.wavehouse.dev";

/** The one table the public demo role exposes. */
export const STATS_TABLE = "gh_events";

/** The timestamp column every time-range filter targets. */
export const TIME_COLUMN = "event_ts";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

export type ColumnKind = "string" | "number" | "datetime";

export interface ColumnDef {
  name: string;
  kind: ColumnKind;
  desc: string;
}

// The public read-only role can't read /v1/schema (it's an admin surface), so
// the column set is curated here from the role's allowed projection — the same
// fields the gh_events ingest contract exposes publicly. `actor_id` and
// `received_timestamp` are deliberately omitted: the demo role rejects them.
// Keep in sync with the Wave-RF/WaveHouse-Stats producer.
export const COLUMNS: readonly ColumnDef[] = [
  { name: "event_type", kind: "string", desc: "GitHub event family (push, pull_request, star…)" },
  { name: "action", kind: "string", desc: "Event sub-action (opened, closed, created…)" },
  { name: "actor_login", kind: "string", desc: "GitHub username that triggered the event" },
  { name: "event_ts", kind: "datetime", desc: "When the event happened (UTC)" },
  { name: "number", kind: "number", desc: "Issue / PR number (0 when not applicable)" },
  { name: "title", kind: "string", desc: "Issue / PR / release title" },
  { name: "ref", kind: "string", desc: "Git ref for push / create / delete" },
  { name: "repo_name", kind: "string", desc: "owner/repo the event belongs to" },
];

export const COLUMN_NAMES: readonly string[] = COLUMNS.map((c) => c.name);

export function columnKind(name: string): ColumnKind {
  return COLUMNS.find((c) => c.name === name)?.kind ?? "string";
}

/** Human-meaningful event types — a curated subset (the table also carries CI noise). */
export const EVENT_TYPE_SUGGESTIONS: readonly string[] = [
  "push",
  "pull_request",
  "issues",
  "issue_comment",
  "pull_request_review",
  "star",
  "fork",
  "release",
  "create",
  "delete",
];

// ---------------------------------------------------------------------------
// Operators, aggregations, time ranges
// ---------------------------------------------------------------------------

export type FilterOp = "=" | "!=" | ">" | ">=" | "<" | "<=" | "in" | "like" | "not_like";

export const FILTER_OPS: readonly { op: FilterOp; label: string }[] = [
  { op: "=", label: "= equals" },
  { op: "!=", label: "≠ not equals" },
  { op: ">", label: "> greater" },
  { op: ">=", label: "≥ at least" },
  { op: "<", label: "< less" },
  { op: "<=", label: "≤ at most" },
  { op: "in", label: "in (a, b, …)" },
  { op: "like", label: "like %pattern%" },
  { op: "not_like", label: "not like %pattern%" },
];

const OP_SET = new Set<string>(FILTER_OPS.map((o) => o.op));

// SDK operator → wire operator. Mirrors OP_MAP in clients/ts/src/query-builder.ts
// (used only to render the raw-AST preview without a network round-trip; the
// actual request is built by the SDK in applySpec).
const WIRE_OP: Record<FilterOp, string> = {
  "=": "eq",
  "!=": "neq",
  ">": "gt",
  ">=": "gte",
  "<": "lt",
  "<=": "lte",
  in: "in",
  like: "like",
  not_like: "not_like",
};

export const AGG_FNS = ["count", "countDistinct", "sum", "avg", "min", "max"] as const;
export type AggFn = (typeof AGG_FNS)[number];

export const TIME_RANGES: readonly { label: string; value: string }[] = [
  { label: "Last hour", value: "1h" },
  { label: "Last 24 hours", value: "24h" },
  { label: "Last 7 days", value: "168h" },
  { label: "Last 30 days", value: "720h" },
  { label: "All time", value: "" },
];

// ---------------------------------------------------------------------------
// QuerySpec — the structured query the builders produce
// ---------------------------------------------------------------------------

export type QueryMode = "rows" | "aggregate";

export interface FilterSpec {
  column: string;
  op: FilterOp;
  value: string;
}

export interface AggSpec {
  fn: AggFn;
  column: string;
  alias: string;
}

export interface OrderSpec {
  column: string;
  dir: "asc" | "desc";
}

export interface QuerySpec {
  mode: QueryMode;
  /** Projected columns in "rows" mode; empty = select every allowed column. */
  columns: string[];
  filters: FilterSpec[];
  /** Group-by keys + aggregations, used only in "aggregate" mode. */
  groupBy: string[];
  aggregations: AggSpec[];
  orderBy: OrderSpec[];
  limit: number;
  /** time_range.since on event_ts ("" = all time). */
  since: string;
}

/** The demo role caps rows at 5000; keep the default tight and the ceiling honest. */
export const MAX_LIMIT = 1000;

export function emptySpec(): QuerySpec {
  return {
    mode: "rows",
    columns: [],
    filters: [],
    groupBy: [],
    aggregations: [],
    orderBy: [],
    limit: 50,
    since: "168h",
  };
}

/** A friendly, runnable starting point: recent human activity, newest first. */
export function exampleSpec(): QuerySpec {
  return {
    mode: "rows",
    columns: ["event_type", "actor_login", "number", "title", "event_ts"],
    filters: [{ column: "event_type", op: "in", value: "pull_request, issues, star, release" }],
    groupBy: [],
    aggregations: [],
    orderBy: [{ column: "event_ts", dir: "desc" }],
    limit: 25,
    since: "720h",
  };
}

// ---------------------------------------------------------------------------
// Value coercion
// ---------------------------------------------------------------------------

/**
 * Coerce a raw text field into the JSON value the backend expects: a number for
 * the numeric column, an array for `in`, a string otherwise. A non-numeric
 * value typed against `number` falls through as a string so the SDK surfaces a
 * clean server error rather than silently sending NaN.
 */
export function coerceValue(column: string, op: FilterOp, raw: string): unknown {
  const numeric = columnKind(column) === "number";
  if (op === "in") {
    const parts = raw
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    return numeric ? parts.map((p) => coerceNumber(p)) : parts;
  }
  return numeric ? coerceNumber(raw) : raw;
}

function coerceNumber(raw: string): number | string {
  const n = Number(raw);
  return raw.trim() !== "" && Number.isFinite(n) ? n : raw;
}

// ---------------------------------------------------------------------------
// Spec → SDK builder (the one that actually runs) + code/AST previews
// ---------------------------------------------------------------------------

export function createStatsClient(): WaveHouseClient {
  return createClient({ baseURL: STATS_BASE_URL });
}

/** Apply a spec to a fresh QueryBuilder — the single path both Run and live use. */
export function applySpec(wh: WaveHouseClient, spec: QuerySpec): QueryBuilder<Row> {
  const projected = spec.mode === "aggregate" ? spec.groupBy : spec.columns;
  let q = wh.from(STATS_TABLE).select(...projected);
  if (spec.mode === "aggregate") {
    for (const a of spec.aggregations) {
      q =
        a.fn === "count" ? q.count(a.column || "*", a.alias) : q.aggregate(a.fn, a.column, a.alias);
    }
    if (spec.groupBy.length > 0) q = q.groupBy(...spec.groupBy);
  }
  for (const f of spec.filters) {
    if (!f.column) continue;
    q = q.where(f.column, f.op, coerceValue(f.column, f.op, f.value));
  }
  for (const o of spec.orderBy) q = q.orderBy(o.column, o.dir);
  if (spec.since) q = q.timeRange(TIME_COLUMN, spec.since);
  if (spec.limit) q = q.limit(spec.limit);
  return q;
}

/**
 * Run a spec and return its rows (the Result error arm is surfaced, not thrown).
 * Queries are user-authored and ad-hoc (not pre-defined pipes) against a shared
 * public endpoint, so the caller passes an AbortSignal to bound a slow run
 * client-side.
 */
export async function runSpec(
  wh: WaveHouseClient,
  spec: QuerySpec,
  signal?: AbortSignal,
): Promise<Result<Row[]>> {
  return applySpec(wh, spec).fetch({ signal });
}

/** Default client-side timeout (ms) for a playground run on the shared demo. */
export const RUN_TIMEOUT_MS = 12_000;

/** Subscribe to live events matching a spec's filters (column projection + filters applied client-side). */
export function streamSpec(
  wh: WaveHouseClient,
  spec: QuerySpec,
  subscriber: StreamSubscriber<Row>,
): () => void {
  const projected = spec.mode === "aggregate" ? spec.groupBy : spec.columns;
  let q = wh.from(STATS_TABLE).select(...projected);
  for (const f of spec.filters) {
    if (f.column) q = q.where(f.column, f.op, coerceValue(f.column, f.op, f.value));
  }
  const controller = q.stream();
  const unsub = controller.subscribe(subscriber);
  return () => {
    unsub();
    controller.close();
  };
}

function quote(s: string): string {
  return `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function literal(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(literal).join(", ")}]`;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return quote(String(v));
}

function aggCall(a: AggSpec): string {
  if (a.fn === "count") return `.count(${quote(a.column || "*")}, ${quote(a.alias)})`;
  if (a.fn === "countDistinct") return `.countDistinct(${quote(a.column)}, ${quote(a.alias)})`;
  return `.${a.fn}(${quote(a.column)}, ${quote(a.alias)})`;
}

/** Render the spec as a copy-pasteable @wavehouse/sdk chain. */
export function specToCode(spec: QuerySpec): string {
  const chain: string[] = [`.from(${quote(STATS_TABLE)})`];
  if (spec.mode === "aggregate") {
    if (spec.groupBy.length > 0) chain.push(`.select(${spec.groupBy.map(quote).join(", ")})`);
    for (const a of spec.aggregations) chain.push(aggCall(a));
    if (spec.groupBy.length > 0) chain.push(`.groupBy(${spec.groupBy.map(quote).join(", ")})`);
  } else if (spec.columns.length > 0) {
    chain.push(`.select(${spec.columns.map(quote).join(", ")})`);
  } else {
    chain.push(".selectAll()");
  }
  for (const f of spec.filters) {
    if (!f.column) continue;
    chain.push(
      `.where(${quote(f.column)}, ${quote(f.op)}, ${literal(coerceValue(f.column, f.op, f.value))})`,
    );
  }
  for (const o of spec.orderBy) chain.push(`.orderBy(${quote(o.column)}, ${quote(o.dir)})`);
  if (spec.since) chain.push(`.timeRange(${quote(TIME_COLUMN)}, ${quote(spec.since)})`);
  if (spec.limit) chain.push(`.limit(${spec.limit})`);

  return [
    `import { createClient } from "@wavehouse/sdk";`,
    "",
    `const wh = createClient({ baseURL: ${quote(STATS_BASE_URL)} });`,
    "",
    "const { data, error } = await wh",
    ...chain.map((c) => `  ${c}`),
    "  ;",
  ].join("\n");
}

/**
 * Render the raw JSON AST the SDK posts to /v1/query. Mirrors the projection
 * rules in QueryBuilder._buildAST (clients/ts/src/query-builder.ts) so the
 * preview matches the wire body without issuing a request.
 */
export function specToAst(spec: QuerySpec): Record<string, unknown> {
  const ast: Record<string, unknown> = {};
  const hasAggs = spec.mode === "aggregate" && spec.aggregations.length > 0;
  const projected = spec.mode === "aggregate" ? spec.groupBy : spec.columns;
  if (projected.length > 0) ast.columns = [...projected];
  else if (!hasAggs) ast.select_all = true;
  if (hasAggs) {
    ast.aggregations = spec.aggregations.map((a) => ({
      fn: a.fn,
      column: a.fn === "count" ? a.column || "*" : a.column,
      alias: a.alias,
    }));
  }
  const filters = spec.filters
    .filter((f) => f.column)
    .map((f) => ({
      column: f.column,
      op: WIRE_OP[f.op],
      value: coerceValue(f.column, f.op, f.value),
    }));
  if (filters.length > 0) ast.filters = filters;
  // group_by tracks applySpec/_buildAST (group-by keys present in aggregate
  // mode regardless of whether aggregations were added yet), not hasAggs —
  // otherwise the AST preview would drop group_by while the SDK-code preview
  // and the real request still carry it.
  if (spec.mode === "aggregate" && spec.groupBy.length > 0) ast.group_by = [...spec.groupBy];
  if (spec.orderBy.length > 0)
    ast.order_by = spec.orderBy.map((o) => ({ column: o.column, dir: o.dir }));
  ast.limit = spec.limit || MAX_LIMIT;
  if (spec.since) ast.time_range = { column: TIME_COLUMN, since: spec.since };
  return ast;
}

// ---------------------------------------------------------------------------
// Results rendering
// ---------------------------------------------------------------------------

function cellText(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** Build a results <table> (or an empty-state <p>) from query rows. */
export function buildResultsTable(rows: Row[]): HTMLElement {
  if (rows.length === 0) {
    const p = document.createElement("p");
    p.className = "wh-pg__empty";
    p.textContent = "No rows matched. Loosen the filters or widen the time range.";
    return p;
  }
  // Column order: first row's keys, then any keys later rows introduce.
  const cols: string[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    for (const k of Object.keys(row)) {
      if (!seen.has(k)) {
        seen.add(k);
        cols.push(k);
      }
    }
  }

  const table = document.createElement("table");
  table.className = "wh-pg__table";

  const thead = table.createTHead();
  const headRow = thead.insertRow();
  for (const c of cols) {
    const th = document.createElement("th");
    th.textContent = c;
    if (columnKind(c) === "number") th.classList.add("wh-pg__td--num");
    headRow.appendChild(th);
  }

  const tbody = table.createTBody();
  for (const row of rows) {
    const tr = tbody.insertRow();
    for (const c of cols) {
      const td = tr.insertCell();
      const text = cellText(row[c]);
      td.textContent = text;
      td.title = text;
      if (typeof row[c] === "number") td.classList.add("wh-pg__td--num");
    }
  }
  return table;
}

// ---------------------------------------------------------------------------
// URL state (structured, never code — safe to share)
// ---------------------------------------------------------------------------

/** Serialize a spec to a single compact `q` URL parameter. */
export function encodeSpec(spec: QuerySpec): string {
  return encodeURIComponent(JSON.stringify(spec));
}

/**
 * Parse a `q` parameter back into a spec, hard-validating every field against
 * the known column/op/agg sets. Anything unrecognized is dropped — the result
 * only ever drives the typed SDK builder, never eval, but staying strict keeps
 * a hand-edited URL from producing a confusing builder state.
 */
export function decodeSpec(raw: string | null): QuerySpec | null {
  if (!raw) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(decodeURIComponent(raw));
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null) return null;
  const p = parsed as Record<string, unknown>;
  const base = emptySpec();

  const isCol = (c: unknown): c is string => typeof c === "string" && COLUMN_NAMES.includes(c);
  const cols = (v: unknown): string[] => (Array.isArray(v) ? v.filter(isCol) : []);

  const spec: QuerySpec = {
    mode: p.mode === "aggregate" ? "aggregate" : "rows",
    columns: cols(p.columns),
    groupBy: cols(p.groupBy),
    filters: Array.isArray(p.filters)
      ? p.filters
          .filter(
            (f): f is FilterSpec =>
              typeof f === "object" &&
              f !== null &&
              isCol((f as FilterSpec).column) &&
              OP_SET.has((f as FilterSpec).op) &&
              typeof (f as FilterSpec).value === "string",
          )
          .map((f) => ({ column: f.column, op: f.op, value: f.value }))
      : [],
    aggregations: Array.isArray(p.aggregations)
      ? (p.aggregations as unknown[])
          .filter(
            (a): a is AggSpec =>
              typeof a === "object" &&
              a !== null &&
              (AGG_FNS as readonly string[]).includes((a as AggSpec).fn) &&
              typeof (a as AggSpec).alias === "string",
          )
          .map((a) => ({
            fn: a.fn,
            column: typeof a.column === "string" ? a.column : "",
            alias: a.alias,
          }))
      : [],
    orderBy: Array.isArray(p.orderBy)
      ? p.orderBy
          .filter(
            (o): o is OrderSpec =>
              typeof o === "object" && o !== null && isCol((o as OrderSpec).column),
          )
          .map((o) => ({ column: o.column, dir: o.dir === "desc" ? "desc" : "asc" }))
      : [],
    limit:
      typeof p.limit === "number" && Number.isFinite(p.limit)
        ? Math.min(MAX_LIMIT, Math.max(1, Math.floor(p.limit)))
        : base.limit,
    since:
      typeof p.since === "string" && TIME_RANGES.some((t) => t.value === p.since)
        ? p.since
        : base.since,
  };
  return spec;
}

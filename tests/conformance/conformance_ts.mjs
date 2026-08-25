#!/usr/bin/env node
/**
 * Cross-language wire-format conformance test for the TypeScript SDK.
 *
 * Replays the shared fixture (clients/go/testdata/wire_cases.json, owned by
 * the Go module and also replayed by clients/go/conformance_test.go) and
 * verifies the TS SDK produces the same HTTP request: method, path,
 * content-type, body.
 *
 * Run: node tests/conformance/conformance_ts.mjs
 * Exit 0 = all pass, exit 1 = failures.
 */

import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

let createClient;
try {
  ({ createClient } = await import(join(__dirname, "../../clients/ts/dist/index.js")));
} catch (err) {
  console.error(
    "Cannot load the TypeScript SDK build. Run the SDK build first (e.g. `pnpm --dir clients/ts build`).",
  );
  console.error(err.message);
  process.exit(1);
}

const cases = JSON.parse(
  readFileSync(join(__dirname, "../../clients/go/testdata/wire_cases.json"), "utf-8"),
);

let lastCapture = { method: "", path: "", contentType: "", body: "" };

// Canned responses, matching the real server's shapes (internal/api/*.go) so
// the SDK never errors on decode. Mirrors the Go harness's handler.
function cannedResponse(url, method, contentType) {
  if (url.startsWith("/v1/ops/dlq")) return { tables: {}, total: 0 };
  if (url.startsWith("/v1/ops/schema") && method === "GET") return [];
  if (url === "/v1/ops/policy/validate" && method === "POST") return { valid: true };
  if (url.startsWith("/v1/ops/policy") && method === "GET") return { tables: {} };
  if (url.startsWith("/v1/ops/pipes/") && method === "GET")
    return { name: "test", sql: "SELECT 1" };
  if (url === "/v1/ops/pipes" && method === "GET") return [];
  if (url.startsWith("/v1/ingest")) {
    return contentType === "application/x-ndjson"
      ? { total: 0, succeeded: 0, failed: 0, duplicates: 0 }
      : { ok: true };
  }
  if (url === "/v1/health") return { status: "ok" };
  return [];
}

const server = createServer((req, res) => {
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    lastCapture = {
      method: req.method ?? "",
      path: req.url ?? "",
      contentType: req.headers["content-type"] ?? "",
      body: Buffer.concat(chunks).toString("utf-8"),
    };
    res.setHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify(cannedResponse(lastCapture.path, lastCapture.method, lastCapture.contentType)),
    );
  });
});

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const { port } = server.address();
const baseURL = `http://127.0.0.1:${port}`;

// Fixture ops that map one-to-one onto a (column, alias) aggregation method.
// An empty arg falls through to the SDK's own default.
const AGGREGATIONS = new Set(["count", "sum", "avg", "min", "max", "countDistinct"]);

// applyQueryOps replays a fixture operation chain onto a query builder.
// Fixtures always put select first, so rebuilding on select is safe.
function applyQueryOps(wh, table, operations) {
  let q = wh.from(table).select();
  for (const op of operations) {
    if (AGGREGATIONS.has(op.method)) {
      q = q[op.method](op.args[0] || undefined, op.args[1] || undefined);
      continue;
    }
    switch (op.method) {
      case "select":
        q = wh.from(table).select(...op.args);
        break;
      case "selectAll":
        q = q.selectAll();
        break;
      case "where":
        q = q.where(op.args[0], op.args[1], op.args[2]);
        break;
      case "aggregate":
        q = q.aggregate(op.args[0], op.args[1], op.args[2]);
        break;
      case "groupBy":
        q = q.groupBy(...op.args);
        break;
      case "orderBy":
        q = q.orderBy(op.args[0], op.args[1] || "asc");
        break;
      case "limit":
        q = q.limit(op.args[0]);
        break;
      case "timeRange":
        q = q.timeRange(op.args[0], op.args[1], op.args[2] || undefined);
        break;
      case "cacheTTL":
        q = q.cacheTTL(op.args[0]);
        break;
    }
  }
  return q;
}

// insertArg returns an ingest case's payload. Hard failure on a malformed
// case, matching the Go harness.
function insertArg(tc) {
  const op = tc.operations?.[0];
  if (op?.method !== "insert" || !op.args?.length) {
    throw new Error(`${tc.name}: ingest case needs an insert operation with one arg`);
  }
  return op.args[0];
}

// One entry per fixture `endpoint` value. A case naming an endpoint that is
// missing here counts as skipped and fails the run — see below.
const ENDPOINTS = {
  query: (wh, tc) => applyQueryOps(wh, tc.table, tc.operations ?? []).fetch(),
  ingest: (wh, tc) => wh.from(tc.table).insert(insertArg(tc)),
  ingest_batch: (wh, tc) => wh.from(tc.table).insert(insertArg(tc)),
  pipe: (wh, tc) => wh.pipe(tc.pipe_name, tc.pipe_params ?? undefined).fetch(),
  sql: (wh, tc) => wh.sql(tc.sql),
  health: (wh) => wh.sys.health(),
  schema_list: (wh) => wh.schema.list(),
  schema_refresh: (wh) => wh.schema.refresh(),
  policy_get: (wh) => wh.policy.get(),
  policy_set: (wh, tc) => wh.policy.set(tc.policy_body),
  policy_validate: (wh, tc) => wh.policy.validate(tc.policy_body),
  dlq_list: (wh) => wh.dlq.list(),
  dlq_table: (wh, tc) => wh.dlq.table(tc.table),
  pipes_list: (wh) => wh.pipes.list(),
  pipes_get: (wh, tc) => wh.pipes.get(tc.pipe_name),
  pipes_set: (wh, tc) => wh.pipes.set(tc.pipe_name, tc.pipe_def),
  pipes_delete: (wh, tc) => wh.pipes.delete(tc.pipe_name),
};

// Compare request URIs by meaning: same path, same decoded query values,
// regardless of + vs %20 spelling or parameter order (mirrors the Go harness).
function normalizePath(p) {
  let u;
  try {
    u = new URL(p, "http://conformance.invalid");
  } catch {
    return p;
  }
  u.searchParams.sort();
  return `${u.pathname}?${u.searchParams.toString()}`;
}

function deepEqual(a, b) {
  return JSON.stringify(sortKeys(a)) === JSON.stringify(sortKeys(b));
}

function sortKeys(v) {
  if (v === null || v === undefined) return v;
  if (Array.isArray(v)) return v.map(sortKeys);
  if (typeof v === "object") {
    const sorted = {};
    for (const k of Object.keys(v).sort()) {
      sorted[k] = sortKeys(v[k]);
    }
    return sorted;
  }
  return v;
}

let passed = 0;
let failed = 0;
const skippedNames = [];
const failures = [];

for (const tc of cases) {
  lastCapture = { method: "", path: "", contentType: "", body: "" };
  const wh = createClient({ baseURL, options: { maxRetries: 0 } });

  const run = ENDPOINTS[tc.endpoint];
  if (!run) {
    skippedNames.push(`${tc.name} (endpoint: ${tc.endpoint})`);
    continue;
  }

  try {
    await run(wh, tc);

    const errs = [];
    for (const [what, want, got] of [
      ["method", tc.expected_method, lastCapture.method],
      ["content-type", tc.expected_content_type, lastCapture.contentType],
    ]) {
      if (want && want !== got) errs.push(`${what}: want ${want}, got ${got}`);
    }
    if (tc.expected_path && normalizePath(lastCapture.path) !== normalizePath(tc.expected_path)) {
      errs.push(`path: want ${tc.expected_path}, got ${lastCapture.path}`);
    }

    if (tc.expected_raw_body !== undefined) {
      if (lastCapture.body !== tc.expected_raw_body) {
        errs.push(`raw body:\n  want: ${tc.expected_raw_body}\n  got:  ${lastCapture.body}`);
      }
    } else if (tc.expected_body !== undefined && tc.expected_body !== null) {
      let captured;
      try {
        captured = JSON.parse(lastCapture.body);
      } catch {
        errs.push(`body not valid JSON: ${lastCapture.body}`);
      }
      if (captured !== undefined && !deepEqual(captured, tc.expected_body)) {
        errs.push(
          `body mismatch:\n  want: ${JSON.stringify(tc.expected_body)}\n  got:  ${JSON.stringify(captured)}`,
        );
      }
    }

    if (errs.length > 0) {
      failed++;
      failures.push({ name: tc.name, errors: errs });
    } else {
      passed++;
    }
  } catch (err) {
    failed++;
    failures.push({ name: tc.name, errors: [`exception: ${err.message}`] });
  }
}

server.closeAllConnections?.();
server.close();

const skipped = skippedNames.length;
console.log(
  `\nWire-format conformance (TS SDK): ${passed} passed, ${failed} failed, ${skipped} skipped, ${cases.length} total\n`,
);

for (const name of skippedNames) {
  console.log(`  - skipped: ${name}`);
}

for (const f of failures) {
  console.log(`  ✗ ${f.name}`);
  for (const e of f.errors) {
    console.log(`    ${e}`);
  }
}

if (failed > 0 || skipped > 0 || passed === 0) {
  if (passed === 0) console.log("  ✗ nothing ran — every case skipped or the fixture is empty\n");
  if (skipped > 0)
    console.log("  ✗ skipped cases break cross-SDK parity — wire up the endpoint above\n");
  process.exit(1);
} else {
  console.log("  ✓ All cases passed\n");
  process.exit(0);
}

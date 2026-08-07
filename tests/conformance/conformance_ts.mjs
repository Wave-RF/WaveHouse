#!/usr/bin/env node
/**
 * Cross-language wire-format conformance test for the TypeScript SDK.
 *
 * Reads wire_cases.json (owned by the Go module, at clients/go/testdata/)
 * and verifies the TS SDK produces identical HTTP requests (method, path,
 * content-type, body) to the shared fixture.
 *
 * Run: node tests/conformance/conformance_ts.mjs
 * Exit 0 = all pass, exit 1 = failures.
 */

import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Import the built SDK.
let createClient;
try {
  ({ createClient } = await import(join(__dirname, "../../clients/ts/dist/index.js")));
} catch (err) {
  console.error("Cannot load the TypeScript SDK build. Run the SDK build first (e.g. `pnpm --dir clients/ts build`).");
  console.error(err.message);
  process.exit(1);
}

const cases = JSON.parse(readFileSync(join(__dirname, "../../clients/go/testdata/wire_cases.json"), "utf-8"));

let lastCapture = { method: "", path: "", contentType: "", body: "" };

function resetCapture() {
  lastCapture = { method: "", path: "", contentType: "", body: "" };
}

// Start echo server.
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
    if (req.url?.startsWith("/v1/dlq")) {
      res.end(JSON.stringify({ tables: {}, total: 0 }));
    } else if (req.url?.startsWith("/v1/schema") && req.method === "GET") {
      res.end(JSON.stringify([]));
    } else if (req.url === "/v1/admin/policy/validate" && req.method === "POST") {
      res.end(JSON.stringify({ valid: true }));
    } else if (req.url?.startsWith("/v1/admin/policy") && req.method === "GET") {
      res.end(JSON.stringify({ tables: {} }));
    } else if (req.url?.startsWith("/v1/admin/pipes/") && req.method === "GET") {
      res.end(JSON.stringify({ name: "test", sql: "SELECT 1" }));
    } else if (req.url === "/v1/admin/pipes" && req.method === "GET") {
      res.end(JSON.stringify([]));
    } else if (req.url?.startsWith("/v1/ingest")) {
      // Same shapes the real server returns (internal/api/ingest.go).
      if (lastCapture.contentType === "application/x-ndjson") {
        res.end(JSON.stringify({ total: 0, succeeded: 0, failed: 0, duplicates: 0 }));
      } else {
        res.end(JSON.stringify({ ok: true }));
      }
    } else if (req.url === "/v1/health") {
      // Real server shape (internal/api/health.go).
      res.end(JSON.stringify({ status: "ok" }));
    } else {
      res.end(JSON.stringify([]));
    }
  });
});

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const { port } = server.address();
const baseURL = `http://127.0.0.1:${port}`;

function applyQueryOps(wh, table, operations) {
  let q = wh.from(table).select();
  for (const op of operations) {
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
      case "count":
        q = q.count(op.args[0] || "*", op.args[1] || "count");
        break;
      case "sum":
        q = q.sum(op.args[0], op.args[1] || undefined);
        break;
      case "avg":
        q = q.avg(op.args[0], op.args[1] || undefined);
        break;
      case "min":
        q = q.min(op.args[0], op.args[1] || undefined);
        break;
      case "max":
        q = q.max(op.args[0], op.args[1] || undefined);
        break;
      case "countDistinct":
        q = q.countDistinct(op.args[0], op.args[1] || undefined);
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
let skipped = 0;
const skippedNames = [];
const failures = [];

for (const tc of cases) {
  resetCapture();
  const wh = createClient({ baseURL, options: { maxRetries: 0 } });

  try {
    switch (tc.endpoint) {
      case "query": {
        const q = applyQueryOps(wh, tc.table, tc.operations ?? []);
        await q.fetch();
        break;
      }
      case "ingest":
        if (tc.operations?.[0]?.method === "insert") {
          await wh.from(tc.table).insert(tc.operations[0].args[0]);
        }
        break;
      case "ingest_batch":
        if (tc.operations?.[0]?.method === "insert") {
          await wh.from(tc.table).insert(tc.operations[0].args[0]);
        }
        break;
      case "pipe":
        await wh.pipe(tc.pipe_name, tc.pipe_params ?? undefined).fetch();
        break;
      case "sql":
        await wh.sql(tc.sql);
        break;
      case "health":
        await wh.sys.health();
        break;
      case "schema_list":
        await wh.schema.list();
        break;
      case "schema_refresh":
        await wh.schema.refresh();
        break;
      case "policy_get":
        await wh.policy.get();
        break;
      case "policy_set":
        await wh.policy.set(tc.policy_body);
        break;
      case "policy_validate":
        await wh.policy.validate(tc.policy_body);
        break;
      case "dlq_list":
        await wh.dlq.list();
        break;
      case "dlq_table":
        await wh.dlq.table(tc.table);
        break;
      case "pipes_list":
        await wh.pipes.list();
        break;
      case "pipes_get":
        await wh.pipes.get(tc.pipe_name);
        break;
      case "pipes_set":
        await wh.pipes.set(tc.pipe_name, tc.pipe_def);
        break;
      case "pipes_delete":
        await wh.pipes.delete(tc.pipe_name);
        break;
      default:
        // Not a pass — the Go harness skips these too. Fixture cases with a
        // new endpoint value must be wired up here before they count.
        skipped++;
        skippedNames.push(`${tc.name} (endpoint: ${tc.endpoint})`);
        continue;
    }

    const errs = [];

    if (tc.expected_method && lastCapture.method !== tc.expected_method) {
      errs.push(`method: want ${tc.expected_method}, got ${lastCapture.method}`);
    }

    if (tc.expected_path && normalizePath(lastCapture.path) !== normalizePath(tc.expected_path)) {
      errs.push(`path: want ${tc.expected_path}, got ${lastCapture.path}`);
    }

    if (tc.expected_content_type && lastCapture.contentType !== tc.expected_content_type) {
      errs.push(`content-type: want ${tc.expected_content_type}, got ${lastCapture.contentType}`);
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
  if (skipped > 0) console.log("  ✗ skipped cases break cross-SDK parity — wire up the endpoint above\n");
  process.exit(1);
} else {
  console.log("  ✓ All cases passed\n");
  process.exit(0);
}

#!/usr/bin/env node
/// <reference types="node" />
/**
 * WaveHouse Codegen CLI
 *
 * Introspects a WaveHouse server's /v1/ops/schema endpoint and generates
 * a TypeScript Database interface for use with createClient<DB>().
 *
 * Usage:
 *   npx tsx src/cli/codegen.ts --url http://localhost:8080 --out ./db.d.ts
 *   npm run codegen -- --url http://localhost:8080 --out ./db.d.ts
 */

import { realpathSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { resolveURL } from "../url.js";

// ── Arg parsing (zero deps) ────────────────────────────────────────────────

interface CliArgs {
  url: string;
  out: string;
  auth?: string;
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = { url: "http://localhost:8080", out: "./wavehouse.d.ts" };
  for (let i = 2; i < argv.length; i++) {
    switch (argv[i]) {
      case "--url":
      case "-u":
        args.url = argv[++i];
        break;
      case "--out":
      case "-o":
        args.out = argv[++i];
        break;
      case "--auth":
      case "-a":
        args.auth = argv[++i];
        break;
      case "--help":
      case "-h":
        console.log(`wavehouse codegen — Generate TypeScript types from WaveHouse schema

Options:
  --url, -u   WaveHouse base URL (default: http://localhost:8080)
  --out, -o   Output file path   (default: ./wavehouse.d.ts)
  --auth, -a  Bearer token for authenticated endpoints
  --help, -h  Show this help`);
        process.exit(0);
    }
  }
  return args;
}

// ── ClickHouse → TypeScript type mapping ───────────────────────────────────

function chTypeToTS(chType: string): string {
  // Unwrap Nullable
  if (chType.startsWith("Nullable(") && chType.endsWith(")")) {
    const inner = chType.slice(9, -1);
    return `${chTypeToTS(inner)} | null`;
  }

  // Unwrap LowCardinality
  if (chType.startsWith("LowCardinality(") && chType.endsWith(")")) {
    return chTypeToTS(chType.slice(15, -1));
  }

  // String-like types
  if (
    chType === "String" ||
    chType.startsWith("FixedString(") ||
    chType === "UUID" ||
    chType.startsWith("DateTime") ||
    chType.startsWith("Date") ||
    chType.startsWith("Enum8(") ||
    chType.startsWith("Enum16(") ||
    chType === "IPv4" ||
    chType === "IPv6"
  ) {
    return "string";
  }

  // Boolean
  if (chType === "Bool") return "boolean";

  // Numeric types
  if (isNumeric(chType)) return "number";

  // Array
  if (chType.startsWith("Array(") && chType.endsWith(")")) {
    const inner = chType.slice(6, -1);
    return `${chTypeToTS(inner)}[]`;
  }

  // Map
  if (chType.startsWith("Map(") && chType.endsWith(")")) {
    const inner = chType.slice(4, -1);
    const comma = findTopLevelComma(inner);
    if (comma !== -1) {
      const keyType = chTypeToTS(inner.slice(0, comma).trim());
      const valType = chTypeToTS(inner.slice(comma + 1).trim());
      return `Record<${keyType}, ${valType}>`;
    }
    return "Record<string, unknown>";
  }

  // Tuple → object or array (fall back to unknown[])
  if (chType.startsWith("Tuple(")) {
    return "unknown[]";
  }

  return "unknown";
}

function isNumeric(t: string): boolean {
  const numericPrefixes = [
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "UInt128",
    "UInt256",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Int256",
    "Float32",
    "Float64",
    "Decimal",
  ];
  return numericPrefixes.some((p) => t === p || t.startsWith(`${p}(`));
}

/** Find the first comma that isn't inside parentheses. */
function findTopLevelComma(s: string): number {
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === "(") depth++;
    else if (s[i] === ")") depth--;
    else if (s[i] === "," && depth === 0) return i;
  }
  return -1;
}

// ── Fetch schema + generate ────────────────────────────────────────────────

interface Column {
  name: string;
  type: string;
  is_nullable: boolean;
  has_default: boolean;
}

interface TableSchema {
  name: string;
  columns: Column[];
}

// Runtime guards mirroring the interfaces above, limited to the fields whose
// absence would crash generation (`is_nullable` is unused and `has_default` is
// truthiness-safe, so a server that omits a boolean still generates fine) — a
// malformed member should fail fetchSchemas' loud shape error, not surface as
// a TypeError mid-generation.
function isColumn(value: unknown): value is Column {
  if (typeof value !== "object" || value === null) return false;
  const col = value as Record<string, unknown>;
  return typeof col.name === "string" && typeof col.type === "string";
}

function isTableSchema(value: unknown): value is TableSchema {
  if (typeof value !== "object" || value === null) return false;
  const table = value as Record<string, unknown>;
  return (
    typeof table.name === "string" && Array.isArray(table.columns) && table.columns.every(isColumn)
  );
}

async function fetchSchemas(url: string, auth?: string): Promise<TableSchema[]> {
  const headers: Record<string, string> = {};
  if (auth) headers.Authorization = `Bearer ${auth}`;

  const res = await fetch(resolveURL(url, "/v1/ops/schema").toString(), { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Schema fetch failed (${res.status}): ${text}`);
  }
  // /v1/ops/schema returns a JSON array of tables — reject any other shape,
  // malformed members included, loudly rather than crashing mid-generation.
  const body = (await res.json()) as unknown;
  if (!Array.isArray(body) || !body.every(isTableSchema)) {
    throw new Error("Unexpected /v1/ops/schema response: expected a JSON array of table schemas");
  }
  return body as TableSchema[];
}

function generateTypes(schemas: TableSchema[]): string {
  const lines: string[] = [
    "// Auto-generated by @wavehouse/sdk codegen",
    `// Generated at: ${new Date().toISOString()}`,
    "// Do not edit manually — re-run: npm run codegen",
    "",
    "export interface Database {",
  ];

  // Code-unit comparison, not localeCompare: db.d.ts is a committed artifact,
  // so its ordering must not vary with the generating host's ICU locale.
  const tables = [...schemas].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  for (const table of tables) {
    lines.push(`  ${table.name}: ${pascalCase(table.name)}Row;`);
  }
  lines.push("}");
  lines.push("");

  // Generate row interfaces
  for (const table of tables) {
    const rowType = `${pascalCase(table.name)}Row`;
    lines.push(`export interface ${rowType} {`);

    for (const col of table.columns) {
      const tsType = chTypeToTS(col.type);
      const optional = col.has_default ? "?" : "";
      lines.push(`  ${col.name}${optional}: ${tsType};`);
    }

    lines.push("}");
    lines.push("");
  }

  return lines.join("\n");
}

function pascalCase(s: string): string {
  return s
    .split(/[_\-\s]+/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join("");
}

// ── Main ───────────────────────────────────────────────────────────────────

async function main() {
  const args = parseArgs(process.argv);

  console.log(`Fetching schema from ${args.url}...`);
  const schemas = await fetchSchemas(args.url, args.auth);

  if (schemas.length === 0) {
    console.warn("No tables found. Is WaveHouse running with tables in ClickHouse?");
    process.exit(1);
  }

  console.log(`Found ${schemas.length} table(s): ${schemas.map((t) => t.name).join(", ")}`);

  const output = generateTypes(schemas);

  const { writeFile } = await import("node:fs/promises");
  const { resolve } = await import("node:path");
  const outPath = resolve(args.out);
  await writeFile(outPath, output, "utf-8");

  console.log(`✓ Types written to ${outPath}`);
}

// Exported for unit tests; the package entry point (src/index.ts) does not
// re-export the CLI.
export { chTypeToTS, fetchSchemas, generateTypes };

// Run only when invoked as a script (the `wavehouse-codegen` bin or tsx), not
// when imported by tests. realpath the argv side: npm bin shims are symlinks,
// while the ESM loader resolves import.meta.url to the real file.
function isDirectInvocation(): boolean {
  if (!process.argv[1]) return false;
  try {
    return import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href;
  } catch {
    return false;
  }
}

if (isDirectInvocation()) {
  main().catch((err) => {
    console.error("Codegen failed:", err.message);
    process.exit(1);
  });
}

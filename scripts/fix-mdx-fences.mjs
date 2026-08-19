#!/usr/bin/env node
// Phase 1 of `make fix` / `pnpm run fix:md`: insert the blank line an MDX code
// fence needs when it sits directly against a JSX tag (WH002).
//
// Why this is a script and not just `markdownlint-cli2 --fix`: the generic
// markdownlint rules are never run against MDX at all. While a fence sits glued
// to a JSX tag, CommonMark does not see a code block, so a YAML block's `#`
// comments look like ATX headings — and MD022/MD023/MD026/MD034 then
// "helpfully" de-indent them out of the block, space them apart, and rewrite
// bare URLs inside what is supposed to be verbatim code. `fix:md` therefore
// scopes the generic pass to **/*.md, and this is the ONLY fixer that touches
// .mdx. It can only ever insert a blank line, so its worst failure mode is a
// render-neutral blank line rather than rewritten code.
//
// The detection logic is shared with the WH002 markdownlint rule (which owns
// reporting for CI and the editor) — see markdownlint-rules/lib/mdx-fences.mjs.
//
// Usage:
//   node scripts/fix-mdx-fences.mjs [--check] [file.mdx ...]
//
// With no file arguments it fixes every tracked-or-untracked .mdx in the repo.
// --check reports instead of writing, and exits 1 if anything would change.

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { findFenceTagViolations } from "./markdownlint-rules/lib/mdx-fences.mjs";

const args = process.argv.slice(2);
const check = args.includes("--check");
const files = args.filter((a) => a !== "--check");

if (files.length === 0) {
  // -c -o --exclude-standard: tracked plus untracked-but-not-ignored, so a
  // brand-new page is covered before it is ever committed.
  const listed = execFileSync("git", ["ls-files", "-co", "--exclude-standard", "--", "*.mdx"], {
    encoding: "utf8",
  });
  files.push(...listed.split("\n").filter(Boolean));
}

let changed = 0;

for (const file of files) {
  if (!file.endsWith(".mdx")) continue;

  let source;
  try {
    source = readFileSync(file, "utf8");
  } catch {
    continue; // deleted between listing and reading; nothing to fix
  }

  const eol = source.includes("\r\n") ? "\r\n" : "\n";
  const lines = source.split(/\r?\n/);
  const violations = findFenceTagViolations(lines);
  if (violations.length === 0) continue;

  changed++;
  if (check) {
    for (const { line, detail } of violations) {
      console.error(`${file}:${line} WH002/mdx-fence-needs-blank-line ${detail}`);
    }
    continue;
  }

  // Insert from the bottom up so earlier line numbers stay valid.
  for (const { line } of [...violations].reverse()) {
    lines.splice(line - 1, 0, "");
  }
  writeFileSync(file, lines.join(eol));
  console.log(`fixed ${violations.length} MDX fence(s) in ${file}`);
}

if (check && changed > 0) process.exitCode = 1;

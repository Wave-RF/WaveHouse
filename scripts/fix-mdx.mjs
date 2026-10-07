#!/usr/bin/env node
// Phase 1 of `make fix` / `pnpm run fix:md`: the only STRUCTURAL fixer that
// touches .mdx (misspell still corrects spelling there). It applies the two
// repo-local rules and nothing else — WH002's blank line beside a JSX tag, then
// WH001's paragraph joins — repeated until the file stops changing.
//
// Why a script and not `markdownlint-cli2 --fix`: the generic markdownlint
// rules are never run against MDX at all. Where a fence sits glued to a JSX
// tag, CommonMark does not see a code block, so a YAML block's `#` comments
// look like ATX headings — and MD022/MD023/MD026/MD034 then "helpfully"
// de-indent them out of the block, space them apart, and rewrite bare URLs
// inside what is supposed to be verbatim code. So .markdownlint-cli2.jsonc
// globs .md only (the .mdx glob lives on `lint:md`), which keeps every generic
// pass — `fix:md` or a bare `--fix` — away from MDX. And "WH001 and WH002 only"
// cannot be said on the CLI, which always merges the nearest
// .markdownlint.json into a run. The markdownlint library takes its config as
// given, so this runs the very rules `make lint` runs, through the markdownlint
// the CLI ships, with every other rule off.
//
// Two guards keep it render-neutral, both read from the MDX parse:
//   - no blank line is inserted inside a list or a blockquote. In a list it
//     makes the list loose (every item gains a <p>); in a blockquote it ends
//     the quote. Those are left for a human, and `make lint` still reports them.
//   - before anything is written, the result must keep the original's block
//     structure, every code/math/ESM/JSX/table block byte for byte, and every
//     non-whitespace character in order. A file that fails is left untouched
//     and the script exits 1.
//
// Usage:
//   node scripts/fix-mdx.mjs [--check] [file.mdx ...]
//
// With no file arguments it fixes every tracked-or-untracked .mdx in the repo.
// --check reports instead of writing, and exits 1 if anything would change.

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { applyFixes } from "markdownlint-cli2/markdownlint";
import helpers from "markdownlint-cli2/markdownlint/helpers";
import { lint } from "markdownlint-cli2/markdownlint/promise";
import {
  descendants,
  mdxInvariant,
  parseMdx,
  splitFrontMatter,
} from "./markdownlint-rules/lib/mdx.mjs";
import wh002 from "./markdownlint-rules/mdx-fence-needs-blank-line.mjs";
import wh001 from "./markdownlint-rules/no-hard-wrapped-prose.mjs";

const CONTAINERS = new Set(["listOrdered", "listUnordered", "blockQuote"]);
// Each pass either inserts a blank line where there was none or deletes lines,
// so a file settles in a handful; this only catches a bug.
const MAX_PASSES = 20;

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

async function violations(file, text) {
  const results = await lint({
    strings: { [file]: text },
    customRules: [wh001, wh002],
    config: { default: false, WH001: true, WH002: true },
  });
  // A "not checked" report says the rule could not parse the file; it marks no
  // place to insert a blank line.
  return results[file]
    .filter((error) => !error.errorDetail?.startsWith("not checked"))
    .map((error) => ({ ...error, rule: error.ruleNames[0] }));
}

/** Why a blank line above `lineNumber` would not be a safe fix, or null. */
function unsafeInsert(lines, lineNumber, mdx, offset) {
  if (!lines[lineNumber - 2]?.trim()) return "a blank line above it does not separate it";
  const line = lineNumber - offset;
  const container = [...descendants(mdx.tokens)].find(
    (t) => CONTAINERS.has(t.type) && t.startLine < line && line <= t.endLine,
  );
  if (!container) return null;
  return container.type === "blockQuote"
    ? "a blank line would end the blockquote"
    : "a blank line would make the list loose";
}

async function fix(file, source) {
  const eol = source.includes("\r\n") ? "\r\n" : "\n";
  let text = source;
  for (let pass = 0; pass < MAX_PASSES; pass++) {
    const { body, offset } = splitFrontMatter(text);
    const mdx = parseMdx(body.split(helpers.newLineRe));
    if (mdx.error)
      return {
        text,
        skip: `not valid MDX at line ${mdx.error.line + offset}: ${mdx.error.reason}`,
      };

    const errors = await violations(file, text);
    const lines = text.split(helpers.newLineRe);
    const inserts = errors
      .filter((e) => e.rule === "WH002" && !unsafeInsert(lines, e.lineNumber, mdx, offset))
      .map((e) => e.lineNumber);
    if (inserts.length > 0) {
      // Bottom up, so earlier line numbers stay valid.
      for (const line of [...new Set(inserts)].sort((a, b) => b - a)) lines.splice(line - 1, 0, "");
      text = lines.join(eol);
      continue;
    }
    const joins = errors.filter((e) => e.rule === "WH001" && e.fixInfo);
    if (joins.length === 0) {
      const left = errors
        .filter((e) => e.rule === "WH002")
        .map(
          (e) =>
            `${file}:${e.lineNumber} WH002 not fixed: ${unsafeInsert(lines, e.lineNumber, mdx, offset)}`,
        );
      return { text, left };
    }
    text = applyFixes(text, joins);
  }
  return { text, bug: `still changing after ${MAX_PASSES} passes` };
}

for (const file of files) {
  if (!file.endsWith(".mdx")) continue;

  let source;
  try {
    source = readFileSync(file, "utf8");
  } catch {
    continue; // deleted between listing and reading; nothing to fix
  }

  const { text, skip, bug, left = [] } = await fix(file, source);
  if (skip) {
    // `make lint` reports an unparseable file; refusing to guess is the fix.
    console.error(`skipped ${file}: ${skip}`);
    if (check) process.exitCode = 1;
    continue;
  }
  if (bug) {
    console.error(`refusing to write ${file}: ${bug}`);
    process.exitCode = 1;
    continue;
  }
  for (const line of left) console.error(line);
  if (text === source) continue;

  if (mdxInvariant(text) !== mdxInvariant(source)) {
    console.error(
      `refusing to write ${file}: the fix would change more than whitespace around prose`,
    );
    process.exitCode = 1;
    continue;
  }
  if (check) {
    for (const e of await violations(file, source)) {
      if (e.fixInfo || e.rule === "WH002")
        console.error(`${file}:${e.lineNumber} ${e.ruleNames.join("/")} ${e.errorDetail}`);
    }
    process.exitCode = 1;
    continue;
  }
  writeFileSync(file, text);
  console.log(`fixed ${file}`);
}

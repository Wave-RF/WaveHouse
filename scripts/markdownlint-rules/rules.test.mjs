#!/usr/bin/env node --test
// Fixtures for the repo-local markdownlint rules (WH001, WH002) and the WH002
// autofix pass. Run by `make test-md-rules`, a `make verify` leaf.
//
// These drive the REAL markdownlint-cli2 binary rather than calling the rule
// functions directly, because the defects worth guarding against live in the
// interaction — how markdownlint's fix applier combines one rule's line-delete
// with another rule's edit on the same line — not in the rule's return value.
//
// Every construct classify() recognizes should have a fixture here. Two
// corrupting bugs (pipe-less tables, single-character setext underlines) got
// through review because nothing exercised those shapes.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "../..");
const cli2 = path.join(repoRoot, "node_modules/.bin/markdownlint-cli2");
const workdir = mkdtempSync(path.join(tmpdir(), "wh-md-rules-"));
after(() => rmSync(workdir, { recursive: true, force: true }));

/** Write `content` to a file in an isolated dir, run the CLI, return the result. */
function run(name, content, { fix = false, config } = {}) {
  const dir = mkdtempSync(path.join(workdir, "case-"));
  const file = path.join(dir, name);
  writeFileSync(file, content);
  writeFileSync(
    path.join(dir, ".markdownlint-cli2.jsonc"),
    JSON.stringify({
      customRules: [
        path.join(here, "no-hard-wrapped-prose.mjs"),
        path.join(here, "mdx-fence-needs-blank-line.mjs"),
      ],
      globs: ["*.md", "*.mdx"],
      config: config ?? { default: false, WH001: true, WH002: true },
    }),
  );
  let stdout = "";
  try {
    stdout = execFileSync(cli2, fix ? ["--fix"] : [], {
      cwd: dir,
      encoding: "utf8",
      stdio: "pipe",
    });
  } catch (err) {
    stdout = `${err.stdout ?? ""}${err.stderr ?? ""}`;
  }
  return { output: readFileSync(file, "utf8"), stdout };
}

const unchanged = (name, content, label) =>
  it(label, () => assert.equal(run(name, content, { fix: true }).output, content));

describe("WH001 joins hard-wrapped prose", () => {
  it("joins a wrapped paragraph", () => {
    const { output } = run("t.md", "This is a paragraph that was\nwrapped by an AI.\n", {
      fix: true,
    });
    assert.equal(output, "This is a paragraph that was wrapped by an AI.\n");
  });

  it("joins the body of a ::: aside without touching its delimiters", () => {
    const { output } = run("t.md", ":::note[Title]\nBody line one\nbody line two.\n:::\n", {
      fix: true,
    });
    assert.equal(output, ":::note[Title]\nBody line one body line two.\n:::\n");
  });

  it("joins a list item's continuation onto its marker line", () => {
    const { output } = run("t.md", "- a bullet wrapped\n  across three\n  lines here\n", {
      fix: true,
    });
    assert.equal(output, "- a bullet wrapped across three lines here\n");
  });

  it("is a fixpoint — a second pass changes nothing", () => {
    const once = run("t.md", "Wrapped one\ntwo three.\n", { fix: true }).output;
    assert.equal(run("t.md", once, { fix: true }).output, once);
  });

  it("still fires when a leading --- is a thematic break, not frontmatter", () => {
    const { output } = run("t.md", "---\n\nA paragraph that is\nhard wrapped here.\n", {
      fix: true,
    });
    assert.equal(output, "---\n\nA paragraph that is hard wrapped here.\n");
  });
});

describe("WH001 leaves non-prose alone", () => {
  unchanged(
    "t.md",
    "Name | Type | Notes\n---- | ---- | -----\n`a`  | int  | first\n`b`  | str  | second\n",
    "a GFM table written without leading pipes",
  );
  unchanged(
    "t.md",
    "| Name | Type |\n| ---- | ---- |\n| a    | int  |\n| b    | str  |\n",
    "a pipe-led table",
  );
  unchanged("t.md", "Section Title\n=\n", "a single-character setext h1 underline");
  unchanged("t.md", "Section Title\n-\n", "a single-character setext h2 underline");
  unchanged("t.md", '```go\nfoo := "not\nwrapped"\n```\n', "fenced code");
  unchanged("t.md", "~~~\ntilde fenced\ncode block\n~~~\n", "tilde-fenced code");
  unchanged("t.md", "Line one  \nline two.\n", "a two-space hard line break");
  unchanged("t.md", "Line one\\\nline two.\n", "a backslash hard line break");
  unchanged("t.md", "> quoted line\n> second quoted line\n", "a blockquote");
  unchanged("t.md", "# Heading\n\nBody.\n", "a heading followed by a paragraph");
  unchanged("t.md", "    indented code\n    second line\n", "an indented code block");
  unchanged(
    "t.mdx",
    'export const meta = {\n  // the id used by the demo\n  id: "abc",\n  kind: "demo",\n};\n',
    "a multi-line MDX export with a comment inside",
  );
  unchanged(
    "t.mdx",
    "import { Tabs } from '@astrojs/starlight/components';\nimport Cta from '../x.astro';\n",
    "consecutive MDX imports",
  );
  unchanged(
    "t.mdx",
    'export const meta = {\n  // the closing } is below\n  title: "a",\n  // another comment\n  body: "b",\n};\n',
    "an MDX export whose comments hold unbalanced braces",
  );
  // markdownlint masks HTML-comment interiors in the buffer rules are handed, so
  // joining such a line would write the mask back and destroy the comment text.
  unchanged(
    "t.md",
    "This paragraph is wrapped and ends with\na note <!-- TODO: ask legal about the wording --> right here.\n",
    "a paragraph whose continuation carries an inline HTML comment",
  );
  unchanged(
    "t.md",
    "<!-- a note about the block below\nit is generated -->\nGenerated table follows.\n",
    "prose directly after a multi-line HTML comment",
  );
  unchanged(
    "t.mdx",
    '<Cta\n  variant="band"\n  title="Managed"\n/>\n',
    "a JSX tag with attributes across lines",
  );
});

describe("WH002 flags an MDX fence glued to a JSX tag", () => {
  const glued = '<TabItem label="YAML">\n```yaml\nkey: value\n```\n</TabItem>\n';

  it("reports both the opening and closing side", () => {
    const { stdout } = run("t.mdx", glued);
    assert.match(stdout, /WH002/);
    assert.equal((stdout.match(/WH002/g) ?? []).length, 2);
  });

  it("does not autofix — the repair is ordered by scripts/fix-mdx-fences.mjs", () => {
    assert.equal(run("t.mdx", glued, { fix: true }).output, glued);
  });

  it("is silent once the blank lines are present", () => {
    const ok = '<TabItem label="YAML">\n\n```yaml\nkey: value\n```\n\n</TabItem>\n';
    assert.doesNotMatch(run("t.mdx", ok).stdout, /WH002/);
  });

  it("ignores .md, which has no JSX", () => {
    assert.doesNotMatch(run("t.md", glued).stdout, /WH002/);
  });

  it("sees an opening tag whose attributes span lines", () => {
    const multiline = '<TabItem\n  label="YAML">\n```yaml\nkey: value\n```\n</TabItem>\n';
    const { stdout } = run("t.mdx", multiline);
    // Both sides, or the fixer inserts one blank line and leaves the block broken.
    assert.equal((stdout.match(/WH002/g) ?? []).length, 2);
  });
});

describe("fix-mdx-fences.mjs repairs the structure", () => {
  it("inserts the blank lines on both sides", () => {
    const dir = mkdtempSync(path.join(workdir, "fixer-"));
    const file = path.join(dir, "t.mdx");
    writeFileSync(file, '<TabItem label="YAML">\n```yaml\nkey: value\n```\n</TabItem>\n');
    execFileSync("node", [path.join(repoRoot, "scripts/fix-mdx-fences.mjs"), file], {
      stdio: "pipe",
    });
    assert.equal(
      readFileSync(file, "utf8"),
      '<TabItem label="YAML">\n\n```yaml\nkey: value\n```\n\n</TabItem>\n',
    );
  });

  it("--check reports without writing", () => {
    const dir = mkdtempSync(path.join(workdir, "check-"));
    const file = path.join(dir, "t.mdx");
    const glued = '<TabItem label="YAML">\n```yaml\nkey: value\n```\n</TabItem>\n';
    writeFileSync(file, glued);
    assert.throws(() =>
      execFileSync("node", [path.join(repoRoot, "scripts/fix-mdx-fences.mjs"), "--check", file], {
        stdio: "pipe",
      }),
    );
    assert.equal(readFileSync(file, "utf8"), glued);
  });
});

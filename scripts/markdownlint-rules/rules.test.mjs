#!/usr/bin/env node --test
// Fixtures for the repo-local markdownlint rules (WH001, WH002) and the MDX
// fixer, scripts/fix-mdx.mjs. Run by `make test-md-rules`, a `make verify` leaf.
//
// These drive the REAL markdownlint-cli2 binary rather than calling the rule
// functions directly, because the defects worth guarding against live in the
// interaction — how markdownlint's fix applier combines one rule's line-delete
// with another rule's edit on the same line — not in the rule's return value.
//
// Every construct the rules must leave alone has a fixture here — keep it that
// way. The rules used to classify lines by shape, and two corrupting bugs
// (pipe-less tables, single-character setext underlines) got through review
// because nothing exercised those shapes. They read a parse tree now; the
// fixtures are what proves the parse is the one the docs actually render with.

import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { after, describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { evaluate } from "@mdx-js/mdx";
import remarkDirective from "remark-directive";
import remarkGfm from "remark-gfm";
import remarkMath from "remark-math";
import { mdxInvariant, splitFrontMatter } from "./lib/mdx.mjs";

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

/** Run scripts/fix-mdx.mjs over `content` saved as t.mdx; return the file and stderr. */
function fixMdx(content, flags = []) {
  const dir = mkdtempSync(path.join(workdir, "fixer-"));
  const file = path.join(dir, "t.mdx");
  writeFileSync(file, content);
  const result = spawnSync("node", [path.join(repoRoot, "scripts/fix-mdx.mjs"), ...flags, file], {
    encoding: "utf8",
  });
  return { output: readFileSync(file, "utf8"), stderr: result.stderr, status: result.status };
}

const count = (stdout, rule) => (stdout.match(new RegExp(rule, "g")) ?? []).length;

// Render MDX the way the docs site parses it (remark-gfm, remark-directive,
// remark-math), to a string in which whitespace counts only inside code. Equal
// strings mean a fix changed nothing a reader sees, which no line-level
// assertion can promise.
const Fragment = Symbol("Fragment");
const jsx = (type, props) => ({ type, props });
const directiveElements = () => (tree) => {
  const walk = (node) => {
    if (/Directive$/.test(node.type)) {
      node.data = { hName: "x-directive", hProperties: { name: node.name, ...node.attributes } };
    }
    node.children?.forEach(walk);
  };
  walk(tree);
};
function serialize(node, verbatim = false) {
  if (node == null || typeof node === "boolean") return "";
  if (Array.isArray(node)) return node.map((child) => serialize(child, verbatim)).join("");
  if (typeof node !== "object") return verbatim ? String(node) : String(node).replace(/\s+/g, " ");
  const { type, props = {} } = node;
  if (type === Fragment) return serialize(props.children, verbatim);
  const attrs = Object.keys(props)
    .filter((key) => key !== "children")
    .sort()
    .map((key) => ` ${key}=${JSON.stringify(props[key])}`)
    .join("");
  const inner = serialize(props.children, verbatim || type === "code" || type === "pre");
  return `<${String(type)}${attrs}>${inner}</${String(type)}>`;
}
async function renderMdx(source) {
  const body = splitFrontMatter(source).body.replace(/^import .*$/gm, "");
  const names = new Set([...body.matchAll(/<\/?([A-Z]\w*)/g)].map((match) => match[1]));
  const components = Object.fromEntries(
    [...names].map((name) => [name, (props) => ({ type: `x-${name}`, props })]),
  );
  const { default: Content } = await evaluate(body, {
    Fragment,
    jsx,
    jsxs: jsx,
    remarkPlugins: [remarkGfm, remarkDirective, remarkMath, directiveElements],
  });
  return serialize(Content({ components }));
}
const assertSameRender = async (before, after) =>
  assert.equal(await renderMdx(after), await renderMdx(before));

/** Copy the repo's real markdownlint configs into `dir`, with loadable rule paths. */
function copyRepoConfig(dir) {
  writeFileSync(
    path.join(dir, ".markdownlint.json"),
    readFileSync(path.join(repoRoot, ".markdownlint.json"), "utf8"),
  );
  const cli2Config = readFileSync(
    path.join(repoRoot, ".markdownlint-cli2.jsonc"),
    "utf8",
  ).replaceAll('"./scripts/', `"${repoRoot}/scripts/`);
  writeFileSync(path.join(dir, ".markdownlint-cli2.jsonc"), cli2Config);
}

const unchanged = (name, content, label) =>
  it(label, () => assert.equal(run(name, content, { fix: true }).output, content));

/** `unchanged` in .md and again in .mdx, for constructs both languages share. */
const unchangedBoth = (content, label) => {
  unchanged("t.md", content, label);
  unchanged("t.mdx", content, `${label} (.mdx)`);
};

/** WH001's fix turns `src` into `expected`; in .mdx it must also render the same. */
function fixesTo(label, src, expected, { md = true, mdx = true } = {}) {
  if (md) it(label, () => assert.equal(run("t.md", src, { fix: true }).output, expected));
  if (mdx) {
    it(`${label} (.mdx)`, async () => {
      const { output } = run("t.mdx", src, { fix: true });
      assert.equal(output, expected);
      await assertSameRender(src, output);
    });
  }
}

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

  it("joins prose after real frontmatter without touching the frontmatter", () => {
    const { output } = run(
      "t.md",
      '---\ntitle: "A page"\ndescription: "Its description"\n---\n\nA paragraph that is\nwrapped here.\n',
      { fix: true },
    );
    assert.equal(
      output,
      '---\ntitle: "A page"\ndescription: "Its description"\n---\n\nA paragraph that is wrapped here.\n',
    );
  });

  it("still fires when a leading --- is a thematic break, not frontmatter", () => {
    const { output } = run("t.md", "---\n\nA paragraph that is\nhard wrapped here.\n", {
      fix: true,
    });
    assert.equal(output, "---\n\nA paragraph that is hard wrapped here.\n");
  });

  // Four spaces is an ordinary list indent, and the line-shape rule read every
  // such line as indented code — so none of these was ever reported or fixed.
  it("joins a nested list item indented four spaces", () => {
    const { output } = run("t.md", "- top\n    - nested item\n      wrapped here\n", { fix: true });
    assert.equal(output, "- top\n    - nested item wrapped here\n");
  });

  it("joins a list item whose continuation is indented four spaces", () => {
    const { output } = run("t.md", "- item wrapped\n    across lines\n", { fix: true });
    assert.equal(output, "- item wrapped across lines\n");
  });

  it("joins an item three levels deep", () => {
    const src = "1. step\n   - sub\n     - leaf item\n       wrapped twice\n       over here\n";
    assert.equal(
      run("t.md", src, { fix: true }).output,
      "1. step\n   - sub\n     - leaf item wrapped twice over here\n",
    );
  });

  it("joins a paragraph continued at four spaces, which cannot open indented code", () => {
    const { output } = run("t.md", "A paragraph\n    continued here.\n", { fix: true });
    assert.equal(output, "A paragraph continued here.\n");
  });

  // markdownlint masks everything from `<!--` to `-->` in the lines it hands a
  // rule, inside code spans and across paragraphs too. A fix built from those
  // lines overwrote real prose with dots.
  fixesTo(
    "joins prose between comment markers that sit in code spans",
    "Type `<!--` to open an HTML comment.\n\nA wrapped paragraph\nthat spans lines.\n\nThen close it with `-->`.\n",
    "Type `<!--` to open an HTML comment.\n\nA wrapped paragraph that spans lines.\n\nThen close it with `-->`.\n",
  );
  fixesTo(
    "joins a paragraph whose code spans hold both comment markers",
    "Use `<!--` to open\na comment, and\nthen write the body,\nand `-->` closes it.\n",
    "Use `<!--` to open a comment, and then write the body, and `-->` closes it.\n",
  );
  // A code span turns a line ending into one space but keeps the spaces before it.
  fixesTo(
    "keeps the line a code span breaks after trailing spaces",
    "Run `foo   \nbar` now and\nthen.\n",
    "Run `foo   \nbar` now and then.\n",
  );
  fixesTo(
    "keeps the line a code span breaks before an indented line",
    "Run `foo\n     bar` now and\nthen.\n",
    "Run `foo\n     bar` now and then.\n",
  );
  unchangedBoth("Use ``\n    `a` `` here.\n", "a line ending that pads a code span");
  fixesTo(
    "joins a code span across a list item's own indent",
    "- Run `foo\n  bar` now and\n  then.\n",
    "- Run `foo bar` now and then.\n",
  );
  fixesTo(
    "joins a code span broken at a plain line ending",
    "Run `foo\nbar` now.\n",
    "Run `foo bar` now.\n",
  );
  fixesTo(
    "trims only spaces and tabs, never a non-breaking space",
    "Price: 5\u00a0\nEUR today.\n",
    "Price: 5\u00a0 EUR today.\n",
  );
  fixesTo(
    "joins a line that opens with a bare `<`",
    "Values\n<= 5 are fine\nand more.\n",
    "Values <= 5 are fine and more.\n",
    { mdx: false },
  );
  fixesTo(
    "keeps a line ending inside a directive's attributes",
    'Word :abbr[x]{title="a\nb"} and\nmore.\n',
    'Word :abbr[x]{title="a\nb"} and more.\n',
    { md: false },
  );
});

describe("WH001 leaves non-prose alone", () => {
  unchangedBoth(
    "Name | Type | Notes\n---- | ---- | -----\n`a`  | int  | first\n`b`  | str  | second\n",
    "a GFM table written without leading pipes",
  );
  unchangedBoth(
    "| Name | Type |\n| ---- | ---- |\n| a    | int  |\n| b    | str  |\n",
    "a pipe-led table",
  );
  unchangedBoth("Section Title\n=\n", "a single-character setext h1 underline");
  unchangedBoth("Section Title\n-\n", "a single-character setext h2 underline");
  unchangedBoth('```go\nfoo := "not\nwrapped"\n```\n', "fenced code");
  unchangedBoth("~~~\ntilde fenced\ncode block\n~~~\n", "tilde-fenced code");
  unchangedBoth("Line one  \nline two.\n", "a two-space hard line break");
  unchangedBoth("Line one\\\nline two.\n", "a backslash hard line break");
  unchangedBoth("> quoted line\n> second quoted line\n", "a blockquote");
  unchangedBoth("# Heading\n\nBody.\n", "a heading followed by a paragraph");
  unchanged("t.md", "    indented code\n    second line\n", "an indented code block");
  unchangedBoth("$$\nE = mc^2\n\\sum_{i=1}^{n} x_i\n$$\n", "a $$ display-math block");
  unchangedBoth("$$ E = mc^2 $$\n\nA paragraph.\n", "single-line $$ display math");
  unchangedBoth("Prose above it.\n[ref]: https://example.com\n", "a link-reference definition");
  unchangedBoth("Prose above it.\n[^1]: The footnote body.\n", "a footnote definition");
  unchangedBoth("Paragraph.\n***\n", "an asterisk thematic break");
  unchangedBoth("Paragraph.\n___\n", "an underscore thematic break");
  unchanged(
    "t.mdx",
    'export const meta = {\n  // the id used by the demo\n  id: "abc",\n  kind: "demo",\n};\n',
    "a multi-line MDX export with a comment inside",
  );
  it("keeps a two-space hard break carried by a continuation line", () => {
    // The run stopped at the break correctly, but the line carrying it was
    // absorbed and trimmed, silently deleting the <br>. alpha+beta still join —
    // they are one paragraph — and the break after beta must survive.
    const { output } = run("t.md", "alpha line\nbeta line  \ngamma line\n", { fix: true });
    assert.equal(output, "alpha line beta line  \ngamma line\n");
  });
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
  unchanged(
    "t.md",
    "- item\n\n      code inside the item\n      second line\n",
    "an indented code block inside a list item",
  );
  // A line ending in these is not a space: JS and TeX comments run to it.
  unchangedBoth(
    "Inline math $a % a TeX comment\nb$ spans lines.\n",
    "a line ending inside inline math",
  );
  unchanged(
    "t.mdx",
    "Total {a + // a JS comment\n  b} items.\n",
    "a line ending inside an MDX expression",
  );
  unchangedBoth(
    'See [the docs](https://example.com "a title\nthat wraps") for more.\n',
    "a line ending inside a link title",
  );
  unchangedBoth("See ![an alt\ntext](x.png) here.\n", "a line ending inside image alt text");
  // An HTML block's interior is HTML to CommonMark, and in <pre> every newline
  // shows. MDX calls the same text a paragraph, so .mdx tracks the element.
  const pre = "<pre>\nline one\nline two\n</pre>\n";
  unchangedBoth(pre, "the inside of a <pre> block");
  unchangedBoth(`Prose above it.\n${pre}`, "a <pre> under prose");
  unchangedBoth(`<Aside>\n${pre}</Aside>\n`, "a <pre> directly inside a JSX tag");
  unchangedBoth(`<div>\n${pre}</div>\n`, "a <pre> directly inside a <div>");
  unchangedBoth(
    `<Tabs>\n<TabItem label="a">\n${pre}</TabItem>\n</Tabs>\n`,
    "a <pre> two tags deep",
  );
  unchangedBoth(`- item\n\n${pre.replace(/^(?=.)/gm, "  ")}`, "a <pre> in a list item");
  unchangedBoth(
    `- item\n\n${pre.replace(/^(?=.)/gm, "      ")}`,
    "a <pre> indented as code past a list item, which MDX reads as JSX",
  );
  unchangedBoth(
    "<script>\nconst a = 1\nconst b = 2\n</script>\n",
    "the inside of a <script> block",
  );
  fixesTo(
    "joins wrapped prose after a closed <pre>",
    "<pre>x</pre>\nwrapped\nprose\n",
    "<pre>x</pre>\nwrapped prose\n",
  );
  fixesTo(
    "joins around, but not inside, a <pre> opened mid-line",
    "Text with <pre>a\nb</pre> inline\nand wrapped.\n",
    "Text with <pre>a\nb</pre> inline and wrapped.\n",
    { md: false },
  );
  it("neither reports nor joins a line that opens with a tag", () => {
    const src = "Press\n<kbd>Enter</kbd> to go on.\n";
    const { output, stdout } = run("t.md", src, { fix: true });
    assert.equal(output, src);
    assert.doesNotMatch(stdout, /WH001/);
  });
  // MDX parses inline elements on their own lines inside a layout block as one
  // paragraph; joining them is render-neutral but makes the markup unreadable.
  unchanged(
    "t.mdx",
    '<div class="stat">\n  <span class="value">1</span>\n  <span class="unit">binary</span>\n</div>\n',
    "lines of inline markup that MDX calls a paragraph",
  );
});

describe("WH001 reads .mdx with an MDX parser", () => {
  fixesTo(
    "joins prose inside a JSX block",
    "<Aside>\n\nProse inside the\naside, wrapped.\n\n</Aside>\n",
    "<Aside>\n\nProse inside the aside, wrapped.\n\n</Aside>\n",
    { md: false },
  );
  fixesTo(
    "joins prose inside a <div>, an HTML block a blank line ends",
    "<div>\nProse inside the\ndiv, wrapped.\n</div>\n",
    "<div>\nProse inside the div, wrapped.\n</div>\n",
    { md: false },
  );
  fixesTo(
    "joins four-space-indented prose, since MDX has no indented code",
    "<Aside>\n\n    Indented prose\n    wrapped here.\n\n</Aside>\n",
    "<Aside>\n\n    Indented prose wrapped here.\n\n</Aside>\n",
    { md: false },
  );

  it("parses the real text, not markdownlint's comment mask", () => {
    // Masked, the code spans' `<!--` … `-->` turn everything between them into
    // dots, and the fence between them disappears from the parse.
    const src =
      "Use `<!--` here.\n\n<TabItem>\n```xml\n<a/>\n```\n</TabItem>\n\nand `-->` there.\n";
    const { stdout } = run("t.mdx", src);
    assert.doesNotMatch(stdout, /not checked/);
    assert.equal(count(stdout, "WH002"), 2);
  });

  it("reports a file it cannot parse instead of guessing at it", () => {
    const src = "An unclosed {expression\nand wrapped\nprose.\n";
    const { output, stdout } = run("t.mdx", src, { fix: true });
    assert.equal(output, src);
    assert.match(
      stdout,
      /WH001\/no-hard-wrapped-prose .*not checked: the file does not parse as MDX/,
    );
    assert.match(stdout, /WH002\/mdx-fence-needs-blank-line .*not checked/);
  });
});

describe("WH002 flags an MDX fence glued to a JSX tag", () => {
  const glued = '<TabItem label="YAML">\n```yaml\nkey: value\n```\n</TabItem>\n';

  it("reports both the opening and closing side", () => {
    const { stdout } = run("t.mdx", glued);
    assert.match(stdout, /WH002/);
    assert.equal((stdout.match(/WH002/g) ?? []).length, 2);
  });

  it("does not autofix — the repair is ordered by scripts/fix-mdx.mjs", () => {
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

  it("does not fire on a complete inline element above a fence", () => {
    // `<span>text</span>` starts with a tag and ends with `>`, but it is not an
    // opening tag — firing here would make the shared fixer insert a blank line
    // into valid MDX.
    const src = "<span>text</span>\n```yaml\nkey: value\n```\n";
    assert.doesNotMatch(run("t.mdx", src).stdout, /WH002/);
    assert.equal(run("t.mdx", src, { fix: true }).output, src);
  });

  it("reports a fence glued to prose inside an open block, not the tag above the prose", () => {
    // CommonMark's HTML block from `<Foo>` runs to the next blank line, so the
    // fence is swallowed even with prose between it and the tag. A line-shape
    // detector never saw this; the prose ending in `>` must still not be read
    // as the tail of a multi-line opening tag.
    const src = "<Foo>\nprose that ends in a >\n```yaml\nkey: value\n```\n";
    const { stdout } = run("t.mdx", src);
    assert.equal((stdout.match(/WH002/g) ?? []).length, 1);
    assert.match(stdout, /t\.mdx:3 .*inside the HTML block CommonMark starts at <Foo>/);
    assert.doesNotMatch(stdout, /directly after/);
  });

  it("still sees both sides when an attribute value contains >", () => {
    // Rejecting the opening side alone would be worse than not detecting at all:
    // the fixer inserts one of the two blank lines, WH002 falls silent, and the
    // generic pass then rewrites the fenced code.
    const src = '<TabItem label="a>b">\n```yaml\nkey: value\n```\n</TabItem>\n';
    assert.equal((run("t.mdx", src).stdout.match(/WH002/g) ?? []).length, 2);
  });

  it("still sees both sides when an expression container contains >", () => {
    const src = "<Foo onClick={() => f()}>\n```yaml\nkey: value\n```\n</Foo>\n";
    assert.equal((run("t.mdx", src).stdout.match(/WH002/g) ?? []).length, 2);
  });

  it("still sees both sides for a nested-brace attribute on an HTML tag name", () => {
    // CommonMark HTML block type 6 needs only a known block-level name, so an
    // unmasked `>` does NOT disqualify it the way it does for type 7 — the fence
    // really is swallowed here, and a one-sided report would half-fix it.
    const src = "<div onClick={() => open({tab: 1})}>\n```yaml\nkey: value\n```\n</div>\n";
    assert.equal((run("t.mdx", src).stdout.match(/WH002/g) ?? []).length, 2);
  });

  it("reports both sides of a fence glued to prose inside an open block", () => {
    // The line-shape detector saw only the closing side here, so its fixer
    // inserted one blank line and left the fence inside the HTML block.
    const src = "<Aside>\nRun this first:\n```sh\nmake tools\n```\n</Aside>\n";
    const { stdout } = run("t.mdx", src);
    assert.equal(count(stdout, "WH002"), 2);
    assert.match(stdout, /t\.mdx:3 .*inside the HTML block CommonMark starts at <Aside>/);
    assert.match(stdout, /t\.mdx:6 .*<\/Aside> follows a fence directly/);
  });

  it("sees the HTML block a self-closing tag opens", () => {
    const { stdout } = run("t.mdx", "<Badge />\n```yaml\nkey: value\n```\n");
    assert.equal(count(stdout, "WH002"), 1);
    assert.match(stdout, /t\.mdx:2 .*inside the HTML block CommonMark starts at <Badge \/>/);
  });

  it("is silent when prose, not a tag, sits on the fence", () => {
    // A fence interrupts a paragraph in both parsers, so they agree.
    assert.doesNotMatch(run("t.mdx", "Run this:\n```sh\nmake\n```\n").stdout, /WH002/);
  });
});

describe("the repo config keeps a bare --fix away from MDX", () => {
  // The guard is structural, not prose: `.markdownlint-cli2.jsonc` globs `.md`
  // only, and the `.mdx` glob lives on `lint:md`. If someone moves it back into
  // the config, a bare `markdownlint-cli2 --fix` silently starts rewriting the
  // inside of MDX code blocks again — the exact corruption this arrangement
  // exists to prevent, and previously prevented only by a sentence in AGENTS.md.
  // The interior blank line matters: without one, CommonMark's HTML block runs
  // past the whole fence and the generic rules stay quiet, so a fixture without
  // it would pass even with the guard removed.
  const glued =
    '<TabItem label="YAML">\n```yaml\n# see https://example.com/config\ncache:\n   ttl: 60s\n\n# second section\nother: 1\n```\n</TabItem>\n';

  it("leaves a glued-fence .mdx byte-identical under a bare --fix", () => {
    const dir = mkdtempSync(path.join(workdir, "bare-"));
    const file = path.join(dir, "t.mdx");
    writeFileSync(file, glued);
    copyRepoConfig(dir);
    try {
      execFileSync(cli2, ["--fix"], { cwd: dir, encoding: "utf8", stdio: "pipe" });
    } catch {
      // A nonzero exit just means findings remain; the file is what matters.
    }
    assert.equal(readFileSync(file, "utf8"), glued);
  });

  it("still reports .mdx when the lint glob is supplied, as lint:md does", () => {
    const dir = mkdtempSync(path.join(workdir, "lint-"));
    writeFileSync(path.join(dir, "t.mdx"), glued);
    copyRepoConfig(dir);
    let stdout = "";
    try {
      stdout = execFileSync(cli2, ["**/*.mdx"], { cwd: dir, encoding: "utf8", stdio: "pipe" });
    } catch (err) {
      stdout = `${err.stdout ?? ""}${err.stderr ?? ""}`;
    }
    assert.match(stdout, /WH002/);
  });
});

describe("fix-mdx.mjs repairs .mdx with the two rules and nothing else", () => {
  it("inserts the blank lines on both sides", () => {
    const dir = mkdtempSync(path.join(workdir, "fixer-"));
    const file = path.join(dir, "t.mdx");
    writeFileSync(file, '<TabItem label="YAML">\n```yaml\nkey: value\n```\n</TabItem>\n');
    execFileSync("node", [path.join(repoRoot, "scripts/fix-mdx.mjs"), file], {
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
      execFileSync("node", [path.join(repoRoot, "scripts/fix-mdx.mjs"), "--check", file], {
        stdio: "pipe",
      }),
    );
    assert.equal(readFileSync(file, "utf8"), glued);
  });

  const page = [
    "---",
    'title: "A page"',
    "---",
    "",
    "import { Aside } from '@astrojs/starlight/components';",
    "",
    "export const meta = {",
    "  // keep this comment",
    '  id: "x",',
    "};",
    "",
    "A paragraph that was",
    "hard-wrapped by a tool.",
    "",
    "- A list item that",
    "    wraps with four spaces",
    "  - and a nested item",
    "    that wraps too",
    "",
    "Name | Type",
    "---- | ----",
    "a    | int",
    "",
    ":::note[Heads up]",
    "Aside body that",
    "wraps.",
    ":::",
    "",
    "$$",
    "E = mc^2",
    "$$",
    "",
    "<Aside>",
    "```yaml",
    "# a comment",
    "",
    "key: value",
    "```",
    "</Aside>",
    "",
  ].join("\n");

  it("unwraps prose and separates fences, leaving every other construct byte-identical", async () => {
    const { output, stderr, status } = fixMdx(page);
    assert.equal(status, 0);
    assert.equal(stderr, "");
    const expected = page
      .replace("A paragraph that was\nhard-wrapped", "A paragraph that was hard-wrapped")
      .replace("that\n    wraps with", "that wraps with")
      .replace("item\n    that wraps", "item that wraps")
      .replace("Aside body that\nwraps.", "Aside body that wraps.")
      .replace("<Aside>\n```yaml", "<Aside>\n\n```yaml")
      .replace("```\n</Aside>", "```\n\n</Aside>");
    assert.equal(output, expected);
    await assertSameRender(page, output);
  });

  it("is a fixpoint — a second run changes nothing", () => {
    const once = fixMdx(page).output;
    assert.equal(fixMdx(once).output, once);
  });

  it("closes the hazard: a glued fence's code is no longer read as Markdown", async () => {
    // Before the fix CommonMark ends the HTML block at the blank line inside the
    // fence and reads `# second` as a heading, which MD022 wants spaced out.
    const src = "<Aside>\nRun this first:\n```yaml\n# first\n\n# second\n```\n</Aside>\n";
    const heading = { config: { default: false, MD022: true } };
    assert.match(run("t.mdx", src, heading).stdout, /MD022/);
    const { output } = fixMdx(src);
    assert.equal(
      output,
      "<Aside>\nRun this first:\n\n```yaml\n# first\n\n# second\n```\n\n</Aside>\n",
    );
    assert.doesNotMatch(run("t.mdx", output, heading).stdout, /MD022/);
    await assertSameRender(src, output);
  });

  it("leaves a fence inside a list item for a human — a blank line there makes the list loose", () => {
    const src = "1. Step:\n   <Foo>\n   ```sh\n   x\n   ```\n   </Foo>\n2. Next\n";
    const { output, stderr, status } = fixMdx(src);
    assert.equal(status, 0);
    assert.equal(output, src);
    assert.match(stderr, /t\.mdx:3 WH002 not fixed: a blank line would make the list loose/);
    assert.match(stderr, /t\.mdx:6 WH002 not fixed: a blank line would make the list loose/);
  });

  it("leaves a fence inside a blockquote for a human — a blank line there ends the quote", () => {
    const src = "> <Foo>\n> ```sh\n> x\n> ```\n> </Foo>\n";
    const { output, stderr, status } = fixMdx(src);
    assert.equal(status, 0);
    assert.equal(output, src);
    assert.match(stderr, /t\.mdx:2 WH002 not fixed: a blank line would end the blockquote/);
  });

  it("leaves a fence inside <pre> for a human — a blank line does not end that HTML block", () => {
    const src = "<pre>\n```sh\nx\n```\n</pre>\n";
    const { output, stderr, status } = fixMdx(src);
    assert.equal(status, 0);
    assert.equal(output, src);
    assert.match(
      stderr,
      /t\.mdx:2 WH002 not fixed: a blank line does not end the <pre> HTML block/,
    );
    assert.match(
      stderr,
      /t\.mdx:5 WH002 not fixed: a blank line does not end the <pre> HTML block/,
    );
  });

  it("reads the real text, not markdownlint's comment mask", async () => {
    // The rules used to parse the masked lines, call this file unparseable, and
    // hand the fixer that report as a place to insert a blank line.
    const src =
      "Use `<!--` here.\n\n<TabItem>\n```xml\n<a/>\n```\n</TabItem>\n\nand `-->` there\nwrapped.\n";
    const { output, stderr, status } = fixMdx(src);
    assert.equal(status, 0);
    assert.equal(stderr, "");
    assert.equal(
      output,
      "Use `<!--` here.\n\n<TabItem>\n\n```xml\n<a/>\n```\n\n</TabItem>\n\nand `-->` there wrapped.\n",
    );
    await assertSameRender(src, output);
  });

  it("leaves a file it cannot parse untouched", () => {
    const src = "An unclosed {expression\nand wrapped\nprose.\n";
    const { output, stderr, status } = fixMdx(src);
    assert.equal(status, 0);
    assert.equal(fixMdx(src, ["--check"]).status, 1);
    assert.equal(output, src);
    assert.match(
      stderr,
      /skipped .*t\.mdx: not valid MDX at line \d+: Unexpected end of file in expression/,
    );
  });
});

describe("the invariant fix-mdx.mjs checks before writing", () => {
  const base = "A paragraph\nwrapped.\n\n```sh\nx  y\n```\n";
  it("holds across a join and a blank line beside a fence", () => {
    assert.notEqual(mdxInvariant(base), null);
    assert.equal(mdxInvariant("A paragraph wrapped.\n\n\n```sh\nx  y\n```\n"), mdxInvariant(base));
  });
  it("breaks when a paragraph splits", () => {
    assert.notEqual(
      mdxInvariant("A paragraph\n\nwrapped.\n\n```sh\nx  y\n```\n"),
      mdxInvariant(base),
    );
  });
  it("breaks when only the whitespace inside code changes", () => {
    assert.notEqual(mdxInvariant("A paragraph\nwrapped.\n\n```sh\nx y\n```\n"), mdxInvariant(base));
  });
  it("breaks when a non-breaking space goes", () => {
    assert.notEqual(mdxInvariant("a\u00a0\nb\n"), mdxInvariant("a b\n"));
  });
  it("breaks when the front matter changes", () => {
    assert.notEqual(mdxInvariant("---\nt: a\n---\n\nx\n"), mdxInvariant("---\nt:  a\n---\n\nx\n"));
  });
  it("is null for a file that does not parse", () => {
    assert.equal(mdxInvariant("{unclosed\n"), null);
  });
});

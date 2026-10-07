// WH001/no-hard-wrapped-prose — one paragraph is one line.
//
// Hard-wrapped prose (a paragraph broken at ~72-80 columns) makes every later
// edit rewrap the whole block, so a one-word change shows up as a five-line
// diff. AI-authored docs arrive wrapped by default, which is what motivated
// this rule; the fix is mechanical, so it autofixes rather than nagging.
//
// Paragraphs come from a parser, not from line shape: markdownlint's own
// micromark tokens for .md, and an MDX parse (./lib/mdx.mjs) for .mdx, where
// CommonMark's reading is wrong. So tables, code, headings, setext underlines,
// math, asides, JSX, ESM and indented code are untouched by construction, and a
// nested list item indented four spaces is joined like any other (an earlier
// line-shape version read it as indented code and skipped it). Within a
// paragraph, the few lines below are still kept apart: a paragraph left wrapped
// is a nit, a corrupted one is data loss. Every case has a fixture in
// rules.test.mjs.

import { descendants, hasAncestor, parseMdx, unmaskedLines } from "./lib/mdx.mjs";

// A line ending inside one of these is not interchangeable with a space: JS
// (expressions, JSX attributes) can carry a `//` comment, TeX a `%` comment, a
// link title, image alt text or directive attribute keeps its newline, and
// inline HTML passes through verbatim.
const VERBATIM_INLINE = new Set([
  "directiveTextAttributes",
  "htmlText",
  "image",
  "mathText",
  "mdxJsxTextTag",
  "mdxTextExpression",
  "resource",
]);

// Blockquote prose would need its `>` markers rewritten, and an HTML block is
// HTML to CommonMark however much it looks like prose.
const SKIPPED_CONTAINERS = new Set(["blockQuote", "htmlFlow"]);

// Inside these a line break is content: it renders, or ends a statement. MDX
// calls their text a paragraph, at any depth and even mid-line.
const PREFORMATTED = new Set(["pre", "script", "style", "textarea"]);

// A line that opens with a tag keeps its own line. Joining it would be
// render-neutral, but the parser calls `<span>…</span>` lines inside a layout
// `<div>` a paragraph, and one 300-column line of markup reads worse than five.
const TAGS = new Set(["htmlText", "mdxJsxTextTag"]);

// A link reference definition cannot interrupt a paragraph, so one written
// straight under prose is already broken; joining it would bury it mid-line.
const DEFINITION = /^[ \t]*\[[^\]]+\]:[ \t]/;

const HARD_BREAK = /(\\| {2})$/;

// A code span turns a line ending into one space but keeps the spaces around
// it (the docs' parser keeps the next line's indent too), and a line ending can
// be its padding. So the only line ending inside one that is ours to join is a
// plain one with code, not whitespace, on both sides; container prefixes (a list
// indent, a `>`) are tokens of their own and don't count.
function joinableInCode(codeText, line) {
  const children = codeText.children;
  const i = children.findIndex((t) => t.type === "lineEnding" && t.startLine === line);
  if (i < 0) return false;
  const before = children[i - 1];
  const after = children.slice(i + 1).find((t) => t.type.startsWith("codeText"));
  return (
    before?.type === "codeTextData" &&
    /[^ \t]$/.test(before.text) &&
    after?.type === "codeTextData" &&
    /^[^ \t]/.test(after.text)
  );
}

/** In an MDX parse, each line whose line ending falls inside a PREFORMATTED element. */
function preformattedLineEnds(tokens) {
  const ends = new Set();
  let depth = 0;
  let from = 0;
  for (const tag of descendants(tokens)) {
    if (tag.type !== "mdxJsxFlowTag" && tag.type !== "mdxJsxTextTag") continue;
    const child = (suffix) => tag.children.find((t) => t.type === tag.type + suffix);
    if (!PREFORMATTED.has(child("Name")?.text.trim()) || child("SelfClosingMarker")) continue;
    if (!child("ClosingMarker")) {
      if (depth++ === 0) from = tag.endLine;
    } else if (depth > 0 && --depth === 0) {
      for (let line = from; line < tag.startLine; line++) ends.add(line);
    }
  }
  return ends;
}

/** Each `[first, last]` (1-based, inclusive) run of lines that should be one. */
function wrappedRuns(tokens, lines, preformatted) {
  const runs = [];
  for (const paragraph of descendants(tokens)) {
    if (paragraph.type !== "paragraph" || paragraph.startLine === paragraph.endLine) continue;
    if (hasAncestor(paragraph, SKIPPED_CONTAINERS)) continue;

    // `breaks` holds line N when line N must not be joined to line N + 1;
    // `verbatimEnd` the subset whose line ending sits inside verbatim inline
    // content, where even trailing whitespace is not ours to trim.
    const breaks = new Set();
    const verbatimEnd = new Set();
    const isolate = (from, to) => {
      for (let line = from - 1; line <= to; line++) breaks.add(line);
    };
    for (let line = paragraph.startLine; line < paragraph.endLine; line++) {
      if (!preformatted.has(line)) continue;
      breaks.add(line);
      verbatimEnd.add(line);
    }
    const inner = [...descendants(paragraph.children)];
    for (const token of inner) {
      if (token.type === "hardBreakEscape" || token.type === "hardBreakTrailing") {
        breaks.add(token.startLine);
      } else if (token.type === "htmlText" && token.text.startsWith("<!--")) {
        // An inline-config comment governs its own line, so it never moves.
        isolate(token.startLine, token.endLine);
      } else if (token.type === "lineEnding" && hasAncestor(token, VERBATIM_INLINE, paragraph)) {
        breaks.add(token.startLine);
        verbatimEnd.add(token.startLine);
      } else if (token.type === "codeText") {
        for (let line = token.startLine; line < token.endLine; line++) {
          if (joinableInCode(token, line)) continue;
          breaks.add(line);
          verbatimEnd.add(line);
        }
      }
    }
    for (let line = paragraph.startLine; line <= paragraph.endLine; line++) {
      const text = lines[line - 1];
      const column =
        line === paragraph.startLine ? paragraph.startColumn : text.search(/[^ \t]/) + 1;
      const opensWithTag = inner.some(
        (t) => TAGS.has(t.type) && t.startLine === line && t.startColumn === column,
      );
      if (opensWithTag || (line > paragraph.startLine && DEFINITION.test(text))) {
        isolate(line, line);
      }
    }

    let first = paragraph.startLine;
    for (let line = first; line <= paragraph.endLine; line++) {
      if (line < paragraph.endLine && !breaks.has(line)) continue;
      if (line > first) runs.push({ first, last: line, keepTrailing: verbatimEnd.has(line) });
      first = line + 1;
    }
  }
  return runs;
}

export default {
  names: ["WH001", "no-hard-wrapped-prose"],
  description: "Prose paragraphs must not be hard-wrapped (one paragraph = one line)",
  tags: ["whitespace", "prose"],
  parser: "micromark",
  function: (params, onError) => {
    // The fix is built from these lines, so they must be the real ones: in
    // `params.lines` everything between `<!--` and `-->` is masked, even inside
    // code spans and across paragraphs. Unrecoverable text gets reports only.
    const source = unmaskedLines(params);
    const lines = source ?? params.lines;
    let tokens = params.parsers.micromark.tokens;
    let preformatted = new Set();
    if (params.name.endsWith(".mdx")) {
      const mdx = parseMdx(lines);
      if (mdx.error) {
        onError({
          lineNumber: Math.min(Math.max(mdx.error.line, 1), lines.length),
          detail: `not checked: the file does not parse as MDX (${mdx.error.reason})`,
        });
        return;
      }
      tokens = mdx.tokens;
      preformatted = preformattedLineEnds(tokens);
    }

    for (const { first, last, keepTrailing } of wrappedRuns(tokens, lines, preformatted)) {
      const head = lines[first - 1].replace(/[ \t]+$/, "");
      const parts = [];
      for (let line = first + 1; line <= last; line++) {
        const continuation = lines[line - 1].replace(/^[ \t]+/, "");
        // The run's last line may carry a hard break (`\` or two spaces) or end
        // inside verbatim content; either way its trailing whitespace stays.
        const keep = line === last && (keepTrailing || HARD_BREAK.test(continuation));
        parts.push(keep ? continuation : continuation.replace(/[ \t]+$/, ""));
      }

      // One edit carrying the whole joined remainder, plus a delete per
      // continuation line. Appending each line to its immediate predecessor
      // instead would lose text, since that predecessor is itself deleted.
      const fix = (fixInfo) => (source ? { fixInfo } : {});
      onError({
        lineNumber: first,
        detail: `paragraph is hard-wrapped across ${parts.length + 1} lines`,
        ...fix({
          editColumn: head.length + 1,
          deleteCount: lines[first - 1].length - head.length,
          insertText: ` ${parts.join(" ")}`,
        }),
      });
      for (let line = first + 1; line <= last; line++) {
        onError({
          lineNumber: line,
          detail: "continuation of a hard-wrapped paragraph",
          ...fix({ deleteCount: -1 }),
        });
      }
    }
  },
};

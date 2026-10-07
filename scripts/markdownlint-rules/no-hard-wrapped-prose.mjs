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

import { descendants, hasAncestor, parseMdx } from "./lib/mdx.mjs";

// A line ending inside one of these is not interchangeable with a space: JS
// (expressions, JSX attributes) can carry a `//` comment, TeX a `%` comment, a
// link title keeps its newline, and inline HTML passes through verbatim.
const VERBATIM_INLINE = new Set([
  "mdxTextExpression",
  "mdxJsxTextTag",
  "mathText",
  "htmlText",
  "resource",
]);

// Blockquote prose would need its `>` markers rewritten, and an HTML block is
// HTML to CommonMark however much it looks like prose.
const SKIPPED_CONTAINERS = new Set(["blockQuote", "htmlFlow"]);

// markdownlint hands rules a MASKED copy of every HTML comment's interior, and
// the fix is built from those lines, so joining a line that touches a comment
// would write the mask back over the comment's text.
const COMMENT = /<!--|-->/;

// A link reference definition cannot interrupt a paragraph, so one written
// straight under prose is already broken; joining it would bury it mid-line.
const DEFINITION = /^\s*\[[^\]]+\]:\s/;

// A line that opens with a tag keeps its own line. Joining it would be
// render-neutral, but the parser calls `<span>…</span>` lines inside a layout
// `<div>` a paragraph, and one 300-column line of markup reads worse than five.
const MARKUP = /^\s*</;

const HARD_BREAK = /(\\|\s\s)$/;

/** Each `[first, last]` (1-based, inclusive) run of lines that should be one. */
function wrappedRuns(tokens, lines) {
  const runs = [];
  for (const paragraph of descendants(tokens)) {
    if (paragraph.type !== "paragraph" || paragraph.startLine === paragraph.endLine) continue;
    if (hasAncestor(paragraph, SKIPPED_CONTAINERS)) continue;

    // `breaks` holds line N when line N must not be joined to line N + 1;
    // `verbatimEnd` the subset whose line ending sits inside verbatim inline
    // content, where even trailing whitespace is not ours to trim.
    const breaks = new Set();
    const verbatimEnd = new Set();
    for (const token of descendants(paragraph.children)) {
      if (token.type === "hardBreakEscape" || token.type === "hardBreakTrailing") {
        breaks.add(token.startLine);
      } else if (token.type === "lineEnding" && hasAncestor(token, VERBATIM_INLINE, paragraph)) {
        breaks.add(token.startLine);
        verbatimEnd.add(token.startLine);
      }
    }
    for (let line = paragraph.startLine; line <= paragraph.endLine; line++) {
      const text = lines[line - 1];
      if (
        COMMENT.test(text) ||
        MARKUP.test(text) ||
        (line > paragraph.startLine && DEFINITION.test(text))
      ) {
        breaks.add(line - 1);
        breaks.add(line);
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
    const { lines } = params;
    let tokens = params.parsers.micromark.tokens;
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
    }

    for (const { first, last, keepTrailing } of wrappedRuns(tokens, lines)) {
      const head = lines[first - 1].replace(/\s+$/, "");
      const parts = [];
      for (let line = first + 1; line <= last; line++) {
        const continuation = lines[line - 1].replace(/^\s+/, "");
        // The run's last line may carry a hard break (`\` or two spaces) or end
        // inside verbatim content; either way its trailing whitespace stays.
        const keep = line === last && (keepTrailing || HARD_BREAK.test(continuation));
        parts.push(keep ? continuation : continuation.replace(/\s+$/, ""));
      }

      // One edit carrying the whole joined remainder, plus a delete per
      // continuation line. Appending each line to its immediate predecessor
      // instead would lose text, since that predecessor is itself deleted.
      onError({
        lineNumber: first,
        detail: `paragraph is hard-wrapped across ${parts.length + 1} lines`,
        fixInfo: {
          editColumn: head.length + 1,
          deleteCount: lines[first - 1].length - head.length,
          insertText: ` ${parts.join(" ")}`,
        },
      });
      for (let line = first + 1; line <= last; line++) {
        onError({
          lineNumber: line,
          detail: "continuation of a hard-wrapped paragraph",
          fixInfo: { deleteCount: -1 },
        });
      }
    }
  },
};

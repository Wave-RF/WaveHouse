// WH001/no-hard-wrapped-prose — one paragraph is one line.
//
// Hard-wrapped prose (a paragraph broken at ~72-80 columns) makes every later
// edit rewrap the whole block, so a one-word change shows up as a five-line
// diff. AI-authored docs arrive wrapped by default, which is what motivated
// this rule; the fix is mechanical, so it autofixes rather than nagging.
//
// parser: "none" — this works on raw lines, never on a parse tree, so the same
// rule applies to .md and .mdx alike without markdownlint having to understand
// MDX (it doesn't: it parses CommonMark).
//
// That shape is also the hazard: every construct this rule must not touch has
// to be recognized from line shape alone, and a construct it fails to recognize
// is silently destroyed rather than merely missed. Anything added to classify()
// needs a fixture in rules.test.mjs — two corrupting bugs
// (pipe-less tables, short setext underlines) got through review on exactly
// that gap. Prefer skipping too much: a paragraph left wrapped is a nit, a
// joined table is data loss.

// Lines that open a block. A block line is never joined to the line above it,
// and (unless noted) never absorbs the line below.
const BLOCK = new RegExp(
  [
    "^\\s{0,3}#{1,6}\\s", // ATX heading
    "^\\s{0,3}>", // blockquote
    "^\\s{0,3}\\|", // pipe-led table row
    "^\\s*<", // HTML / JSX / comment
    "^\\s*:::", // Starlight aside (remark-directive)
    "^\\s{0,3}\\[[^\\]]+\\]:\\s", // link reference / footnote definition
    // Thematic break, and setext underlines — which may be a SINGLE character,
    // so `Title` + `=` is an h1 and joining it would demote it to a paragraph.
    "^\\s{0,3}(=+|-+|_{3,}|\\*{3,})\\s*$",
  ].join("|"),
);

// List markers. Unlike the rest of BLOCK these DO absorb their continuation
// lines — `- text` + `  more` is one item and belongs on one line. Joining only
// the continuations to each other (and not to the marker) would leave a
// half-wrapped item the rule could never touch again.
const LIST_ITEM = /^\s{0,3}([-*+]|\d+[.)])\s/;

// A GFM table delimiter row. Leading/trailing pipes are optional in GFM, so a
// table can be written without any line matching BLOCK's pipe-led alternative.
const TABLE_DELIM = /^\s{0,3}[|\s:-]*-[|\s:-]*$/;

// A paragraph line ending in a backslash or two spaces is an intentional hard
// break; joining it would change what renders.
const HARD_BREAK = /(\\|\s\s)$/;

const FENCE = /^\s{0,3}(`{3,}|~{3,})(.*)$/;
const ESM_OPEN = /^\s*(import|export)\s/;

/** Tag every line as prose / list / blank / block / skip. */
function classify(lines) {
  const kind = new Array(lines.length).fill("prose");
  // Frontmatter only counts when it is actually closed. A lone leading `---` is
  // a thematic break, and treating it as an unterminated block would mark every
  // remaining line "skip" and silently disable the rule for the whole file.
  const hasFrontmatter =
    lines[0]?.trim() === "---" && lines.slice(1).some((line) => line.trim() === "---");
  let fence = null;
  let frontmatter = false;
  let jsxTag = false;
  let esm = false;
  let htmlComment = false;
  let mathBlock = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    if (i === 0 && hasFrontmatter) {
      frontmatter = true;
      kind[i] = "skip";
      continue;
    }
    if (frontmatter) {
      kind[i] = "skip";
      if (line.trim() === "---") frontmatter = false;
      continue;
    }

    const fenceMatch = line.match(FENCE);
    if (fence) {
      kind[i] = "skip";
      // A closing fence is the same character, at least as long, no info string.
      if (
        fenceMatch &&
        fenceMatch[1][0] === fence[0] &&
        fenceMatch[1].length >= fence.length &&
        !fenceMatch[2].trim()
      ) {
        fence = null;
      }
      continue;
    }
    if (fenceMatch) {
      fence = fenceMatch[1];
      kind[i] = "skip";
      continue;
    }

    // $$ display math — remark-math + rehype-katex are wired into
    // docs/astro.config.mjs, so this is a supported construct here. Tracked
    // like a fence rather than matched per line: guarding only the delimiters
    // still lets the expressions between them be joined into one.
    if (mathBlock) {
      kind[i] = "skip";
      if (line.includes("$$")) mathBlock = false;
      continue;
    }
    if (/^\s{0,3}\$\$/.test(line)) {
      kind[i] = "skip";
      // A second $$ on the same line closes it there.
      if (line.split("$$").length - 1 < 2) mathBlock = true;
      continue;
    }

    // markdownlint hands rules a MASKED copy of any HTML comment's interior —
    // every non-whitespace character replaced by `.` — so that rules don't match
    // inside one. The fix's insertText is built from those same lines, so
    // joining a line that touches a comment writes the mask back to disk and
    // destroys the comment's text. Never join one.
    if (htmlComment) {
      kind[i] = "skip";
      if (line.includes("-->")) htmlComment = false;
      continue;
    }
    if (line.includes("<!--")) {
      kind[i] = "skip";
      if (!line.includes("-->")) htmlComment = true;
      continue;
    }

    if (!line.trim()) {
      kind[i] = "blank";
      esm = false; // an ESM block cannot span a blank line in MDX
      continue;
    }
    if (/^(\s{4,}\S|\t)/.test(line)) {
      kind[i] = "skip"; // indented code block
      continue;
    }

    // A multi-line ESM statement: `export const meta = {` … `};`. Only the
    // opener looks like ESM, so the body must be skipped by state, not shape —
    // joining it collapses any `//` comment over the rest of the statement and
    // breaks the MDX parse. The run ends at the blank line that separates ESM
    // from markdown; counting braces instead would miscount the ones inside
    // comments and string literals and end the skip early.
    if (esm) {
      kind[i] = "skip";
      continue;
    }
    if (ESM_OPEN.test(line)) {
      kind[i] = "skip";
      esm = true;
      continue;
    }

    // A JSX tag whose attributes span lines: everything up to the closing `>`
    // is markup, not prose (`<CloudCta`, `variant="band"`, `/>`).
    if (jsxTag) {
      kind[i] = "skip";
      if (/>\s*$/.test(line)) jsxTag = false;
      continue;
    }
    if (/^\s*<[A-Za-z]/.test(line) && !/>\s*$/.test(line)) {
      kind[i] = "skip";
      jsxTag = true;
      continue;
    }

    // A GFM table, with or without leading pipes. The delimiter row is what
    // makes it a table, so look ahead one line: everything from the header to
    // the last contiguous row is off limits. Guarding only the delimiter row
    // would still let a join START there and swallow the body.
    if (
      i + 1 < lines.length &&
      lines[i + 1].includes("|") &&
      TABLE_DELIM.test(lines[i + 1]) &&
      line.includes("|")
    ) {
      kind[i] = "block";
      for (i++; i < lines.length && lines[i].trim() && lines[i].includes("|"); i++)
        kind[i] = "block";
      i--;
      continue;
    }

    if (LIST_ITEM.test(line)) kind[i] = "list";
    else if (BLOCK.test(line)) kind[i] = "block";
  }
  return kind;
}

export default {
  names: ["WH001", "no-hard-wrapped-prose"],
  description: "Prose paragraphs must not be hard-wrapped (one paragraph = one line)",
  tags: ["whitespace", "prose"],
  parser: "none",
  function: (params, onError) => {
    const { lines } = params;
    const kind = classify(lines);

    for (let i = 0; i < lines.length; i++) {
      if (kind[i] !== "prose" && kind[i] !== "list") continue;

      const first = i;
      const parts = [lines[i].replace(/\s+$/, "")];
      // Only plain prose continues a paragraph — a list item starts a new one.
      while (i + 1 < lines.length && kind[i + 1] === "prose" && !HARD_BREAK.test(lines[i])) {
        i++;
        const continuation = lines[i].replace(/^\s+/, "");
        // The guard above stops the run from continuing PAST a hard break, but
        // the line carrying it is still absorbed — and trimming its trailing
        // whitespace would delete the <br> it encodes. Keep it: the loop exits
        // on the next pass, so such a line is always the last part.
        parts.push(HARD_BREAK.test(continuation) ? continuation : continuation.replace(/\s+$/, ""));
      }
      if (parts.length === 1) continue;

      // One insert carrying the whole joined remainder, plus a delete per
      // continuation line. Appending each line to its immediate predecessor
      // instead would lose text, since that predecessor is itself deleted.
      onError({
        lineNumber: first + 1,
        detail: `paragraph is hard-wrapped across ${parts.length} lines`,
        fixInfo: {
          editColumn: parts[0].length + 1,
          deleteCount: 0,
          insertText: ` ${parts.slice(1).join(" ")}`,
        },
      });
      for (let line = first + 1; line <= i; line++) {
        onError({
          lineNumber: line + 1,
          detail: "continuation of a hard-wrapped paragraph",
          fixInfo: { deleteCount: -1 },
        });
      }
    }
  },
};

// WH001/no-hard-wrapped-prose — one paragraph is one line.
//
// Hard-wrapped prose (a paragraph broken at ~72-80 columns) makes every later
// edit rewrap the whole block, so a one-word change shows up as a five-line
// diff. AI-authored docs arrive wrapped by default, which is what motivated
// this rule; the fix is mechanical, so it autofixes rather than nagging.
//
// parser: "none" — this works on raw lines, never on a parse tree, so the same
// rule applies to .md and .mdx alike without markdownlint having to understand
// MDX (it doesn't: it parses CommonMark). Everything that is not plain prose is
// skipped by shape, listed in SKIP/BLOCK below.
//
// Scope is docs prose only: .github/ and .claude/ opt out via their own
// .markdownlint.json, matching the line scripts/docs-prose.sh already draws.

// Lines that open a block whose next line must not be pulled up into it, and
// which must never be pulled up into a preceding paragraph.
const BLOCK = new RegExp(
  [
    "^\\s{0,3}#{1,6}\\s", // ATX heading
    "^\\s{0,3}>", // blockquote
    "^\\s{0,3}[-*+]\\s", // bullet list
    "^\\s{0,3}\\d+[.)]\\s", // ordered list
    "^\\s{0,3}\\|", // table row
    "^\\s*<", // HTML / JSX / comment
    "^\\s*:::", // Starlight aside (remark-directive)
    "^\\s*(import|export)\\s", // MDX ESM
    "^\\s{0,3}\\[[^\\]]+\\]:\\s", // link reference / footnote definition
    "^\\s{0,3}(-{3,}|={3,}|_{3,}|\\*{3,})\\s*$", // thematic break / setext underline
  ].join("|"),
);

// A table delimiter row written without leading pipes (`--|--`) — joining the
// header into it would silently destroy the table.
const TABLE_DELIM = /^[\s:|-]+$/;

// A paragraph line ending in a backslash or two spaces is an intentional hard
// break; joining it would change what renders.
const HARD_BREAK = /(\\|\s\s)$/;

const FENCE = /^\s{0,3}(`{3,}|~{3,})(.*)$/;

/** Tag every line as prose / blank / skip, tracking multi-line constructs. */
function classify(lines) {
  const kind = new Array(lines.length).fill("prose");
  let fence = null;
  let frontmatter = false;
  let jsxTag = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    if (i === 0 && line.trim() === "---") {
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
      // A closing fence is the same character, at least as long, with no info string.
      if (fenceMatch && fenceMatch[1][0] === fence[0] && fenceMatch[1].length >= fence.length && !fenceMatch[2].trim()) {
        fence = null;
      }
      continue;
    }
    if (fenceMatch) {
      fence = fenceMatch[1];
      kind[i] = "skip";
      continue;
    }

    if (!line.trim()) {
      kind[i] = "blank";
      continue;
    }
    if (/^(\s{4,}\S|\t)/.test(line)) {
      kind[i] = "skip"; // indented code block
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

    if (BLOCK.test(line)) kind[i] = "block";
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
      if (kind[i] !== "prose") continue;

      const first = i;
      const parts = [lines[i].replace(/\s+$/, "")];
      while (
        i + 1 < lines.length &&
        kind[i + 1] === "prose" &&
        !HARD_BREAK.test(lines[i]) &&
        !TABLE_DELIM.test(lines[i + 1])
      ) {
        i++;
        parts.push(lines[i].trim());
      }
      if (parts.length === 1) continue;

      // The fix is expressed as one insert (the whole joined remainder appended
      // to the first line) plus a delete per continuation line. Appending each
      // line to its immediate predecessor instead would lose text, since that
      // predecessor is itself deleted.
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

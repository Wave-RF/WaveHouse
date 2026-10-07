// WH002/mdx-fence-needs-blank-line — an MDX code fence needs a blank line
// between it and a JSX tag, and anywhere CommonMark would read it as HTML.
//
// The shape this catches:
//
//     <TabItem label="YAML">
//     ```yaml
//     key: value    # a comment
//     ```
//     </TabItem>
//
// MDX renders that correctly — compiling both shapes with the same
// @mdx-js/mdx Astro uses produces identical output, so the site is fine either
// way. The blank line matters because markdownlint does NOT see what MDX sees:
// it parses CommonMark, where `<TabItem …>` opens an HTML block that runs to
// the next blank line. The fence inside is therefore not a code block to any
// generic rule, so the YAML's `#` comments read as ATX headings and
// MD022/MD023/MD026/MD034 rewrite the code — de-indenting it out of the block
// and turning bare URLs into autolinks. The blank line is what keeps the two
// parsers agreeing about where the code is.
//
// Reproducing it needs one more condition: while the fenced content has NO
// blank line, CommonMark's HTML block runs past the whole thing and the generic
// rules stay silent. The rewriting starts once a blank line inside the content
// ends that HTML block and exposes the remainder. A minimal sample without an
// interior blank line will therefore look harmless — that is the trap, not a
// counter-example.
//
// Both checks below read parse trees, never line shape:
//   - adjacency, the house style: in the MDX parse (./lib/mdx.mjs) the construct
//     directly above a fence is an opening JSX tag, or the one directly below it
//     is a closing tag;
//   - interop, the hazard itself: markdownlint's own CommonMark tokens put the
//     fence's opening line inside an HTML block. This is what catches a fence
//     glued to prose inside an open block (`<Aside>` / `Run this first:` /
//     fence), which no adjacency test can see.
//
// This rule reports (CI + editor). The *fix* is applied by scripts/fix-mdx.mjs,
// because the generic markdownlint fixers never run over .mdx at all —
// .markdownlint-cli2.jsonc globs .md only, and the .mdx glob lives on
// `lint:md`. Deliberately no fixInfo: were someone to run markdownlint --fix
// over .mdx by hand, it would apply the blank-line insert AND, in the same
// pass, the generic fixes computed against the swallowed-fence parse, repairing
// the symptom while corrupting the code. Reporting only keeps the violation
// visible for scripts/fix-mdx.mjs to repair.

import { descendants, parseMdx } from "./lib/mdx.mjs";

// What may sit between two flow constructs without separating them the way a
// blank line (`lineEndingBlank`) does.
const TRIVIA = new Set(["lineEnding", "linePrefix", "listItemIndent", "blockQuotePrefix"]);

function neighbor(tokens, token, step) {
  const siblings = token.parent?.children ?? tokens;
  for (let i = siblings.indexOf(token) + step; i >= 0 && i < siblings.length; i += step) {
    if (!TRIVIA.has(siblings[i].type)) return siblings[i];
  }
  return null;
}

function isJsxTag(token, closing) {
  if (token?.type !== "mdxJsxFlowTag") return false;
  const has = (type) => token.children.some((child) => child.type === type);
  return has("mdxJsxFlowTagClosingMarker") === closing && !has("mdxJsxFlowTagSelfClosingMarker");
}

const firstLine = (token) => {
  const [head, ...rest] = token.text.split("\n");
  return rest.length ? `${head.trim()} …` : head.trim();
};

export default {
  names: ["WH002", "mdx-fence-needs-blank-line"],
  description: "A code fence adjacent to a JSX tag needs a blank line between them",
  tags: ["code", "mdx"],
  parser: "micromark",
  function: (params, onError) => {
    // CommonMark has no JSX, so this only applies to MDX.
    if (!params.name.endsWith(".mdx")) return;

    const mdx = parseMdx(params.lines);
    if (mdx.error) {
      onError({
        lineNumber: Math.min(Math.max(mdx.error.line, 1), params.lines.length),
        detail: `not checked: the file does not parse as MDX (${mdx.error.reason})`,
      });
      return;
    }

    const htmlBlocks = [...descendants(params.parsers.micromark.tokens)].filter(
      (token) => token.type === "htmlFlow" && !token.text.startsWith("<!--"),
    );
    // Line → detail, so a fence caught by both checks is reported once.
    const found = new Map();
    for (const fence of descendants(mdx.tokens)) {
      if (fence.type !== "codeFenced") continue;

      const above = neighbor(mdx.tokens, fence, -1);
      const block = htmlBlocks.find(
        (b) => b.startLine <= fence.startLine && fence.startLine <= b.endLine,
      );
      if (isJsxTag(above, false) && above.endLine === fence.startLine - 1) {
        found.set(fence.startLine, `fence opens directly after ${firstLine(above)}`);
      } else if (block) {
        found.set(
          fence.startLine,
          `fence opens inside the HTML block CommonMark starts at ${firstLine(block)}, so it is not read as code`,
        );
      }

      const below = neighbor(mdx.tokens, fence, 1);
      if (isJsxTag(below, true) && below.startLine === fence.endLine + 1) {
        found.set(below.startLine, `${firstLine(below)} follows a fence directly`);
      }
    }

    for (const [lineNumber, detail] of [...found].sort(([a], [b]) => a - b)) {
      onError({ lineNumber, detail });
    }
  },
};

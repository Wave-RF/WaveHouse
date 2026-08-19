// WH002/mdx-fence-needs-blank-line — a code fence adjacent to a JSX tag needs a
// blank line between them.
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
// This rule reports (CI + editor). The *fix* is applied earlier, by
// scripts/fix-mdx-fences.mjs, because it has to land before any other fixer:
// until the blank line exists CommonMark sees no code block, so a YAML block's
// `#` comments parse as ATX headings and MD022/MD023/MD026/MD034 will happily
// "fix" them — de-indenting them out of the block and rewriting bare URLs
// inside what is meant to be verbatim code. markdownlint-cli2 always merges the
// nearest .markdownlint.json into a --config run, so a "WH002-only" markdownlint
// pass is not expressible; hence the standalone script. Both read the same
// detector in ./lib/mdx-fences.mjs.
//
// parser: "none" — fences are detected lexically, which is the only thing that
// still works once MDX and CommonMark disagree about what the file contains.

import { findFenceTagViolations } from "./lib/mdx-fences.mjs";

export default {
  names: ["WH002", "mdx-fence-needs-blank-line"],
  description: "A code fence adjacent to a JSX tag needs a blank line between them",
  tags: ["code", "mdx"],
  parser: "none",
  function: (params, onError) => {
    // CommonMark has no JSX, so this only applies to MDX.
    if (!params.name.endsWith(".mdx")) return;

    // Deliberately no fixInfo. A bare `markdownlint-cli2 --fix` would apply the
    // blank-line insert AND, in the same pass, the generic fixes computed
    // against the swallowed-fence parse — repairing the symptom while
    // corrupting the code. Reporting only keeps the violation visible until the
    // ordered pass in scripts/fix-mdx-fences.mjs runs.
    for (const { line, detail } of findFenceTagViolations(params.lines)) {
      onError({ lineNumber: line, detail });
    }
  },
};

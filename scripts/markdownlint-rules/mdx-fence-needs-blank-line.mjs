// WH002/mdx-fence-needs-blank-line — a code fence adjacent to a JSX tag needs a
// blank line between them.
//
// In MDX, markdown inside a JSX element is only parsed as markdown when a blank
// line separates it from the tag. Without one:
//
//     <TabItem label="YAML">
//     ```yaml
//     key: value    # a comment
//     ```
//     </TabItem>
//
// the fence is swallowed into the JSX block and the code renders raw. It is a
// silent failure — the build succeeds and the page is simply wrong — which is
// why it needs a lint rule rather than trust.
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

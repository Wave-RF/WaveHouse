// Shared detector for WH002 (MDX code fence glued to a JSX tag).
//
// One implementation, two consumers — the markdownlint rule next door (lint,
// CI, editor squiggles) and scripts/fix-mdx-fences.mjs (the only STRUCTURAL
// fixer allowed near an .mdx file — misspell still corrects spelling there).
// Keeping the logic here is
// what stops those two from drifting; see AGENTS.md §"DRY — one source of truth".

const CLOSE_TAG = /^\s*<\/[A-Za-z][^>]*>\s*$/; // </TabItem>
const FENCE = /^\s{0,3}(`{3,}|~{3,})(.*)$/;

/**
 * If the line above `index` ends a JSX *opening* tag, return that tag's text.
 *
 * The tag may span lines — `<TabItem` / `label="YAML">` is one opening tag, and
 * checking only the line above would see `label="YAML">`, match nothing, and
 * report just the closing-side violation. The fixer would then insert one of
 * the two blank lines the block needs and leave it broken.
 *
 * The candidate is validated as a WHOLE, because "starts with a tag, ends with
 * `>`" is not enough on its own: `<span>text</span>` satisfies it, and so does
 * an open tag followed by prose that happens to end in `>`. Both would make the
 * shared fixer insert a blank line into valid MDX. The joined text must
 * therefore be a single opening tag — exactly one `>`, at the very end, and no
 * `</` anywhere.
 *
 * Attribute values and expression containers are masked before that test, and
 * that masking is load-bearing rather than polish. A `>` inside them
 * (`label="a>b"`, `when={a > b}`, `onClick={() => f()}`) is ordinary JSX; without
 * masking such a tag is rejected on the OPENING side while the closing side
 * still fires, so the fixer inserts one of the two blank lines, WH002 falls
 * silent, and the generic pass then rewrites the code inside the fence. A miss
 * on both sides would be harmless; a miss on one side is the corruption this
 * whole rule exists to prevent.
 */
function openingTagAbove(lines, index) {
  const prev = lines[index - 1];
  // Must end a tag, and must not be self-closing (`<Foo />` has no children).
  if (prev === undefined || !/>\s*$/.test(prev) || /\/>\s*$/.test(prev)) return null;

  for (let i = index - 1; i >= 0; i--) {
    const line = lines[i];
    if (!line.trim()) return null; // a blank line ends the run — nothing to report
    if (/^\s*<\/[A-Za-z]/.test(line)) return null; // a closing tag, not an opening one
    if (!/^\s*<[A-Za-z]/.test(line)) continue; // keep walking back to the tag's first line

    const joined = lines
      .slice(i, index)
      .map((l) => l.trim())
      .join(" ");
    // Mask strings first, so a brace inside a quoted value can't confuse the
    // container pass. Then collapse `{…}` innermost-first to a fixpoint: a
    // single pass leaves the OUTER level of a nested container intact, and a `>`
    // between the two levels survives — `{() => open({tab: 1})}` is the shape.
    // The replacement is brace-free so the loop can actually converge.
    let masked = joined.replace(/"[^"]*"|'[^']*'/g, '""');
    for (let previous; previous !== masked; ) {
      previous = masked;
      masked = masked.replace(/\{[^{}]*\}/g, '""');
    }
    if (masked.includes("</") || masked.indexOf(">") !== masked.length - 1) return null;
    return i === index - 1 ? joined : `${line.trim()} …`;
  }
  return null;
}

/**
 * Find every place an MDX code fence sits directly against a JSX tag.
 *
 * Fences are matched lexically rather than from a parse tree, because the whole
 * point is that MDX and CommonMark disagree about what the file contains once
 * this bug is present.
 *
 * @param {string[]} lines
 * @returns {{line: number, detail: string}[]} 1-based lines needing a blank line inserted ABOVE them
 */
export function findFenceTagViolations(lines) {
  const found = [];
  let fence = null;

  lines.forEach((line, i) => {
    const match = line.match(FENCE);
    if (!match) return;

    if (fence === null) {
      fence = match[1];
      const tag = openingTagAbove(lines, i);
      if (tag) found.push({ line: i + 1, detail: `fence opens directly after ${tag}` });
      return;
    }

    // A closing fence: same character, at least as long, no info string.
    const closes = match[1][0] === fence[0] && match[1].length >= fence.length && !match[2].trim();
    if (!closes) return;
    fence = null;

    if (i + 1 < lines.length && CLOSE_TAG.test(lines[i + 1])) {
      found.push({ line: i + 2, detail: `${lines[i + 1].trim()} follows a fence directly` });
    }
  });

  return found;
}

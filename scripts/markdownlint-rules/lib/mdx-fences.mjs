// Shared detector for WH002 (MDX code fence glued to a JSX tag).
//
// One implementation, two consumers — the markdownlint rule next door (lint,
// CI, editor squiggles) and scripts/fix-mdx-fences.mjs (the autofix pass that
// has to run before markdownlint touches the file). Keeping the logic here is
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
 * `</` anywhere. An attribute value containing a literal `>` is rejected too;
 * that is a miss rather than a corruption, which is the direction to err in.
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
    if (joined.includes("</") || joined.indexOf(">") !== joined.length - 1) return null;
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

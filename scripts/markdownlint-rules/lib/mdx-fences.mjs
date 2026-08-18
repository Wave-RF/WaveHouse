// Shared detector for WH002 (MDX code fence glued to a JSX tag).
//
// One implementation, two consumers — the markdownlint rule next door (lint,
// CI, editor squiggles) and scripts/fix-mdx-fences.mjs (the autofix pass that
// has to run before markdownlint touches the file). Keeping the logic here is
// what stops those two from drifting; see AGENTS.md §"DRY — one source of truth".

const OPEN_TAG = /^\s*<[A-Za-z][^>]*[^/>]>\s*$/; // <TabItem label="YAML">
const CLOSE_TAG = /^\s*<\/[A-Za-z][^>]*>\s*$/; // </TabItem>
const FENCE = /^\s{0,3}(`{3,}|~{3,})(.*)$/;

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
      if (i > 0 && OPEN_TAG.test(lines[i - 1])) {
        found.push({ line: i + 1, detail: `fence opens directly after ${lines[i - 1].trim()}` });
      }
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

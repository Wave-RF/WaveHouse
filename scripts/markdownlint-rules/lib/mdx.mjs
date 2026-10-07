// Parsing shared by the repo-local rules and scripts/fix-mdx.mjs.
//
// markdownlint parses CommonMark, so its `parser: "micromark"` tokens are
// authoritative for .md but wrong for .mdx: JSX reads as HTML blocks, ESM as
// prose, and MDX has no indented code at all. parseMdx() runs the same micromark
// with the extensions the docs site compiles MDX with (remark-mdx, remark-gfm,
// plus the remark-directive Starlight uses for asides and the remark-math in
// docs/astro.config.mjs), and returns tokens in markdownlint's own shape, so a
// rule can walk either parse with the same code.

import helpers from "markdownlint-cli2/markdownlint/helpers";
import { parse, postprocess, preprocess } from "micromark";
import { directive } from "micromark-extension-directive";
import { gfm } from "micromark-extension-gfm";
import { math } from "micromark-extension-math";
import { mdxjs } from "micromark-extension-mdxjs";

// WH001 and WH002 run over the same file with the same frozen `params.lines`,
// so keying on that array unmasks and parses each file once.
const unmaskCache = new WeakMap();
const parseCache = new WeakMap();

/**
 * The file's real lines, or null if they cannot be recovered.
 *
 * markdownlint hands rules `params.lines` with everything from a `<!--` to the
 * next `-->` replaced by dots — across paragraphs, and inside code spans too —
 * while its micromark tokens are cut from the unmasked text. Laying each
 * top-level token's text back at its position recovers the source; a result
 * that does not mask back to `params.lines` exactly is not trusted.
 *
 * @param {{lines: readonly string[], parsers: {micromark: {tokens: object[]}}}} params
 * @returns {readonly string[] | null}
 */
export function unmaskedLines(params) {
  if (unmaskCache.has(params.lines)) return unmaskCache.get(params.lines);
  const lines = [...params.lines];
  for (const token of params.parsers.micromark.tokens) {
    token.text.split(helpers.newLineRe).forEach((part, i) => {
      const index = token.startLine - 1 + i;
      if (lines[index] === undefined) return;
      const column = i === 0 ? token.startColumn - 1 : 0;
      lines[index] =
        lines[index].slice(0, column) + part + lines[index].slice(column + part.length);
    });
  }
  const trusted = helpers.clearHtmlCommentText(lines.join("\n")) === params.lines.join("\n");
  const result = trusted ? Object.freeze(lines) : null;
  unmaskCache.set(params.lines, result);
  return result;
}

/**
 * Parse MDX source lines into micromark tokens shaped like markdownlint's.
 *
 * MDX can fail to parse (an unclosed `{`, a stray `<`), and a rule that cannot
 * see the structure must not guess at it, so the error is returned rather than
 * thrown: `{ tokens }` on success, `{ error }` with a 1-based `error.line`
 * otherwise.
 *
 * @param {readonly string[]} lines
 * @returns {{tokens: object[]} | {error: {line: number, reason: string}}}
 */
export function parseMdx(lines) {
  let result = parseCache.get(lines);
  if (!result) {
    try {
      result = { tokens: toTokens(lines.join("\n")) };
    } catch (err) {
      result = { error: { line: err.line ?? 1, reason: err.reason ?? String(err) } };
    }
    parseCache.set(lines, result);
  }
  return result;
}

function toTokens(markdown) {
  const extensions = [mdxjs(), gfm(), directive(), math()];
  const chunks = preprocess()(markdown, undefined, true);
  const events = postprocess(parse({ extensions }).document().write(chunks));
  const root = { children: [] };
  let current = root;
  for (const [kind, token] of events) {
    if (kind === "exit") {
      current = current.parent ?? root;
      continue;
    }
    const node = {
      type: token.type,
      startLine: token.start.line,
      startColumn: token.start.column,
      endLine: token.end.line,
      endColumn: token.end.column,
      text: markdown.slice(token.start.offset, token.end.offset),
      children: [],
      parent: current === root ? null : current,
    };
    current.children.push(node);
    current = node;
  }
  return root.children;
}

/** Every token under `tokens`, depth first, in document order. */
export function* descendants(tokens) {
  for (const token of tokens) {
    yield token;
    yield* descendants(token.children);
  }
}

/** Whether any ancestor of `token` below `stop` has a type in `types`. */
export function hasAncestor(token, types, stop = null) {
  for (let node = token.parent; node && node !== stop; node = node.parent) {
    if (types.has(node.type)) return true;
  }
  return false;
}

/** Split off front matter exactly as markdownlint does, so line numbers agree. */
export function splitFrontMatter(text) {
  const match = text.match(helpers.frontMatterRe);
  const head = match && match.index === 0 ? match[0] : "";
  const headLines = head ? head.split(helpers.newLineRe) : [];
  if (headLines.at(-1) === "") headLines.pop();
  return { head, body: text.slice(head.length), offset: headLines.length };
}

const BLOCKS = new Set([
  "atxHeading",
  "blockQuote",
  "codeFenced",
  "definition",
  "directiveContainer",
  "directiveContainerFence",
  "directiveLeaf",
  "gfmFootnoteDefinition",
  "listItemPrefix",
  "listOrdered",
  "listUnordered",
  "mathFlow",
  "mdxFlowExpression",
  "mdxJsxFlowTag",
  "mdxjsEsm",
  "paragraph",
  "setextHeading",
  "table",
  "thematicBreak",
]);
const VERBATIM = new Set([
  "codeFenced",
  "directiveContainerFence",
  "mathFlow",
  "mdxFlowExpression",
  "mdxJsxFlowTag",
  "mdxjsEsm",
  "table",
]);

/**
 * What no MDX fix may change, as one comparable string: the front matter, the
 * block structure, every verbatim block byte for byte, and the text minus the
 * only whitespace a fix may touch (spaces, tabs, line breaks). Null if the text
 * does not parse as MDX.
 */
export function mdxInvariant(text) {
  const { head, body } = splitFrontMatter(text);
  const mdx = parseMdx(body.split(helpers.newLineRe));
  if (mdx.error) return null;
  const blocks = [...descendants(mdx.tokens)]
    .filter((t) => BLOCKS.has(t.type))
    .map((t) => (VERBATIM.has(t.type) ? `${t.type}\n${t.text}` : t.type));
  return JSON.stringify([head, blocks, text.replace(/[ \t\r\n]+/g, "")]);
}

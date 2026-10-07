// An MDX parse for the repo-local rules and scripts/fix-mdx.mjs.
//
// markdownlint parses CommonMark, so its `parser: "micromark"` tokens are
// authoritative for .md but wrong for .mdx: JSX reads as HTML blocks, ESM as
// prose, and MDX has no indented code at all. This runs the same micromark with
// the extensions the docs site compiles MDX with (remark-mdx, remark-gfm, plus
// the remark-directive Starlight uses for asides and the remark-math in
// docs/astro.config.mjs), and returns tokens in markdownlint's own shape, so a
// rule can walk either parse with the same code.

import { parse, postprocess, preprocess } from "micromark";
import { directive } from "micromark-extension-directive";
import { gfm } from "micromark-extension-gfm";
import { math } from "micromark-extension-math";
import { mdxjs } from "micromark-extension-mdxjs";

// WH001 and WH002 run over the same file with the same frozen `params.lines`,
// so keying on that array parses each file once.
const cache = new WeakMap();

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
  let result = cache.get(lines);
  if (!result) {
    try {
      result = { tokens: toTokens(lines.join("\n")) };
    } catch (err) {
      result = { error: { line: err.line ?? 1, reason: err.reason ?? String(err) } };
    }
    cache.set(lines, result);
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

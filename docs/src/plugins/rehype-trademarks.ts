// Appends ® / ™ to the FIRST mention of each third-party mark on a page.
//
// Doing this in a rehype pass rather than by hand in the prose is what keeps it
// true: authors write "ClickHouse", the symbol lands on whichever mention turns
// out to be first after an edit, and a page can't drift out of compliance by
// having its opening paragraph rewritten. The marks themselves live in
// src/config/trademarks.ts, shared with the footer notices.
//
// Deliberately NOT marked:
//   - code, pre, kbd, samp, var — a symbol inside a copy-pasteable command or a
//     JSON key would be a bug, not a notice.
//   - h1–h6 — Astro collects heading text for the TOC before this plugin runs,
//     so marking a heading desyncs the visible heading from its TOC entry.
//   - svg — Mermaid diagrams are already-rendered SVG by this point.
//   - .katex / .expressive-code — rendered math and code-block chrome.
//
// The symbol is wrapped in <span class="wh-tm"> (styled in global.css) so it
// superscripts without disturbing line height, and still copies as plain text.

import {
  findTrademarks,
  heroText,
  markFirstMentions,
  type PageFrontmatter,
  type Trademark,
} from "../config/trademarks";

/**
 * Structural subset of hast we touch. Hand-rolled rather than pulling in
 * @types/hast for one plugin — anything wider than this is passed through.
 */
interface HastNode {
  type: string;
  tagName?: string;
  value?: string;
  children?: HastNode[];
  properties?: Record<string, unknown>;
  /** MDX JSX element name — "LinkButton", or "div" for raw markup in .mdx. */
  name?: string | null;
  /** MDX JSX attributes. The .mdx counterpart of `properties`. */
  attributes?: { type: string; name?: string | null; value?: unknown }[];
}

const SKIP_TAGS = new Set([
  "code",
  "pre",
  "kbd",
  "samp",
  "var",
  "script",
  "style",
  "svg",
  "math",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
]);

// Same exclusion set global.css uses for the external-link arrow, plus the
// rendered-math and code-block wrappers. The shared reason: these are UI
// chrome, not prose. A ® on a "Star on GitHub" button reads as part of the
// label, and an aside's title is aria-hidden — spending the page's one marking
// there would mean the symbol never reaches the sentence a screen reader gets.
const SKIP_CLASSES = new Set([
  "katex",
  "expressive-code",
  "not-content",
  "sl-link-button",
  "sl-link-card",
  "starlight-aside__title",
]);

// The Starlight components whose text is a control label, not prose. Their
// rendered markup carries the classes above, but in .mdx we only see the
// unrendered JSX element, so they need naming too.
const SKIP_COMPONENTS = new Set(["LinkButton", "LinkCard"]);

/** Class names on a node, from hast `properties` or MDX JSX `attributes`. */
function classNames(node: HastNode): string[] {
  const property = node.properties?.className;
  if (Array.isArray(property)) return property.filter((c): c is string => typeof c === "string");

  // In .mdx, raw markup like `<div class="wh-stats not-content">` stays a JSX
  // node right through the rehype pass — no `properties`, one string `class`.
  const attribute = node.attributes?.find(
    (a) => a.type === "mdxJsxAttribute" && (a.name === "class" || a.name === "className"),
  );
  return typeof attribute?.value === "string" ? attribute.value.split(/\s+/) : [];
}

function isSkipped(node: HastNode): boolean {
  if (node.tagName && SKIP_TAGS.has(node.tagName)) return true;
  if (node.name && (SKIP_TAGS.has(node.name) || SKIP_COMPONENTS.has(node.name))) return true;
  return classNames(node).some((c) => SKIP_CLASSES.has(c));
}

function symbolNode(mark: Trademark): HastNode {
  return {
    type: "element",
    tagName: "span",
    properties: { className: ["wh-tm"] },
    children: [{ type: "text", value: mark.symbol }],
  };
}

/**
 * Split one text node around the first mention of each not-yet-marked mark.
 * Returns the original node untouched when there's nothing to mark, so pages
 * without third-party names come out of the pass byte-identical.
 */
function markText(node: HastNode, marked: Set<Trademark>): HastNode[] {
  const runs = markFirstMentions(node.value ?? "", marked);
  if (runs.length === 0) return [node];

  return runs.flatMap((run) =>
    run.mark
      ? [{ type: "text", value: run.text }, symbolNode(run.mark)]
      : [{ type: "text", value: run.text }],
  );
}

function walk(parent: HastNode, marked: Set<Trademark>): void {
  const children = parent.children;
  if (!children) return;

  const next: HastNode[] = [];
  for (const child of children) {
    if (child.type === "text") {
      next.push(...markText(child, marked));
      continue;
    }
    if (!isSkipped(child)) walk(child, marked);
    next.push(child);
  }
  parent.children = next;
}

/** Astro exposes the page's parsed frontmatter to plugins here. */
interface VFileLike {
  data?: { astro?: { frontmatter?: PageFrontmatter } };
}

/**
 * Rehype plugin. `marked` is per-file, which is what makes "first mention on
 * this page" the unit rather than "first mention on the site".
 *
 * It starts pre-seeded with whatever the splash hero names, because the hero
 * renders above the content from frontmatter — never through this pass — and
 * Hero.astro marks it there off the same registry. Without the seed, the
 * homepage would read "…API gateway for ClickHouse" in the hero and then mark
 * a later body mention instead, which is not the first occurrence a reader
 * actually sees.
 */
export function rehypeTrademarks() {
  return (tree: HastNode, file: VFileLike): void => {
    const frontmatter = file?.data?.astro?.frontmatter ?? {};
    const marked = new Set<Trademark>(findTrademarks(heroText(frontmatter)));
    walk(tree, marked);
  };
}

export default rehypeTrademarks;

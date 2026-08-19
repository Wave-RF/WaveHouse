// Third-party trademarks named in the docs — one registry, two consumers.
//
//   1. src/plugins/rehype-trademarks.ts appends the symbol to the FIRST mention
//      of each mark in a page's prose (once per page, never inside code blocks,
//      headings, or Mermaid diagrams).
//   2. src/components/Trademarks.astro renders the attribution notices in the
//      footer — but only for the marks that actually appear on THAT page, so a
//      page that never mentions Kubernetes doesn't carry a Kubernetes notice.
//
// Both read this file, so adding a mark here is the whole change: prose symbol
// and footer notice both follow.
//
// Two deliberate conservatisms, because getting these wrong is the only way
// this file can do harm:
//
//   - `symbol` is ® only where the owner's own notice says the mark is
//     registered. ™ (an unregistered common-law claim) is the default: marking
//     a registered mark with ™ is harmless, claiming ® for a mark that isn't
//     registered in a given jurisdiction is not.
//
//   - `owner` is OPTIONAL. A mark with no verified owner still gets its symbol
//     in prose, and is covered by the "all other trademarks are the property of
//     their respective owners" catch-all in the footer, rather than by a notice
//     that names the wrong company.
//
// Nothing here is legal advice — it's the customary notice practice. Worth a
// once-over by counsel before anything with real money attached.

export type TrademarkSymbol = "®" | "™";

export interface Trademark {
  /** Full name as it should read in the footer notice ("Apache Kafka"). */
  name: string;
  /**
   * How the mark is written in our prose, when that differs from `name` — we
   * write "Kafka", the notice says "Apache Kafka". Every alias is a candidate
   * for the symbol; the longest match at a given position wins, so listing
   * both "Grafana Loki" and "Loki" is safe.
   *
   * Defaults to [`name`].
   */
  aliases?: string[];
  /** ® only where registration is confirmed by the owner's own notice. */
  symbol: TrademarkSymbol;
  /** Legal owner, verbatim. Omit when unverified — see the header note. */
  owner?: string;
  /** Appended to the owner in the notice, e.g. "in the U.S. and other countries". */
  qualifier?: string;
  /**
   * Verbatim notice, used instead of the generated sentence. For owners who
   * publish required wording (Linux Mark Institute being the classic).
   */
  notice?: string;
}

export const TRADEMARKS: Trademark[] = [
  // ---- the one we front, and therefore the one with real confusion risk ----
  { name: "ClickHouse", symbol: "®", owner: "ClickHouse, Inc." },

  // ---- Linux Foundation / CNCF ----
  { name: "Kubernetes", symbol: "®", owner: "The Linux Foundation" },
  { name: "Prometheus", symbol: "®", owner: "The Linux Foundation" },
  { name: "OpenTelemetry", symbol: "™", owner: "The Linux Foundation" },
  { name: "Envoy", symbol: "™", owner: "The Linux Foundation" },
  { name: "Istio", symbol: "™", owner: "The Linux Foundation" },
  { name: "NATS", symbol: "™", owner: "The Linux Foundation" },
  { name: "Fluent Bit", symbol: "™", owner: "The Linux Foundation" },

  {
    name: "Linux",
    symbol: "®",
    owner: "Linus Torvalds",
    // Wording the Linux Mark Institute asks for, so it's used verbatim.
    notice: "Linux® is the registered trademark of Linus Torvalds in the U.S. and other countries.",
  },

  // ---- Grafana Labs ----
  { name: "Grafana", symbol: "®", owner: "Grafana Labs" },
  { name: "Grafana Loki", aliases: ["Grafana Loki", "Loki"], symbol: "™", owner: "Grafana Labs" },
  {
    name: "Grafana Tempo",
    aliases: ["Grafana Tempo", "Tempo"],
    symbol: "™",
    owner: "Grafana Labs",
  },
  {
    name: "Grafana Mimir",
    aliases: ["Grafana Mimir", "Mimir"],
    symbol: "™",
    owner: "Grafana Labs",
  },
  {
    name: "Grafana Pyroscope",
    aliases: ["Grafana Pyroscope", "Pyroscope"],
    symbol: "™",
    owner: "Grafana Labs",
  },
  { name: "Grafana Alloy", symbol: "™", owner: "Grafana Labs" },
  { name: "Promtail", symbol: "™", owner: "Grafana Labs" },

  // ---- Apache Software Foundation ----
  {
    name: "Apache Kafka",
    aliases: ["Apache Kafka", "Kafka"],
    symbol: "®",
    owner: "The Apache Software Foundation",
  },
  {
    name: "Apache Pulsar",
    aliases: ["Apache Pulsar", "Pulsar"],
    symbol: "™",
    owner: "The Apache Software Foundation",
  },
  // NB: no bare "Apache" entry. "Apache 2.0" is the license we ship under, and
  // a symbol on a license name would be wrong.

  // ---- everything else, alphabetical by owner ----
  {
    name: "Amazon Web Services",
    aliases: ["Amazon Web Services", "AWS"],
    symbol: "™",
    owner: "Amazon.com, Inc. or its affiliates",
  },
  // Two entries, not one with both aliases: the /claude-code page names the
  // product AND the model, and a single entry would put "Claude Code™" in the
  // prose while the notice below claimed only "Claude™".
  { name: "Claude", symbol: "™", owner: "Anthropic PBC" },
  { name: "Claude Code", symbol: "™", owner: "Anthropic PBC" },
  { name: "Cloudflare", symbol: "®", owner: "Cloudflare, Inc." },
  { name: "Datadog", symbol: "®", owner: "Datadog, Inc." },
  { name: "Docker", symbol: "®", owner: "Docker, Inc." },
  { name: "NGINX", aliases: ["NGINX", "Nginx"], symbol: "®", owner: "F5, Inc." },
  { name: "GitHub", symbol: "®", owner: "GitHub, Inc." },
  { name: "Hasura", symbol: "™", owner: "Hasura Inc." },
  { name: "HAProxy", symbol: "®", owner: "HAProxy Technologies" },
  { name: "TypeScript", symbol: "®", owner: "Microsoft Corporation" },
  { name: "Node.js", symbol: "®", owner: "the OpenJS Foundation" },
  {
    name: "PostgreSQL",
    aliases: ["PostgreSQL", "Postgres"],
    symbol: "®",
    owner: "the PostgreSQL Community Association of Canada",
  },
  { name: "Redis", symbol: "®", owner: "Redis Ltd." },
  // caddyserver/caddy README, verbatim: "Caddy is a registered trademark of
  // Stack Holdings GmbH." (ZeroSSL is the project's operator, not the holder.)
  { name: "Caddy", symbol: "®", owner: "Stack Holdings GmbH" },
  { name: "Supabase", symbol: "™", owner: "Supabase, Inc." },
  // tinybird.co/terms-and-conditions: "Tinybird, Inc., a Delaware Corporation".
  // ™ not ®: the entity is confirmed, a USPTO registration for the word mark is
  // not.
  { name: "Tinybird", symbol: "™", owner: "Tinybird, Inc." },
  { name: "Traefik", symbol: "™", owner: "Traefik Labs" },
  { name: "Vercel", symbol: "™", owner: "Vercel, Inc." },
];

/** The aliases of a mark, defaulting to its name. */
function aliasesOf(mark: Trademark): string[] {
  return mark.aliases ?? [mark.name];
}

const escapeRegExp = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// Longest alias first so "Grafana Alloy" wins over "Grafana", and "Claude Code"
// over "Claude", at the same start position.
const ALIAS_INDEX: ReadonlyMap<string, Trademark> = new Map(
  TRADEMARKS.flatMap((mark) => aliasesOf(mark).map((alias) => [alias, mark] as const)).sort(
    ([a], [b]) => b.length - a.length,
  ),
);

// Boundaries are hand-rolled rather than \b because several aliases end in a
// non-word character ("Node.js"), where \b would anchor to the wrong side.
// Letters/digits only: "ClickHouse-backed" and "ClickHouse's" should still match,
// but "clickhouse.com" (lowercase) and "NATSy" should not.
const PATTERN_SOURCE = `(?<![A-Za-z0-9])(?:${[...ALIAS_INDEX.keys()]
  .map(escapeRegExp)
  .join("|")})(?![A-Za-z0-9])`;

/**
 * A fresh sticky-free global matcher. Returned per call rather than shared
 * because `lastIndex` on a /g regex is mutable state, and both consumers scan
 * many strings.
 */
export function trademarkMatcher(): RegExp {
  return new RegExp(PATTERN_SOURCE, "g");
}

/** The mark an already-matched alias belongs to. */
export function trademarkFor(alias: string): Trademark | undefined {
  return ALIAS_INDEX.get(alias);
}

/**
 * Every mark named in `text`, deduped, in order of first appearance — which is
 * also the order the footer lists them, so the mark the page is actually about
 * leads its notice.
 */
export function findTrademarks(text: string): Trademark[] {
  const seen = new Set<Trademark>();
  const found: Trademark[] = [];
  for (const match of text.matchAll(trademarkMatcher())) {
    const mark = trademarkFor(match[0]);
    if (mark && !seen.has(mark)) {
      seen.add(mark);
      found.push(mark);
    }
  }
  return found;
}

/**
 * A run of text, plus the mark (if any) whose symbol belongs immediately after
 * it. Rendering is `runs.map(r => r.text + (r.mark ? symbol : ""))`.
 */
export interface MarkedRun {
  text: string;
  mark?: Trademark;
}

/**
 * Split `text` at the first mention of each mark NOT already in `marked`, and
 * record those marks in it. Returns [] when there's nothing to mark, so callers
 * can keep their original node/string untouched.
 *
 * `marked` is the page's running state, which is what makes "first mention on
 * this page" the unit. Passing a pre-seeded set is how the body pass avoids
 * re-marking something the splash hero already marked above it.
 */
export function markFirstMentions(text: string, marked: Set<Trademark>): MarkedRun[] {
  const runs: MarkedRun[] = [];
  let cursor = 0;

  for (const found of text.matchAll(trademarkMatcher())) {
    const mark = trademarkFor(found[0]);
    if (!mark || marked.has(mark)) continue;
    marked.add(mark);

    const end = (found.index ?? 0) + found[0].length;
    runs.push({ text: text.slice(cursor, end), mark });
    cursor = end;
  }

  if (runs.length === 0) return [];
  if (cursor < text.length) runs.push({ text: text.slice(cursor) });
  return runs;
}

/**
 * The frontmatter fields that become visible words on the page. Deliberately
 * NOT the whole object: `description` is `<meta>`-only and sidebar labels are
 * nav chrome, so listing a mark that appears in either would put a notice in
 * the footer for something the reader never sees on that page.
 */
export interface PageFrontmatter {
  title?: string;
  hero?: { title?: string; tagline?: string };
  cloudCta?: boolean | { title?: string; body?: string };
}

/**
 * Text rendered ABOVE the page content — the splash hero's tagline, and only
 * that. It must stay exactly what Hero.astro runs markFirstMentions() over:
 * the rehype pass pre-seeds itself from this, so anything counted here and not
 * marked there would lose its symbol on the page entirely.
 *
 * hero.title is excluded for that reason — Starlight renders it as the h1 and
 * it's the product's own name by construction, so there's nothing to mark.
 */
export function heroText(data: PageFrontmatter): string {
  return data.hero?.tagline ?? "";
}

/**
 * Every string that renders as visible words on the page, for the footer's
 * "which marks are on THIS page" question. Includes the Cloud CTA copy, which
 * Footer.astro renders just above the notice itself.
 */
export function pageText(data: PageFrontmatter, body: string): string {
  const cta = typeof data.cloudCta === "object" ? data.cloudCta : undefined;
  return [heroText(data), data.title, cta?.title, cta?.body, body].filter(Boolean).join("\n");
}

/** "a, b, and c" — serial comma, which is what the docs prose uses. */
function joinNames(names: string[]): string {
  if (names.length <= 1) return names[0] ?? "";
  if (names.length === 2) return `${names[0]} and ${names[1]}`;
  return `${names.slice(0, -1).join(", ")}, and ${names[names.length - 1]}`;
}

/**
 * One attribution sentence per owner, e.g.
 * "Kubernetes® and Prometheus® are registered trademarks of The Linux Foundation."
 *
 * Marks with no `owner` are skipped — the footer's catch-all covers them.
 */
export function trademarkNotices(marks: Trademark[]): string[] {
  const byOwner = new Map<string, Trademark[]>();
  for (const mark of marks) {
    if (!mark.owner) continue;
    const group = byOwner.get(mark.owner);
    if (group) group.push(mark);
    else byOwner.set(mark.owner, [mark]);
  }

  return [...byOwner].map(([owner, marks]) => {
    // Owners appear in the order the PAGE names them, but the marks inside one
    // owner's sentence follow registry order — so the parent brand leads its
    // own family ("Grafana®, Grafana Loki™ and Grafana Alloy™") instead of
    // landing wherever the page happened to mention it first.
    const group = [...marks].sort((a, b) => TRADEMARKS.indexOf(a) - TRADEMARKS.indexOf(b));
    // A publisher-mandated wording only makes sense when the mark stands alone.
    if (group.length === 1 && group[0]?.notice) return group[0].notice;

    const names = joinNames(group.map((m) => `${m.name}${m.symbol}`));
    // "registered" only when every mark in the sentence is; otherwise the
    // generic "trademarks", which is true of both kinds.
    const registered = group.every((m) => m.symbol === "®");
    const plural = group.length > 1;
    const verb = plural
      ? `are ${registered ? "registered " : ""}trademarks of`
      : `is a ${registered ? "registered " : ""}trademark of`;
    const qualifier = group.find((m) => m.qualifier)?.qualifier;
    return endSentence(`${names} ${verb} ${owner}${qualifier ? ` ${qualifier}` : ""}`);
  });
}

/** Terminate a sentence without doubling the period on "…, Inc." / "…Ltd.". */
function endSentence(sentence: string): string {
  return sentence.endsWith(".") ? sentence : `${sentence}.`;
}

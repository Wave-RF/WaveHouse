// Third-party trademarks named in the docs: the registry @wave-rf/astro-trademarks
// reads (wired in astro.config.mjs). It puts the symbol on the FIRST mention of
// each mark in a page's <main> (never in code, headings, diagrams, menus or the
// `not-content` footer), and fills the footer's <TrademarkNotices/> with exactly
// the marks it marked on that page, so a page that never mentions Kubernetes
// carries no Kubernetes notice. Adding a mark here is the whole change.
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
//     in prose, and is covered by the "all other … property of their respective
//     owners" catch-all in the footer, rather than by a notice that names the
//     wrong company.
//
// `aliases` are how a mark is written in our prose when that differs from
// `name` (we write "Kafka", the notice says "Apache Kafka"); the longest match
// at a position wins, so "Grafana Loki" and "Loki" can both be listed. Matching
// is case-sensitive. Field reference: the package README, "The registry".
//
// Nothing here is legal advice — it's the customary notice practice. Worth a
// once-over by counsel before anything with real money attached.

import { defineTrademarks } from "@wave-rf/astro-trademarks";

export default defineTrademarks([
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
]);

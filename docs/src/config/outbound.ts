// Links from the docs out to the other Wave RF properties — the holding-company
// site (wave-rf.com) and the managed service (wavehouse.cloud).
//
// Centralised because every such link needs the same two things, and both are
// easy to get wrong in a one-off <a>:
//
//  1. UTM params. PostHog on the destination reads them off the landing URL, so
//     `utm_source=wavehouse.dev` is what makes "came from the docs" show up
//     there at all, and `utm_content` names the exact placement that sent them.
//
//  2. rel="noopener" WITHOUT noreferrer. The Referer header is what PostHog
//     turns into `$referring_domain`, and `noreferrer` suppresses it outright —
//     which is exactly what we do NOT want on first-party links. `noopener`
//     alone still closes the reverse-tabnabbing hole, so dropping `noreferrer`
//     costs nothing. (Third-party links keep both — see REL_EXTERNAL.)
//
// Referer survives the trip because we never override Referrer-Policy, so the
// browser default `strict-origin-when-cross-origin` applies: cross-origin HTTPS
// destinations get the origin `https://wavehouse.dev`. That is origin-only, not
// the full path — which is why the per-placement detail rides in utm_content
// rather than being inferred from the referring URL.

/** Wave RF — the holding company / marketing site. */
export const WAVE_RF_URL = "https://wave-rf.com";

/** WaveHouse Cloud — the managed WaveHouse + ClickHouse service. */
export const CLOUD_URL = "https://wavehouse.cloud";

/**
 * Hosts we own. Links to these keep their Referer (see the header comment);
 * anything else is third-party and gets the full `noopener noreferrer`.
 */
const FIRST_PARTY_HOSTS = new Set([
  "wave-rf.com",
  "www.wave-rf.com",
  "wavehouse.cloud",
  "www.wavehouse.cloud",
]);

/** `rel` for first-party outbound links — no `noreferrer`, so PostHog attributes the visit. */
export const REL_KEEP_REFERRER = "noopener";

/** `rel` for third-party outbound links (GitHub, standards bodies, …). */
export const REL_EXTERNAL = "noopener noreferrer";

/**
 * True when `href` is an absolute URL pointing at a Wave RF property, so it
 * should keep its Referer and carry UTMs.
 *
 * Deliberately no base URL: a site-relative href like `/getting-started` is an
 * INTERNAL docs link, not an outbound one, and resolving it against a base
 * would make it look first-party and get it tagged with UTMs. Relative hrefs
 * throw here and fall through to `false`.
 */
export function isFirstParty(href: string): boolean {
  try {
    return FIRST_PARTY_HOSTS.has(new URL(href).hostname);
  } catch {
    return false;
  }
}

/** `rel` value appropriate for `href` — keeps the Referer only for our own hosts. */
export function relFor(href: string): string {
  return isFirstParty(href) ? REL_KEEP_REFERRER : REL_EXTERNAL;
}

type UtmOptions = {
  /** Campaign bucket. Defaults per helper below. */
  campaign?: string;
  /** The specific placement that sent the click, e.g. "footer-brand". */
  content?: string;
  /** Channel. "referral" for site chrome, "docs" for in-content CTAs. */
  medium?: string;
};

/**
 * Append our standard UTM params to an outbound URL.
 *
 * Existing query params and the fragment are preserved, so a URL that already
 * carries an anchor (`https://wavehouse.cloud/#waitlist`) still lands on it.
 */
export function withUtm(
  base: string,
  { campaign = "wavehouse-docs", content, medium = "referral" }: UtmOptions = {},
): string {
  const url = new URL(base);
  url.searchParams.set("utm_source", "wavehouse.dev");
  url.searchParams.set("utm_medium", medium);
  url.searchParams.set("utm_campaign", campaign);
  if (content) url.searchParams.set("utm_content", content);
  return url.href;
}

/**
 * Link to wave-rf.com, tagged so their PostHog sees the docs as the source.
 * `content` names the placement: "footer-brand", "homepage-org", …
 */
export function waveRfLink(content: string, path = "/"): string {
  return withUtm(new URL(path, WAVE_RF_URL).href, {
    campaign: "wavehouse-docs",
    content,
  });
}

/**
 * Link to wavehouse.cloud. `medium` defaults to "docs" (not "referral") so the
 * in-content "we'll run this for you" CTAs are separable from site chrome in
 * PostHog without having to pattern-match on utm_content.
 */
export function cloudLink(
  content: string,
  { path = "/", medium = "docs" }: { path?: string; medium?: string } = {},
): string {
  return withUtm(new URL(path, CLOUD_URL).href, {
    campaign: "docs-to-cloud",
    content,
    medium,
  });
}

/**
 * Tag an already-written href if — and only if — it points at one of our hosts,
 * picking the campaign from the destination. Non-first-party hrefs pass through
 * untouched.
 *
 * This exists for links authored somewhere that can't call the helpers above,
 * namely YAML frontmatter (the homepage hero actions). Routing those through
 * here means a first-party link can't ship untagged just because it was written
 * in frontmatter instead of a component.
 */
export function tagFirstParty(href: string, content: string, medium = "referral"): string {
  if (!isFirstParty(href)) return href;
  const campaign = new URL(href).hostname.endsWith("wavehouse.cloud")
    ? "docs-to-cloud"
    : "wavehouse-docs";
  return withUtm(href, { campaign, content, medium });
}

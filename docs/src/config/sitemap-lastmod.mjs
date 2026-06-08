import { execFileSync } from "node:child_process";
import { readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

// Per-page <lastmod> for @astrojs/sitemap, sourced from git so the sitemap
// reflects when each doc was actually last edited rather than when the site was
// built. Astro's default sitemap emits no <lastmod> at all; this fills it from
// the same git history Starlight's `lastUpdated` footer uses.
//
// This needs real git history at build time — CI checks out with
// `fetch-depth: 0` for exactly this reason. On a shallow clone git returns
// nothing for most files, so the page simply ships without a <lastmod>; it
// never emits a wrong (build-time) one. Google only trusts <lastmod> when it's
// consistently accurate, so "absent" is the right failure mode.

const CONTENT_DIR = fileURLToPath(new URL("../content/docs", import.meta.url));

// slug → ISO-8601 committer date of the file's last commit. Built once at
// config load (~one `git log` per page; a handful of pages).
const lastmodBySlug = buildLastmodMap();

function buildLastmodMap() {
  const map = new Map();
  let entries;
  try {
    entries = readdirSync(CONTENT_DIR, { recursive: true });
  } catch {
    return map; // no content dir (shouldn't happen) → no lastmods
  }
  for (const entry of entries) {
    const rel = String(entry);
    if (!/\.mdx?$/.test(rel)) continue;
    if (/(^|\/)404\.mdx?$/.test(rel)) continue; // 404 isn't in the sitemap
    const iso = gitLastCommitIso(rel);
    if (iso) map.set(slugFor(rel), iso);
  }
  return map;
}

// `api.md` → "api"; `index.mdx` (or any `.../index.md`) → its parent path, so
// the root index maps to "" — matching how Starlight routes content to URLs.
function slugFor(relPath) {
  return relPath
    .replace(/\.mdx?$/, "")
    .replace(/(^|\/)index$/, "$1")
    .replace(/^\/+|\/+$/g, "");
}

function gitLastCommitIso(relPath) {
  try {
    const out = execFileSync("git", ["log", "-1", "--format=%cI", "--", relPath], {
      cwd: CONTENT_DIR,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
    return out.trim() || null;
  } catch {
    return null;
  }
}

/**
 * @astrojs/sitemap `serialize` hook. Attaches our git-derived <lastmod> to an
 * entry, leaving it untouched (but never dropped) when we have no date for it.
 * @param {{ url: string, lastmod?: string }} item
 */
export function sitemapLastmod(item) {
  const slug = new URL(item.url).pathname.replace(/^\/+|\/+$/g, "");
  const lastmod = lastmodBySlug.get(slug);
  return lastmod ? { ...item, lastmod } : item;
}

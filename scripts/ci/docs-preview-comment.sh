#!/usr/bin/env bash
# Post/refresh the sticky docs-preview status comment on a PR — one
# comment, updated in place (matched by the marker) so re-pushes don't
# spam the thread. Called by ci.yml's docs-preview job, which checks out
# trusted MAIN — so this file executes from the default branch, never a
# PR tree (the job's trust model; see .github/workflows/README.md).
#
# Builds a skimmable status comment (one labelled bullet per fact).
# Commit metadata — subject, author names/emails, and the commit's
# own-timezone timestamp (%cI) — is read from git, not the REST API,
# which normalizes commit dates to UTC and drops the zone. GitHub
# @-logins aren't in git, so author/committer logins come from one
# commits-API call (GitHub matches them by verified email), used only
# when the call succeeds, with a fallback that reads the login out of
# a github.com noreply address. The working tree is main, so the PR
# head commit is fetched by SHA first. All user-controlled text
# (subject, names) is only ever a printf %s arg, never the format
# string, so a `%` can't act as a format directive.
#
# Env: GH_TOKEN, GITHUB_REPOSITORY, PR (number), SHA (PR head),
#      PREVIEW_OUTCOME (success|failure), URL (preview URL, may be empty).

set -uo pipefail

marker='<!-- docs-preview-comment -->'
git cat-file -e "${SHA}^{commit}" 2>/dev/null || git fetch --quiet origin "$SHA" 2>/dev/null || true
server="${GITHUB_SERVER_URL:-https://github.com}"
# @-link a person when a login is known, else show their plain name;
# login_from_email reads it out of [ID+]login@users.noreply.github.com.
gh_link() { if [ -n "$2" ]; then printf '[@%s](%s/%s)' "$2" "$server" "$2"; else printf '%s' "$1"; fi; }
login_from_email() { case "$1" in *@users.noreply.github.com) e="${1%@users.noreply.github.com}"; printf '%s' "${e#*+}";; esac; }

subject="$(git show -s --format=%s "$SHA" 2>/dev/null || true)"
[ -z "$subject" ] && subject='(commit subject unavailable)'
author_name="$(git show -s --format=%an "$SHA" 2>/dev/null || true)"
author_email="$(git show -s --format=%ae "$SHA" 2>/dev/null || true)"
committer_name="$(git show -s --format=%cn "$SHA" 2>/dev/null || true)"
committer_email="$(git show -s --format=%ce "$SHA" 2>/dev/null || true)"

# Logins, best-effort: one commits-API call (TSV: author, committer),
# used only on success so a transient API error can't leak its body
# into the comment; then a noreply-email fallback.
author_login=''; committer_login=''
if logins="$(gh api "repos/${GITHUB_REPOSITORY}/commits/${SHA}" --jq '[.author.login // "", .committer.login // ""] | @tsv' 2>/dev/null)"; then
  author_login="$(printf '%s' "$logins" | cut -f1)"
  committer_login="$(printf '%s' "$logins" | cut -f2)"
fi
[ -z "$author_login" ] && author_login="$(login_from_email "$author_email")"
[ -z "$committer_login" ] && committer_login="$(login_from_email "$committer_email")"

# Author(s): commit author, then Co-authored-by names, each @-linked
# when possible, de-duplicated by name.
authors="$(gh_link "$author_name" "$author_login")"
seen="|$author_name|"
while IFS= read -r line; do
  [ -z "$line" ] && continue
  cname="$(printf '%s' "$line" | sed -E 's/ *<[^>]*> *$//; s/[[:space:]]+$//')"
  cmail="$(printf '%s' "$line" | sed -nE 's/.*<([^>]*)>.*/\1/p')"
  [ -z "$cname" ] && continue
  case "$seen" in *"|$cname|"*) continue ;; esac
  authors="$authors, $(gh_link "$cname" "$(login_from_email "$cmail")")"
  seen="$seen$cname|"
done < <(git show -s --format=%B "$SHA" 2>/dev/null \
  | grep -iE '^[[:space:]]*Co-authored-by:' | sed -E 's/^[^:]*:[[:space:]]*//')
[ -z "$author_name" ] && authors='(author unavailable)'

# A distinct human committer (someone else applied the patch); skip
# GitHub's web-flow bot and anyone already credited above.
if [ -n "$committer_name" ] && [ "$committer_name" != "$author_name" ] && [ "$committer_name" != "GitHub" ]; then
  case "$seen" in
    *"|$committer_name|"*) ;;
    *) authors="$authors (committed by $(gh_link "$committer_name" "$committer_login"))" ;;
  esac
fi

# Commit time in its own timezone: 2026-06-09T10:04:27-04:00 -> 2026-06-09 10:04 (UTC-04:00)
ciso="$(git show -s --format=%cI "$SHA" 2>/dev/null || true)"
committed='(unknown)'
if [ -n "$ciso" ]; then
  day="${ciso%%T*}"; hm="${ciso#*T}"; hm="${hm:0:5}"
  case "$ciso" in *Z|*+00:00) off='UTC' ;; *) off="UTC${ciso: -6}" ;; esac
  committed="$day $hm ($off)"
fi
# Deploy time in the maintainer team's zone, DST-aware (EST/EDT); UTC
# if the runner lacks the zone. Distinct from the commit time — a
# re-run, or a push made long after authoring, makes them differ.
deployed="$(TZ='America/Detroit' date '+%Y-%m-%d %H:%M %Z')"
commit_url="$server/${GITHUB_REPOSITORY}/commit/${SHA}"

# Common bullets, built once (one labelled fact per line). `printf --`
# because the format starts with a list dash; user-controlled values
# are %s args here, and again in the final body printf below, so a `%`
# never reaches a format string.
# shellcheck disable=SC2016  # the markdown template legitimately holds %s placeholders in single quotes
bullets="$(printf -- '- **Commit** — [`%s`](%s): %s\n- **Author** — %s\n- **Committed** — %s' \
  "${SHA:0:7}" "$commit_url" "$subject" "$authors" "$committed")"
# Headline + final time-label reflect how the upload actually went.
case "$PREVIEW_OUTCOME/${URL:+url}" in
  success/url) headline="📚 **Docs preview is live** → $URL"; last="- **Deployed** — $deployed" ;;
  success/)    headline="📚 **Docs preview** uploaded, but its URL could not be parsed from the wrangler output — see the run log."; last="- **Deployed** — $deployed" ;;
  *)           headline="⚠️ **Docs preview failed to upload** (3 attempts) — any earlier preview is now stale. See the run log."; last="- **Upload failed** — $deployed" ;;
esac
body="$(printf '%s\n%s\n\n%s\n%s' "$marker" "$headline" "$bullets" "$last")"
cid="$(gh api "repos/${GITHUB_REPOSITORY}/issues/${PR}/comments" --paginate \
  --jq 'map(select(.body | startswith("<!-- docs-preview-comment -->"))) | .[0].id // empty' | head -1 || true)"
if [ -n "$cid" ]; then
  gh api -X PATCH "repos/${GITHUB_REPOSITORY}/issues/comments/${cid}" -f body="$body" >/dev/null
else
  gh api "repos/${GITHUB_REPOSITORY}/issues/${PR}/comments" -f body="$body" >/dev/null
fi
echo "Updated docs-preview comment on PR #${PR} (outcome=${PREVIEW_OUTCOME}, url=${URL:-none})"

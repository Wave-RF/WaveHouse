#!/usr/bin/env bash
# Regenerate every WaveHouse logo / favicon / social-card asset from a single
# source: scripts/branding/mark.svg.
#
# Outputs:
#   docs/public/favicon.svg              browser favicon, CSS-swaps light/dark
#   docs/public/favicon.ico              16/32/48 multi-resolution legacy fallback
#   docs/public/apple-touch-icon.png     180x180, white mark on solid teal (iOS)
#   docs/public/og.png                   1200x630 social card (rendered from og.template.svg)
#   docs/src/assets/branding/wavehouse-mark-light.svg   Starlight nav logo (light theme)
#   docs/src/assets/branding/wavehouse-mark-dark.svg    Starlight nav logo (dark theme)
#
# Edit scripts/branding/mark.svg (one or more SVG elements, single line, using
# `stroke="currentColor"` for any element that should pick up the brand color)
# or the COLOR_* variables below, then run `make branding`.
#
# Requires rsvg-convert (librsvg) and magick (ImageMagick 7+) on PATH:
#   brew install librsvg imagemagick
#   apt install librsvg2-bin imagemagick

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=scripts/_colors.sh
source "$ROOT/scripts/_colors.sh"

# --- Branding configuration ---------------------------------------------------
# Mark stroke colors — must coexist with --wh-accent in docs/src/styles/global.css.
COLOR_LIGHT='#086D77'        # deep teal — mark on light backgrounds (matches --wh-accent in light mode)
COLOR_DARK='#06B0BF'         # electric blue — mark on dark backgrounds (Wave RF parent brand)
# Apple-touch icon is a solid app-icon tile (iOS doesn't theme-swap home-screen icons).
COLOR_TOUCH_BG='#0A0B0E'
COLOR_TOUCH_FG='#06B0BF'
# OG card mark color — must match the brightest accent the template uses.
COLOR_OG='#06B0BF'
# OG card wordmark color — off-white for contrast against the mark accent.
# Picked to match the tagline / wordmark color elsewhere in the OG template.
COLOR_OG_WM='#F1F3F7'

# --- Source + output paths ----------------------------------------------------
MARK_SRC="$HERE/mark.svg"
LOCKUP_SRC="$HERE/lockup.svg"
OG_TEMPLATE="$HERE/og.template.svg"
OUT_PUBLIC="$ROOT/docs/public"
OUT_BRANDING="$ROOT/docs/src/assets/branding"

# --- Tool availability check --------------------------------------------------
missing=0
for tool in rsvg-convert magick; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf '%serror:%s %s not found on PATH\n' "$RED" "$RESET" "$tool" >&2
    missing=1
  fi
done
if [ "$missing" -ne 0 ]; then
  printf '%shint:%s  brew install librsvg imagemagick   (macOS)\n' "$YELLOW" "$RESET" >&2
  printf '       apt install librsvg2-bin imagemagick    (Debian/Ubuntu)\n' >&2
  exit 1
fi

# --- Parse the master mark + lockup -------------------------------------------
# Pull viewBox and the inner SVG content (everything between <svg ...> and
# </svg>) out of each source file. Both are kept on a single line so a one-pass
# sed extraction works — if you reformat them across lines, fix this parser.
#
# mark.svg is the standalone bug (used in favicon, apple-touch-icon, Starlight
# nav). lockup.svg is the horizontal mark+wordmark unit (used in the OG card
# so the wordmark's font metrics + mark scale + gap are all designed-in instead
# of laid out ad-hoc inside og.template.svg).
MARK_VIEWBOX=$(grep -oE 'viewBox="[^"]+"' "$MARK_SRC" | head -1 | sed 's/^viewBox="//;s/"$//')
MARK_INNER=$(sed -E 's|^.*<svg[^>]*>||; s|</svg>.*$||' "$MARK_SRC")
LOCKUP_INNER=$(sed -E 's|^.*<svg[^>]*>||; s|</svg>.*$||' "$LOCKUP_SRC")

if [ -z "$MARK_INNER" ] || [ -z "$MARK_VIEWBOX" ]; then
  printf '%serror:%s could not extract inner content or viewBox from %s\n' "$RED" "$RESET" "$MARK_SRC" >&2
  exit 1
fi
if [ -z "$LOCKUP_INNER" ]; then
  printf '%serror:%s could not extract inner content from %s\n' "$RED" "$RESET" "$LOCKUP_SRC" >&2
  exit 1
fi

# Helpers: print mark/lockup inner content with color placeholders swapped for
# real hex strings. Avoids forking a subshell per output.
#
# The lockup is intentionally two-color: `currentColor` is the mark stroke/fill
# (the "WaveHouse" pun-mark) and `__WM_FILL__` is the wordmark text. On
# light/dark single-color renders we pass the same hex for both so the lockup
# reads as one unit; on the OG card we pass an accent for the mark + white for
# the wordmark, which preserves the visual hierarchy of "logo + headline".
mark_with_color() {
  local color=$1
  printf '%s' "$MARK_INNER" | sed "s|currentColor|$color|g"
}
lockup_with_colors() {
  local mark_color=$1
  local wm_color=$2
  printf '%s' "$LOCKUP_INNER" | sed "s|currentColor|$mark_color|g; s|__WM_FILL__|$wm_color|g"
}

printf '%s==> Generating branding from %s%s\n' "$CYAN" "${MARK_SRC#"$ROOT/"}" "$RESET"

mkdir -p "$OUT_PUBLIC" "$OUT_BRANDING"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# --- 1. favicon.svg -----------------------------------------------------------
# Browsers respect prefers-color-scheme inside SVG favicons specifically — the
# <style> block sets `color` on :root, which the mark's `currentColor` stroke
# and fill attributes inherit (CSS color inheritance).
cat > "$OUT_PUBLIC/favicon.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$MARK_INNER<style>:root{color:$COLOR_LIGHT}@media (prefers-color-scheme:dark){:root{color:$COLOR_DARK}}</style></svg>
EOF

# --- 2. Starlight nav logos (light + dark) ------------------------------------
# Starlight loads these via <img>, where CSS media queries inside the SVG
# don't fire — so we ship two separate files and let Starlight swap them.
cat > "$OUT_BRANDING/wavehouse-mark-light.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_LIGHT")</svg>
EOF
cat > "$OUT_BRANDING/wavehouse-mark-dark.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_DARK")</svg>
EOF

# --- 3. favicon.ico (16 / 32 / 48 multi-resolution) ---------------------------
rsvg-convert -w 16 -h 16 "$OUT_PUBLIC/favicon.svg" -o "$TMP/fav-16.png"
rsvg-convert -w 32 -h 32 "$OUT_PUBLIC/favicon.svg" -o "$TMP/fav-32.png"
rsvg-convert -w 48 -h 48 "$OUT_PUBLIC/favicon.svg" -o "$TMP/fav-48.png"
magick "$TMP/fav-16.png" "$TMP/fav-32.png" "$TMP/fav-48.png" "$OUT_PUBLIC/favicon.ico"

# --- 4. apple-touch-icon.png (180x180, solid teal tile) -----------------------
# Mark scaled to ~70% of the tile, centred, white on teal. iOS clips to a
# rounded square automatically — keep the mark well clear of the edges.
cat > "$TMP/touch.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 180 180"><rect width="180" height="180" fill="$COLOR_TOUCH_BG"/><g transform="translate(28 28) scale(1.24)">$(mark_with_color "$COLOR_TOUCH_FG")</g></svg>
EOF
rsvg-convert -w 180 -h 180 "$TMP/touch.svg" -o "$OUT_PUBLIC/apple-touch-icon.png"

# --- 5. og.png (1200x630 social card) -----------------------------------------
# og.template.svg has the composition (gradient, accent grid, glow, tagline,
# footer row) baked in — we splice the canonical lockup (mark + wordmark unit)
# into its __LOCKUP_GROUP__ slot, recolored for the dark background.
# Escape `&` (special on the sed replacement side) but nothing else: SVG path
# data + attributes never contain `|`, so we can use `|` as the s delimiter.
lockup_og_escaped=$(lockup_with_colors "$COLOR_OG" "$COLOR_OG_WM" | sed 's/&/\\&/g')
sed "s|__LOCKUP_GROUP__|$lockup_og_escaped|" "$OG_TEMPLATE" > "$TMP/og.svg"
rsvg-convert -w 1200 -h 630 "$TMP/og.svg" -o "$OUT_PUBLIC/og.png"

# --- Report -------------------------------------------------------------------
for out in \
  "${OUT_PUBLIC#"$ROOT/"}/favicon.svg" \
  "${OUT_PUBLIC#"$ROOT/"}/favicon.ico" \
  "${OUT_PUBLIC#"$ROOT/"}/apple-touch-icon.png" \
  "${OUT_PUBLIC#"$ROOT/"}/og.png" \
  "${OUT_BRANDING#"$ROOT/"}/wavehouse-mark-light.svg" \
  "${OUT_BRANDING#"$ROOT/"}/wavehouse-mark-dark.svg"; do
  printf '  %s✓%s %s\n' "$GREEN" "$RESET" "$out"
done

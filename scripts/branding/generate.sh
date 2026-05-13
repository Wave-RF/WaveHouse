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
# Mark stroke colors — must coexist with $sl-color-accent in docs/src/styles/custom.css.
COLOR_LIGHT='#0e7f8f'        # deep teal — mark on light backgrounds
COLOR_DARK='#5bbfcf'         # bright cyan — mark on dark backgrounds
# Apple-touch icon is a solid app-icon tile (iOS doesn't theme-swap home-screen icons).
COLOR_TOUCH_BG='#0e7f8f'
COLOR_TOUCH_FG='#ffffff'
# OG card mark color — must match the brightest accent the template uses.
COLOR_OG='#5bbfcf'

# --- Source + output paths ----------------------------------------------------
MARK_SRC="$HERE/mark.svg"
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

# --- Parse the master mark ----------------------------------------------------
# Pull viewBox and the inner SVG content (everything between <svg ...> and
# </svg>) out of mark.svg. We keep mark.svg on a single line so a one-pass
# sed extraction works — if you reformat it across lines, fix this parser.
MARK_VIEWBOX=$(grep -oE 'viewBox="[^"]+"' "$MARK_SRC" | head -1 | sed 's/^viewBox="//;s/"$//')
MARK_INNER=$(sed -E 's|^.*<svg[^>]*>||; s|</svg>.*$||' "$MARK_SRC")

if [ -z "$MARK_INNER" ] || [ -z "$MARK_VIEWBOX" ]; then
  printf '%serror:%s could not extract inner content or viewBox from %s\n' "$RED" "$RESET" "$MARK_SRC" >&2
  exit 1
fi

# Helper: print the inner content with currentColor swapped for a hex string.
# Avoids forking a subshell per output by going through a function.
mark_with_color() {
  local color=$1
  printf '%s' "$MARK_INNER" | sed "s|currentColor|$color|g"
}

printf '%s==> Generating branding from %s%s\n' "$CYAN" "${MARK_SRC#"$ROOT/"}" "$RESET"

mkdir -p "$OUT_PUBLIC" "$OUT_BRANDING"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# --- 1. favicon.svg -----------------------------------------------------------
# Browsers respect prefers-color-scheme inside SVG favicons specifically — the
# <style> block sets stroke on every <g> in the mark, which (per CSS spec)
# overrides the presentation attribute stroke="currentColor" inside.
cat > "$OUT_PUBLIC/favicon.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$MARK_INNER<style>g{stroke:$COLOR_LIGHT}@media (prefers-color-scheme:dark){g{stroke:$COLOR_DARK}}</style></svg>
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
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 180 180"><rect width="180" height="180" fill="$COLOR_TOUCH_BG"/><g transform="translate(28 28) scale(1.9375)">$(mark_with_color "$COLOR_TOUCH_FG")</g></svg>
EOF
rsvg-convert -w 180 -h 180 "$TMP/touch.svg" -o "$OUT_PUBLIC/apple-touch-icon.png"

# --- 5. og.png (1200x630 social card) -----------------------------------------
# og.template.svg has the composition (gradient, wordmark, tagline, wave
# texture) baked in — we splice the mark's inner content into its
# __MARK_GROUP__ slot, recolored for the dark background.
# Escape `&` (special on the sed replacement side) but nothing else: SVG path
# data + attributes never contain `|`, so we can use `|` as the s delimiter.
mark_og_escaped=$(mark_with_color "$COLOR_OG" | sed 's/&/\\&/g')
sed "s|__MARK_GROUP__|$mark_og_escaped|" "$OG_TEMPLATE" > "$TMP/og.svg"
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

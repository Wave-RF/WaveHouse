#!/usr/bin/env bash
# Regenerate the WaveHouse brand kit from TWO single sources, with NO hardcoded
# colors anywhere in this script:
#   - shape:  docs/src/assets/branding/{mark,lockup,og.template}.svg
#   - color:  the --brand-* palette in docs/src/styles/global.css
#
# Everything is written to docs/public/branding/ — a copy-out-able brand kit
# served at /branding/* — plus docs/public/favicon.ico at the site root for the
# legacy auto-probe:
#   branding/favicon.svg            browser favicon, CSS-swaps light/dark
#   branding/favicon.ico            16/32/48 multi-resolution legacy fallback
#   branding/apple-touch-icon.png   180x180 app-icon tile (iOS)
#   branding/og.png                 1200x630 social card
#   branding/mark-light.svg         mark in the light-mode accent
#   branding/mark-dark.svg          mark in the dark-mode accent
#   branding/lockup-dark.svg        mark + wordmark, for dark backgrounds
#   favicon.ico                     (site root, for /favicon.ico auto-probe)
#
# To change the brand: edit a hue in docs/src/styles/global.css (--brand-*) or
# a source SVG, then run `make branding-docs` from the repo root. The live site
# picks up CSS instantly; this script propagates the same colors into the
# generated raster/SVG assets. Requires rsvg-convert (librsvg) and magick
# (ImageMagick 7+):
#   brew install librsvg imagemagick      (macOS)
#   apt install librsvg2-bin imagemagick  (Debian/Ubuntu)

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Repo root via git rather than a relative climb — survives the script moving.
ROOT="$(git -C "$HERE" rev-parse --show-toplevel)"
# shellcheck source=../../../scripts/_colors.sh
source "$ROOT/scripts/_colors.sh"   # terminal colors for output ($RED/$GREEN/…)

# --- Single color source: the --brand-* palette in global.css -----------------
# No brand hex lives in this script; we read each one out of the stylesheet so
# the assets and the live site can never drift.
GLOBAL_CSS="$ROOT/docs/src/styles/global.css"
brand_color() {
  local name=$1 val
  val=$(grep -oE -- "--$name:[[:space:]]*#[0-9A-Fa-f]{3,8}" "$GLOBAL_CSS" \
        | head -1 | grep -oE '#[0-9A-Fa-f]{3,8}')
  if [ -z "$val" ]; then
    printf '%serror:%s brand color --%s not found in %s\n' \
      "$RED" "$RESET" "$name" "${GLOBAL_CSS#"$ROOT/"}" >&2
    exit 1
  fi
  printf '%s' "$val"
}

COLOR_LIGHT=$(brand_color brand-blue-deep)   # mark on light backgrounds (AA)
COLOR_DARK=$(brand_color brand-blue)         # mark on dark backgrounds / accent
OG_BG=$(brand_color brand-bg)                # OG gradient start / touch tile bg
OG_SURFACE=$(brand_color brand-surface)      # OG gradient end
OG_INK=$(brand_color brand-ink)              # wordmark / OG headline text
OG_INK_MUTED=$(brand_color brand-ink-muted)  # OG tagline / footer text
COLOR_TOUCH_BG=$OG_BG
COLOR_TOUCH_FG=$COLOR_DARK

# --- Source + output paths ----------------------------------------------------
SRC="$ROOT/docs/src/assets/branding"
MARK_SRC="$SRC/mark.svg"
LOCKUP_SRC="$SRC/lockup.svg"
OG_TEMPLATE="$SRC/og.template.svg"
OUT_KIT="$ROOT/docs/public/branding"
OUT_ROOT="$ROOT/docs/public"

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
# Pull viewBox + inner content (between <svg ...> and </svg>) from each source.
# Both must stay single-line for this one-pass sed extraction.
MARK_VIEWBOX=$(grep -oE 'viewBox="[^"]+"' "$MARK_SRC" | head -1 | sed 's/^viewBox="//;s/"$//')
MARK_INNER=$(sed -E 's|^.*<svg[^>]*>||; s|</svg>.*$||' "$MARK_SRC")
LOCKUP_VIEWBOX=$(grep -oE 'viewBox="[^"]+"' "$LOCKUP_SRC" | head -1 | sed 's/^viewBox="//;s/"$//')
LOCKUP_INNER=$(sed -E 's|^.*<svg[^>]*>||; s|</svg>.*$||' "$LOCKUP_SRC")
if [ -z "$MARK_INNER" ] || [ -z "$MARK_VIEWBOX" ]; then
  printf '%serror:%s could not parse %s\n' "$RED" "$RESET" "$MARK_SRC" >&2; exit 1
fi
if [ -z "$LOCKUP_INNER" ]; then
  printf '%serror:%s could not parse %s\n' "$RED" "$RESET" "$LOCKUP_SRC" >&2; exit 1
fi

# Mark inner with `currentColor` swapped for a real hex. The lockup is two-color:
# `currentColor` is the mark, `__WM_FILL__` is the wordmark text.
mark_with_color() { printf '%s' "$MARK_INNER" | sed "s|currentColor|$1|g"; }
lockup_with_colors() { printf '%s' "$LOCKUP_INNER" | sed "s|currentColor|$1|g; s|__WM_FILL__|$2|g"; }

printf '%s==> Generating brand kit from %s + %s%s\n' \
  "$CYAN" "${MARK_SRC#"$ROOT/"}" "${GLOBAL_CSS#"$ROOT/"}" "$RESET"

mkdir -p "$OUT_KIT" "$OUT_ROOT"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# --- 1. favicon.svg (theme-swapping) ------------------------------------------
# Browsers honor prefers-color-scheme inside SVG favicons: the <style> sets
# `color` on :root, which the mark's currentColor stroke/fill inherit.
cat > "$OUT_KIT/favicon.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$MARK_INNER<style>:root{color:$COLOR_LIGHT}@media (prefers-color-scheme:dark){:root{color:$COLOR_DARK}}</style></svg>
EOF

# --- 2. Standalone marks (copy-out) -------------------------------------------
cat > "$OUT_KIT/mark-light.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_LIGHT")</svg>
EOF
cat > "$OUT_KIT/mark-dark.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_DARK")</svg>
EOF

# --- 3. lockup-dark.svg (mark + wordmark for dark backgrounds) -----------------
cat > "$OUT_KIT/lockup-dark.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$LOCKUP_VIEWBOX" role="img" aria-label="WaveHouse">$(lockup_with_colors "$COLOR_DARK" "$OG_INK")</svg>
EOF

# --- 4. favicon.ico (16/32/48) → kit + site root ------------------------------
rsvg-convert -w 16 -h 16 "$OUT_KIT/favicon.svg" -o "$TMP/fav-16.png"
rsvg-convert -w 32 -h 32 "$OUT_KIT/favicon.svg" -o "$TMP/fav-32.png"
rsvg-convert -w 48 -h 48 "$OUT_KIT/favicon.svg" -o "$TMP/fav-48.png"
magick "$TMP/fav-16.png" "$TMP/fav-32.png" "$TMP/fav-48.png" "$OUT_KIT/favicon.ico"
cp "$OUT_KIT/favicon.ico" "$OUT_ROOT/favicon.ico"

# --- 5. apple-touch-icon.png (180x180 solid tile) -----------------------------
cat > "$TMP/touch.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 180 180"><rect width="180" height="180" fill="$COLOR_TOUCH_BG"/><g transform="translate(28 28) scale(1.24)">$(mark_with_color "$COLOR_TOUCH_FG")</g></svg>
EOF
rsvg-convert -w 180 -h 180 "$TMP/touch.svg" -o "$OUT_KIT/apple-touch-icon.png"

# --- 6. og.png (1200x630 social card) -----------------------------------------
# Fill the template's color placeholders from --brand-*, splice the lockup
# (mark accent + wordmark ink) into __LOCKUP_GROUP__. Escape `&` (sed RHS).
lockup_og_escaped=$(lockup_with_colors "$COLOR_DARK" "$OG_INK" | sed 's/&/\\&/g')
sed -e "s|__BG__|$OG_BG|g" \
    -e "s|__SURFACE__|$OG_SURFACE|g" \
    -e "s|__ACCENT__|$COLOR_DARK|g" \
    -e "s|__INK_MUTED__|$OG_INK_MUTED|g" \
    -e "s|__LOCKUP_GROUP__|$lockup_og_escaped|" \
    "$OG_TEMPLATE" > "$TMP/og.svg"
rsvg-convert -w 1200 -h 630 "$TMP/og.svg" -o "$OUT_KIT/og.png"

# --- Report -------------------------------------------------------------------
printf '%s    colors%s light=%s dark=%s bg=%s ink=%s\n' \
  "$CYAN" "$RESET" "$COLOR_LIGHT" "$COLOR_DARK" "$OG_BG" "$OG_INK"
for out in favicon.svg favicon.ico apple-touch-icon.png og.png \
           mark-light.svg mark-dark.svg lockup-dark.svg; do
  printf '  %s✓%s %s\n' "$GREEN" "$RESET" "docs/public/branding/$out"
done
printf '  %s✓%s %s\n' "$GREEN" "$RESET" "docs/public/favicon.ico (site root)"

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
#   branding/favicon-{light,dark}.svg  static variants (Head.astro live swap)
#   branding/favicon.ico            16/32/48 multi-resolution legacy fallback
#   branding/apple-touch-icon.png   180x180 app-icon tile (iOS)
#   branding/avatar.png             512x512 solid tile (GitHub org/team avatar, Slack, …)
#   branding/og.png                 1200x630 social card
#   branding/mark-light.svg         mark in the light-mode accent
#   branding/mark-dark.svg          mark in the dark-mode accent
#   branding/lockup-dark.svg        mark + wordmark (outlined), dark backgrounds
#   branding/lockup-light.svg       mark + wordmark (outlined), light backgrounds
#   branding/mark-{light,dark}.png  1024x1024 transparent raster marks
#   branding/lockup-{light,dark}.png 1200-wide transparent raster lockups
#   favicon.ico                     (site root, for /favicon.ico auto-probe)
#
# To change the brand: edit a hue in docs/src/styles/global.css (--brand-*) or
# a source SVG, then run `make branding-docs` from the repo root. The live site
# picks up CSS instantly; this script propagates the same colors into the
# generated raster/SVG assets. Requires rsvg-convert (librsvg), magick
# (ImageMagick 7+), and resvg (text-bearing PNGs, pinned to the vendored Inter
# in fonts/Inter-Variable.ttf — see the INTER_FONT note below):
#   brew install librsvg imagemagick resvg     (macOS)
#   apt install librsvg2-bin imagemagick; cargo install resvg  (Debian/Ubuntu)

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
OG_INK=$(brand_color brand-ink)              # wordmark / OG headline text (dark bg)
OG_INK_MUTED=$(brand_color brand-ink-muted)  # OG tagline / footer text
WORDMARK_LIGHT=$(brand_color brand-charcoal) # lockup wordmark on light bg (dark ink)
COLOR_TOUCH_BG=$OG_BG
COLOR_TOUCH_FG=$COLOR_DARK
# The avatar tile (GitHub org/team, Slack, …) sits on a white tile rather than
# the dark site bg, so it reads as a distinct profile picture instead of a black
# square in those light UIs. Pure white is a plain neutral, not a brand hue —
# nothing on the live site to drift from — so it's the one literal color here;
# the mark stays the brand-blue accent.
COLOR_AVATAR_BG="#FFFFFF"
COLOR_AVATAR_FG=$COLOR_DARK

# --- Source + output paths ----------------------------------------------------
SRC="$ROOT/docs/src/assets/branding"
MARK_SRC="$SRC/mark.svg"
LOCKUP_SRC="$SRC/lockup.svg"
OG_TEMPLATE="$SRC/og.template.svg"
OUT_KIT="$ROOT/docs/public/branding"
OUT_ROOT="$ROOT/docs/public"

# Vendored Inter (SIL OFL 1.1, see fonts/OFL.txt) — the SAME file the live site
# loads via @fontsource-variable/inter, so rasterized text matches the site
# exactly. Text-bearing assets are rendered with resvg + this explicit font file
# (NOT rsvg-convert): rsvg/Pango here resolves fonts through Core Text, which
# only sees installed fonts and would silently fall back to the system sans —
# resvg's --use-font-file pins the real Inter deterministically on any machine.
INTER_FONT="$HERE/fonts/Inter-Variable.ttf"

# --- Tool availability check --------------------------------------------------
missing=0
for tool in rsvg-convert magick resvg usvg; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf '%serror:%s %s not found on PATH\n' "$RED" "$RESET" "$tool" >&2
    missing=1
  fi
done
if [ ! -f "$INTER_FONT" ]; then
  printf '%serror:%s vendored font not found: %s\n' "$RED" "$RESET" "${INTER_FONT#"$ROOT/"}" >&2
  missing=1
fi
if [ "$missing" -ne 0 ]; then
  printf '%shint:%s  brew install librsvg imagemagick resvg   (macOS, resvg ships usvg too)\n' "$YELLOW" "$RESET" >&2
  printf '       apt install librsvg2-bin imagemagick; cargo install resvg usvg  (Debian/Ubuntu)\n' >&2
  exit 1
fi

# Render a text-bearing SVG to PNG with the pinned Inter (deterministic, no
# system-font fallback). Args: <in.svg> <out.png> <width> <height>.
render_text_png() {
  resvg --skip-system-fonts --use-font-file "$INTER_FONT" \
        -w "$3" -h "$4" "$1" "$2"
}

# Outline the wordmark <text> to vector paths, in place, using the pinned Inter.
# GitHub/markdown render SVG <text> in the VIEWER's system font (no webfonts), so
# a text-based lockup would show the wrong typeface there. usvg flattens text →
# paths so the shipped lockup is true Inter everywhere with zero font dependency.
# usvg drops the viewBox (emits width/height) + a11y attrs, so we restore the
# canonical <svg> header. Args: <lockup.svg>.
outline_lockup_svg() {
  usvg --skip-system-fonts --use-font-file "$INTER_FONT" "$1" "$TMP/outlined.svg"
  sed -E "1 s|<svg[^>]*>|<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"$LOCKUP_VIEWBOX\" role=\"img\" aria-label=\"WaveHouse\">|" \
    "$TMP/outlined.svg" > "$1"
}

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

# A solid square tile: a filled background with the mark placed on top via an
# explicit transform — the shared composition behind both the apple-touch-icon
# and the larger avatar.png. Vector source, so the caller rasterizes it to any
# size and stays crisp. Args: <canvas-units> <bg> <fg> <mark-transform>.
solid_tile_svg() {
  printf '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %s %s"><rect width="%s" height="%s" fill="%s"/><g transform="%s">%s</g></svg>' \
    "$1" "$1" "$1" "$1" "$2" "$4" "$(mark_with_color "$3")"
}

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

# Statically-colored variants for Head.astro's live theme-flip swap (the
# GitHub favicon.svg/favicon-dark.svg pattern): a distinct URL per scheme
# keeps Chromium's per-URL favicon cache scheme-keyed, and the fetched file
# carries no media query for the favicon rasterizer to mis-evaluate. The
# self-styling favicon.svg above remains the no-JS default.
cat > "$OUT_KIT/favicon-light.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_LIGHT")</svg>
EOF
cat > "$OUT_KIT/favicon-dark.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_DARK")</svg>
EOF

# --- 2. Standalone marks (copy-out) -------------------------------------------
cat > "$OUT_KIT/mark-light.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_LIGHT")</svg>
EOF
cat > "$OUT_KIT/mark-dark.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$MARK_VIEWBOX" role="img" aria-label="WaveHouse">$(mark_with_color "$COLOR_DARK")</svg>
EOF

# --- 3. Lockup (mark + wordmark) — dark + light variants ----------------------
# Two-color: mark = accent, wordmark = ink. Dark bg uses the dark accent + near-
# white ink; light bg uses the AA light accent + charcoal ink. The light variant
# is what a README <picture>/<source media="(prefers-color-scheme:light)"> needs.
# Emitted with the wordmark as <text>, then outlined to paths (see
# outline_lockup_svg) so they render true Inter on GitHub / in markdown, which
# ignore webfonts and would otherwise show the wordmark in a system font.
cat > "$OUT_KIT/lockup-dark.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$LOCKUP_VIEWBOX" role="img" aria-label="WaveHouse">$(lockup_with_colors "$COLOR_DARK" "$OG_INK")</svg>
EOF
outline_lockup_svg "$OUT_KIT/lockup-dark.svg"
cat > "$OUT_KIT/lockup-light.svg" <<EOF
<svg xmlns="http://www.w3.org/2000/svg" viewBox="$LOCKUP_VIEWBOX" role="img" aria-label="WaveHouse">$(lockup_with_colors "$COLOR_LIGHT" "$WORDMARK_LIGHT")</svg>
EOF
outline_lockup_svg "$OUT_KIT/lockup-light.svg"

# Themeable inline lockup for in-app components (Logo.astro inlines it via
# set:html). Same OUTLINED art as lockup-dark, but colours swapped for CSS hooks
# so ONE asset themes itself with no light/dark variants: mark → currentColor
# (the consumer sets color: var(--wh-accent)), wordmark → var(--wh-ink). The
# substitution is case-insensitive because usvg may lower-case the hex on outline.
{
  printf '%s\n' '<!-- Themeable inline lockup: mark=currentColor (set color:var(--wh-accent)), wordmark=var(--wh-ink). GENERATED from lockup-dark.svg by generate.sh — do not hand-edit. -->'
  sed -E -e "s|$COLOR_DARK|currentColor|Ig" -e "s|fill=\"$OG_INK\"|style=\"fill:var(--wh-ink)\"|Ig" "$OUT_KIT/lockup-dark.svg"
} > "$SRC/lockup-themed.svg"

# --- 4. favicon.ico (16/32/48) → kit + site root ------------------------------
rsvg-convert -w 16 -h 16 "$OUT_KIT/favicon.svg" -o "$TMP/fav-16.png"
rsvg-convert -w 32 -h 32 "$OUT_KIT/favicon.svg" -o "$TMP/fav-32.png"
rsvg-convert -w 48 -h 48 "$OUT_KIT/favicon.svg" -o "$TMP/fav-48.png"
magick "$TMP/fav-16.png" "$TMP/fav-32.png" "$TMP/fav-48.png" "$OUT_KIT/favicon.ico"
cp "$OUT_KIT/favicon.ico" "$OUT_ROOT/favicon.ico"

# --- 5. apple-touch-icon.png (180x180 solid tile) -----------------------------
solid_tile_svg 180 "$COLOR_TOUCH_BG" "$COLOR_TOUCH_FG" "translate(28 28) scale(1.24)" > "$TMP/touch.svg"
rsvg-convert -w 180 -h 180 "$TMP/touch.svg" -o "$OUT_KIT/apple-touch-icon.png"

# --- 5b. avatar.png (512x512 solid tile) --------------------------------------
# A larger sibling of the touch icon for places that crop a square/rounded
# avatar and render it big: GitHub org/team avatars, Slack, social profiles.
# apple-touch-icon is only 180px and looks soft when blown up to a team avatar;
# this renders the same solid-bg + centered-mark composition at 512px (vector
# source → crisp at any size), but on a white tile + brand-blue mark (see the
# COLOR_AVATAR_* note above) so it reads as a profile picture in light UIs. The
# mark's stroked content bbox is x 3..96 (the leftmost round-capped wave's
# stroke reaches ~x3), y 11..89 — center (49.5,50). scale 0.7416 maps it to ~69%
# of the canvas; tx=50-49.5*0.7416=13.29, ty=50-50*0.7416=12.92 center it
# exactly, leaving even padding so the mark clears GitHub's rounded-square crop.
solid_tile_svg 100 "$COLOR_AVATAR_BG" "$COLOR_AVATAR_FG" "translate(13.29 12.92) scale(0.7416)" > "$TMP/avatar.svg"
rsvg-convert -w 512 -h 512 "$TMP/avatar.svg" -o "$OUT_KIT/avatar.png"

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
# Text-bearing → resvg + pinned Inter (wordmark, tagline, footer all real Inter).
render_text_png "$TMP/og.svg" "$OUT_KIT/og.png" 1200 630

# --- 7. Raster marks + lockups (PNG, transparent) -----------------------------
# For non-SVG consumers: slide decks, GitHub social-preview upload, email sig,
# anywhere SVG isn't accepted. Marks are pure geometry → rsvg-convert is fine.
# Lockups carry the wordmark; the committed lockup-*.svg are already outlined
# to paths in place (outline_lockup_svg, section 3), so rendering their PNGs
# through resvg + the pinned Inter is belt-and-braces.
rsvg-convert -w 1024 -h 1024 "$OUT_KIT/mark-light.svg" -o "$OUT_KIT/mark-light.png"
rsvg-convert -w 1024 -h 1024 "$OUT_KIT/mark-dark.svg"  -o "$OUT_KIT/mark-dark.png"
render_text_png "$OUT_KIT/lockup-light.svg" "$OUT_KIT/lockup-light.png" 1200 250
render_text_png "$OUT_KIT/lockup-dark.svg"  "$OUT_KIT/lockup-dark.png"  1200 250

# --- Report -------------------------------------------------------------------
printf '%s    colors%s light=%s dark=%s bg=%s ink=%s\n' \
  "$CYAN" "$RESET" "$COLOR_LIGHT" "$COLOR_DARK" "$OG_BG" "$OG_INK"
for out in favicon.svg favicon-light.svg favicon-dark.svg favicon.ico \
           apple-touch-icon.png avatar.png og.png \
           mark-light.svg mark-dark.svg lockup-dark.svg lockup-light.svg \
           mark-light.png mark-dark.png lockup-light.png lockup-dark.png; do
  printf '  %s✓%s %s\n' "$GREEN" "$RESET" "docs/public/branding/$out"
done
printf '  %s✓%s %s\n' "$GREEN" "$RESET" "docs/public/favicon.ico (site root)"

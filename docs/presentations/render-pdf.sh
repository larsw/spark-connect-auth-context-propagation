#!/usr/bin/env bash
# Renders slide decks to PDF, one page per slide, at docs/dist/<deck>.pdf: builds each with
# build.sh, then prints it with DeckTape in headless Chrome.
#
# Usage: docs/presentations/render-pdf.sh [deck ...]    (every deck, unless named)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
docs="$(dirname "$here")"
out="$docs/build/presentations"

if (( $# > 0 )); then
  decks=("$@")
else
  decks=()
  for dir in "$here"/*/; do
    deck="$(basename "$dir")"
    [[ -f "$dir/$deck.adoc" ]] && decks+=("$deck")
  done
fi

"$here/build.sh" "${decks[@]}"

cd "$here"
# DeckTape prints with CHROME_PATH, or else the first Chrome or Chromium on the PATH. Without either,
# it drives the Chrome that Puppeteer pins, installed here into ~/.cache/puppeteer the first time
# (about 170 MB): npm does not run Puppeteer's own install script.
chrome_path="${CHROME_PATH:-$(command -v google-chrome google-chrome-stable chromium chromium-browser | head -n 1)}"
if [[ -n "$chrome_path" ]]; then
  chrome=(--chrome-path "$chrome_path")
else
  chrome=()
  node_modules/.bin/puppeteer browsers install chrome >/dev/null
fi

mkdir -p "$docs/dist"
for deck in "${decks[@]}"; do
  echo "==> $deck (PDF)"
  # The size the decks are laid out at (:revealjs_width: and :revealjs_height:), so a page is a slide.
  node_modules/.bin/decktape reveal --size 1280x720 --pause 500 "${chrome[@]}" \
    "file://$out/$deck/index.html" "$docs/dist/$deck.pdf"
  echo "PDF rendered: $docs/dist/$deck.pdf"
done

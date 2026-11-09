#!/usr/bin/env bash
# Builds the slide decks into docs/build/presentations. A deck is a folder here holding an AsciiDoc
# file of the folder's name, such as identity-through-spark-connect/identity-through-spark-connect.adoc;
# asciidoctor-revealjs converts it to <folder>/index.html, a reveal.js presentation.
#
# Everything a deck loads is put beside it: reveal.js, highlight.js, and the theme (theme/), whose
# fonts are inlined into its stylesheet, because browsers refuse a web font from a file:// URL. A
# deck therefore works opened as a file, offline. Press S in it for the speaker notes.
#
# The code on the slides is included from the repository's own sources by tag, so it is the code
# that runs and is tested, not a copy of it.
#
# Usage: docs/presentations/build.sh [deck ...]    (every deck, unless named)
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

cd "$here"
# npm records what it installed in node_modules/.package-lock.json; reinstall once package-lock.json
# is newer, so a checkout that adds a package gets it. Without install scripts: the only one is
# Puppeteer's (through DeckTape), which downloads a Chrome that only render-pdf.sh needs, and installs.
[[ node_modules/.package-lock.json -nt package-lock.json ]] \
  || npm ci --ignore-scripts --no-audit --no-fund --loglevel=error

echo "==> reveal.js, highlight.js and the theme"
rm -rf "$out/reveal.js" "$out/highlight.js" "$out/theme"
mkdir -p "$out/reveal.js" "$out/highlight.js" "$out/theme"
cp -r node_modules/reveal.js/dist node_modules/reveal.js/plugin "$out/reveal.js/"
cp node_modules/@highlightjs/cdn-assets/highlight.min.js "$out/highlight.js/"
cp theme/highlight.css "$out/theme/"
# Writes stylesheet $1 to $2 with each url() inlined as a data URI. A url() names a file by its base
# name, found in the first of the folders after them that has it.
inline_css() {
  node - "$@" <<'EOF'
const fs = require('fs')
const path = require('path')
const [from, to, ...folders] = process.argv.slice(2)
const types = { '.woff2': 'font/woff2', '.svg': 'image/svg+xml' }
const css = fs.readFileSync(from, 'utf8').replace(/url\(([^)]+)\)/g, (_, url) => {
  const name = path.basename(url)
  const folder = folders.find((it) => fs.existsSync(path.join(it, name)))
  if (!folder) throw new Error(`${from}: no ${name} in ${folders.join(', ')}`)
  const data = fs.readFileSync(path.join(folder, name)).toString('base64')
  return `url(data:${types[path.extname(name)]};base64,${data})`
})
fs.writeFileSync(to, css)
EOF
}
inline_css theme/poc.css "$out/theme/poc.css" \
  node_modules/@fontsource/roboto/files node_modules/@fontsource/roboto-mono/files

# Inlined, every diagram's <style> applies to the whole deck, so a class two diagrams define
# differently would restyle one of them with the other's rule. Refuse that across every deck.
node - "$here"/*/images <<'EOF'
const fs = require('fs')
const path = require('path')
const seen = new Map()
let clashes = 0
for (const folder of process.argv.slice(2).filter((it) => fs.existsSync(it))) {
  for (const name of fs.readdirSync(folder).filter((it) => it.endsWith('.svg')).sort()) {
    const file = path.join(path.basename(path.dirname(folder)), name)
    const style = /<style>([\s\S]*?)<\/style>/.exec(fs.readFileSync(path.join(folder, name), 'utf8'))
    for (const [, cls, body] of (style ? style[1] : '').matchAll(/\.([\w-]+)\s*\{([^}]*)\}/g)) {
      const rule = body.trim().replace(/\s+/g, ' ')
      const earlier = seen.get(cls)
      if (!earlier) seen.set(cls, { rule, file })
      else if (earlier.rule !== rule) {
        console.error(`${file}: .${cls} { ${rule} } differs from ${earlier.file}: { ${earlier.rule} }`)
        clashes++
      }
    }
  }
}
if (clashes) {
  console.error('Give a class one definition in every diagram, or a name of its own.')
  process.exit(1)
}
EOF

for deck in "${decks[@]}"; do
  echo "==> $deck"
  [[ -f "$here/$deck/$deck.adoc" ]] || { echo "No deck $here/$deck/$deck.adoc" >&2; exit 1; }
  # Unsafe mode, so the deck may include source files and images from outside its own folder.
  node_modules/.bin/asciidoctor-revealjs --safe-mode unsafe --failure-level WARN \
    --attribute revealjsdir=../reveal.js \
    --attribute highlightjsdir=../highlight.js \
    --attribute highlightjs-theme=../theme/highlight.css \
    --attribute customcss=../theme/poc.css \
    --out-file "$out/$deck/index.html" \
    "$here/$deck/$deck.adoc"
  echo "Deck built: $out/$deck/index.html"
done

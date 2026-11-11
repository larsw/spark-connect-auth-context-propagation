# Presentations

Slide decks about this PoC, written in AsciiDoc and converted to [reveal.js](https://revealjs.com) by
[asciidoctor-revealjs](https://docs.asciidoctor.org/reveal.js-converter/latest/). The toolchain and
look follow svipul's decks (`../svipul/docs/presentations`): Roboto, a navy title band, code on light
grey, and diagrams drawn as SVG.

| Deck | What it covers |
| --- | --- |
| `identity-through-spark-connect` | A tour of the PoC: the problem, the stack, the path a query takes, the four hops (wire, token exchange, thread boundary, Polaris and vended keys), session integrity, the correlation ID, three clients, jobs, SPARQL, the lineage conflict, the upstream findings and how it is verified. |
| `authzen-with-cerbos` | The `feat/authzen-cerbos` branch: Polaris and the Spark Connect plugin ask Cerbos for authorization decisions over AuthZEN. What changes from main, the request format, both enforcement points, the policies, the Cerbos roles mapping, write delegation and the S3 key, what stays in code, the comparison with Keycloak as the PDP, the tests, and the costs. |
| `authzen-with-openfga` | The `feat/authzen-openfga` branch: OpenFGA replaces Cerbos as the PDP. How OpenFGA reads an AuthZEN request, why both callers send a fixed resource id, getting the store id to them, the policy as holders and operations, the JSON model, the questions OpenFGA answers with HTTP 400, both enforcement points, the correlation ID without a decision log, the comparison with Cerbos, the tests, and the costs (FINDINGS §21). |

## Building

```shell
docs/presentations/build.sh        # every deck, into docs/build/presentations/<deck>/index.html
docs/presentations/render-pdf.sh   # also as a PDF, at docs/dist/<deck>.pdf
```

Requires Node.js; the scripts install the toolchain into `docs/presentations/node_modules` on first
use. A built deck needs nothing else: reveal.js, highlight.js and the theme are put beside it, with
the fonts inlined, so its `index.html` opens straight from disk, offline. `render-pdf.sh` prints with
DeckTape through `CHROME_PATH`, or the first Chrome or Chromium on the `PATH`.

## Presenting

Press `S` for the speaker view, which has each slide's notes; `F` for full screen; `Esc` for an
overview. Most diagrams build up one click at a time; the PDF shows every step at once.

## Writing

- Each `==` section is a slide; `[.notes]` blocks are its speaker notes. The first slide,
  `[.cover,state=bookend]`, is the title slide; there is no `= Title`.
- Draw a diagram as SVG in the deck's `images` folder, 1184 wide (a slide's content width), and include
  it with `image::{diagrams}/name.svg["Alt text",opts=inline,role=diagram]`. A `<g class="fragment">`
  in it appears on a click. Build up with fragments that only appear, never ones that fade out.
- Inlined, every diagram's `<style>` applies to the whole deck, so a class must mean the same thing in
  every diagram; `build.sh` refuses two definitions of one class. The palette: blues for structure,
  ember (`.d-token`) for tokens and keys, purple (`.d-cid`) for the correlation ID, green and red
  (`.d-ok`, `.d-no`) for allowed and refused, yellow (`.d-note`) for notes on a lifeline.
- Show code by including it from the repository by tag, never by pasting it:
  `include::{server}/PropagatedIdentityHolder.java[tag=parse-job-tag,indent=0]`. The tags are
  comments in the sources (`// tag::name[]` ... `// end::name[]`). A slide holds about 22 lines.
- Check a new slide for overflow in the PDF: reveal.js does not stop content running off a slide.

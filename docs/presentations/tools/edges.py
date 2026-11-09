"""Lists the shapes in SVG diagrams whose stroke touches the edge of their viewBox.

    python3 docs/presentations/tools/edges.py docs/presentations/*/images/*.svg

An inline SVG clips at its viewBox, so such a stroke would be drawn half as thick on that side.
The theme draws diagrams with overflow: visible, which shows it whole in a deck; this finds the
shapes that still need it, for a diagram shown anywhere else."""
import re
import sys
from pathlib import Path

for f in sys.argv[1:]:
    s = Path(f).read_text()
    x0, y0, w, h = map(float, re.search(r'viewBox="([-\d.]+) ([-\d.]+) ([\d.]+) ([\d.]+)"', s).groups())
    hits = []
    for m in re.finditer(r'<rect class="([^"]+)" x="([-\d.]+)" y="([-\d.]+)" width="([\d.]+)" height="([\d.]+)"', s):
        cls, x, y, rw, rh = m.group(1), *map(float, m.groups()[1:])
        if "d-veil" in cls:
            continue
        sides = [n for n, c in [("left", x <= x0), ("top", y <= y0), ("right", x + rw >= x0 + w), ("bottom", y + rh >= y0 + h)] if c]
        if sides:
            hits.append(f"rect {cls} at {x:g},{y:g}: {' '.join(sides)}")
    for m in re.finditer(r'<path class="([^"]+)" d="M([-\d.]+),([-\d.]+)[^"]*"', s):
        cls, x, y = m.group(1), float(m.group(2)), float(m.group(3))
        if any(c in cls for c in ("d-note", "d-pkg", "d-frametag")) and (x <= x0 or y <= y0):
            hits.append(f"path {cls} from {x:g},{y:g}")
    if hits:
        print(Path(f).name, len(hits))
        for hit in hits:
            print("   ", hit)

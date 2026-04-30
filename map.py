"""
Generate map.html from arctic.duckdb.
Run: python map.py
Then open map.html in any modern browser.
"""
import json
import os
import sys

import duckdb

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(REPO_ROOT, "arctic.duckdb")
DEFAULT_OUT = os.path.join(REPO_ROOT, "map.html")

SECTOR_COLORS = {
    "Oil & Gas":          [239, 68,  68 ],
    "Mining":             [245, 158, 11 ],
    "Transportation":     [34,  197, 94 ],
    "Defense":            [168, 85,  247],
    "Energy & Utilities": [234, 179, 8  ],
    "Community":          [59,  130, 246],
}


def load_data(db_path: str) -> list[dict]:
    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute("""
        SELECT lat, lon, sector, location_name, asset_type, operator_owner, data_source
        FROM sites
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY sector
    """).fetchall()
    con.close()
    return [
        {
            "lat": r[0],
            "lon": r[1],
            "sector": r[2] or "",
            "name":  (r[3] or "")[:80],
            "type":  (r[4] or "")[:60],
            "op":    (r[5] or "")[:60],
            "src":   (r[6] or ""),
        }
        for r in rows
    ]


def _legend_html() -> str:
    items = []
    for sector, rgb in SECTOR_COLORS.items():
        color = f"rgb({rgb[0]},{rgb[1]},{rgb[2]})"
        items.append(
            f'<div class="leg-row">'
            f'<span class="dot" style="background:{color}"></span>'
            f'{sector}'
            f'</div>'
        )
    return "\n".join(items)


HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Arctic Atlas — Infrastructure Map</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600&family=Instrument+Serif&display=swap" rel="stylesheet">
  <link href="https://unpkg.com/maplibre-gl@4.5.0/dist/maplibre-gl.css" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #0a0a0f;
      font-family: 'DM Sans', sans-serif;
      color: #f5f0e8;
      overflow: hidden;
    }

    #container { width: 100vw; height: 100vh; position: relative; }

    /* ---- header ---- */
    #header {
      position: absolute;
      top: 20px; left: 20px;
      z-index: 10;
      background: rgba(10,10,15,0.75);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      padding: 14px 18px;
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      max-width: 260px;
    }
    #header h1 {
      font-family: 'Instrument Serif', serif;
      font-size: 18px;
      font-weight: 400;
      color: #f5f0e8;
      margin-bottom: 4px;
    }
    #header .sub {
      font-size: 12px;
      color: rgba(245,240,232,0.5);
      margin-bottom: 10px;
    }
    #header .count {
      font-size: 22px;
      font-weight: 600;
      color: #4ade80;
      line-height: 1;
    }
    #header .count-label {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: rgba(245,240,232,0.45);
      margin-top: 2px;
    }

    /* ---- legend ---- */
    #legend {
      position: absolute;
      top: 20px; right: 20px;
      z-index: 10;
      background: rgba(10,10,15,0.75);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      padding: 14px 18px;
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      min-width: 160px;
    }
    #legend h3 {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: #4ade80;
      margin-bottom: 10px;
    }
    .leg-row {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      color: rgba(245,240,232,0.85);
      margin-bottom: 6px;
    }
    .dot {
      width: 9px; height: 9px;
      border-radius: 50%;
      flex-shrink: 0;
    }

    /* ---- tooltip ---- */
    #tip {
      position: absolute;
      pointer-events: none;
      z-index: 20;
      display: none;
      background: rgba(10,10,15,0.95);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 8px;
      padding: 10px 13px;
      font-size: 12px;
      max-width: 240px;
    }
    #tip .tip-name { font-weight: 600; color: #4ade80; margin-bottom: 3px; }
    #tip .tip-row  { color: rgba(245,240,232,0.75); margin-top: 2px; }
    #tip .tip-src  { color: rgba(245,240,232,0.4); font-size: 10px; margin-top: 5px; }
  </style>
</head>
<body>
<div id="container">
  <div id="header">
    <h1>Arctic Atlas</h1>
    <div class="sub">Verified subarctic infrastructure</div>
    <div class="count">__COUNT__</div>
    <div class="count-label">assets on record</div>
  </div>

  <div id="legend">
    <h3>Sector</h3>
    __LEGEND__
  </div>

  <div id="tip"></div>
</div>

<script src="https://unpkg.com/maplibre-gl@4.5.0/dist/maplibre-gl.js"></script>
<script src="https://unpkg.com/deck.gl@9.0.7/dist.min.js"></script>
<script>
const DATA = __DATA__;

const COLORS = {
  "Oil & Gas":          [239, 68,  68 ],
  "Mining":             [245, 158, 11 ],
  "Transportation":     [34,  197, 94 ],
  "Defense":            [168, 85,  247],
  "Energy & Utilities": [234, 179, 8  ],
  "Community":          [59,  130, 246],
};
const DEFAULT_COLOR = [156, 163, 175];

const tip = document.getElementById('tip');

const deckgl = new deck.DeckGL({
  container: 'container',
  mapLib: maplibregl,
  mapStyle: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  initialViewState: { longitude: -120, latitude: 65, zoom: 3 },
  controller: true,
  layers: [
    new deck.ScatterplotLayer({
      id: 'sites',
      data: DATA,
      getPosition: d => [d.lon, d.lat],
      getFillColor: d => COLORS[d.sector] || DEFAULT_COLOR,
      getRadius: 8000,
      radiusMinPixels: 2,
      radiusMaxPixels: 10,
      pickable: true,
      opacity: 0.85,
      updateTriggers: { getFillColor: [] },
    })
  ],
  onHover: ({object, x, y}) => {
    if (object) {
      tip.style.display = 'block';
      tip.style.left = (x + 14) + 'px';
      tip.style.top  = (y + 14) + 'px';
      tip.innerHTML =
        `<div class="tip-name">${object.name || object.type || '—'}</div>` +
        (object.type   ? `<div class="tip-row">${object.type}</div>` : '') +
        (object.op     ? `<div class="tip-row">${object.op}</div>` : '') +
        (object.sector ? `<div class="tip-row">${object.sector}</div>` : '') +
        `<div class="tip-src">${object.src} &middot; ${object.lat.toFixed(4)}, ${object.lon.toFixed(4)}</div>`;
    } else {
      tip.style.display = 'none';
    }
  },
});
</script>
</body>
</html>
"""


def generate(db_path: str = DEFAULT_DB, out_path: str = DEFAULT_OUT) -> None:
    if not os.path.exists(db_path):
        sys.exit(f"Database not found: {db_path}\nRun: python ingest.py")

    data = load_data(db_path)
    html = (
        HTML
        .replace("__DATA__", json.dumps(data, separators=(",", ":")))
        .replace("__COUNT__", f"{len(data):,}")
        .replace("__LEGEND__", _legend_html())
    )
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {out_path} — {len(data):,} points. Open in your browser.")


if __name__ == "__main__":
    db_arg  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    out_arg = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT
    generate(db_path=db_arg, out_path=out_arg)

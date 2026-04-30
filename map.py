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

# CSS hex strings (for match expressions in maplibre paint)
SECTOR_HEX = {
    "Oil & Gas":          "#ef4444",
    "Mining":             "#f59e0b",
    "Transportation":     "#22c55e",
    "Defense":            "#a855f7",
    "Energy & Utilities": "#eab308",
    "Community":          "#3b82f6",
}
# RGB tuples (for the legend dots)
SECTOR_RGB = {
    "Oil & Gas":          (239, 68,  68),
    "Mining":             (245, 158, 11),
    "Transportation":     (34,  197, 94),
    "Defense":            (168, 85,  247),
    "Energy & Utilities": (234, 179, 8),
    "Community":          (59,  130, 246),
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
            "name":   (r[3] or "")[:80],
            "type":   (r[4] or "")[:60],
            "op":     (r[5] or "")[:60],
            "src":    (r[6] or ""),
        }
        for r in rows
    ]


def _legend_html() -> str:
    items = []
    for sector, (r, g, b) in SECTOR_RGB.items():
        items.append(
            f'<div class="leg-row">'
            f'<span class="dot" style="background:rgb({r},{g},{b})"></span>'
            f'{sector}'
            f'</div>'
        )
    return "\n".join(items)


def _color_match_expr() -> str:
    """Build a maplibre 'match' paint expression for sector → hex color."""
    pairs = []
    for sector, hex_color in SECTOR_HEX.items():
        pairs.append(json.dumps(sector))
        pairs.append(json.dumps(hex_color))
    return f'["match", ["get", "sector"], {", ".join(pairs)}, "#9ca3af"]'


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
    body { background: #0a0a0f; font-family: 'DM Sans', sans-serif; overflow: hidden; }

    #map { position: absolute; top: 0; left: 0; width: 100vw; height: 100vh; }

    .panel {
      position: absolute;
      z-index: 10;
      background: rgba(10,10,15,0.80);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      padding: 14px 18px;
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      color: #f5f0e8;
    }

    #header { top: 20px; left: 20px; max-width: 220px; }
    #header h1 {
      font-family: 'Instrument Serif', serif;
      font-size: 18px; font-weight: 400;
      margin-bottom: 3px;
    }
    #header .sub  { font-size: 11px; color: rgba(245,240,232,0.45); margin-bottom: 10px; }
    #header .cnt  { font-size: 24px; font-weight: 600; color: #4ade80; line-height: 1; }
    #header .clbl { font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em;
                    color: rgba(245,240,232,0.4); margin-top: 2px; }

    #legend { top: 20px; right: 20px; min-width: 160px; }
    #legend h3 { font-size: 10px; text-transform: uppercase; letter-spacing: 0.12em;
                 color: #4ade80; margin-bottom: 10px; }
    .leg-row { display: flex; align-items: center; gap: 8px; font-size: 12px;
               color: rgba(245,240,232,0.85); margin-bottom: 6px; }
    .dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }

    /* override maplibre popup chrome */
    .maplibregl-popup-content {
      background: rgba(10,10,15,0.95) !important;
      border: 1px solid rgba(255,255,255,0.12) !important;
      border-radius: 8px !important;
      padding: 10px 13px !important;
      color: #f5f0e8 !important;
      font-family: 'DM Sans', sans-serif !important;
      font-size: 12px !important;
      max-width: 240px !important;
    }
    .maplibregl-popup-tip { display: none !important; }
    .maplibregl-popup-close-button { color: rgba(245,240,232,0.5) !important; font-size: 16px !important; }
    .pop-name { font-weight: 600; color: #4ade80; margin-bottom: 3px; }
    .pop-row  { color: rgba(245,240,232,0.75); margin-top: 2px; }
    .pop-src  { color: rgba(245,240,232,0.4); font-size: 10px; margin-top: 5px; }
  </style>
</head>
<body>

<div id="map"></div>

<div id="header" class="panel">
  <h1>Arctic Atlas</h1>
  <div class="sub">Verified subarctic infrastructure</div>
  <div class="cnt">__COUNT__</div>
  <div class="clbl">assets on record</div>
</div>

<div id="legend" class="panel">
  <h3>Sector</h3>
  __LEGEND__
</div>

<script src="https://unpkg.com/maplibre-gl@4.5.0/dist/maplibre-gl.js"></script>
<script>
const GEOJSON = {
  type: "FeatureCollection",
  features: __DATA__
};

const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  center: [-120, 65],
  zoom: 3,
  attributionControl: false,
});

map.addControl(new maplibregl.AttributionControl({ compact: true }), "bottom-right");

map.on("load", () => {
  map.addSource("sites", { type: "geojson", data: GEOJSON });

  map.addLayer({
    id: "sites",
    type: "circle",
    source: "sites",
    paint: {
      "circle-radius": ["interpolate", ["linear"], ["zoom"], 2, 2, 8, 5],
      "circle-opacity": 0.85,
      "circle-color": __COLOR_EXPR__,
      "circle-stroke-width": 0.5,
      "circle-stroke-color": "rgba(0,0,0,0.3)",
    },
  });

  map.on("click", "sites", (e) => {
    const p = e.features[0].properties;
    const html =
      `<div class="pop-name">${p.name || p.type || "—"}</div>` +
      (p.type ? `<div class="pop-row">${p.type}</div>` : "") +
      (p.op   ? `<div class="pop-row">${p.op}</div>`   : "") +
      (p.sector ? `<div class="pop-row">${p.sector}</div>` : "") +
      `<div class="pop-src">${p.src} · ${Number(p.lat).toFixed(4)}, ${Number(p.lon).toFixed(4)}</div>`;
    new maplibregl.Popup({ closeButton: true, maxWidth: "260px" })
      .setLngLat(e.lngLat)
      .setHTML(html)
      .addTo(map);
  });

  map.on("mouseenter", "sites", () => { map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", "sites", () => { map.getCanvas().style.cursor = ""; });
});
</script>
</body>
</html>
"""


def _geojson_features(data: list[dict]) -> str:
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [d["lon"], d["lat"]]},
            "properties": {k: v for k, v in d.items() if k not in ("lat", "lon")},
        }
        for d in data
    ]
    return json.dumps(features, separators=(",", ":"))


def generate(db_path: str = DEFAULT_DB, out_path: str = DEFAULT_OUT) -> None:
    if not os.path.exists(db_path):
        sys.exit(f"Database not found: {db_path}\nRun: python ingest.py")

    data = load_data(db_path)
    html = (
        HTML
        .replace("__DATA__", _geojson_features(data))
        .replace("__COUNT__", f"{len(data):,}")
        .replace("__LEGEND__", _legend_html())
        .replace("__COLOR_EXPR__", _color_match_expr())
    )
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {out_path} — {len(data):,} points. Open in your browser.")


if __name__ == "__main__":
    db_arg  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    out_arg = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT
    generate(db_path=db_arg, out_path=out_arg)

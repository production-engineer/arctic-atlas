# install dependencies once in your environment
# python3 -m pip install pandas folium

import pandas as pd
import folium
from folium.plugins import MarkerCluster

# Load your CSV file
df = pd.read_csv('arctic_infrastructure_sites.csv')

# Center the map roughly over Alaska/northern Canada
m = folium.Map(location=[65, -120], zoom_start=4, tiles='cartodbpositron')

# Use a marker cluster so thousands of points remain responsive
marker_cluster = MarkerCluster().add_to(m)

# Colour-code markers by site type
colors = {
    'oil_gas': 'red',
    'mine': 'orange',
    'remote_community': 'blue',
    'transportation_infrastructure': 'green',
    'dod_infrastructure': 'purple'
}

for _, row in df.iterrows():
    popup_text = (
        f"<b>{row['site_type'].replace('_', ' ').title()}</b><br>"
        f"Owner: {row['owner_operator']}<br>"
        f"Q: {row['environmental_insight_question']}"
    )
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=3,
        color=colors.get(row['site_type'], 'gray'),
        fill=True,
        fill_opacity=0.7,
        popup=folium.Popup(popup_text, max_width=300)
    ).add_to(marker_cluster)

# Save the map as an interactive HTML file
m.save('arctic_map.html')
# install dependencies once in your environment
# python3 -m pip install pandas folium

import pandas as pd
import folium
from folium.plugins import MarkerCluster

CSV_PATH = 'arctic_infrastructure_sites.csv'
OUT_HTML = 'arctic_map.html'

# --- helpers ---
def _norm(col: str) -> str:
    """Normalize column names so 'Site Type', 'siteType', 'site_type' all align."""
    return (
        str(col)
        .strip()
        .replace('-', '_')
        .replace(' ', '_')
        .lower()
    )


def _first_present(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name (in df.columns) that matches any normalized candidate."""
    norm_to_actual = {_norm(c): c for c in df.columns}
    for cand in candidates:
        cand_n = _norm(cand)
        if cand_n in norm_to_actual:
            return norm_to_actual[cand_n]
    return None


def _require(df: pd.DataFrame, key: str, candidates: list[str]) -> str:
    """Find a required column by aliases; raise a helpful error if missing."""
    col = _first_present(df, candidates)
    if col is None:
        cols = ', '.join([str(c) for c in df.columns])
        raise KeyError(
            f"Missing required column for '{key}'. Tried: {candidates}. "
            f"Found columns: [{cols}]"
        )
    return col


# --- load ---
df = pd.read_csv(CSV_PATH)

# Resolve actual column names (handles different header spellings)
COL_LAT = _require(df, 'latitude', ['latitude', 'lat', 'y', 'site_lat', 'site_latitude'])
COL_LON = _require(df, 'longitude', ['longitude', 'lon', 'lng', 'long', 'x', 'site_lon', 'site_longitude'])
COL_TYPE = _require(df, 'site_type', ['site_type', 'type', 'category', 'site_category', 'infrastructure_type'])
COL_OWNER = _require(df, 'owner_operator', ['owner_operator', 'owner', 'operator', 'owneroperator', 'owner/operator'])
COL_Q = _require(df, 'environmental_insight_question', ['environmental_insight_question', 'insight_question', 'question', 'env_question'])

# Basic cleanup
for col in [COL_TYPE, COL_OWNER, COL_Q]:
    df[col] = df[col].fillna('').astype(str)

df[COL_LAT] = pd.to_numeric(df[COL_LAT], errors='coerce')
df[COL_LON] = pd.to_numeric(df[COL_LON], errors='coerce')
df = df.dropna(subset=[COL_LAT, COL_LON])

# Center the map roughly over Alaska/northern Canada
m = folium.Map(location=[65, -120], zoom_start=4, tiles='cartodbpositron')

# Use a marker cluster so thousands of points remain responsive
marker_cluster = MarkerCluster().add_to(m)

# Colour-code markers by site type (normalized)
colors = {
    'oil_gas': 'red',
    'mine': 'orange',
    'remote_community': 'blue',
    'transportation_infrastructure': 'green',
    'dod_infrastructure': 'purple'
}

def _type_key(raw: str) -> str:
    return _norm(raw)

for _, row in df.iterrows():
    site_type_raw = row[COL_TYPE]
    site_type_key = _type_key(site_type_raw)

    popup_text = (
        f"<b>{str(site_type_raw).replace('_', ' ').title()}</b><br>"
        f"Owner: {row[COL_OWNER]}<br>"
        f"Q: {row[COL_Q]}"
    )

    folium.CircleMarker(
        location=[row[COL_LAT], row[COL_LON]],
        radius=3,
        color=colors.get(site_type_key, 'gray'),
        fill=True,
        fill_opacity=0.7,
        popup=folium.Popup(popup_text, max_width=300)
    ).add_to(marker_cluster)

m.save(OUT_HTML)
print(f"Wrote {OUT_HTML} with {len(df):,} points")
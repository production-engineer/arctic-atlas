"""
arctic-atlas ETL pipeline.
Ingests verified government data sources into a DuckDB database.
Run: python ingest.py [output.duckdb]
"""
import hashlib
import json
import os
import sys
import uuid

import duckdb
import pandas as pd

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.join(os.path.expanduser("~"), "repos", "BeadedCloud All Arctic Locations")
GIS_DIR = os.path.join(DATA_ROOT, "GIS FILES")
SUPPORT_DIR = os.path.join(DATA_ROOT, "Supporting Data")

DEFAULT_DB = os.path.join(REPO_ROOT, "arctic.duckdb")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sites (
    id              VARCHAR PRIMARY KEY,
    lat             DOUBLE NOT NULL,
    lon             DOUBLE NOT NULL,
    centroid_lat    DOUBLE NOT NULL,
    centroid_lon    DOUBLE NOT NULL,
    location_name   VARCHAR,
    country         VARCHAR,
    state_province  VARCHAR,
    sector          VARCHAR,
    asset_type      VARCHAR,
    ownership_type  VARCHAR,
    operator_owner  VARCHAR,
    data_source     VARCHAR NOT NULL,
    source_id       VARCHAR NOT NULL,
    source_url      VARCHAR NOT NULL,
    data_quality    VARCHAR,
    year_of_data    INTEGER,
    attributes      VARCHAR,
    UNIQUE (data_source, source_id)
)
"""


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sanitize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading formula-injection chars (=+@-) from all string columns."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.replace(r"^[=+@\-]+", "", regex=True)
    return df


def validate_and_clean(
    df: pd.DataFrame, lat_col: str = "lat", lon_col: str = "lon"
) -> tuple[pd.DataFrame, int]:
    """Drop rows where lat/lon are out of WGS84 range or null. Returns (clean_df, dropped_count)."""
    before = len(df)
    df = df.copy()
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    mask = (
        df[lat_col].notna() & df[lon_col].notna()
        & df[lat_col].between(-90, 90)
        & df[lon_col].between(-180, 180)
    )
    return df[mask].copy(), before - len(df[mask])


def load_into_db(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """
    Bulk-insert df into sites, skipping rows that violate the (data_source, source_id) unique key.
    Returns the number of rows actually inserted.
    """
    con.execute(SCHEMA_SQL)
    rows_before = con.execute("SELECT COUNT(*) FROM sites").fetchone()[0]
    con.register("_staging", df)
    con.execute("INSERT OR IGNORE INTO sites SELECT * FROM _staging")
    con.unregister("_staging")
    return con.execute("SELECT COUNT(*) FROM sites").fetchone()[0] - rows_before


def _uuids(n: int) -> list[str]:
    return [str(uuid.uuid4()) for _ in range(n)]


def _attrs(df: pd.DataFrame, col_map: dict[str, str]) -> pd.Series:
    """Build a JSON attributes column from a column-name mapping {source_col: attr_key}."""
    present = {k: v for k, v in col_map.items() if k in df.columns}
    sub = df[[*present.keys()]].rename(columns=present).fillna("").astype(str)
    return sub.apply(lambda r: json.dumps(r.to_dict()), axis=1)


def _ownership(series: pd.Series) -> pd.Series:
    low = series.fillna("").str.lower()
    result = pd.Series("Private", index=series.index)
    result = result.where(
        ~low.str.contains("government|state|federal|public|municipality|dot&pf|controlled"),
        "Public",
    )
    result = result.where(
        ~low.str.contains("first nation|indigenous|tribal|native corporation"),
        "Indigenous",
    )
    result = result.where(
        ~low.str.contains("dod|army|navy|air force|defense|military"),
        "Defense",
    )
    return result


def ingest_airports(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = sanitize_strings(df)
    df, dropped = validate_and_clean(df, lat_col="LAT_DD", lon_col="LONG_DD")
    if dropped:
        print(f"  airports: dropped {dropped} rows with invalid coordinates")

    source_id = df["OBJECTID"].astype(str) if "OBJECTID" in df.columns else df.index.astype(str)
    return pd.DataFrame({
        "id": _uuids(len(df)),
        "lat": df["LAT_DD"].astype(float),
        "lon": df["LONG_DD"].astype(float),
        "centroid_lat": df["LAT_DD"].astype(float),
        "centroid_lon": df["LONG_DD"].astype(float),
        "location_name": df["NAME"].fillna("").astype(str),
        "country": "US",
        "state_province": "AK",
        "sector": "Transportation",
        "asset_type": "airport",
        "ownership_type": _ownership(df["OWNER"]),
        "operator_owner": df["OWNER"].fillna("").astype(str),
        "data_source": "DOT_PF_AIRPORTS",
        "source_id": source_id,
        "source_url": "https://gis.data.alaska.gov/datasets/dot-pf-airports",
        "data_quality": "High",
        "year_of_data": 2026,
        "attributes": _attrs(df, {"FAA_ID": "faa_id", "ICAO": "icao", "STATUS": "status", "REGION": "region"}),
    })


def ingest_wells(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df = sanitize_strings(df)
    df, dropped = validate_and_clean(df, lat_col="WellHeadLat", lon_col="WellHeadLong")
    if dropped:
        print(f"  wells: dropped {dropped} rows with invalid coordinates")

    return pd.DataFrame({
        "id": _uuids(len(df)),
        "lat": df["WellHeadLat"].astype(float),
        "lon": df["WellHeadLong"].astype(float),
        "centroid_lat": df["WellHeadLat"].astype(float),
        "centroid_lon": df["WellHeadLong"].astype(float),
        "location_name": df["WellName"].fillna("").astype(str),
        "country": "US",
        "state_province": "AK",
        "sector": "Oil & Gas",
        "asset_type": "well head",
        "ownership_type": "Private",
        "operator_owner": df["Operator"].fillna("").astype(str),
        "data_source": "AK_DNR",
        "source_id": df["OBJECTID"].astype(str),
        "source_url": "https://aogknowledgebase.alaska.gov/",
        "data_quality": "High",
        "year_of_data": 2026,
        "attributes": _attrs(df, {
            "PermitNumber": "permit_number",
            "CurrentClass": "well_class",
            "CurrentStatus": "well_status",
            "GeographicArea": "geographic_area",
            "Field": "field",
            "SpudDate": "spud_date",
        }),
    })


def ingest_nrcan_csv(path: str, data_source: str, asset_type: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = sanitize_strings(df)
    df, dropped = validate_and_clean(df, lat_col="Latitude", lon_col="Longitude")
    if dropped:
        print(f"  {data_source}: dropped {dropped} rows with invalid coordinates")

    return pd.DataFrame({
        "id": _uuids(len(df)),
        "lat": df["Latitude"].astype(float),
        "lon": df["Longitude"].astype(float),
        "centroid_lat": df["Latitude"].astype(float),
        "centroid_lon": df["Longitude"].astype(float),
        "location_name": df["PropertyNameEN"].fillna("").astype(str),
        "country": "CA",
        "state_province": df["ProvincesEN"].fillna("").astype(str),
        "sector": "Mining",
        "asset_type": asset_type,
        "ownership_type": _ownership(df["OperatorOwnersEN"]),
        "operator_owner": df["OperatorOwnersEN"].fillna("").astype(str),
        "data_source": data_source,
        "source_id": df["OBJECTID"].astype(str),
        "source_url": "https://open.canada.ca/data/en/dataset/fdc9c72e-0e31-4f40-a5c9-1c0bf0f8f2da",
        "data_quality": "High",
        "year_of_data": 2026,
        "attributes": _attrs(df, {
            "CommoditiesEN": "commodities",
            "DevelopmentStageEN": "development_stage",
            "ActivityStatusEN": "activity_status",
            "Website": "website",
        }),
    })


def ingest_energy_layers(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = sanitize_strings(df)
    if "LAT_DD" not in df.columns or "LONG_DD" not in df.columns:
        print("  energy layers: no lat/lon columns found, skipping")
        return pd.DataFrame()
    df, dropped = validate_and_clean(df, lat_col="LAT_DD", lon_col="LONG_DD")
    if dropped:
        print(f"  energy layers: dropped {dropped} rows with invalid coordinates")

    return pd.DataFrame({
        "id": _uuids(len(df)),
        "lat": df["LAT_DD"].astype(float),
        "lon": df["LONG_DD"].astype(float),
        "centroid_lat": df["LAT_DD"].astype(float),
        "centroid_lon": df["LONG_DD"].astype(float),
        "location_name": df["NAME"].fillna("").astype(str),
        "country": "CA",
        "state_province": df["COMMUNITY"].fillna("").astype(str),
        "sector": "Energy & Utilities",
        "asset_type": "power generation",
        "ownership_type": "Public",
        "operator_owner": df["NAME"].fillna("").astype(str),
        "data_source": "ARCGIS_ENERGY",
        "source_id": df["OBJECTID"].astype(str) if "OBJECTID" in df.columns else df.index.astype(str),
        "source_url": "https://www.arcgis.com/home/item.html?id=arctic-energy-layers",
        "data_quality": "Medium",
        "year_of_data": 2026,
        "attributes": _attrs(df, {"SOURCE_ENERGY_TYPE": "source_energy_type", "NET_CAPACITY_V": "net_capacity"}),
    })


PIPELINE: list[tuple[str, str, callable, tuple]] = [
    ("Airports.csv",                                  os.path.join(GIS_DIR,     "Airports.csv"),                                          ingest_airports,    ()),
    ("Well_Surface_Hole_Location.csv",                os.path.join(GIS_DIR,     "Well_Surface_Hole_Location.csv"),                        ingest_wells,       ()),
    ("table-Mines-and-other-primary-producing-sites.csv", os.path.join(GIS_DIR, "table-Mines-and-other-primary-producing-sites.csv"),     ingest_nrcan_csv,   ("NRCAN_MINES", "mine")),
    ("table-Processing-facilities.csv",               os.path.join(GIS_DIR,     "table-Processing-facilities.csv"),                       ingest_nrcan_csv,   ("NRCAN_PROCESSING", "processing facility")),
    ("table-Advanced-projects.csv",                   os.path.join(SUPPORT_DIR, "table-Advanced-projects.csv"),                           ingest_nrcan_csv,   ("NRCAN_ADVANCED", "mining project")),
    ("table-ArcGIS-Online-layers.csv",                os.path.join(GIS_DIR,     "table-ArcGIS-Online-layers.csv"),                        ingest_energy_layers, ()),
]


def run_ingest(db_path: str = DEFAULT_DB) -> None:
    print("=== arctic-atlas ingest ===")
    for name, path, _, _ in PIPELINE:
        if os.path.exists(path):
            print(f"SHA256 {name}: {sha256_file(path)}")
        else:
            print(f"MISSING {name}: {path}")

    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    total = 0

    for name, path, fn, args in PIPELINE:
        if not os.path.exists(path):
            continue
        label = name.replace(".csv", "")
        print(f"\nIngesting {label}...")
        df = fn(path, *args)
        if len(df) == 0:
            continue
        n = load_into_db(con, df)
        print(f"  {label}: {n} rows inserted ({len(df)} parsed)")
        total += n

    print(f"\n=== DONE: {total} total rows inserted into {db_path} ===")
    for src, n in con.execute(
        "SELECT data_source, COUNT(*) FROM sites GROUP BY data_source ORDER BY 2 DESC"
    ).fetchall():
        print(f"  {src}: {n:,}")
    con.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    run_ingest(db_path)

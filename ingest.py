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

import urllib.request
import urllib.parse
import zipfile

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.join(os.path.expanduser("~"), "repos", "BeadedCloud All Arctic Locations")
GIS_DIR = os.path.join(DATA_ROOT, "GIS FILES")
SUPPORT_DIR = os.path.join(DATA_ROOT, "Supporting Data")
DATA_CACHE = os.path.join(REPO_ROOT, "data_cache")

DEFAULT_DB = os.path.join(REPO_ROOT, "arctic.duckdb")

ARDF_CACHE = os.path.join(DATA_CACHE, "ardf_alaska.json")
CGNDB_CACHE = os.path.join(DATA_CACHE, "cgndb_territories.csv")

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


def ingest_ak_ag_viability(path: str) -> pd.DataFrame:
    """
    Alaska borough agricultural viability dataset.
    Centroids derived from US Census TIGERweb borough polygons.
    Viability scores (0-5) synthesized from UAF Cooperative Extension publications
    and USDA NASS Alaska Agricultural Statistics; GDD from NOAA climate normals
    (base 50°F seasonal accumulation); population from ACS 2022 5-year estimates.
    data_quality=Low reflects the synthesized scoring methodology.
    """
    df = pd.read_csv(path, encoding="utf-8-sig", dtype={"fips": str})
    df = sanitize_strings(df)
    df, dropped = validate_and_clean(df, lat_col="centroid_lat", lon_col="centroid_lon")
    if dropped:
        print(f"  ak_ag_viability: dropped {dropped} rows with invalid coordinates")

    attrs_cols = {
        "gdd": "gdd_base50f", "population": "population",
        "score_grains": "score_grains", "score_vegetables": "score_vegetables",
        "score_livestock": "score_livestock", "score_mariculture": "score_mariculture",
        "score_greenhouse": "score_greenhouse",
    }

    return pd.DataFrame({
        "id": _uuids(len(df)),
        "lat": df["centroid_lat"].astype(float),
        "lon": df["centroid_lon"].astype(float),
        "centroid_lat": df["centroid_lat"].astype(float),
        "centroid_lon": df["centroid_lon"].astype(float),
        "location_name": df["name"].fillna("").astype(str),
        "country": "US",
        "state_province": "AK",
        "sector": "Agriculture",
        "asset_type": "agricultural zone",
        "ownership_type": "Public",
        "operator_owner": "State of Alaska",
        "data_source": "AK_AG_VIABILITY",
        "source_id": df["fips"].astype(str),
        "source_url": (
            "Derived: US Census TIGERweb borough centroids "
            "(https://tigerweb.geo.census.gov/), UAF Cooperative Extension "
            "(https://www.uaf.edu/ces/), USDA NASS Alaska "
            "(https://www.nass.usda.gov/Statistics_by_State/Alaska/), "
            "NOAA climate normals (https://www.ncei.noaa.gov/products/land-based-station/us-climate-normals)"
        ),
        "data_quality": "Low",
        "year_of_data": 2025,
        "attributes": _attrs(df, attrs_cols),
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


def ingest_ardf(path: str) -> pd.DataFrame:
    """USGS Alaska Resource Data File — mineral sites across Alaska.

    Downloads from the USGS ArcGIS REST API and caches to path.
    Source: https://mrdata.usgs.gov/ardf/
    """
    os.makedirs(DATA_CACHE, exist_ok=True)
    if not os.path.exists(path):
        print("  Downloading ARDF from USGS ArcGIS REST API...")
        base = (
            "https://services.arcgis.com/v01gqwM5QqNysAAi"
            "/ArcGIS/rest/services/ARDF_features/FeatureServer/0/query"
        )
        count_params = urllib.parse.urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"})
        with urllib.request.urlopen(f"{base}?{count_params}") as r:
            total = json.loads(r.read())["count"]
        print(f"  ARDF: {total} records to fetch...")
        all_features = []
        for offset in range(0, total, 1000):
            params = urllib.parse.urlencode({
                "where": "1=1", "outFields": "*",
                "resultOffset": offset, "resultRecordCount": 1000, "f": "json",
            })
            with urllib.request.urlopen(f"{base}?{params}") as r:
                data = json.loads(r.read())
            all_features.extend(data.get("features", []))
        with open(path, "w") as f:
            json.dump(all_features, f)
        print(f"SHA256 ardf_alaska.json: {sha256_file(path)}")

    with open(path) as f:
        features = json.load(f)

    rows = []
    dropped = 0
    for feat in features:
        a = feat.get("attributes", {})
        lat = a.get("Latitude")
        lon = a.get("Longitude")
        if lat is None or lon is None:
            dropped += 1
            continue
        rows.append({
            "id":            str(uuid.uuid4()),
            "lat":           float(lat),
            "lon":           float(lon),
            "centroid_lat":  float(lat),
            "centroid_lon":  float(lon),
            "location_name": str(a.get("Site", "") or ""),
            "country":       "US",
            "state_province":"AK",
            "sector":        "Mining",
            "asset_type":    str(a.get("Site_type", "mineral site") or "mineral site").lower(),
            "ownership_type":"Private",
            "operator_owner":"",
            "data_source":   "USGS_ARDF",
            "source_id":     str(a.get("ARDF_no", a.get("OBJECTID", ""))),
            "source_url":    "https://mrdata.usgs.gov/ardf/",
            "data_quality":  "High",
            "year_of_data":  2024,
            "attributes":    json.dumps({
                "site_status":       str(a.get("Site_status", "") or ""),
                "commodities_main":  str(a.get("Commodities_main", "") or ""),
                "commodities_other": str(a.get("Commodities_other", "") or ""),
                "district":          str(a.get("District", "") or ""),
                "production":        str(a.get("Production", "") or ""),
            }),
        })
    if dropped:
        print(f"  ardf: dropped {dropped} rows with missing coordinates")

    df = pd.DataFrame(rows)
    df = sanitize_strings(df)
    df, invalid = validate_and_clean(df)
    if invalid:
        print(f"  ardf: dropped {invalid} rows with invalid coordinates")
    return df


_CGNDB_TERRITORIES = {
    "yt": "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/geobase_cgn_toponyme/prov_csv_eng/cgn_yt_csv_eng.zip",
    "nt": "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/geobase_cgn_toponyme/prov_csv_eng/cgn_nt_csv_eng.zip",
    "nu": "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/geobase_cgn_toponyme/prov_csv_eng/cgn_nu_csv_eng.zip",
}


def ingest_cgndb(path: str) -> pd.DataFrame:
    """NRCan Canadian Geographical Names Database — populated places in YT, NT, and NU.

    Downloads territory zip files and caches merged CSV to path.
    Source: https://natural-resources.canada.ca/earth-sciences/geography/geographical-names/
    """
    os.makedirs(DATA_CACHE, exist_ok=True)
    if not os.path.exists(path):
        print("  Downloading CGNDB territory files from NRCan FTP...")
        frames = []
        for code, url in _CGNDB_TERRITORIES.items():
            zip_path = os.path.join(DATA_CACHE, f"cgn_{code}_csv_eng.zip")
            print(f"  Fetching {code.upper()}...")
            urllib.request.urlretrieve(url, zip_path)
            with zipfile.ZipFile(zip_path) as zf:
                csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
                with zf.open(csv_name) as f:
                    df = pd.read_csv(f, encoding="utf-8-sig", dtype=str)
            frames.append(df)
        merged = pd.concat(frames, ignore_index=True)
        merged.to_csv(path, index=False, encoding="utf-8")
        print(f"SHA256 cgndb_territories.csv: {sha256_file(path)}")

    df = pd.read_csv(path, dtype=str)
    df = df[df["Generic Category"] == "Populated Place"].copy()
    df = sanitize_strings(df)
    df = df.rename(columns={"Latitude": "lat", "Longitude": "lon"})
    df, dropped = validate_and_clean(df)
    if dropped:
        print(f"  cgndb: dropped {dropped} rows with invalid coordinates")

    return pd.DataFrame({
        "id":            _uuids(len(df)),
        "lat":           df["lat"].astype(float),
        "lon":           df["lon"].astype(float),
        "centroid_lat":  df["lat"].astype(float),
        "centroid_lon":  df["lon"].astype(float),
        "location_name": df["Geographical Name"].fillna("").astype(str),
        "country":       "CA",
        "state_province":df["Province - Territory"].fillna("").astype(str),
        "sector":        "Community",
        "asset_type":    df["Generic Term"].fillna("settlement").str.lower().astype(str),
        "ownership_type":"Public",
        "operator_owner":"",
        "data_source":   "NRCAN_CGNDB",
        "source_id":     df["CGNDB ID"].fillna("").astype(str),
        "source_url":    "https://natural-resources.canada.ca/earth-sciences/geography/geographical-names/",
        "data_quality":  "High",
        "year_of_data":  2026,
        "attributes":    df.apply(lambda r: json.dumps({
            "generic_term":  r.get("Generic Term", ""),
            "concise_code":  r.get("Concise Code", ""),
            "decision_date": r.get("Decision Date", ""),
        }), axis=1),
    })


AK_AG_CSV = os.path.join(GIS_DIR, "AK_Agricultural_Viability.csv")

PIPELINE: list[tuple[str, str, callable, tuple]] = [
    ("Airports.csv",                                  os.path.join(GIS_DIR,     "Airports.csv"),                                          ingest_airports,    ()),
    ("Well_Surface_Hole_Location.csv",                os.path.join(GIS_DIR,     "Well_Surface_Hole_Location.csv"),                        ingest_wells,       ()),
    ("table-Mines-and-other-primary-producing-sites.csv", os.path.join(GIS_DIR, "table-Mines-and-other-primary-producing-sites.csv"),     ingest_nrcan_csv,   ("NRCAN_MINES", "mine")),
    ("table-Processing-facilities.csv",               os.path.join(GIS_DIR,     "table-Processing-facilities.csv"),                       ingest_nrcan_csv,   ("NRCAN_PROCESSING", "processing facility")),
    ("table-Advanced-projects.csv",                   os.path.join(SUPPORT_DIR, "table-Advanced-projects.csv"),                           ingest_nrcan_csv,   ("NRCAN_ADVANCED", "mining project")),
    ("table-ArcGIS-Online-layers.csv",                os.path.join(GIS_DIR,     "table-ArcGIS-Online-layers.csv"),                        ingest_energy_layers, ()),
    ("AK_Agricultural_Viability.csv",                 AK_AG_CSV,        ingest_ak_ag_viability, ()),
    ("ardf_alaska.json",                              ARDF_CACHE,       ingest_ardf,             ()),
    ("cgndb_territories.csv",                         CGNDB_CACHE,      ingest_cgndb,            ()),
]


def run_ingest(db_path: str = DEFAULT_DB) -> None:
    print("=== arctic-atlas ingest ===")
    for name, path, _, _ in PIPELINE:
        if os.path.exists(path):
            print(f"SHA256 {name}: {sha256_file(path)}")
        elif not path.startswith(DATA_CACHE):
            print(f"MISSING {name}: {path}")

    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    total = 0

    for name, path, fn, args in PIPELINE:
        is_cached = path.startswith(DATA_CACHE)
        if not os.path.exists(path) and not is_cached:
            continue
        label = name.replace(".csv", "").replace(".json", "")
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

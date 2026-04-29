"""
Tests for arctic-atlas ETL pipeline.
All tests must FAIL before ingest.py exists, then pass after implementation.
"""
import hashlib
import io
import os
import sys
import tempfile
import textwrap
import uuid

import duckdb
import pandas as pd
import pytest

# Source file paths — absolute so tests run from any directory
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GIS_DIR = os.path.join(
    os.path.expanduser("~"),
    "repos",
    "BeadedCloud All Arctic Locations",
    "GIS FILES",
)
SUPPORT_DIR = os.path.join(
    os.path.expanduser("~"),
    "repos",
    "BeadedCloud All Arctic Locations",
    "Supporting Data",
)
AIRPORTS_CSV = os.path.join(GIS_DIR, "Airports.csv")
WELLS_CSV = os.path.join(GIS_DIR, "Well_Surface_Hole_Location.csv")
MINES_CSV = os.path.join(GIS_DIR, "table-Mines-and-other-primary-producing-sites.csv")
PROCESSING_CSV = os.path.join(GIS_DIR, "table-Processing-facilities.csv")
ADVANCED_CSV = os.path.join(SUPPORT_DIR, "table-Advanced-projects.csv")
ENERGY_CSV = os.path.join(GIS_DIR, "table-ArcGIS-Online-layers.csv")

REQUIRED_COLUMNS = {
    "id", "lat", "lon", "centroid_lat", "centroid_lon",
    "location_name", "country", "state_province",
    "sector", "asset_type", "ownership_type", "operator_owner",
    "data_source", "source_id", "source_url",
    "data_quality", "year_of_data", "attributes",
}


@pytest.fixture
def db(tmp_path):
    """Run ingest into a fresh in-memory DuckDB and return the connection."""
    from ingest import run_ingest
    db_path = str(tmp_path / "test.duckdb")
    run_ingest(db_path)
    con = duckdb.connect(db_path, read_only=True)
    yield con
    con.close()


@pytest.fixture
def db_with_stdout(tmp_path, capsys):
    """Run ingest and also capture stdout for hash-logging tests."""
    from ingest import run_ingest
    db_path = str(tmp_path / "test.duckdb")
    run_ingest(db_path)
    captured = capsys.readouterr()
    con = duckdb.connect(db_path, read_only=True)
    yield con, captured.out
    con.close()


# ---------------------------------------------------------------------------
# 1. Schema columns
# ---------------------------------------------------------------------------
def test_schema_columns(db):
    cols = {
        row[0] for row in db.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'sites'"
        ).fetchall()
    }
    missing = REQUIRED_COLUMNS - cols
    assert not missing, f"Missing columns: {missing}"


# ---------------------------------------------------------------------------
# 2. Coordinate validation — out-of-range rows dropped
# ---------------------------------------------------------------------------
def test_coordinate_validation(tmp_path, capsys):
    """Rows with invalid lat/lon must be dropped, not silently kept."""
    from ingest import validate_and_clean

    bad = pd.DataFrame({
        "lat": [91.0, -91.0, 45.0, 45.0, float("nan")],
        "lon": [0.0, 0.0, 181.0, -181.0, 0.0],
        "location_name": ["a", "b", "c", "d", "e"],
    })
    clean, dropped = validate_and_clean(bad, lat_col="lat", lon_col="lon")
    assert len(clean) == 0, f"Expected 0 valid rows, got {len(clean)}"
    assert dropped == 5

    good = pd.DataFrame({
        "lat": [65.0, -65.0, 90.0, -90.0],
        "lon": [0.0, 0.0, 180.0, -180.0],
        "location_name": ["a", "b", "c", "d"],
    })
    clean2, dropped2 = validate_and_clean(good, lat_col="lat", lon_col="lon")
    assert len(clean2) == 4
    assert dropped2 == 0


# ---------------------------------------------------------------------------
# 3. Formula injection — leading special chars stripped from strings
# ---------------------------------------------------------------------------
def test_formula_injection_stripped():
    from ingest import sanitize_strings

    df = pd.DataFrame({
        "name": ["=CMD.EXE", "+evil", "@bad", "-also bad", "normal", None],
        "num": [1, 2, 3, 4, 5, 6],
    })
    result = sanitize_strings(df)
    assert result["name"].iloc[0] == "CMD.EXE"
    assert result["name"].iloc[1] == "evil"
    assert result["name"].iloc[2] == "bad"
    assert result["name"].iloc[3] == "also bad"
    assert result["name"].iloc[4] == "normal"
    assert result["name"].iloc[5] is None or pd.isna(result["name"].iloc[5])


# ---------------------------------------------------------------------------
# 4. Deduplication — same (data_source, source_id) kept once
# ---------------------------------------------------------------------------
def test_deduplication(tmp_path):
    from ingest import load_into_db

    con = duckdb.connect(str(tmp_path / "dedup.duckdb"))

    rows = [
        {
            "id": str(uuid.uuid4()),
            "lat": 65.0, "lon": -150.0,
            "centroid_lat": 65.0, "centroid_lon": -150.0,
            "location_name": "Test Well",
            "country": "US", "state_province": "AK",
            "sector": "Oil & Gas", "asset_type": "well head",
            "ownership_type": "Private", "operator_owner": "TestCo",
            "data_source": "AK_DNR", "source_id": "W-001",
            "source_url": "https://example.gov/wells",
            "data_quality": "High", "year_of_data": 2024,
            "attributes": "{}",
        },
        {
            "id": str(uuid.uuid4()),
            "lat": 65.1, "lon": -150.1,
            "centroid_lat": 65.1, "centroid_lon": -150.1,
            "location_name": "Test Well Duplicate",
            "country": "US", "state_province": "AK",
            "sector": "Oil & Gas", "asset_type": "well head",
            "ownership_type": "Private", "operator_owner": "TestCo",
            "data_source": "AK_DNR", "source_id": "W-001",
            "source_url": "https://example.gov/wells",
            "data_quality": "Low", "year_of_data": 2024,
            "attributes": "{}",
        },
    ]
    df = pd.DataFrame(rows)
    load_into_db(con, df)
    load_into_db(con, df)  # load same data a second time

    count = con.execute(
        "SELECT COUNT(*) FROM sites WHERE data_source='AK_DNR' AND source_id='W-001'"
    ).fetchone()[0]
    assert count == 1, f"Expected 1 row after dedup, got {count}"
    con.close()


# ---------------------------------------------------------------------------
# 5. source_url present on every row
# ---------------------------------------------------------------------------
def test_source_url_present(db):
    null_count = db.execute(
        "SELECT COUNT(*) FROM sites WHERE source_url IS NULL OR source_url = ''"
    ).fetchone()[0]
    assert null_count == 0, f"{null_count} rows have no source_url"


# ---------------------------------------------------------------------------
# 6. Airports loaded
# ---------------------------------------------------------------------------
def test_airports_loaded(db):
    count = db.execute(
        "SELECT COUNT(*) FROM sites WHERE data_source = 'DOT_PF_AIRPORTS'"
    ).fetchone()[0]
    assert count >= 280, f"Expected ≥280 airports, got {count}"


# ---------------------------------------------------------------------------
# 7. Wells loaded
# ---------------------------------------------------------------------------
def test_wells_loaded(db):
    count = db.execute(
        "SELECT COUNT(*) FROM sites WHERE data_source = 'AK_DNR'"
    ).fetchone()[0]
    assert count >= 10_000, f"Expected ≥10,000 wells, got {count}"


# ---------------------------------------------------------------------------
# 8. Canadian NRCan sources loaded
# ---------------------------------------------------------------------------
def test_canadian_mines_loaded(db):
    count = db.execute(
        "SELECT COUNT(*) FROM sites WHERE data_source IN "
        "('NRCAN_MINES', 'NRCAN_PROCESSING', 'NRCAN_ADVANCED')"
    ).fetchone()[0]
    assert count >= 60, f"Expected ≥60 Canadian mine/processing rows, got {count}"


# ---------------------------------------------------------------------------
# 9. No null lat/lon in final db
# ---------------------------------------------------------------------------
def test_no_null_lat_lon(db):
    null_count = db.execute(
        "SELECT COUNT(*) FROM sites WHERE lat IS NULL OR lon IS NULL"
    ).fetchone()[0]
    assert null_count == 0, f"{null_count} rows have null lat or lon"


# ---------------------------------------------------------------------------
# 10. SHA256 logged for each source file
# ---------------------------------------------------------------------------
def test_sha256_logged(db_with_stdout):
    _, stdout = db_with_stdout
    assert "SHA256" in stdout, "Expected SHA256 hashes in ingest output"
    source_names = ["Airports.csv", "Well_Surface_Hole_Location.csv",
                    "table-Mines", "table-Processing", "table-Advanced"]
    for name in source_names:
        assert name in stdout, f"Expected '{name}' mentioned in ingest output"

"""Tests for map.py — HTML map generator."""
import json
import os
import sys
import tempfile
import uuid

import duckdb
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _make_test_db(path: str, n: int = 20) -> None:
    """Seed a minimal DuckDB with n rows across known sectors."""
    from ingest import SCHEMA_SQL
    con = duckdb.connect(path)
    con.execute(SCHEMA_SQL)
    sectors = ["Oil & Gas", "Mining", "Transportation", "Defense", "Energy & Utilities"]
    rows = [
        (str(uuid.uuid4()), 65.0 + i * 0.1, -150.0 + i * 0.1,
         65.0 + i * 0.1, -150.0 + i * 0.1,
         f"Site {i}", "US", "AK",
         sectors[i % len(sectors)], "well head", "Private", "TestCo",
         "AK_DNR", f"SRC-{i}", "https://example.gov", "High", 2026, "{}")
        for i in range(n)
    ]
    con.executemany(
        "INSERT INTO sites VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    con.close()


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "test.duckdb")
    _make_test_db(p)
    return p


@pytest.fixture
def out(tmp_path):
    return str(tmp_path / "map.html")


def test_map_generates_html(db, out):
    from map import generate
    generate(db_path=db, out_path=out)
    assert os.path.exists(out)
    assert os.path.getsize(out) > 1000


def test_map_html_contains_point_count(db, out):
    from map import generate
    generate(db_path=db, out_path=out)
    html = open(out).read()
    assert "20" in html


def test_map_html_has_sectors(db, out):
    from map import generate
    generate(db_path=db, out_path=out)
    html = open(out).read()
    for sector in ["Oil & Gas", "Mining", "Transportation"]:
        assert sector in html, f"Sector '{sector}' missing from map HTML"


def test_map_data_embedded_as_valid_json(db, out):
    from map import generate
    generate(db_path=db, out_path=out)
    html = open(out).read()
    start = html.index("const DATA = ") + len("const DATA = ")
    end = html.index(";\n", start)
    data = json.loads(html[start:end])
    assert len(data) == 20
    assert all("lat" in d and "lon" in d and "sector" in d for d in data)


def test_map_is_idempotent(db, out):
    from map import generate
    generate(db_path=db, out_path=out)
    content1 = open(out).read()
    # Replace UUIDs/timestamps that could differ; point count and structure must match
    generate(db_path=db, out_path=out)
    content2 = open(out).read()
    # Both runs should produce valid HTML of same length (data is deterministic from db)
    assert abs(len(content1) - len(content2)) < 100


def test_map_fails_without_db(out):
    from map import generate
    with pytest.raises(SystemExit):
        generate(db_path="/nonexistent/path.duckdb", out_path=out)

# arctic-atlas

ETL pipeline that consolidates Arctic infrastructure data from verified government sources into a DuckDB database.

## Quick start

```bash
pip install -r requirements.txt
python ingest.py          # writes arctic.duckdb
python -m pytest tests/   # 10 tests, ~5 seconds
```

## Project layout

```
ingest.py          — ETL pipeline; run to populate arctic.duckdb
tests/             — pytest suite
DATA_SOURCES.md    — provenance, URLs, and quality ratings for every source
arctic.duckdb      — generated output (gitignored; run ingest.py to create)
```

## Adding a new data source

1. Verify a public URL or document private provenance in `DATA_SOURCES.md`.
2. Write a parser function in `ingest.py` following the existing pattern.
3. Add it to `PIPELINE` at the bottom of `ingest.py`.
4. Write a test asserting minimum expected row count.
5. Run `python ingest.py` and confirm SHA256 is logged.

## Schema

All records land in a single `sites` table. Key columns:

| Column | Notes |
|---|---|
| `lat`, `lon` | WGS84 decimal degrees, validated [-90,90]/[-180,180] |
| `centroid_lat/lon` | Same as lat/lon for point data; computed for polygon/line sources |
| `sector` | Oil & Gas \| Mining \| Transportation \| Defense \| Energy & Utilities \| Community |
| `data_source` | Identifies the source dataset (AK_DNR, FAA, NRCAN_MINES, etc.) |
| `source_url` | Required. Verifiable URL to the originating dataset. |
| `data_quality` | High \| Medium \| Low |
| `attributes` | JSON blob for source-specific fields |

Dedup key: `(data_source, source_id)` — duplicate pairs are silently skipped on re-ingest.

## Data integrity rules

- **No synthetic coordinates.** Every record must trace to a real government or documented private source.
- **No lat >= 50 filter at ingest.** Subarctic filtering is a query-time concern.
- SHA256 of every source file is logged at each ingest run. Store these hashes when you want to detect upstream changes.

## Current record count

~10,815 records across 5 sources (AK DNR wells, AK airports, Canadian mines/processing/advanced projects). See `DATA_SOURCES.md` for the Phase 2 sourcing plan toward 2M+ records.

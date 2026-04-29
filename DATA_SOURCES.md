# Data Sources

All records in arctic-atlas must have a verifiable `source_url`. Synthetic or
AI-generated coordinates are not permitted. Private datasets must be labeled
`data_quality=Low` and have a prose description in place of a public URL.

## Current Sources

### AK_DNR — Alaska Oil & Gas Well Heads
- **File:** `GIS FILES/Well_Surface_Hole_Location.csv`
- **URL:** https://aogknowledgebase.alaska.gov/
- **Agency:** Alaska Department of Natural Resources, Division of Oil and Gas
- **Records:** ~10,250 well head locations across Alaska
- **Quality:** High
- **Notes:** 11 rows dropped at ingest for invalid coordinates (null WellHeadLat/Long)

### DOT_PF_AIRPORTS — Alaska DOT&PF Airports
- **File:** `GIS FILES/Airports.csv`
- **URL:** https://gis.data.alaska.gov/datasets/dot-pf-airports
- **Agency:** Alaska Department of Transportation & Public Facilities
- **Records:** 285 airports and airstrips
- **Quality:** High

### NRCAN_MINES — Canadian NRCan Mines and Primary Producing Sites
- **File:** `GIS FILES/table-Mines-and-other-primary-producing-sites.csv`
- **URL:** https://open.canada.ca/data/en/dataset/fdc9c72e-0e31-4f40-a5c9-1c0bf0f8f2da
- **Agency:** Natural Resources Canada
- **Records:** 67 mines and primary producing sites
- **Quality:** High

### NRCAN_PROCESSING — Canadian NRCan Processing Facilities
- **File:** `GIS FILES/table-Processing-facilities.csv`
- **URL:** https://open.canada.ca/data/en/dataset/fdc9c72e-0e31-4f40-a5c9-1c0bf0f8f2da
- **Agency:** Natural Resources Canada
- **Records:** 31 processing facilities
- **Quality:** High

### NRCAN_ADVANCED — Canadian NRCan Advanced Mining Projects
- **File:** `Supporting Data/table-Advanced-projects.csv`
- **URL:** https://open.canada.ca/data/en/dataset/fdc9c72e-0e31-4f40-a5c9-1c0bf0f8f2da
- **Agency:** Natural Resources Canada
- **Records:** 193 advanced mining/exploration projects
- **Quality:** High

## Adding New Sources

1. Verify the data has a public URL or documented private provenance.
2. Add an entry to this file with agency, URL, and quality rating.
3. Write an ingest function in `ingest.py` following the pattern of existing parsers.
4. Add a test asserting minimum expected row count.
5. Run `python ingest.py` and confirm the SHA256 is logged.

## Planned Sources (Phase 2)

| Source | Agency | Estimated Records | URL |
|---|---|---|---|
| OSM Arctic extract | OpenStreetMap | ~500k | https://overpass-api.de/ |
| USGS Mineral Resources | USGS | ~50k | https://mrdata.usgs.gov/ |
| Canadian NRCAN full dataset | NRCan | ~10k | https://open.canada.ca/ |
| Norway NPD wells | Norwegian Petroleum Directorate | ~8k | https://factpages.npd.no/ |
| AK community buildings | OpenStreetMap / AHFC | ~200k | TBD |

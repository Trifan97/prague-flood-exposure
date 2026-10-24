# Prague flood exposure: a distributed spatial join in Apache Sedona

> **Status: work in progress (step 1 of 8: inputs downloaded and pinned).** There is no
> analysis code and there are no results yet. The sections below describe what the pipeline will
> do and what it already knows it cannot do.

Which Prague buildings sit inside official flood zones, and which of the city's 57
districts (*městské části*) have the most exposed building stock? The pipeline answers
that with a distributed spatial join in [Apache Sedona](https://sedona.apache.org/)
on [Wherobots](https://wherobots.com/), and exports one clean GeoParquet table for a
CARTO map.

## What it will do

1. Load the official IPR Praha flood zones, Overture building footprints and the
   57 district polygons.
2. Re-project every layer to one metric CRS, UTM 33N (EPSG:32633).
3. Join buildings to flood zones in Sedona, so each building gets the most frequent
   flood it is exposed to (Q5, Q20, Q50 or Q100).
4. Count exposed buildings and their footprint area per flood zone and per district.
5. Rank districts by exposure and export GeoParquet for CARTO.

## Scope

- **In scope:** consuming an existing, official flood-hazard layer, the spatial join and
  the aggregation.
- **Out of scope:** flood *simulation* (hydraulic or rain-on-grid modelling), machine
  learning, and any city other than Prague.

## Known limitations (so far)

- **These are river and small-stream floods, not flash floods from rain.** The zones
  are design floods on the Vltava and Berounka (Q5 to Q100) and Q100 zones on small
  streams. Surface-water ("pluvial") flooding after cloudbursts is not mapped in any open
  layer the project found, so it is not covered.
- **Extents, not depths.** A building is either inside a zone or not. The open layers
  carry no water depth.
- **Flood defences.** Central Prague is protected by permanent and mobile barriers. The
  plain Q-zones do not show that protection, so exposure behind the barriers will be
  flagged separately using IPR's categorised layer (protected by the city or not).
- **Hazard vintage.** IPR's catalogue dates the Vltava Q-zones to 2021, the small-stream
  zones to 2024 and the categorised layer to 2014. The Metropolitan Plan publishes a
  newer (2026) categorised layer, but its published code list does not describe its
  categories, so it is not used.
- **Buildings are not complete or valued.** Of the 208,636 Overture footprints that
  intersect Prague, 83.6% come from OpenStreetMap and the rest from Microsoft's
  machine-learned footprints. Counts and areas measure exposure, not value or loss.
  Height is recorded for only 0.5% of buildings, so volume or floor area cannot be
  estimated reliably; the number of floors is known for 51%.
- **Inputs can disappear.** Overture keeps only recent releases online, so the extract
  can be rebuilt from source only until release 2026-10-21.0 is deleted. IPR may
  republish its layers at any time. In both cases the SHA-256 pins make the pipeline stop
  instead of silently using different data.
- **This is not big data.** About 210,000 buildings would fit a laptop. Sedona is used
  to learn a pattern that scales unchanged to national or global extents. The README
  will report a GeoPandas comparison on the same data.

## Data

`python -m prague_flood download` fetches everything into `data/raw/` and checks every
file against the SHA-256 recorded in [`config.py`](src/prague_flood/config.py). Raw
files are never edited; all cleaning happens in code.

| File | Contents | Source | Licence |
|---|---|---|---|
| `ipr_vltava_q5/q20/q50/q100.geojson` | Vltava flood extents for discharges of 1,770 / 2,720 / 3,440 / 4,020 m³/s below the Berounka confluence. Each is essentially one polygon of 56,000 to 105,000 vertices | [IPR Praha open data](https://www.geoportalpraha.cz/en/data-and-services/open-data) | CC BY 4.0, "data base © IPR Praha" |
| `ipr_small_streams.geojson` | 2,201 polygons on small streams: active zone (`kateg` 1) and Q100 flood area (2) | IPR Praha | CC BY 4.0 |
| `ipr_vltava_categories.geojson` | 231 Vltava/Berounka zone polygons by category; `kateg` 4 = designated for protection by the city's flood defences | IPR Praha | CC BY 4.0 |
| `ipr_districts.geojson` | The 57 city districts (*městské části*) | IPR Praha | CC BY 4.0 |
| `overture_buildings_prague_2026-10-21.0.parquet` | 306,290 building footprints whose bounding boxes overlap Prague's, as GeoParquet, cut from [Overture Maps](https://overturemaps.org/) release 2026-10-21.0 with DuckDB | Overture Maps | ODbL |

The IPR files are GeoJSON in WGS84, exactly as the portal exports them. Overture's
buildings cover the planet, so `overture.py` reads only the parts of the S3 files whose
bounding-box statistics overlap Prague. That takes about 6 minutes. The extract is
sorted by building id, so rerunning it produces the same bytes; that was checked by
running it twice.

## Environment

The local environment copies the Wherobots v2 runtime, so code tested here runs there:

- **Spark 4.0** (`pyspark` 4.0.x) with **Apache Sedona 1.9**.
- **Java 17.** Spark runs on the JVM, so local runs and the Sedona tests need it.
- **Python 3.11.** Spark's Python support trails new Python releases, and 3.11 is
  supported by every Spark 4.0 build.

### Java

Java lives inside the project, so nothing is installed system-wide. The project uses the
portable Temurin JDK 17.0.20.1+1 zip from Eclipse Adoptium, unpacked to `.tools/jdk-17`
(git-ignored). `scripts/dev_env.ps1` points the current PowerShell window at it and
activates the venv. Other windows and projects are unaffected. To install it:

```powershell
$zip = "OpenJDK17U-jdk_x64_windows_hotspot_17.0.20.1_1.zip"
Invoke-WebRequest "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/$zip" -OutFile $zip
(Get-FileHash $zip -Algorithm SHA256).Hash   # must be E53A79C3C3D86865BD7E787903884331068E71321714FFD44F145785AFFC7CB0
Expand-Archive $zip .tools
Rename-Item ".tools\jdk-17.0.20.1+1" "jdk-17"
```

### Spark on Windows: what works locally and what doesn't

Hadoop, which Spark uses for file access, needs a helper program (`winutils.exe`) on
Windows to set file permissions. Apache does not distribute it, and this project does not
use the unofficial builds. Two consequences:

- **The Sedona jars are not fetched by Spark.** `spark.jars.packages` would make Spark
  copy the jars and set their permissions. Instead, `download.py` fetches them from Maven
  Central, checks their SHA-256 values against `config.py`, and `session.py` puts them
  straight onto the JVM classpath.
- **Local Spark can read files but cannot write them.** Reading Parquet, spatial joins
  and shuffles all work. Writing Parquet fails. Local runs are for tests on small
  in-memory data; the full pipeline writes its outputs on Wherobots, which runs on Linux.

## Quick start (Windows, PowerShell)

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e . --no-deps
# Install Java (see above), then in every new window:
. .\scripts\dev_env.ps1
python -m prague_flood download   # ~80 MB of inputs into data/raw (about 7 minutes)
pytest                            # everything, including the Spark tests
pytest -m "not spark"             # fast tests only, no JVM
```

Tests marked `data` check the files in `data/raw` and are skipped, with the reason
printed, until the download has run.

`requirements.txt` pins the exact versions that were tested. `--no-deps` stops pip from
re-resolving them. The first Spark run downloads the Sedona jars (about 110 MB) into
`.spark/jars`.

PySpark is published only as a 434 MB source archive, so pip builds the wheel locally in
the temp folder. If the system drive is nearly full, point the temp folder at another
drive for that one install, for example `$env:TMP = "F:\pip-tmp"; $env:TEMP = $env:TMP`.

## Repository layout

```
pyproject.toml      package metadata, loose dependency ranges, pytest/ruff/mypy settings
requirements.txt    exact tested versions (pip freeze)
scripts/
  dev_env.ps1       points this PowerShell window at the project's JDK and venv
src/prague_flood/   the pipeline package
  __main__.py       command line: python -m prague_flood download
  config.py         paths, CRSs, pinned inputs (URL + SHA-256)
  download.py       checksum-verified downloads of the IPR layers
  overture.py       DuckDB extract of Prague's buildings from Overture on S3
  session.py        local Spark session with Sedona registered
tests/              pytest suite (markers: `spark` needs Java, `data` needs data/raw)
data/raw/           downloaded inputs (git-ignored, re-created by the download stage)
```

## Licence

Code: MIT (see `LICENSE`). Data keeps its source licence (see the Data table).

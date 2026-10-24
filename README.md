# Prague flood exposure: a distributed spatial join in Apache Sedona

> **Status: work in progress (step 0, the repository skeleton).** There is no analysis
> code and there are no results yet. The sections below describe what the pipeline will
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
  zones to 2024 and the categorised layer to 2014.
- **Buildings are not complete or valued.** In Overture release 2026-10-21.0, 84% of
  Prague's footprints come from OpenStreetMap and 16% from Microsoft's machine-learned
  footprints. Counts and areas measure exposure, not value or loss.
- **This is not big data.** About 210,000 buildings would fit a laptop. Sedona is used
  to learn a pattern that scales unchanged to national or global extents. The README
  will report a GeoPandas comparison on the same data.

## Data (planned)

| Layer | Source | Licence |
|---|---|---|
| Flood zones Q5, Q20, Q50, Q100 (Vltava, Berounka), small streams Q100, categorised zones | [IPR Praha open data](https://www.geoportalpraha.cz/en/data-and-services/open-data) | CC BY 4.0, "data base © IPR Praha" |
| District polygons (57 *městské části*) | IPR Praha open data | CC BY 4.0 |
| Building footprints | [Overture Maps](https://overturemaps.org/), release 2026-10-21.0 | ODbL |

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
pytest                    # everything, including the Spark tests (~20 s)
pytest -m "not spark"     # fast tests only, no JVM
```

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
  config.py         paths, CRSs, pinned downloads (URL + SHA-256)
  download.py       checksum-verified downloads
  session.py        local Spark session with Sedona registered
tests/              pytest suite (Spark tests are marked `spark`)
```

## Licence

Code: MIT (see `LICENSE`). Data keeps its source licence (see the Data table).

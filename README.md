# Prague flood exposure: a distributed spatial join in Apache Sedona

> **Status: work in progress (step 7 of 8: the Wherobots run is prepared, not yet run).**
> The results below come from a local run and are confirmed by an independent GeoPandas
> implementation.

Which Prague buildings sit inside official flood zones, and which of the city's 57
districts (*městské části*) have the most exposed building stock? The pipeline answers
that with a distributed spatial join in [Apache Sedona](https://sedona.apache.org/)
on [Wherobots](https://wherobots.com/), and exports one clean GeoParquet table for a
CARTO map.

## Results so far

Of Prague's 208,621 buildings, **5,421 (2.6%) touch an official river or small-stream
flood zone** while the flood defences hold. That is 108.6 ha of building footprint.
**Behind the defences lie another 4,610 buildings with 181.4 ha of footprint**, which
flood only if the defences fail. Prague's flood exposure is mostly a question of whether
the barriers hold.

| Most frequent flood a building touches | Buildings | Footprint (ha) | Of which point inside zone | At least 30 m² |
|---|---:|---:|---:|---:|
| Q5 | 1,264 | 12.9 | 1,253 | 799 |
| Q20 | 1,454 | 36.3 | 1,448 | 1,032 |
| Q50 | 451 | 16.2 | 433 | 262 |
| Q100 | 2,252 | 43.2 | 1,575 | 1,623 |
| **Exposed, defences holding** | **5,421** | **108.6** | **4,709** | **3,716** |
| Residual: behind the defences only | 4,610 | 181.4 | n/a | 4,058 |

Districts ranked by exposed footprint (defences holding):

| Rank | District | Exposed footprint (ha) | Share of the district's footprint | Exposed buildings | Rank by count |
|---:|---|---:|---:|---:|---:|
| 1 | Praha 16 (Radotín) | 18.8 | 35% | 598 | 2 |
| 2 | Praha 7 | 14.2 | 12% | 297 | 7 |
| 3 | Praha 6 | 12.0 | 3% | 492 | 4 |
| 4 | Praha 4 | 10.2 | 3% | 310 | 6 |
| 5 | Praha-Zbraslav | 8.4 | 18% | 785 | 1 |
| 6 | Praha 10 | 7.5 | 2% | 250 | 9 |
| 7 | Praha-Lipence | 6.9 | 29% | 577 | 3 |
| 8 | Praha-Velká Chuchle | 5.6 | 25% | 237 | 10 |

Behind the defences, the order changes completely: **Praha 8 (61.2 ha), Praha 1
(49.5 ha) and Praha 7 (31.7 ha)** hold the most residual-risk footprint. These are the
central riverside districts, including Karlín (Praha 8), one of the areas worst hit by
the 2002 flood. 11 of the 57 districts have no exposed buildings at all.

## What it will do

1. Load the official IPR Praha flood zones, Overture building footprints and the
   57 district polygons.
2. Re-project every layer to one metric CRS, UTM 33N (EPSG:32633), and repair invalid
   polygons.
3. Join buildings to flood zones in Sedona, so each building gets the most frequent
   flood it is exposed to (Q5, Q20, Q50 or Q100). Buildings behind the city's flood
   defences are flagged separately as residual risk.
4. Count exposed buildings and their footprint area per flood zone and per district.
5. Rank districts by exposure and export GeoParquet for CARTO.

## Scope

- **In scope:** consuming an existing, official flood-hazard layer, the spatial join and
  the aggregation.
- **Out of scope:** flood *simulation* (hydraulic or rain-on-grid modelling), machine
  learning, and any city other than Prague.

## Method

### Hazard zones

`hazard.py` stacks the flood layers into one table with a `hazard`, `return_period` and
`zone` per polygon:

- **Vltava extents** Q5, Q20, Q50 and Q100. They stay nested (the Q100 zone contains the
  Q5 zone): each building later takes the smallest return period it touches, so no
  geometry has to be cut into bands.
- **Small streams.** Both parts (active zone and the rest) count as Q100.
- **Residual risk:** the area behind the city's flood defences, with no return period.

### Splitting giant polygons

A spatial join first compares bounding boxes, and only pairs whose boxes overlap get the
exact, expensive geometry test. The Q5 extent arrives as one polygon of 105,496 vertices
whose box covers much of central Prague. Every building in that box passes the cheap
test, and each exact test then walks a 105,000-vertex outline. `ST_SubDivideExplode`
cuts every zone into pieces of at most 256 vertices, which have tight boxes and short
outlines:

| Zone | Polygons before | Largest before (vertices) | Pieces after |
|---|---:|---:|---:|
| Vltava Q5 | 3 | 105,496 | 897 |
| Vltava Q20 | 1 | 77,454 | 657 |
| Vltava Q50 | 2 | 68,430 | 565 |
| Vltava Q100 | 2 | 56,223 | 458 |
| Small streams, active zone | 449 | 55,071 | 1,585 |
| Small streams, rest of Q100 | 1,752 | 9,741 | 3,656 |
| Residual (behind defences) | 20 | 2,965 | 122 |
| **Total** | **2,229** | | **7,940** |

The pieces do not overlap, so every zone's area is unchanged (checked by a data test).
On the development laptop (local Spark, 4 cores), joining the 306,290 buildings to the
7,940 pieces with `ST_Intersects` takes about 10 seconds. The same join against the
2,229 unsplit zones was cancelled after 10 minutes without finishing, so splitting made
it at least 60 times faster here. The two runs could not be compared row by row because
the unsplit one never finished; the area test is what shows the split loses nothing.

### The join

`join.py` produces one row per building inside Prague:

1. **District.** Each building gets the district that contains `ST_PointOnSurface` of its
   footprint. Unlike the centroid, that point always lies on the footprint, even for an
   L-shaped building. A building whose point falls outside every district is outside
   Prague and is dropped (306,290 in the extract, 208,621 inside the city).
2. **Hazard.** Every building is joined to every hazard piece its footprint touches
   (`ST_Intersects`), and the matches are collapsed to one row per building: the
   smallest Vltava return period, whether it touches the small-stream Q100 area or its
   active zone, and whether it touches the area behind the city's defences.
3. **Two rules.** A building counts as exposed when any part of its footprint touches a
   zone. The stricter rule, that its representative point lies in a zone, is evaluated in
   the same join: the point lies on the footprint, so any zone that contains it is already
   among the matched pairs.

**Broadcast or partition?** The 7,940 zone pieces and 57 districts fit in memory, so
they are *broadcast*: every worker gets a copy with an R-tree index (Sedona's
`BroadcastIndexJoin`), and the 300,000 buildings never move between workers. The
alternative, Sedona's `RangeJoin`, partitions both sides by space and shuffles them.
Both give identical results (checked in a unit test and on the full city). On the
development laptop (4 cores, two runs each), the whole exposure step took 57-63 s
broadcast and 105-132 s with the RangeJoin: roughly half the time. A RangeJoin is for
when neither side fits in memory, for example all of Europe's buildings against all of
its flood zones.

### District totals and ranking

`aggregate.py` sums the per-building table by district and by flood class. Districts are
**ranked by exposed footprint area, not by building count**. A garage and an apartment
block each count as one building, and many exposed buildings are small:

- Half of the exposed machine-learned footprints are under 30 m².
- Among exposed OSM buildings, 315 are garages (median 19 m²).

Ranked by count, the top three are Zbraslav, Praha 16 and Lipence, where cottages and
sheds crowd the floodplain at the Vltava–Berounka confluence. Ranked by area, Praha 7
and Praha 6 move up and Zbraslav drops to fifth.

The district table also carries:
- counts under the stricter point rule;
- counts of buildings of at least 30 m², a judgement-call threshold set in `config.py`;
- cumulative counts and areas for Q5, Q20 and Q50;
- a separate ranking for residual risk;
- the share of each district's exposed buildings that are machine-learned footprints.

## Sedona against a single machine

[`benchmarks/geopandas_baseline.py`](benchmarks/geopandas_baseline.py) repeats the
analysis with GeoPandas: same files, same rules, one core, no Spark. It is the honest
baseline, and an independent check. GeoPandas computes on GEOS and Sedona on JTS, and the
two classify **all 10,031 exposed and residual buildings identically, with no
differences**, under both exposure rules.

Timings on the development laptop:

| Engine | Flood zones | Building-to-zone join | Total |
|---|---|---:|---:|
| GeoPandas, 1 core | whole (2,229) | 620 s, plus 123 s for the point rule | 759 s (read to classified) |
| GeoPandas, 1 core | split, at most 256 vertices (7,065 pieces) | 1.3 s, plus 0.2 s | **19 s** (read to classified) |
| Sedona, local Spark, 4 cores | split (7,940 pieces) | whole exposure step 35-60 s | 127 s (full pipeline, including totals and export) |
| Sedona, Wherobots | split | *pending* | *pending* |

What this shows:

- **For one city, a laptop with GeoPandas is the fastest tool.** 208,621 buildings fit
  in memory. Spark's fixed costs (starting the JVM, scheduling tasks, moving geometries
  between processes) outweigh the actual work.
- **The big win came from splitting the polygons, not from the engine.** Splitting made
  the join about 40 times faster in GeoPandas and at least 60 times faster in Sedona.
- **What Sedona adds is headroom.** The same code, unchanged, partitions the work across
  a cluster when the data no longer fits on one machine, for example every building in a
  country against its national flood zones. At Prague's size that headroom is unused.

## Known limitations (so far)

- **These are river and small-stream floods, not flash floods from rain.** The zones
  are design floods on the Vltava and Berounka (Q5 to Q100) and Q100 zones on small
  streams. Surface-water ("pluvial") flooding after cloudbursts is not mapped in any open
  layer the project found, so it is not covered.
- **Extents, not depths.** A building is either inside a zone or not. The open layers
  carry no water depth.
- **Touching counts as exposed.** A building whose footprint grazes a zone's edge counts
  in full, which overstates exposure. The stricter point rule is reported alongside: on
  the full city it counts 13% fewer exposed buildings (4,709 against 5,421). Footprint
  areas are whole buildings, not the part inside the zone.
- **Flood defences are assumed to hold.** Central Prague is protected by permanent and
  mobile barriers, and IPR draws the Q-zones with them in place: only about 1% of the
  area the city protects lies inside the Q100 extent. The exposure counts are therefore
  for the case where the defences hold. Buildings behind the defences will be reported
  separately as residual risk (flooded only if the defences fail), using IPR's 2014
  categorised layer. That older layer does not cover 1.6 km² of the 2021 Q100 extent.
- **Some source polygons are repaired.** 14 IPR polygons, including the only polygon
  of the Q20 layer, have rings that touch themselves at a point. ArcGIS allows that; the
  OGC rules Sedona follows do not. `ST_MakeValid` repairs them and leaves every area
  unchanged (checked to 0.001%).
- **Hazard vintage.** IPR's catalogue dates the Vltava Q-zones to 2021, the small-stream
  zones to 2024 and the categorised layer to 2014. The Metropolitan Plan publishes a
  newer (2026) categorised layer, but its published code list does not describe its
  categories, so it is not used.
- **Buildings are not complete or valued.** Of the 208,636 Overture footprints that
  intersect Prague, 83.6% come from OpenStreetMap and the rest from Microsoft's
  machine-learned footprints. Counts and areas measure exposure, not value or loss.
  Height is recorded for only 0.5% of buildings, so volume or floor area cannot be
  estimated reliably; the number of floors is known for 51%.
- **Exposed buildings lean on machine-learned footprints.** They are 38% of the exposed
  buildings but only 16% of all buildings, and 80% of the exposed buildings in Praha 8.
  They are mostly small (median 29 m²) and carry no attributes, so the counts in those
  districts rest largely on unverified shapes. The district table reports this share.
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
| `ipr_small_streams.geojson` | 2,201 polygons on small streams, in two non-overlapping parts of the Q100 flood area: the active zone (`kateg` 1) and the rest (2) | IPR Praha | CC BY 4.0 |
| `ipr_vltava_categories.geojson` | 231 Vltava/Berounka zone polygons by category; `kateg` 4 = designated for protection by the city's flood defences | IPR Praha | CC BY 4.0 |
| `ipr_districts.geojson` | The 57 city districts (*městské části*) | IPR Praha | CC BY 4.0 |
| `overture_buildings_prague_2026-10-21.0.parquet` | 306,290 building footprints whose bounding boxes overlap Prague's, as GeoParquet, cut from [Overture Maps](https://overturemaps.org/) release 2026-10-21.0 with DuckDB | Overture Maps | ODbL |

The IPR files are GeoJSON in WGS84, exactly as the portal exports them. Overture's
buildings cover the planet, so `overture.py` reads only the parts of the S3 files whose
bounding-box statistics overlap Prague. That takes about 6 minutes. The extract is
sorted by building id, so rerunning it produces the same bytes; that was checked by
running it twice.

## Outputs

`python -m prague_flood run` writes three GeoParquet files in WGS84 to `data/processed/`.
[`docs/carto.md`](docs/carto.md) walks through building the CARTO map from them, click by
click.

| File | Rows | One row per |
|---|---:|---|
| `prague_flood_districts.parquet` | 57 | district: totals under both exposure rules, cumulative Q5/Q20/Q50, residual risk, machine-learned share, three rankings |
| `prague_flood_buildings.parquet` | 10,031 | building that is exposed (5,421) or behind the defences (4,610), with its class, flags and footprint area |
| `prague_flood_zones.parquet` | 2,229 | whole flood zone with its area, for drawing under the buildings |

The heavy work (re-projection, splitting, the joins) happens in Sedona. The outputs are
small, so they are collected to the driver and written with GeoPandas, which works the
same locally and on Wherobots. Areas are computed in UTM 33N before the geometries go
back to WGS84 for CARTO. A data test re-projects the exported districts with PROJ (via
GeoPandas), a second CRS library, and gets Prague's ~496 km² again.

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
python -m prague_flood run        # the Sedona pipeline; GeoParquet into data/processed (~2 min)
python benchmarks/geopandas_baseline.py   # the same analysis in GeoPandas, compared (~20 s)
pytest                            # everything, including Spark and data tests (~5 min)
pytest -m "not spark"             # fast tests only, no JVM
```

Tests marked `data` check the files in `data/raw` and are skipped, with the reason
printed, until the download has run.

To run the same pipeline on Wherobots, follow [`docs/wherobots.md`](docs/wherobots.md):
upload the inputs to Wherobots Managed Storage, put the notebook and the zipped committed
package in My Files, and run
[`notebooks/wherobots_run.ipynb`](notebooks/wherobots_run.ipynb). The notebook checks
that the inputs match the pinned SHA-256 values and that the results equal the local run.

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
  package_for_wherobots.py   zips the committed package for Wherobots (git archive)
benchmarks/
  geopandas_baseline.py      the same analysis in GeoPandas: timing and cross-check
notebooks/
  wherobots_run.ipynb        the cloud run: input check, timed joins, full pipeline
src/prague_flood/   the pipeline package
  __main__.py       command line: python -m prague_flood download
  config.py         paths, CRSs, pinned inputs (URL + SHA-256)
  download.py       checksum-verified downloads of the IPR layers
  load.py           every layer into Sedona: English columns, decoded codes, EPSG:32633
  hazard.py         one hazard-zone table, cut into pieces of at most 256 vertices
  join.py           buildings to districts and hazard zones: one row per building
  aggregate.py      district and flood-class totals, ranked by exposed footprint area
  export.py         small results to GeoParquet in WGS84, via GeoPandas
  pipeline.py       the whole analysis: python -m prague_flood run
docs/carto.md       click-by-click CARTO Builder guide
docs/wherobots.md   set-up and run checklist for Wherobots
  overture.py       DuckDB extract of Prague's buildings from Overture on S3
  session.py        local Spark session with Sedona registered
tests/              pytest suite (markers: `spark` needs Java, `data` needs data/raw)
data/raw/           downloaded inputs (git-ignored, re-created by the download stage)
data/processed/     GeoParquet outputs (git-ignored, re-created by the run stage)
```

## Licence

Code: MIT (see `LICENSE`). Data keeps its source licence (see the Data table).

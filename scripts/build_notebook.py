"""Build the Wherobots notebooks from the cells below.

- notebooks/wherobots_run.ipynb: checks the inputs, times the join, writes the outputs.
- notebooks/wherobots_maps.ipynb: renders the three kepler.gl maps.

Edit this file, not the .ipynb files: notebooks are JSON and hard to review in a diff,
while this is plain Python. Every code cell is syntax-checked while it is built.

    python scripts/build_notebook.py
"""

import ast
import json
from pathlib import Path
from typing import Any

NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"


def lines(text: str) -> list[str]:
    return text.strip("\n").splitlines(keepends=True)


def md(text: str) -> dict[str, Any]:
    return {"cell_type": "markdown", "metadata": {}, "source": lines(text)}


def code(text: str) -> dict[str, Any]:
    ast.parse(text)  # fail here, not on Wherobots, if a cell has a syntax error
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": lines(text),
    }


# --- Cells both notebooks share -------------------------------------------------------

SESSION = """
# 2. The Sedona session. Wherobots provides Spark and Sedona: no jars or settings needed.
from sedona.spark import SedonaContext

sedona = SedonaContext.create(SedonaContext.builder().getOrCreate())
"""

RUNTIME = """
# 3. What this runtime is. Record these numbers in the README.
import importlib.metadata as metadata
import sys

print("Python", sys.version.split()[0])
print("Spark ", sedona.version)
for package in ("apache-sedona", "geopandas", "pyarrow", "pandas", "shapely"):
    try:
        print(f"{package:<14}", metadata.version(package))
    except metadata.PackageNotFoundError:
        print(f"{package:<14} not installed")
sc = sedona.sparkContext
print("default parallelism (cores)", sc.defaultParallelism)
print("shuffle partitions         ", sedona.conf.get("spark.sql.shuffle.partitions"))
"""

LOAD_CODE = """
# 4. Load our package from the zip next to this notebook. The pipeline has no Python UDFs:
#    all the work runs as Spark SQL and Sedona functions in the JVM, so only this notebook
#    needs the package, and the workers never do.
from pathlib import Path

zips = sorted(Path().glob("prague_flood-*.zip"))
assert len(zips) == 1, f"want one prague_flood-*.zip next to the notebook: {[z.name for z in zips]}"
sc.addPyFile(str(zips[0].resolve()))

# This import must come after addPyFile, which puts the zip on the import path.
from prague_flood import MODULES  # noqa: E402

print("code:", zips[0].name)
"""

COPY_INPUTS = """
# 5. Put the inputs where every worker can read them. The Spark workers cannot read My
#    Files, so each input found next to this notebook is copied into RAW_URI on S3.
#    (Uploading straight into Storage also works, but its web upload skips large files.)
fs = sc._jvm.org.apache.hadoop.fs
hadoop_conf = sc._jsc.hadoopConfiguration()
for name in config.SPARK_INPUTS:
    local = Path(name).resolve()
    if local.exists():
        target = fs.Path(f"{RAW_URI.rstrip('/')}/{name}")
        source = fs.Path(local.as_uri())
        target.getFileSystem(hadoop_conf).copyFromLocalFile(False, True, source, target)
        print(f"copied    {name}")
    else:
        print(f"not here  {name} (it must already be in RAW_URI)")
"""

CHECK_INPUTS = """
# 6. Are the inputs on S3 byte-identical to the pinned ones? Spark reads each file on a
#    worker and hashes it there: the same SHA-256 check the download stage makes locally.
import hashlib

hashes = sc.binaryFiles(RAW_URI).mapValues(lambda data: hashlib.sha256(data).hexdigest())
actual = {uri.rsplit("/", 1)[-1]: digest for uri, digest in hashes.collect()}
for name, sha in config.SPARK_INPUTS.items():
    if actual.get(name) == sha:
        status = "ok"
    elif name not in actual:
        status = "MISSING"
    else:
        status = "DIFFERENT"
    print(f"{status:<9} {name}")
assert all(actual.get(n) == sha for n, sha in config.SPARK_INPUTS.items()), "inputs differ"
"""

RAW_URI_SETTING = """
# The S3 folder the workers read the inputs from. On the Storage page, open
# data > customer-..., then click the copy icon on the prague-flood row, and add /raw.
RAW_URI = "s3://PASTE-YOUR-PATH/prague-flood/raw"
"""


# --- notebooks/wherobots_run.ipynb ------------------------------------------------------

RUN = [
    md(
        """
# Prague flood exposure on Wherobots

Runs the committed `prague_flood` package on a Wherobots runtime:

1. Report the runtime's versions and size.
2. Copy the inputs from My Files to S3, and check they are byte-identical to the pinned ones.
3. Time the spatial join with both strategies (broadcast and RangeJoin) and check that the
   results equal the local run exactly.
4. Run the whole pipeline and write the three GeoParquet files.
5. Download the outputs and **destroy the runtime** (it bills until destroyed).

Before running, put these next to this notebook in My Files (`docs/wherobots.md`): the
code zip `prague_flood-<commit>.zip` and the 8 `.parquet` files from `data/raw`.
"""
    ),
    code(
        "# 1. Settings."
        + RAW_URI_SETTING
        + """
# Rerun the slow join against the unsplit zones for comparison? On a laptop it was
# cancelled after 10 minutes. On a cluster it may finish, but it uses credits.
RUN_UNSPLIT_COMPARISON = False
"""
    ),
    code(SESSION),
    code(RUNTIME),
    code(LOAD_CODE.replace("MODULES", "config, hazard, join, load, pipeline")),
    code(COPY_INPUTS),
    code(CHECK_INPUTS),
    code(
        """
# 7. Load, re-project, repair and split once, and cache: the timings in cell 8 are the joins.
import time

started = time.perf_counter()
buildings = load.load_buildings(sedona, RAW_URI).cache()
districts = load.load_districts(sedona, RAW_URI).cache()
zones_whole = hazard.stack_zones(
    load.load_vltava_zones(sedona, RAW_URI),
    load.load_small_streams(sedona, RAW_URI),
    load.load_vltava_categories(sedona, RAW_URI),
).cache()
pieces = hazard.subdivide(zones_whole).cache()
sizes = [df.count() for df in (buildings, districts, zones_whole, pieces)]
print(f"inputs ready in {time.perf_counter() - started:.1f} s:", sizes)
"""
    ),
    code(
        """
# 8. The join, timed with both strategies (twice each; the first run warms up), and
#    checked against the local run: every class count must be identical.
EXPECTED = {  # from the local run, pinned in tests/test_join_data.py
    "q5": 1_264,
    "q20": 1_454,
    "q50": 451,
    "q100": 2_252,
    "residual": 4_610,
    "none": 198_590,
}

timings = {}
for strategy, broadcast_zones in [("broadcast", True), ("range_join", False)] * 2:
    started = time.perf_counter()
    exposure = join.exposure(buildings, districts, pieces, broadcast_zones=broadcast_zones)
    rows = exposure.groupBy("exposure_class").count().collect()
    timings.setdefault(strategy, []).append(round(time.perf_counter() - started, 1))
    counts = {row["exposure_class"]: row["count"] for row in rows}
    assert counts == EXPECTED, (strategy, counts)
print("seconds per run:", timings)
print("identical to the local run with both strategies")
"""
    ),
    code(
        """
# 9. Optional: the same join against the 2,229 unsplit zones, cancelled after 10 minutes.
import threading

if RUN_UNSPLIT_COMPARISON:
    sc.setJobGroup("unsplit", "join against unsplit zones", interruptOnCancel=True)
    timer = threading.Timer(600, lambda: sc.cancelJobGroup("unsplit"))
    timer.start()
    started = time.perf_counter()
    try:
        unsplit = join.exposure(buildings, districts, zones_whole)
        unsplit.groupBy("exposure_class").count().collect()
        print(f"unsplit zones: {time.perf_counter() - started:.1f} s")
    except Exception as error:
        elapsed = time.perf_counter() - started
        print(f"unsplit zones: cancelled after {elapsed:.0f} s ({type(error).__name__})")
    finally:
        timer.cancel()
        sc.setJobGroup("", "")
"""
    ),
    code(
        """
# 10. The whole pipeline, exactly as `python -m prague_flood run` locally. GeoPandas writes
#     the outputs into an `outputs` folder next to this notebook, in My Files.
started = time.perf_counter()
outputs = pipeline.run(sedona, out_dir=Path("outputs"), raw_uri=RAW_URI)
print(f"pipeline: {time.perf_counter() - started:.1f} s")
for name, path in outputs.items():
    print(f"{name:<10} {path}  {path.stat().st_size / 1e6:.1f} MB")
"""
    ),
    md(
        """
## Finish

1. In the file browser, open `outputs/`, right-click each `.parquet` file and choose
   **Download**. They also stay in My Files after the runtime is gone.
2. Save this notebook with its outputs (**File > Download**), so the run is on record.
3. **Destroy the runtime** in the Wherobots console. Billing stops only when it is
   destroyed; the idle timeout does not stop it.
4. Send back the printed versions and the timings from cells 7, 8 and 10 (and 9 if you
   ran it) for the README.
"""
    ),
]


# --- notebooks/wherobots_maps.ipynb -----------------------------------------------------


def show_map(number: int, name: str, title: str, tip: str) -> list[dict[str, Any]]:
    return [
        md(f"## {title}\n\n{tip}"),
        code(
            f'# {number}. Shown as an interactive map; also saved in outputs/maps.\nmaps_["{name}"]'
        ),
    ]


MAPS = [
    md(
        """
# Prague flood exposure on Wherobots: maps

Renders the three maps with SedonaKepler (kepler.gl) from the committed `prague_flood`
package. Their colours, layers, tooltips and starting views are defined in `maps.py`, so
there is nothing to style by hand: run, take screenshots, destroy the runtime.

Needs the same files next to this notebook as `wherobots_run.ipynb` (the code zip and the
8 `.parquet` inputs), or a `RAW_URI` that already holds the inputs from that notebook.

**Screenshots:** collapse kepler's side panel with the `<` arrow at its top right, and open
the legend with the list icon in the column of buttons on the right. On Windows,
Win+Shift+S takes a screenshot of a region.
"""
    ),
    code("# 1. Settings." + RAW_URI_SETTING),
    code(SESSION),
    code(RUNTIME),
    code(LOAD_CODE.replace("MODULES", "config, pipeline")),
    code(COPY_INPUTS),
    code(CHECK_INPUTS),
    code(
        """
# 7. kepler.gl's Python package. If the runtime lacks it, install it here. The map widget
#    may then need the browser tab reloaded (F5); the kernel and its data survive that.
import importlib.util
import subprocess

if importlib.util.find_spec("keplergl") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "keplergl==0.4.0"])
    print("installed keplergl 0.4.0: if a map below stays blank, reload the tab and rerun it")
print("keplergl", metadata.version("keplergl"))
"""
    ),
    code(
        """
# 8. The analysis, as in the run notebook, then the three maps. Each map is also saved as
#    a standalone HTML file in outputs/maps, which opens in any browser.
import time

from prague_flood import maps

started = time.perf_counter()
results = pipeline.compute(sedona, RAW_URI)
specs = maps.map_specs(results.exposure, results.districts, results.zones)
maps_ = maps.build(specs)
saved = maps.save(maps_, specs, Path("outputs") / "maps")
print(f"maps ready in {time.perf_counter() - started:.1f} s")
for name, path in saved.items():
    print(f"{name:<14} {path}  {path.stat().st_size / 1e6:.1f} MB")
"""
    ),
    *show_map(
        9,
        "districts",
        "Exposed building footprint by district, defences holding",
        "Districts shaded by the footprint of buildings touching a flood zone. "
        "Hover over a district for its numbers.",
    ),
    *show_map(
        10,
        "flood_zones",
        "Buildings in the flood zones: the Vltava-Berounka confluence",
        "Opens on Radotín, Zbraslav and Lipence, where exposure is highest. Buildings are "
        "coloured by the most frequent flood they touch; zoom out to see the whole city.",
    ),
    *show_map(
        11,
        "defences_fail",
        "If the defences fail: residual risk in central Prague",
        "Opens on Holešovice, Karlín and the Old Town: the 4,610 buildings behind the "
        "city's flood defences, with the Q100 extent for context.",
    ),
    md(
        """
## Finish

1. Take the screenshots (collapse the side panel, open the legend first).
2. In the file browser, open `outputs/maps/` and download the three `.html` files: each
   is the interactive map, shareable on its own.
3. **Destroy the runtime** in the Wherobots console. Billing stops only when it is
   destroyed.
"""
    ),
]


def write(name: str, cells: list[dict[str, Any]]) -> None:
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }
    path = NOTEBOOKS / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(notebook, indent=1, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n"
    )
    print(f"wrote {path} ({len(cells)} cells; every code cell parses)")


def main() -> None:
    write("wherobots_run.ipynb", RUN)
    write("wherobots_maps.ipynb", MAPS)


if __name__ == "__main__":
    main()

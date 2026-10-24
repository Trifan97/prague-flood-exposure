"""Project-wide constants: paths and coordinate reference systems.

Anything another module would otherwise hard-code lives here, so every assumption the
pipeline makes can be read in one file.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Final


@dataclass(frozen=True)
class Source:
    """A file the project downloads, pinned to the exact bytes it was built with."""

    url: str
    filename: str
    sha256: str


# src/prague_flood/config.py -> parents[2] is the repository root.
PROJECT_ROOT: Final = Path(__file__).resolve().parents[2]
DATA_DIR: Final = PROJECT_ROOT / "data"
RAW_DIR: Final = DATA_DIR / "raw"  # downloads exactly as published; never edited
PROCESSED_DIR: Final = DATA_DIR / "processed"  # pipeline outputs for CARTO

# Every source layer is downloaded in WGS84 longitude/latitude.
SOURCE_CRS: Final = "EPSG:4326"

# UTM zone 33N, in metres, so lengths and areas come out in m and m².
# Prague lies about 0.6° west of the zone's central meridian (15°E). That close to it,
# UTM's scale error is about 0.04%, so areas are within 0.1% of their true size.
TARGET_CRS: Final = "EPSG:32633"

# --- Local Spark only. Wherobots ships its own Spark and Sedona. ---

# Must equal the installed apache-sedona Python package: the Python side and the jars
# exchange serialised geometries, so their versions have to match.
SEDONA_VERSION: Final = "1.9.1"

# Spark's jars and scratch files. Spark's defaults are under the user profile on C:,
# which has too little free space on the development machine.
SPARK_CACHE_DIR: Final = PROJECT_ROOT / ".spark"
JARS_DIR: Final = SPARK_CACHE_DIR / "jars"

# The Sedona build for Spark 4.0 / Scala 2.13, and the GeoTools bundle that holds the
# EPSG definitions ST_Transform needs. Both come from Maven Central; the SHA-256 values
# were computed after checking Maven's published SHA-1 for each jar.
_MAVEN: Final = "https://repo1.maven.org/maven2"
_SEDONA_JAR: Final = f"sedona-spark-shaded-4.0_2.13-{SEDONA_VERSION}.jar"
_GEOTOOLS_JAR: Final = f"geotools-wrapper-{SEDONA_VERSION}-33.5.jar"
SEDONA_JARS: Final = (
    Source(
        url=f"{_MAVEN}/org/apache/sedona/sedona-spark-shaded-4.0_2.13/{SEDONA_VERSION}/{_SEDONA_JAR}",
        filename=_SEDONA_JAR,
        sha256="144773ebb9b1b8f2754ed487f9a37b1f282ae25e607245e50237c0f0cae1d477",
    ),
    Source(
        url=f"{_MAVEN}/org/datasyslab/geotools-wrapper/{SEDONA_VERSION}-33.5/{_GEOTOOLS_JAR}",
        filename=_GEOTOOLS_JAR,
        sha256="544889c00ca6bfaa80dd1d1bd0ea0cf4aa37d9e4eb1b2f347b9f0169a20e6477",
    ),
)

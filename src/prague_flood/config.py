"""Project-wide constants: paths, coordinate reference systems and pinned inputs.

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


@dataclass(frozen=True)
class BBox:
    """A bounding box in WGS84 longitude/latitude degrees."""

    xmin: float
    ymin: float
    xmax: float
    ymax: float


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

# --- Inputs ---

# IPR Praha open data: CC BY 4.0, attribution "data base © IPR Praha". Each URL is the
# portal's GeoJSON export in WGS84. The portal serves a cached file, so the bytes stay the
# same until IPR republishes a layer, and the SHA-256 then flags the change.
_IPR: Final = "https://opendata.geoportalpraha.cz/api/download/v1/items"


def _ipr(item_id: str, filename: str, sha256: str) -> Source:
    return Source(f"{_IPR}/{item_id}/geojson?layers=0", filename, sha256)


IPR_LAYERS: Final = {
    # Vltava flood extents below the Berounka confluence, one layer per design discharge:
    # 1,770 m³/s (Q5), 2,720 (Q20), 3,440 (Q50) and 4,020 (about Q100).
    "vltava_q5": _ipr(
        "c4ef1880cd5d4e62896f8cb845217881",
        "ipr_vltava_q5.geojson",
        "0f8b73fa468ced1ed6a44b034896cb8495bfd5aa52d12c729e0b3adfaa9d1b2c",
    ),
    "vltava_q20": _ipr(
        "fc295e699c5346de9cf6a7e1e634726b",
        "ipr_vltava_q20.geojson",
        "9582fef0a99226e9ff5b1a5e003785dc73b5173330ec51b9f4624a1dbcd7f71f",
    ),
    "vltava_q50": _ipr(
        "040756d41d294971ad730304b792cb03",
        "ipr_vltava_q50.geojson",
        "0413dd2a00e77e352cc0a0e505ee9b17be05c1a4a447b6a85176157c3410342e",
    ),
    "vltava_q100": _ipr(
        "c9dc5fa395b2478a8db0a7cc5be0e447",
        "ipr_vltava_q100.geojson",
        "2fa80e357e7058bcedc4d369f9fc7adead07842c729a6ecd0a5c58199af9d5f6",
    ),
    # Small streams. Field `kateg`: 1 = active zone, 2 = Q100 flood area.
    "small_streams": _ipr(
        "00242330738c4326af30663eb7c6d7f9",
        "ipr_small_streams.geojson",
        "9c7ec24ae028301622209410e28d1a1532562c13228bb7fb88e369fa536cdba0",
    ),
    # Vltava and Berounka zones by category. Field `kateg`: 1 = active, 2 = flow-through,
    # 3 = non-flow-through, 4 = designated for protection by the city's flood defences,
    # 5 = designated for individual protection.
    "vltava_categories": _ipr(
        "7a4bf2d99de44645ac5768f65d8a9692",
        "ipr_vltava_categories.geojson",
        "4ffbb98e409f6b03afe0adbd9659439f3ec8253f4292bb59fd94164a78f9044e",
    ),
    # The 57 city districts (městské části).
    "districts": _ipr(
        "35ede955057f4815b522ea91b488792a",
        "ipr_districts.geojson",
        "4f71b0ef1b524b768b95622b6858eaf48e80094d15d4eb8f97f9f80a0fdf44fd",
    ),
}

# Overture Maps buildings (ODbL). Overture keeps only recent releases on S3, so this one
# will eventually be deleted. The SHA-256 still identifies the exact extract used.
OVERTURE_RELEASE: Final = "2026-10-21.0"
OVERTURE_BUILDINGS: Final = (
    f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}/theme=buildings/type=building/*"
)
# The IPR district polygons span 14.2244-14.7068 E, 49.9419-50.1774 N; rounded outwards.
# A data test checks that the districts still fit inside.
PRAGUE_BBOX: Final = BBox(xmin=14.22, ymin=49.94, xmax=14.71, ymax=50.18)
BUILDINGS_FILENAME: Final = f"overture_buildings_prague_{OVERTURE_RELEASE}.parquet"
# The extract is written by our own query, so its bytes also depend on the DuckDB version
# pinned in requirements.txt.
BUILDINGS_SHA256: Final = "7e6204fec0162fbca0631773da05feba71781740a351b2b70d9de9c9d60cec68"

# DuckDB's extensions (spatial, httpfs). Its default location is the user profile on C:.
DUCKDB_EXTENSIONS_DIR: Final = PROJECT_ROOT / ".duckdb" / "extensions"

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

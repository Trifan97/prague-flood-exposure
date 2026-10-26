"""Load every input layer into Sedona with English column names and one CRS.

Each loader returns a DataFrame with a valid `geometry` column in EPSG:32633 (metres) and
decoded category codes, so later steps never deal with quirks of the source files.

`raw_uri` is the folder holding the input files: data/raw by default, or an s3:// folder
on Wherobots, where every worker must be able to read the inputs. Every input is
GeoParquet (config.SPARK_INPUTS); convert.py explains why the IPR layers are not read as
GeoJSON.
"""

from collections.abc import Mapping
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from prague_flood import config


def raw_file(raw_uri: str, filename: str) -> str:
    # Joined as text, not with pathlib: Path("s3://bucket") would collapse the "//".
    return f"{raw_uri.rstrip('/')}/{filename}"


def read_layer(spark: SparkSession, layer: str, raw_uri: str = config.RAW_URI) -> DataFrame:
    """One IPR layer from its GeoParquet copy: `geometry`, plus each attribute as a column."""
    path = raw_file(raw_uri, config.IPR_GEOPARQUET[layer].filename)
    return spark.read.format("geoparquet").load(path)


def to_target_crs(df: DataFrame) -> DataFrame:
    """Re-project `geometry` from WGS84 to the target CRS and repair invalid polygons.

    The source CRS is stated here, not read from the data. The SRID label Sedona attaches
    depends on how a file was written (the GeoJSON originals read as SRID 0, "unknown"),
    but every input holds WGS84 longitude/latitude.

    Some IPR polygons have a ring that touches itself at one point. ArcGIS, which made
    them, allows that; the OGC rules Sedona follows do not, and spatial predicates on
    invalid polygons can give wrong answers. ST_MakeValid splits such a ring at the
    touching point without changing its area, and ST_CollectionExtract(..., 3) keeps
    only the polygon parts of the result.
    """
    projected = f"ST_Transform(geometry, '{config.SOURCE_CRS}', '{config.TARGET_CRS}')"
    repaired = (
        "CASE WHEN ST_IsValid(geometry) THEN geometry "
        "ELSE ST_CollectionExtract(ST_MakeValid(geometry), 3) END"
    )
    return (
        df.withColumn("geometry", expr(projected))
        .withColumn("geometry", expr(repaired))
        # Repairs can drop the label, so set it explicitly at the end.
        .withColumn("geometry", expr(f"ST_SetSRID(geometry, {config.TARGET_EPSG})"))
        .where("NOT ST_IsEmpty(geometry)")
    )


def decode(column: str, codes: Mapping[int, str]) -> str:
    """A SQL expression turning integer codes into labels. Unknown codes become NULL."""
    cases = " ".join(f"WHEN {code} THEN '{label}'" for code, label in codes.items())
    return f"CASE CAST({column} AS INT) {cases} END"


def load_districts(spark: SparkSession, raw_uri: str = config.RAW_URI) -> DataFrame:
    """The 57 districts: `district_code`, `district_name`, `geometry`."""
    df = read_layer(spark, "districts", raw_uri)
    return to_target_crs(
        df.selectExpr("kod_mc AS district_code", "nazev_mc AS district_name", "geometry")
    )


def load_vltava_zones(spark: SparkSession, raw_uri: str = config.RAW_URI) -> DataFrame:
    """Vltava flood extents, all return periods stacked: `return_period`, `geometry`.

    The extents are nested: the Q100 area contains the Q50 area, and so on.
    """
    layers = [
        read_layer(spark, layer, raw_uri).selectExpr(f"{years} AS return_period", "geometry")
        for layer, years in config.VLTAVA_RETURN_PERIODS.items()
    ]
    return to_target_crs(reduce(DataFrame.unionByName, layers))


def load_small_streams(spark: SparkSession, raw_uri: str = config.RAW_URI) -> DataFrame:
    """Small-stream Q100 flood area, in two non-overlapping parts: `zone`
    ('active_zone' or 'q100_outside_active_zone'), `geometry`."""
    df = read_layer(spark, "small_streams", raw_uri)
    return to_target_crs(
        df.selectExpr(f"{decode('kateg', config.SMALL_STREAM_ZONES)} AS zone", "geometry")
    )


def load_vltava_categories(spark: SparkSession, raw_uri: str = config.RAW_URI) -> DataFrame:
    """Vltava/Berounka zones by category (see config.VLTAVA_CATEGORIES)."""
    df = read_layer(spark, "vltava_categories", raw_uri)
    return to_target_crs(
        df.selectExpr(f"{decode('kateg', config.VLTAVA_CATEGORIES)} AS category", "geometry")
    )


def load_buildings(spark: SparkSession, raw_uri: str = config.RAW_URI) -> DataFrame:
    """Overture footprints: `building_id`, `source_dataset`, `subtype`, `building_class`,
    `num_floors`, `geometry`.

    Height is dropped: Overture records it for only 0.5% of Prague's buildings.
    """
    df = spark.read.format("geoparquet").load(raw_file(raw_uri, config.BUILDINGS_FILENAME))
    return to_target_crs(
        df.selectExpr(
            "id AS building_id",
            "source_dataset",
            "subtype",
            "class AS building_class",
            "num_floors",
            "geometry",
        )
    )

"""Combine the flood layers into one hazard-zone table, cut into join-friendly pieces.

One row per piece of a zone:

    hazard          'vltava', 'small_stream' or 'vltava_residual'
    return_period   5, 20, 50 or 100 years; NULL for residual risk
    zone            'q5' ... 'q100', 'active_zone', 'q100_outside_active_zone'
                    or 'protected_by_city'
    geometry        at most config.MAX_VERTICES vertices, EPSG:32633

The Vltava extents stay nested (the Q100 zone contains the Q5 zone). In step 4 each building
takes the smallest return period it touches, so the nesting needs no geometry work.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from prague_flood import config


def subdivide(df: DataFrame, max_vertices: int = config.MAX_VERTICES) -> DataFrame:
    """Split every geometry into pieces of at most ``max_vertices`` vertices.

    Why: a spatial join first compares bounding boxes, and only pairs whose boxes overlap
    get the exact (expensive) geometry test. The Q5 extent is one polygon of about
    105,000 vertices whose box covers much of central Prague. Every building in that box
    passes the cheap test, and each exact test then walks a huge outline. Small pieces
    have tight boxes, so most buildings are ruled out cheaply, and each exact test is
    short. The pieces do not overlap, so their areas add up to the original area.
    """
    others = [column for column in df.columns if column != "geometry"]
    return (
        df.selectExpr(*others, f"ST_SubDivideExplode(geometry, {max_vertices}) AS geometry")
        # Set the CRS label again in case the split drops it.
        .withColumn("geometry", expr(f"ST_SetSRID(geometry, {config.TARGET_EPSG})"))
    )


def hazard_zones(
    vltava_zones: DataFrame, small_streams: DataFrame, vltava_categories: DataFrame
) -> DataFrame:
    """The hazard-zone table described in the module docstring."""
    vltava = vltava_zones.selectExpr(
        "'vltava' AS hazard",
        "return_period",
        "concat('q', return_period) AS zone",
        "geometry",
    )
    # Every small-stream zone is part of the Q100 flood area.
    streams = small_streams.selectExpr(
        "'small_stream' AS hazard", "100 AS return_period", "zone", "geometry"
    )
    # The IPR Q-zones are drawn with the flood defences in place. The area the defences
    # protect floods only if they fail, so it has no return period.
    residual = vltava_categories.where("category = 'protected_by_city'").selectExpr(
        "'vltava_residual' AS hazard",
        "CAST(NULL AS INT) AS return_period",
        "category AS zone",
        "geometry",
    )
    return subdivide(vltava.unionByName(streams).unionByName(residual))

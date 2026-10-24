"""Unit tests for hazard.py on small synthetic polygons in UTM metres."""

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood import config
from prague_flood.hazard import hazard_zones, subdivide

pytestmark = pytest.mark.spark

SQUARE = "POLYGON ((0 0, 100 0, 100 100, 0 100, 0 0))"


def frame(spark: SparkSession, *rows: str) -> DataFrame:
    """A DataFrame from SQL SELECT lists, one per row."""
    return spark.sql(" UNION ALL ".join(f"SELECT {row}" for row in rows))


def test_subdivide_caps_vertices_and_keeps_area(spark: SparkSession) -> None:
    # A circle of radius 1 km drawn with 2,000 segments: one polygon, ~2,000 vertices.
    circle = frame(
        spark,
        "'big' AS name, ST_Buffer(ST_Point(0, 0), 1000, false, 'quad_segs=500') AS geometry",
    )
    pieces = subdivide(circle, max_vertices=64)

    before = circle.selectExpr("ST_NPoints(geometry) AS n", "ST_Area(geometry) AS area").first()
    after = pieces.selectExpr(
        "count(*) AS n",
        "max(ST_NPoints(geometry)) AS max_vertices",
        "sum(ST_Area(geometry)) AS area",
        "collect_set(name) AS names",
        "collect_set(ST_SRID(geometry)) AS srids",
    ).first()
    assert before is not None and after is not None
    assert before["n"] > 1_000, "the test polygon must be big enough to need splitting"
    assert after["n"] > 1
    assert after["max_vertices"] <= 64
    assert after["area"] == pytest.approx(before["area"], rel=1e-9)
    assert after["names"] == ["big"]  # other columns are copied onto every piece
    assert after["srids"] == [config.TARGET_EPSG]


def test_hazard_zones_labels_each_source(spark: SparkSession) -> None:
    square = f"ST_GeomFromWKT('{SQUARE}') AS geometry"
    vltava = frame(spark, f"5 AS return_period, {square}", f"100 AS return_period, {square}")
    streams = frame(spark, f"'active_zone' AS zone, {square}")
    categories = frame(
        spark, f"'protected_by_city' AS category, {square}", f"'active' AS category, {square}"
    )

    rows = hazard_zones(vltava, streams, categories).drop("geometry").collect()
    assert sorted((r["hazard"], r["return_period"], r["zone"]) for r in rows) == [
        ("small_stream", 100, "active_zone"),
        ("vltava", 5, "q5"),
        ("vltava", 100, "q100"),
        # Only the area behind the city's defences is residual risk; other categories
        # are already inside the Q-zones.
        ("vltava_residual", None, "protected_by_city"),
    ]

"""hazard.py on the real flood layers: the pieces are valid and small, and lose no area."""

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood import config

pytestmark = [pytest.mark.data, pytest.mark.spark]


@pytest.fixture
def zones(layers: dict[str, DataFrame]) -> DataFrame:
    # Built by the shared `layers` fixture (conftest.py), which also registers the
    # hazard_zones, vltava_zones, small_streams and vltava_categories views.
    return layers["hazard_zones"]


def test_pieces_are_valid_small_and_in_utm(spark: SparkSession, zones: DataFrame) -> None:
    row = spark.sql(
        "SELECT count_if(NOT ST_IsValid(geometry)) AS invalid, "
        "max(ST_NPoints(geometry)) AS max_vertices, "
        "collect_set(ST_SRID(geometry)) AS srids FROM hazard_zones"
    ).first()
    assert row is not None
    assert row["invalid"] == 0
    assert row["max_vertices"] <= config.MAX_VERTICES
    assert row["srids"] == [config.TARGET_EPSG]


def test_every_zone_is_present(spark: SparkSession, zones: DataFrame) -> None:
    found = {(r["hazard"], r["zone"]) for r in zones.select("hazard", "zone").distinct().collect()}
    assert found == {
        ("vltava", "q5"),
        ("vltava", "q20"),
        ("vltava", "q50"),
        ("vltava", "q100"),
        ("small_stream", "active_zone"),
        ("small_stream", "q100_outside_active_zone"),
        ("vltava_residual", "protected_by_city"),
    }


def test_subdivision_keeps_every_zone_area(spark: SparkSession, zones: DataFrame) -> None:
    rows = spark.sql(
        """
        WITH original AS (
            SELECT concat('q', return_period) AS zone, ST_Area(geometry) AS area
            FROM vltava_zones
            UNION ALL SELECT zone, ST_Area(geometry) FROM small_streams
            UNION ALL SELECT category, ST_Area(geometry) FROM vltava_categories
                      WHERE category = 'protected_by_city'
        ), before AS (SELECT zone, sum(area) AS area FROM original GROUP BY zone),
        after AS (SELECT zone, sum(ST_Area(geometry)) AS area FROM hazard_zones GROUP BY zone)
        SELECT zone, before.area AS before, after.area AS after
        FROM before JOIN after USING (zone)
        """
    ).collect()
    assert len(rows) == 7
    for row in rows:
        assert row["after"] == pytest.approx(row["before"], rel=1e-6), row["zone"]


def test_the_giant_q5_polygon_is_split_into_hundreds_of_pieces(
    spark: SparkSession, zones: DataFrame
) -> None:
    # Q5 arrives as one polygon of about 105,000 vertices (plus two slivers).
    pieces = zones.where("zone = 'q5'").count()
    assert pieces > 105_000 // config.MAX_VERTICES

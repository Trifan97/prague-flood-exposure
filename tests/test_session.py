"""Sedona smoke tests: the JVM starts, the spatial functions exist, CRS lookups work."""

import pytest
from pyspark.sql import SparkSession

from prague_flood import config

pytestmark = pytest.mark.spark


def test_sedona_functions_are_registered(spark: SparkSession) -> None:
    row = spark.sql(
        "SELECT ST_Area(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')) AS area"
    ).first()
    assert row is not None
    assert row["area"] == 100.0


def test_transform_puts_prague_in_the_right_place(spark: SparkSession) -> None:
    # Old Town Square, as longitude/latitude. Worked by hand with the UTM formulas, it is
    # at about E 458,580 m, N 5,548,520 m in zone 33N. A 1 km tolerance checks that the
    # EPSG definitions loaded and the axes are not swapped; swapped axes land thousands
    # of kilometres away.
    row = spark.sql(
        "SELECT ST_Transform(ST_Point(14.4211, 50.0875), "
        f"'{config.SOURCE_CRS}', '{config.TARGET_CRS}') AS geom"
    ).first()
    assert row is not None
    point = row["geom"]
    assert point.x == pytest.approx(458_580, abs=1_000)
    assert point.y == pytest.approx(5_548_520, abs=1_000)

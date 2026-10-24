"""join.exposure() on the real city: one row per building, consistent flags.

The inputs are pinned by SHA-256, so the counts here are exact: a change means the
method changed, which should be a deliberate decision.
"""

from collections.abc import Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood import join

pytestmark = [pytest.mark.data, pytest.mark.spark]

# From the first run on the pinned inputs. The extract has 208,636 buildings touching the
# city; 15 of them straddle the city border with their representative point outside.
EXPECTED_BUILDINGS = 208_621
EXPECTED_CLASSES = {
    "q5": 1_264,
    "q20": 1_454,
    "q50": 451,
    "q100": 2_252,
    "residual": 4_610,
    "none": 198_590,
}


@pytest.fixture(scope="module")
def table(layers: dict[str, DataFrame]) -> Iterator[DataFrame]:
    df = join.exposure(layers["buildings"], layers["districts"], layers["hazard_zones"]).cache()
    df.createOrReplaceTempView("exposure")
    # Fill the cache with a query that reads every column. A bare count() would not: Spark
    # sees that it needs nothing from the hazard side of the left join and skips that join.
    df.groupBy("exposure_class").count().collect()
    yield df
    df.unpersist()


def counts(spark: SparkSession, sql: str) -> dict[str, int]:
    row = spark.sql(sql).first()
    assert row is not None
    return row.asDict()


def test_one_row_per_building_in_every_district(table: DataFrame) -> None:
    # Separate queries on purpose. Two COUNT(DISTINCT ...) in one Spark query are run with
    # an Expand that copies every row per distinct column; here that took 384 s instead of 5.
    assert table.count() == EXPECTED_BUILDINGS
    assert table.select("building_id").distinct().count() == EXPECTED_BUILDINGS
    assert table.select("district_code").distinct().count() == 57
    assert table.where("district_code IS NULL").count() == 0


def test_point_rule_is_stricter_than_touching(spark: SparkSession, table: DataFrame) -> None:
    # A building whose point is in a flood zone must also touch that zone.
    c = counts(
        spark,
        "SELECT count_if(exposed_by_point AND return_period IS NULL) AS impossible, "
        "count_if(exposed_by_point) AS by_point, count_if(return_period IS NOT NULL) AS touching "
        "FROM exposure",
    )
    assert c["impossible"] == 0
    assert 0 < c["by_point"] < c["touching"]


def test_exposure_grows_with_return_period(spark: SparkSession, table: DataFrame) -> None:
    c = counts(
        spark,
        "SELECT count_if(return_period <= 5) AS q5, count_if(return_period <= 20) AS q20, "
        "count_if(return_period <= 50) AS q50, count_if(return_period <= 100) AS q100 "
        "FROM exposure",
    )
    assert 0 < c["q5"] <= c["q20"] <= c["q50"] <= c["q100"]


def test_classes_match_their_flags(spark: SparkSession, table: DataFrame) -> None:
    c = counts(
        spark,
        """
        SELECT
            count_if(exposure_class = 'residual'
                     AND NOT (behind_defences AND return_period IS NULL)) AS bad_residual,
            count_if(exposure_class = 'none'
                     AND (behind_defences OR return_period IS NOT NULL)) AS bad_none,
            count_if(small_stream_active AND NOT small_stream) AS active_without_stream
        FROM exposure
        """,
    )
    assert c == {"bad_residual": 0, "bad_none": 0, "active_without_stream": 0}


def test_class_counts_are_unchanged(table: DataFrame) -> None:
    rows = table.groupBy("exposure_class").count().collect()
    assert {row["exposure_class"]: row["count"] for row in rows} == EXPECTED_CLASSES

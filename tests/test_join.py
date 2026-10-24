"""join.py on a synthetic mini-city in UTM metres, one building per rule.

x:  0    200    400    600   700   800  900  1000      1500 1600  1700      2000
    |-q5-|                                  |                |act |rest|
    |----q20----|                           |                |    |    |
    |------q100-------|        |residual|   |                |    |    |
    |------------ district 101 ----------- | ---- district 102 ----------|
"""

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from prague_flood.join import exposure

pytestmark = pytest.mark.spark


def box(x0: float, y0: float, x1: float, y1: float) -> str:
    return f"ST_PolygonFromEnvelope({x0}, {y0}, {x1}, {y1})"


def frame(spark: SparkSession, *rows: str) -> DataFrame:
    return spark.sql(" UNION ALL ".join(f"SELECT {row}" for row in rows))


@pytest.fixture
def mini_city(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    districts = frame(
        spark,
        f"101 AS district_code, 'West' AS district_name, {box(0, 0, 1000, 1000)} AS geometry",
        f"102, 'East', {box(1000, 0, 2000, 1000)}",
    )
    zones = frame(
        spark,
        f"'vltava' AS hazard, 5 AS return_period, 'q5' AS zone, {box(0, 0, 200, 1000)} AS geometry",
        f"'vltava', 20, 'q20', {box(0, 0, 400, 1000)}",
        f"'vltava', 100, 'q100', {box(0, 0, 600, 1000)}",
        f"'vltava_residual', CAST(NULL AS INT), 'protected_by_city', {box(800, 0, 900, 1000)}",
        f"'small_stream', 100, 'active_zone', {box(1500, 0, 1600, 1000)}",
        # One zone in two pieces, as subdivide() leaves it.
        f"'small_stream', 100, 'q100_outside_active_zone', {box(1600, 0, 1700, 500)}",
        f"'small_stream', 100, 'q100_outside_active_zone', {box(1600, 500, 1700, 1000)}",
    )

    def building(name: str, x0: float, x1: float, y0: float = 500, y1: float = 510) -> str:
        return (
            f"'{name}' AS building_id, 'OpenStreetMap' AS source_dataset, "
            f"'residential' AS building_class, 3 AS num_floors, {box(x0, y0, x1, y1)} AS geometry"
        )

    buildings = frame(
        spark,
        building("in_q5", 100, 110),  # inside all three nested Vltava extents
        building("grazes_q100", 595, 645),  # 5 m of 50 m inside Q100; its middle is dry
        building("dry", 700, 710),
        building("behind_defences", 850, 860),
        building("on_district_border", 990, 1030),  # its middle is in district 102
        building("active_zone", 1550, 1560),
        building("across_two_pieces", 1640, 1650, 495, 505),
        building("outside_prague", 2500, 2510),
    )
    return buildings, districts, zones


def summary(df: DataFrame) -> list[Row]:
    return sorted(
        df.select(
            "building_id",
            "district_code",
            "vltava_return_period",
            "return_period",
            "exposure_class",
            "small_stream",
            "small_stream_active",
            "behind_defences",
            "exposed_by_point",
        ).collect()
    )


EXPECTED = [
    # id, district, vltava RP, RP, class, small stream, active, behind defences, by point
    ("across_two_pieces", 102, None, 100, "q100", True, False, False, True),
    ("active_zone", 102, None, 100, "q100", True, True, False, True),
    ("behind_defences", 101, None, None, "residual", False, False, True, False),
    ("dry", 101, None, None, "none", False, False, False, False),
    ("grazes_q100", 101, 100, 100, "q100", False, False, False, False),
    ("in_q5", 101, 5, 5, "q5", False, False, False, True),
    ("on_district_border", 102, None, None, "none", False, False, False, False),
]


@pytest.mark.parametrize("broadcast_zones", [True, False], ids=["broadcast", "range_join"])
def test_every_rule_on_the_mini_city(
    mini_city: tuple[DataFrame, DataFrame, DataFrame], broadcast_zones: bool
) -> None:
    # Both join strategies must give the same answer; only their speed may differ.
    result = exposure(*mini_city, broadcast_zones=broadcast_zones)
    assert [tuple(row) for row in summary(result)] == EXPECTED


def test_footprint_area_is_the_whole_building(
    mini_city: tuple[DataFrame, DataFrame, DataFrame],
) -> None:
    rows = exposure(*mini_city).where("building_id = 'grazes_q100'").collect()
    # 50 m x 10 m, although only 5 m x 10 m of it lies in the zone.
    assert [row["footprint_m2"] for row in rows] == [500.0]

"""aggregate.py on a hand-made exposure table small enough to check by hand."""

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood.aggregate import class_summary, district_summary

pytestmark = pytest.mark.spark

COLUMNS = (
    "district_code INT, footprint_m2 DOUBLE, return_period INT, exposed_by_point BOOLEAN, "
    "small_stream BOOLEAN, small_stream_active BOOLEAN, exposure_class STRING, "
    "source_dataset STRING"
)
OSM, ML = "OpenStreetMap", "Microsoft ML Buildings"


@pytest.fixture
def exposure(spark: SparkSession) -> DataFrame:
    rows = [
        # District 1: a small Q5 building, a tiny ML shed in the small-stream active
        # zone, one behind the defences, one dry.
        (1, 40.0, 5, True, False, False, "q5", OSM),
        (1, 20.0, 100, False, True, True, "q100", ML),
        (1, 100.0, None, False, False, False, "residual", OSM),
        (1, 1000.0, None, False, False, False, "none", OSM),
        # District 2: one large Q20 building.
        (2, 500.0, 20, True, False, False, "q20", OSM),
    ]
    return spark.createDataFrame(rows, COLUMNS)


@pytest.fixture
def districts(spark: SparkSession) -> DataFrame:
    # District 3 has no buildings at all.
    square = "ST_PolygonFromEnvelope(0, 0, 1, 1) AS geometry"
    return spark.sql(
        f"SELECT 1 AS district_code, 'A' AS district_name, {square} "
        f"UNION ALL SELECT 2, 'B', {square} UNION ALL SELECT 3, 'C', {square}"
    )


def by_district(df: DataFrame) -> dict[str, dict[str, object]]:
    return {row["district_name"]: row.asDict() for row in df.drop("geometry").collect()}


def test_district_measures(exposure: DataFrame, districts: DataFrame) -> None:
    a = by_district(district_summary(exposure, districts))["A"]
    assert a["buildings_n"] == 4
    assert a["buildings_ha"] == pytest.approx(0.116)
    assert a["exposed_n"] == 2
    assert a["exposed_ha"] == pytest.approx(0.006)
    assert (a["exposed_n_point"], a["exposed_ha_point"]) == (1, pytest.approx(0.004))
    assert a["exposed_n_min_size"] == 1  # the 20 m² shed is below 30 m²
    assert (a["q5_n"], a["q20_n"], a["q50_n"]) == (1, 1, 1)
    assert (a["small_stream_n"], a["active_zone_n"]) == (1, 1)
    assert (a["residual_n"], a["residual_ha"]) == (1, pytest.approx(0.01))
    assert a["exposed_ml_n"] == 1
    assert a["exposed_ml_share_pct"] == pytest.approx(50.0)
    assert a["exposed_share_pct"] == pytest.approx(100 * 0.006 / 0.116)


def test_district_without_buildings_gets_zeros(exposure: DataFrame, districts: DataFrame) -> None:
    c = by_district(district_summary(exposure, districts))["C"]
    assert c["buildings_n"] == c["exposed_n"] == c["residual_n"] == 0
    assert c["exposed_ha"] == 0.0
    # A share of nothing is undefined, not zero.
    assert c["exposed_share_pct"] is None
    assert c["exposed_ml_share_pct"] is None


def test_ranks_by_area_and_count_can_disagree(exposure: DataFrame, districts: DataFrame) -> None:
    rows = by_district(district_summary(exposure, districts))
    ranks = {
        name: (r["rank_exposed_ha"], r["rank_exposed_n"], r["rank_residual_ha"])
        for name, r in rows.items()
    }
    # B has one big exposed building, A two small ones; B and C tie on residual area.
    assert ranks == {"A": (2, 1, 1), "B": (1, 2, 2), "C": (3, 3, 2)}


def test_columns_keep_identifiers_first_and_geometry_last(
    exposure: DataFrame, districts: DataFrame
) -> None:
    columns = district_summary(exposure, districts).columns
    assert columns[:2] == ["district_code", "district_name"]
    assert columns[-1] == "geometry"


def test_class_summary(exposure: DataFrame) -> None:
    rows = {r["exposure_class"]: r.asDict() for r in class_summary(exposure).collect()}
    assert set(rows) == {"q5", "q20", "q100", "residual", "none"}
    assert rows["q100"]["buildings_ml_n"] == 1
    assert rows["q100"]["buildings_n_min_size"] == 0
    assert rows["none"]["buildings_ha"] == pytest.approx(0.1)

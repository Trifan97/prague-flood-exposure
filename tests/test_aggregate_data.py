"""aggregate.py on the real city: district totals add up and stay consistent.

The headline rankings are pinned: with pinned inputs they can only change when the
method changes.
"""

from typing import Any

import pytest
from pyspark.sql import DataFrame

from prague_flood.aggregate import class_summary, district_summary

pytestmark = [pytest.mark.data, pytest.mark.spark]

District = dict[str, Any]  # one row of district_summary(), without the geometry


@pytest.fixture(scope="module")
def districts(exposure_table: DataFrame, layers: dict[str, DataFrame]) -> list[District]:
    rows = district_summary(exposure_table, layers["districts"]).drop("geometry").collect()
    return [row.asDict() for row in rows]


def test_one_row_per_district_and_totals_match_the_buildings(
    districts: list[District], exposure_table: DataFrame
) -> None:
    def total(column: str) -> int:
        return sum(d[column] for d in districts)

    assert len(districts) == 57
    assert total("buildings_n") == exposure_table.count()
    assert total("exposed_n") == exposure_table.where("return_period IS NOT NULL").count()
    assert total("residual_n") == exposure_table.where("exposure_class = 'residual'").count()


def test_every_district_is_internally_consistent(districts: list[District]) -> None:
    for d in districts:
        name = d["district_name"]
        assert d["q5_n"] <= d["q20_n"] <= d["q50_n"] <= d["exposed_n"], name
        assert d["exposed_n_point"] <= d["exposed_n"], name
        assert d["exposed_n_min_size"] <= d["exposed_n"], name
        assert d["exposed_ml_n"] <= d["exposed_n"], name
        assert d["active_zone_n"] <= d["small_stream_n"], name


def leader(districts: list[District], rank_column: str) -> str:
    (top,) = [d["district_name"] for d in districts if d[rank_column] == 1]
    return str(top)


def test_headline_rankings(districts: list[District]) -> None:
    # With the defences holding, the most exposed footprint is in Praha 16 (Radotín, on
    # the Berounka). By count it is Zbraslav, full of small cottages. Behind the defences,
    # it is Praha 8 (Karlín), one of the districts the 2002 flood hit hardest.
    assert leader(districts, "rank_exposed_ha") == "Praha 16"
    assert leader(districts, "rank_exposed_n") == "Praha-Zbraslav"
    assert leader(districts, "rank_residual_ha") == "Praha 8"


def test_class_summary_matches_the_buildings(exposure_table: DataFrame) -> None:
    rows = class_summary(exposure_table).collect()
    assert sum(r["buildings_n"] for r in rows) == exposure_table.count()

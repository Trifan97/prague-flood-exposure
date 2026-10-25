"""The full pipeline, run into a temporary folder: the three GeoParquet files CARTO gets.

Running the pipeline here, instead of checking data/processed, means the files tested are
the ones the current code writes.
"""

import geopandas as gpd
import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood import config
from prague_flood.pipeline import run

pytestmark = [pytest.mark.data, pytest.mark.spark]


@pytest.fixture(scope="module")
def outputs(
    spark: SparkSession, layers: dict[str, DataFrame], tmp_path_factory: pytest.TempPathFactory
) -> dict[str, gpd.GeoDataFrame]:
    # `layers` is requested so the inputs are already cached: the pipeline's identical
    # load plans then read from that cache instead of the raw files.
    paths = run(spark, tmp_path_factory.mktemp("processed"))
    assert {p.name for p in paths.values()} == set(config.OUTPUTS.values())
    return {name: gpd.read_parquet(path) for name, path in paths.items()}


@pytest.mark.parametrize("name", list(config.OUTPUTS))
def test_every_output_is_valid_wgs84_inside_prague(
    outputs: dict[str, gpd.GeoDataFrame], name: str
) -> None:
    gdf = outputs[name]
    assert gdf.crs == config.SOURCE_CRS
    assert gdf.geometry.is_valid.all()
    xmin, ymin, xmax, ymax = gdf.total_bounds
    b = config.PRAGUE_BBOX
    # 0.01° (about 1 km) of slack for buildings that straddle the city border.
    assert b.xmin - 0.01 <= xmin and xmax <= b.xmax + 0.01
    assert b.ymin - 0.01 <= ymin and ymax <= b.ymax + 0.01


def test_districts_file(outputs: dict[str, gpd.GeoDataFrame]) -> None:
    districts = outputs["districts"]
    assert len(districts) == 57
    assert districts["exposed_n"].sum() == 5_421
    assert districts["residual_n"].sum() == 4_610
    # A second library's check of the CRS round trip: PROJ (via GeoPandas) re-projects
    # the exported WGS84 polygons to UTM 33N, and Prague is still about 496 km².
    km2 = districts.to_crs(config.TARGET_CRS).area.sum() / 1e6
    assert km2 == pytest.approx(496, rel=0.005)


def test_buildings_file(outputs: dict[str, gpd.GeoDataFrame]) -> None:
    buildings = outputs["buildings"]
    assert len(buildings) == 5_421 + 4_610  # exposed + residual only
    assert buildings["building_id"].is_unique
    assert set(buildings["exposure_class"]) == {"q5", "q20", "q50", "q100", "residual"}


def test_zones_file(outputs: dict[str, gpd.GeoDataFrame]) -> None:
    zones = outputs["zones"]
    assert len(zones) == 2_229  # whole zones, not the 7,940 join pieces
    assert zones["zone"].nunique() == 7
    # Sum of the seven zone areas measured in step 3 (97.36 km²).
    assert zones["area_ha"].sum() == pytest.approx(9_735.6, rel=1e-3)

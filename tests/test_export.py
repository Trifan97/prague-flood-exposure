"""export.py: back to WGS84, sorted rows, valid GeoParquet, no half-written files."""

import json
from pathlib import Path

import geopandas as gpd
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession
from shapely.geometry import Point

from prague_flood import config
from prague_flood.export import exposed_buildings, to_geodataframe, write_geoparquet, zones_for_map
from prague_flood.load import to_target_crs


@pytest.mark.spark
def test_utm_round_trip_returns_the_original_lon_lat(spark: SparkSession) -> None:
    lon_lat = spark.sql(
        "SELECT 2 AS id, ST_Point(14.4211, 50.0875) AS geometry "
        "UNION ALL SELECT 1, ST_Point(14.5, 50.0)"
    )
    gdf = to_geodataframe(to_target_crs(lon_lat), sort_by=["id"])

    assert gdf.crs == config.SOURCE_CRS
    assert list(gdf["id"]) == [1, 2]  # sorted, whatever order Spark returned
    assert gdf.geometry[1].x == pytest.approx(14.4211, abs=1e-7)
    assert gdf.geometry[1].y == pytest.approx(50.0875, abs=1e-7)


@pytest.mark.spark
def test_exposed_buildings_leaves_out_the_none_class(spark: SparkSession) -> None:
    classes = spark.createDataFrame([("q5",), ("residual",), ("none",)], ["exposure_class"])
    kept = [row[0] for row in exposed_buildings(classes).collect()]
    assert sorted(kept) == ["q5", "residual"]


@pytest.mark.spark
def test_zone_area_is_in_hectares(spark: SparkSession) -> None:
    square = spark.sql("SELECT ST_PolygonFromEnvelope(0, 0, 100, 100) AS geometry")
    row = zones_for_map(square).first()
    assert row is not None
    assert row["area_ha"] == pytest.approx(1.0)  # 100 m x 100 m


def test_write_geoparquet_round_trips(tmp_path: Path) -> None:
    gdf = gpd.GeoDataFrame(
        {"name": ["a", "b"]}, geometry=[Point(14.4, 50.1), Point(14.5, 50.0)], crs="EPSG:4326"
    )
    path = write_geoparquet(gdf, tmp_path / "out" / "table.parquet")

    back = gpd.read_parquet(path)
    assert list(back["name"]) == ["a", "b"]
    assert back.geometry.geom_equals(gdf.geometry).all()
    assert back.crs == gdf.crs
    # CARTO and Sedona find the geometry through the GeoParquet "geo" metadata key.
    geo = json.loads(pq.read_metadata(path).metadata[b"geo"])
    assert geo["primary_column"] == "geometry"
    assert [p.name for p in path.parent.iterdir()] == ["table.parquet"]  # no .part left

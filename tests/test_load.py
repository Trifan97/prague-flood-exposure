"""Unit tests for load.py on small in-memory geometries (no downloaded data needed)."""

import json
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood import config
from prague_flood.convert import write_geoparquet
from prague_flood.load import decode, read_layer, to_target_crs

pytestmark = pytest.mark.spark


def from_wkt(spark: SparkSession, wkt: str) -> DataFrame:
    return spark.sql(f"SELECT ST_GeomFromWKT('{wkt}') AS geometry")


def near_prague(x: float, y: float) -> str:
    """Grid units of 0.001° (about 70 m east-west, 110 m north-south) near Old Town."""
    return f"{14.40 + x * 0.001} {50.08 + y * 0.001}"


def test_moves_lon_lat_into_utm_metres(spark: SparkSession) -> None:
    row = (
        to_target_crs(from_wkt(spark, "POINT (14.4211 50.0875)"))
        .selectExpr("ST_X(geometry) AS x", "ST_Y(geometry) AS y", "ST_SRID(geometry) AS srid")
        .first()
    )
    assert row is not None
    # Old Town Square; the hand calculation is in test_session.py.
    assert row["x"] == pytest.approx(458_580, abs=1_000)
    assert row["y"] == pytest.approx(5_548_520, abs=1_000)
    assert row["srid"] == config.TARGET_EPSG


def test_repairs_a_self_touching_ring_without_changing_its_area(spark: SparkSession) -> None:
    # A 4 x 4 square whose ring dips in to form a triangular notch and comes back to the
    # same vertex (2, 4): the ArcGIS-valid, OGC-invalid shape found in the IPR layers.
    ring = [(0, 0), (4, 0), (4, 4), (2, 4), (3, 2), (1, 2), (2, 4), (0, 4), (0, 0)]
    wkt = "POLYGON ((" + ", ".join(near_prague(x, y) for x, y in ring) + "))"
    df = from_wkt(spark, wkt)

    before = df.selectExpr(
        "ST_IsValid(geometry) AS valid",
        f"ST_Area(ST_Transform(geometry, '{config.SOURCE_CRS}', '{config.TARGET_CRS}')) AS area",
    ).first()
    after = (
        to_target_crs(df)
        .selectExpr("ST_IsValid(geometry) AS valid", "ST_Area(geometry) AS area")
        .first()
    )
    assert before is not None and after is not None
    assert not before["valid"], "the test shape must start invalid to exercise the repair"
    assert after["valid"]
    assert after["area"] == pytest.approx(before["area"], rel=1e-9)


def test_drops_polygons_that_collapse_to_nothing(spark: SparkSession) -> None:
    # A ring that goes out to one point and straight back: zero area under any
    # projection. The repair reduces it to a line, and ST_CollectionExtract(..., 3) to an
    # empty polygon. (Three points that are merely collinear are not enough: after
    # re-projection they are no longer exactly on a line and survive as a tiny sliver.)
    wkt = "POLYGON ((14.4 50.08, 14.401 50.081, 14.4 50.08, 14.4 50.08))"
    assert to_target_crs(from_wkt(spark, wkt)).count() == 0


def test_decode_maps_known_codes_and_nulls_unknown_ones(spark: SparkSession) -> None:
    df = spark.createDataFrame([("1",), ("2",), ("9",)], ["kateg"])
    labels = [row[0] for row in df.selectExpr(decode("kateg", {1: "a", 2: "b"})).collect()]
    assert labels == ["a", "b", None]


def test_read_layer_reads_the_geoparquet_copy(spark: SparkSession, tmp_path: Path) -> None:
    # GeoJSON -> GeoParquet with convert.py, then read back by Sedona: the path the
    # pipeline takes for every IPR layer.
    features = [
        {
            "type": "Feature",
            "properties": {"objectid": oid, "kateg": code},
            "geometry": {"type": "Point", "coordinates": [14.4, 50.1]},
        }
        for oid, code in ((2, 1), (1, 2))
    ]
    source = tmp_path / "layer.geojson"
    source.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    write_geoparquet(source, tmp_path / config.IPR_GEOPARQUET["small_streams"].filename)

    df = read_layer(spark, "small_streams", tmp_path.as_posix())
    assert set(df.columns) == {"objectid", "kateg", "geometry"}
    assert sorted((row["objectid"], row["kateg"]) for row in df.collect()) == [(1, 2), (2, 1)]

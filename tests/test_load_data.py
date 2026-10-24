"""load.py on the real layers: every output valid, in UTM 33N, inside Prague and sensible.

These tests also pin the facts about the flood layers that later steps rely on.
"""

from collections.abc import Callable, Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood import config, load

pytestmark = [pytest.mark.data, pytest.mark.spark]

LOADERS: dict[str, Callable[[SparkSession], DataFrame]] = {
    "districts": load.load_districts,
    "vltava_zones": load.load_vltava_zones,
    "small_streams": load.load_small_streams,
    "vltava_categories": load.load_vltava_categories,
    "buildings": load.load_buildings,
}


@pytest.fixture(scope="module")
def layers(spark: SparkSession) -> Iterator[dict[str, DataFrame]]:
    raw = [config.RAW_DIR / s.filename for s in config.IPR_LAYERS.values()]
    raw.append(config.RAW_DIR / config.BUILDINGS_FILENAME)
    missing = [p.name for p in raw if not p.exists()]
    if missing:
        pytest.skip(f"not downloaded: {missing}; run: python -m prague_flood download")

    # cache(): each layer is read and re-projected once, not once per test.
    loaded = {name: loader(spark).cache() for name, loader in LOADERS.items()}
    for name, df in loaded.items():
        df.createOrReplaceTempView(name)
    yield loaded
    for df in loaded.values():
        df.unpersist()


def one(spark: SparkSession, sql: str) -> object:
    row = spark.sql(sql).first()
    assert row is not None
    return row[0]


@pytest.mark.parametrize("name", list(LOADERS))
def test_layer_is_valid_in_utm_and_inside_prague(
    spark: SparkSession, layers: dict[str, DataFrame], name: str
) -> None:
    assert one(spark, f"SELECT count(*) FROM {name} WHERE NOT ST_IsValid(geometry)") == 0
    assert one(spark, f"SELECT collect_set(ST_SRID(geometry)) FROM {name}") == [config.TARGET_EPSG]
    # The WGS84 bbox used to cut the buildings, re-projected; 500 m of slack for edge
    # buildings and for bbox edges that curve slightly in UTM.
    b = config.PRAGUE_BBOX
    prague = (
        f"ST_Buffer(ST_Transform(ST_PolygonFromEnvelope({b.xmin}, {b.ymin}, {b.xmax}, {b.ymax}),"
        f" '{config.SOURCE_CRS}', '{config.TARGET_CRS}'), 500)"
    )
    assert one(spark, f"SELECT ST_Within(ST_Envelope_Aggr(geometry), {prague}) FROM {name}")


def test_districts_add_up_to_prague(spark: SparkSession, layers: dict[str, DataFrame]) -> None:
    # Prague's official area is about 496 km². Matching it checks the CRS, the units
    # (m²) and that no district is missing in one go.
    row = spark.sql(
        "SELECT count(*) AS n, count(DISTINCT district_code) AS codes, "
        "count(DISTINCT district_name) AS names, sum(ST_Area(geometry)) / 1e6 AS km2 "
        "FROM districts"
    ).first()
    assert row is not None
    assert row["n"] == row["codes"] == row["names"] == 57
    assert row["km2"] == pytest.approx(496, rel=0.005)


def test_vltava_extents_grow_with_return_period(
    spark: SparkSession, layers: dict[str, DataFrame]
) -> None:
    rows = spark.sql(
        "SELECT return_period, sum(ST_Area(geometry)) AS area "
        "FROM vltava_zones GROUP BY return_period ORDER BY return_period"
    ).collect()
    assert [r["return_period"] for r in rows] == sorted(config.VLTAVA_RETURN_PERIODS.values())
    areas = [r["area"] for r in rows]
    assert areas == sorted(areas)


@pytest.mark.parametrize(
    ("name", "column", "labels"),
    [
        ("small_streams", "zone", config.SMALL_STREAM_ZONES),
        ("vltava_categories", "category", config.VLTAVA_CATEGORIES),
    ],
)
def test_every_code_is_decoded(
    spark: SparkSession,
    layers: dict[str, DataFrame],
    name: str,
    column: str,
    labels: dict[int, str],
) -> None:
    found = one(spark, f"SELECT collect_set({column}) FROM {name}")
    assert isinstance(found, list)
    assert sorted(found) == sorted(labels.values())
    assert one(spark, f"SELECT count(*) FROM {name} WHERE {column} IS NULL") == 0


def test_small_stream_zones_do_not_overlap(
    spark: SparkSession, layers: dict[str, DataFrame]
) -> None:
    # So "q100_outside_active_zone" really is the rest of the Q100 area, and the full
    # small-stream Q100 area is both zones together.
    # A spatial join: Sedona only compares pairs whose bounding boxes overlap, instead of
    # merging all 2,201 polygons first. Neighbouring zones share edges, so pairs that only
    # touch are found too, with zero intersection area.
    overlap = one(
        spark,
        """
        SELECT coalesce(sum(ST_Area(ST_Intersection(a.geometry, b.geometry))), 0.0)
        FROM small_streams a JOIN small_streams b
          ON ST_Intersects(a.geometry, b.geometry)
        WHERE a.zone = 'active_zone' AND b.zone <> 'active_zone'
        """,
    )
    assert isinstance(overlap, float)
    assert overlap < 1.0  # m²


def test_city_defences_lie_outside_the_q100_extent(
    spark: SparkSession, layers: dict[str, DataFrame]
) -> None:
    # The Q-zones are drawn with the defences in place, so the area the city protects is
    # residual risk, not part of the Q100 hazard.
    share = one(
        spark,
        """
        WITH q100 AS (
            SELECT ST_Union_Aggr(geometry) AS g FROM vltava_zones WHERE return_period = 100
        ), protected AS (
            SELECT ST_Union_Aggr(geometry) AS g FROM vltava_categories
            WHERE category = 'protected_by_city'
        )
        SELECT ST_Area(ST_Intersection(protected.g, q100.g)) / ST_Area(protected.g)
        FROM q100, protected
        """,
    )
    assert isinstance(share, float)
    assert share < 0.05


def test_buildings_have_unique_ids(spark: SparkSession, layers: dict[str, DataFrame]) -> None:
    row = spark.sql(
        "SELECT count(*) AS n, count(DISTINCT building_id) AS ids FROM buildings"
    ).first()
    assert row is not None
    assert row["n"] == row["ids"] == 306_290

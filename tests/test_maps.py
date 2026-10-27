"""maps.py: legends in flood order, colours matched to categories, tables ready to draw."""

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood.maps import (
    EXPOSURE_BANDS,
    FLOOD_CLASSES,
    ZONES,
    Layer,
    MapSpec,
    View,
    building_table,
    district_table,
    kepler_config,
    layer_config,
    sql_text,
    zone_table,
)


@pytest.mark.parametrize(
    "labels",
    [
        [label for label, _ in FLOOD_CLASSES.values()],
        [label for label, _ in ZONES.values()],
        [label for _, label, _ in EXPOSURE_BANDS],
    ],
    ids=["flood classes", "zones", "exposure bands"],
)
def test_labels_sort_in_legend_order(labels: list[str]) -> None:
    # kepler.gl sorts categories alphabetically; the leading number keeps flood order.
    assert sorted(labels) == labels


def test_layer_colours_follow_the_sorted_categories() -> None:
    layer = Layer("buildings", "Most frequent flood", "flood_class", ["flood_class"])
    present = [FLOOD_CLASSES["q100"][0], FLOOD_CLASSES["q5"][0]]  # in any order
    colors = layer_config(layer, present)["config"]["visConfig"]["colorRange"]["colors"]
    assert colors == [FLOOD_CLASSES["q5"][1], FLOOD_CLASSES["q100"][1]]


def test_kepler_config_covers_every_layer_and_its_view() -> None:
    spec = MapSpec(
        title="t",
        datasets={},
        layers=[
            Layer("buildings", "b", "flood_class", ["flood_class", "district_name"]),
            Layer("zones", "z", "zone", ["zone"]),
        ],
        view=View(longitude=14.4, latitude=50.1, zoom=12.0),
        categories={"buildings": [FLOOD_CLASSES["q5"][0]], "zones": [ZONES["q5"][0]]},
    )
    config = kepler_config(spec)["config"]
    layers = config["visState"]["layers"]
    # The first layer is drawn on top: buildings above zones.
    assert [layer["config"]["dataId"] for layer in layers] == ["buildings", "zones"]
    tooltip = config["visState"]["interactionConfig"]["tooltip"]["fieldsToShow"]
    assert [field["name"] for field in tooltip["buildings"]] == ["flood_class", "district_name"]
    assert (config["mapState"]["longitude"], config["mapState"]["zoom"]) == (14.4, 12.0)


def test_sql_text_escapes_quotes() -> None:
    assert sql_text("city's") == "'city''s'"


@pytest.mark.spark
def test_district_bands(spark: SparkSession) -> None:
    districts = spark.sql(
        "SELECT * FROM VALUES ('a', 0.0), ('b', 0.5), ('c', 3.0), ('d', 7.0), ('e', 12.0) "
        "AS t(district_name, exposed_ha)"
    ).selectExpr(
        "district_name",
        "1 AS rank_exposed_ha",
        "exposed_ha",
        "0 AS exposed_n",
        "0 AS exposed_n_min_size",
        "0.0 AS exposed_share_pct",
        "0.0 AS residual_ha",
        "0.0 AS exposed_ml_share_pct",
        "ST_PolygonFromEnvelope(450000, 5540000, 450100, 5540100) AS geometry",
    )
    rows = district_table(districts).orderBy("district_name").collect()
    assert [row["exposure_band"] for row in rows] == [label for _, label, _ in EXPOSURE_BANDS]


@pytest.mark.spark
def test_building_table_labels_and_lon_lat(spark: SparkSession) -> None:
    exposure = spark.sql(
        "SELECT * FROM VALUES ('q5'), ('residual'), ('none') AS t(exposure_class)"
    ).selectExpr(
        "exposure_class",
        "'Praha 7' AS district_name",
        "100.0 AS footprint_m2",
        "'house' AS building_class",
        "'OpenStreetMap' AS source_dataset",
        "true AS exposed_by_point",
        # A 10 m square in UTM 33N near Old Town Square.
        "ST_PolygonFromEnvelope(458580, 5548520, 458590, 5548530) AS geometry",
    )
    rows = building_table(exposure).selectExpr("flood_class", "ST_X(ST_Centroid(geometry)) AS x")
    result = sorted((row["flood_class"], round(row["x"], 2)) for row in rows.collect())
    # 'none' is not drawn; the others are labelled and back in longitude/latitude.
    assert result == [(FLOOD_CLASSES["q5"][0], 14.42), (FLOOD_CLASSES["residual"][0], 14.42)]


@pytest.mark.spark
def test_zone_table_handles_quotes_and_measures_hectares(spark: SparkSession) -> None:
    zones = spark.sql(
        "SELECT 'vltava_residual' AS hazard, 'protected_by_city' AS zone, "
        "ST_PolygonFromEnvelope(450000, 5540000, 450100, 5540100) AS geometry"
    )
    row = zone_table(zones).first()
    assert row is not None
    assert row["zone"] == ZONES["protected_by_city"][0]  # the label contains "city's"
    assert row["area_ha"] == pytest.approx(1.0)  # measured in metres, before going to lon/lat


@pytest.mark.spark
def test_build_and_save_a_map(spark: SparkSession, tmp_path: Path) -> None:
    pytest.importorskip("keplergl", reason="the maps need: pip install -e .[maps]")
    from prague_flood import maps

    square = "ST_PolygonFromEnvelope(14.40, 50.08, 14.41, 50.09) AS geometry"
    data: DataFrame = spark.sql(f"SELECT '{ZONES['q5'][0]}' AS zone, 1.0 AS area_ha, {square}")
    spec = MapSpec(
        title="Test map",
        datasets={"zones": data},
        layers=[Layer("zones", "Zones", "zone", ["zone"])],
        view=View(longitude=14.4, latitude=50.08, zoom=12.0),
    )
    paths = maps.save(maps.build({"test": spec}), {"test": spec}, tmp_path)
    html = paths["test"].read_text(encoding="utf-8")
    # The data is embedded as JSON, where "·" may be escaped; check the rest of the label.
    assert "Test map" in html and "Vltava Q5" in html

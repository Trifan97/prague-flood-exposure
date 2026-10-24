"""Checks on the downloaded files in data/raw.

The pipeline never edits raw files. These tests prove it, and pin the facts about the
data that later code relies on. They skip, with the reason printed, until
`python -m prague_flood download` has run.
"""

import json
from functools import cache
from pathlib import Path
from typing import Any

import pytest

from prague_flood import config
from prague_flood.download import sha256_of

pytestmark = pytest.mark.data


def require(path: Path) -> Path:
    if not path.exists():
        pytest.skip(f"{path.name} not downloaded; run: python -m prague_flood download")
    return path


@cache  # parse each file once, however many tests read it
def read_geojson(name: str) -> dict[str, Any]:
    path = require(config.RAW_DIR / config.IPR_LAYERS[name].filename)
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


@pytest.mark.parametrize("name", sorted(config.IPR_LAYERS))
def test_ipr_layer_is_unchanged(name: str) -> None:
    source = config.IPR_LAYERS[name]
    assert sha256_of(require(config.RAW_DIR / source.filename)) == source.sha256


def test_buildings_extract_is_unchanged() -> None:
    path = require(config.RAW_DIR / config.BUILDINGS_FILENAME)
    assert sha256_of(path) == config.BUILDINGS_SHA256


@pytest.mark.parametrize("name", sorted(config.IPR_LAYERS))
def test_ipr_layer_is_lon_lat(name: str) -> None:
    # CRS84 is WGS84 with longitude first, which is what config.SOURCE_CRS assumes.
    crs = read_geojson(name)["crs"]["properties"]["name"]
    assert crs == "urn:ogc:def:crs:OGC:1.3:CRS84"


def test_there_are_57_districts_with_names() -> None:
    features = read_geojson("districts")["features"]
    names = {f["properties"]["nazev_mc"] for f in features}
    assert len(features) == len(names) == 57


def test_districts_fit_inside_the_buildings_bbox() -> None:
    # The Overture extract is cut with config.PRAGUE_BBOX, so any part of the city
    # outside it would silently lose its buildings.
    def coordinates(node: Any) -> Any:
        if isinstance(node[0], (int, float)):
            yield node
        else:
            for child in node:
                yield from coordinates(child)

    points = [
        point
        for feature in read_geojson("districts")["features"]
        for point in coordinates(feature["geometry"]["coordinates"])
    ]
    bbox = config.PRAGUE_BBOX
    assert bbox.xmin <= min(p[0] for p in points) and max(p[0] for p in points) <= bbox.xmax
    assert bbox.ymin <= min(p[1] for p in points) and max(p[1] for p in points) <= bbox.ymax


@pytest.mark.parametrize(
    ("name", "codes"),
    [
        ("small_streams", set(config.SMALL_STREAM_ZONES)),
        ("vltava_categories", set(config.VLTAVA_CATEGORIES)),
    ],
)
def test_category_codes_are_the_documented_ones(name: str, codes: set[int]) -> None:
    # Every code in the data must have a label in config.py, and every label a code.
    found = {int(f["properties"]["kateg"]) for f in read_geojson(name)["features"]}
    assert found == codes

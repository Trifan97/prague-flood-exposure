"""Sanity checks on the pinned inputs, so a typo in config.py fails here, not mid-download."""

import re

from prague_flood import config

SHA256 = re.compile(r"[0-9a-f]{64}")


def all_pins() -> list[config.Source | config.Derived]:
    return [*config.IPR_LAYERS.values(), *config.IPR_GEOPARQUET.values(), *config.SEDONA_JARS]


def test_every_pin_is_a_sha256() -> None:
    for pinned in all_pins():
        assert SHA256.fullmatch(pinned.sha256), pinned.filename
    assert SHA256.fullmatch(config.BUILDINGS_SHA256)


def test_filenames_are_unique() -> None:
    names = [p.filename for p in all_pins()] + [config.BUILDINGS_FILENAME]
    assert len(names) == len(set(names))


def test_every_ipr_layer_has_a_geoparquet_copy() -> None:
    assert config.IPR_GEOPARQUET.keys() == config.IPR_LAYERS.keys()


def test_spark_reads_only_geoparquet() -> None:
    # convert.py: Wherobots' GeoJSON reader fails on the IPR files.
    assert len(config.SPARK_INPUTS) == len(config.IPR_LAYERS) + 1
    assert all(name.endswith(".parquet") for name in config.SPARK_INPUTS)


def test_prague_bbox_is_well_formed() -> None:
    bbox = config.PRAGUE_BBOX
    assert bbox.xmin < bbox.xmax
    assert bbox.ymin < bbox.ymax

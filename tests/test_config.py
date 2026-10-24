"""Sanity checks on the pinned inputs, so a typo in config.py fails here, not mid-download."""

import re

from prague_flood import config

SHA256 = re.compile(r"[0-9a-f]{64}")


def all_sources() -> list[config.Source]:
    return [*config.IPR_LAYERS.values(), *config.SEDONA_JARS]


def test_every_pin_is_a_sha256() -> None:
    for source in all_sources():
        assert SHA256.fullmatch(source.sha256), source.filename
    assert SHA256.fullmatch(config.BUILDINGS_SHA256)


def test_filenames_are_unique() -> None:
    names = [s.filename for s in config.IPR_LAYERS.values()] + [config.BUILDINGS_FILENAME]
    assert len(names) == len(set(names))


def test_prague_bbox_is_well_formed() -> None:
    bbox = config.PRAGUE_BBOX
    assert bbox.xmin < bbox.xmax
    assert bbox.ymin < bbox.ymax

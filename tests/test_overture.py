"""The Overture extract, run against a tiny local file shaped like Overture's schema.

DuckDB's spatial extension is downloaded once into .duckdb/; after that these tests need
no network.
"""

import json
from pathlib import Path

import pytest

from prague_flood.config import BBox
from prague_flood.download import ChecksumError, sha256_of
from prague_flood.overture import connect, extract_buildings, write_buildings

PRAGUE = BBox(xmin=14.22, ymin=49.94, xmax=14.71, ymax=50.18)


def square(x: float, y: float) -> str:
    """A ~70 m square with its south-west corner at (x, y), plus its bbox struct."""
    d = 0.001
    wkt = f"POLYGON(({x} {y}, {x + d} {y}, {x + d} {y + d}, {x} {y + d}, {x} {y}))"
    bbox = f"{{'xmin': {x}, 'ymin': {y}, 'xmax': {x + d}, 'ymax': {y + d}}}"
    return f"ST_GeomFromText('{wkt}'), {bbox}"


@pytest.fixture
def overture_like(tmp_path: Path) -> str:
    path = tmp_path / "buildings.parquet"
    con = connect()
    con.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('b', {square(14.42, 50.08)}, [{{'dataset': 'OpenStreetMap'}}],
                 'residential', 'house', 9.5, 3),
                ('a', {square(14.50, 50.00)}, [{{'dataset': 'Microsoft ML Buildings'}}],
                 NULL, NULL, NULL, NULL),
                -- Brno, far outside Prague
                ('c', {square(16.60, 49.19)}, [{{'dataset': 'OpenStreetMap'}}],
                 NULL, NULL, NULL, NULL),
                -- straddles the bbox's east edge: its bbox overlaps, so it is kept
                ('d', {square(14.7095, 50.00)}, [{{'dataset': 'OpenStreetMap'}}],
                 NULL, NULL, NULL, NULL)
            ) AS t(id, geometry, bbox, sources, subtype, class, height, num_floors)
        ) TO '{path.as_posix()}' (FORMAT parquet)
    """)
    con.close()
    return path.as_posix()


def test_keeps_buildings_overlapping_the_bbox_sorted_by_id(
    overture_like: str, tmp_path: Path
) -> None:
    out = tmp_path / "extract.parquet"
    write_buildings(overture_like, PRAGUE, out)

    con = connect()
    rows = con.execute(
        f"SELECT id, source_dataset, subtype, height FROM '{out.as_posix()}'"
    ).fetchall()
    assert rows == [
        ("a", "Microsoft ML Buildings", None, None),
        ("b", "OpenStreetMap", "residential", 9.5),
        ("d", "OpenStreetMap", None, None),
    ]


def test_writes_geoparquet_metadata(overture_like: str, tmp_path: Path) -> None:
    # Sedona finds the geometry column through the "geo" key in the Parquet metadata.
    out = tmp_path / "extract.parquet"
    write_buildings(overture_like, PRAGUE, out)

    con = connect()
    row = con.execute(
        f"SELECT value FROM parquet_kv_metadata('{out.as_posix()}') WHERE key = 'geo'"
    ).fetchone()
    assert row is not None
    geo = json.loads(row[0])
    assert geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"]["encoding"] == "WKB"


def test_extract_is_reproducible_and_verified(overture_like: str, tmp_path: Path) -> None:
    # Two runs on the same input must give byte-identical files, or a SHA-256 pin is useless.
    first = tmp_path / "first.parquet"
    write_buildings(overture_like, PRAGUE, first)
    pinned = sha256_of(first)

    dest = tmp_path / "raw" / "extract.parquet"
    assert extract_buildings(dest, pinned, source=overture_like, bbox=PRAGUE) == dest
    assert sha256_of(dest) == pinned


def test_extract_with_wrong_pin_raises_and_leaves_nothing(
    overture_like: str, tmp_path: Path
) -> None:
    dest = tmp_path / "raw" / "extract.parquet"
    with pytest.raises(ChecksumError):
        extract_buildings(dest, "0" * 64, source=overture_like, bbox=PRAGUE)
    assert list(dest.parent.iterdir()) == []

"""convert.py: GeoJSON to GeoParquet, attributes kept, rows sorted, bytes reproducible.

DuckDB's spatial extension is downloaded once into .duckdb/; after that these tests need
no network.
"""

import json
from pathlib import Path

import pytest

from prague_flood.convert import to_geoparquet, write_geoparquet
from prague_flood.download import ChecksumError, sha256_of
from prague_flood.duck import connect


@pytest.fixture
def geojson(tmp_path: Path) -> Path:
    """A small FeatureCollection shaped like the IPR files, with rows out of order."""
    square = [[[14.4, 50.0], [14.5, 50.0], [14.5, 50.1], [14.4, 50.1], [14.4, 50.0]]]
    features = [
        {
            "type": "Feature",
            "properties": {"objectid": oid, "kateg": kateg, "nazev": name},
            "geometry": {"type": "MultiPolygon", "coordinates": [square]},
        }
        for oid, kateg, name in ((3, 2, "c"), (1, 1, "a"), (2, 1, "b"))
    ]
    path = tmp_path / "layer.geojson"
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, indent=2),
        encoding="utf-8",
    )
    return path


def test_keeps_attributes_and_sorts_by_objectid(geojson: Path, tmp_path: Path) -> None:
    out = tmp_path / "layer.parquet"
    write_geoparquet(geojson, out)

    con = connect()
    columns = [
        row[0] for row in con.execute(f"DESCRIBE SELECT * FROM '{out.as_posix()}'").fetchall()
    ]
    rows = con.execute(
        f"SELECT objectid, kateg, nazev, ST_GeometryType(geometry) FROM '{out.as_posix()}'"
    ).fetchall()
    # GDAL's own row number (OGC_FID) is dropped; the geometry is called `geometry`.
    assert columns == ["objectid", "kateg", "nazev", "geometry"]
    assert rows == [
        (1, 1, "a", "MULTIPOLYGON"),
        (2, 1, "b", "MULTIPOLYGON"),
        (3, 2, "c", "MULTIPOLYGON"),
    ]


def test_writes_geoparquet_metadata(geojson: Path, tmp_path: Path) -> None:
    out = tmp_path / "layer.parquet"
    write_geoparquet(geojson, out)
    row = (
        connect()
        .execute(f"SELECT value FROM parquet_kv_metadata('{out.as_posix()}') WHERE key = 'geo'")
        .fetchone()
    )
    assert row is not None
    assert json.loads(row[0])["columns"]["geometry"]["encoding"] == "WKB"


def test_conversion_is_reproducible_and_verified(geojson: Path, tmp_path: Path) -> None:
    first = tmp_path / "first.parquet"
    write_geoparquet(geojson, first)
    dest = tmp_path / "raw" / "layer.parquet"
    assert to_geoparquet(geojson, dest, sha256_of(first)) == dest
    assert sha256_of(dest) == sha256_of(first)


def test_wrong_pin_raises_and_leaves_nothing(geojson: Path, tmp_path: Path) -> None:
    dest = tmp_path / "raw" / "layer.parquet"
    with pytest.raises(ChecksumError):
        to_geoparquet(geojson, dest, "0" * 64)
    assert list(dest.parent.iterdir()) == []

"""Convert the IPR GeoJSON layers to GeoParquet, the format Spark reads everywhere.

Wherobots' GeoJSON reader fails on these files with every Spark setting tried. Their big
polygons (up to 105,000 vertices) become JSON text several megabytes long, and the reader
breaks it at a 16 KiB boundary (a zero byte at column 16,386). The identical files read
fine locally and in GeoPandas, and the GeoParquet buildings file reads fine on Wherobots.

GeoParquet stores geometries as binary WKB, is what the brief asks for, and is faster to
read than GeoJSON. The downloaded GeoJSON is never edited. The GeoParquet copies are
derived files, written by the same DuckDB writer as the Overture extract and pinned by
SHA-256 in config.py.
"""

from pathlib import Path

from prague_flood import config
from prague_flood.download import is_verified, promote
from prague_flood.duck import connect, quoted


def write_geoparquet(source: Path, path: Path) -> None:
    """Write the GeoJSON at ``source`` as GeoParquet at ``path``, attributes unchanged.

    Rows are sorted by IPR's `objectid`, so the same input always gives the same file.
    OGC_FID, a row number GDAL adds when it reads the file, is dropped.
    """
    con = connect()
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (OGC_FID, geom), geom AS geometry
            FROM ST_Read({quoted(source.as_posix())})
            ORDER BY objectid
        ) TO {quoted(path.as_posix())} (FORMAT parquet, COMPRESSION zstd)
    """)
    con.close()


def to_geoparquet(source: Path, dest: Path, sha256: str) -> Path:
    """Convert ``source`` to ``dest`` unless a verified copy is already there.

    Like download.fetch, the new file only replaces ``dest`` once its SHA-256 matches.
    """
    if is_verified(dest, sha256):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    write_geoparquet(source, part)
    return promote(part, dest, sha256, f"GeoParquet copy of {source.name}")


def convert_ipr(geojson: dict[str, Path], folder: Path = config.RAW_DIR) -> dict[str, Path]:
    """Convert every downloaded IPR layer (from download.download_ipr) to GeoParquet."""
    return {
        layer: to_geoparquet(
            path,
            folder / config.IPR_GEOPARQUET[layer].filename,
            config.IPR_GEOPARQUET[layer].sha256,
        )
        for layer, path in geojson.items()
    }

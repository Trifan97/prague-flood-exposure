"""Cut Prague's building footprints out of an Overture Maps release with DuckDB.

Overture publishes every building in the world as GeoParquet files on S3. Each row
carries a `bbox` column, and Parquet stores the minimum and maximum of that column for
every block of rows. DuckDB reads those statistics first and downloads only the blocks
that can overlap Prague, which is why one city can be cut out of a planet-sized dataset
from a laptop.

The extract keeps every building whose bounding box overlaps the city's. Deciding which
buildings are really inside Prague, and in which district, is left to the Sedona join.
"""

from pathlib import Path

from prague_flood import config
from prague_flood.config import BBox
from prague_flood.download import is_verified, promote
from prague_flood.duck import connect, quoted


def write_buildings(source: str, bbox: BBox, path: Path) -> None:
    """Write the buildings in ``source`` whose bounding boxes overlap ``bbox`` to ``path``.

    Rows are sorted by Overture id, so the same input always gives the same file.
    """
    con = connect()
    con.execute(f"""
        COPY (
            SELECT
                id,
                sources[1].dataset AS source_dataset,  -- OpenStreetMap or an ML dataset
                subtype,
                class,
                height,
                num_floors,
                geometry
            FROM read_parquet({quoted(source)})
            WHERE bbox.xmin <= {bbox.xmax} AND bbox.xmax >= {bbox.xmin}
              AND bbox.ymin <= {bbox.ymax} AND bbox.ymax >= {bbox.ymin}
            ORDER BY id
        ) TO {quoted(path.as_posix())} (FORMAT parquet, COMPRESSION zstd)
    """)
    con.close()


def extract_buildings(
    dest: Path,
    sha256: str,
    source: str = config.OVERTURE_BUILDINGS,
    bbox: BBox = config.PRAGUE_BBOX,
) -> Path:
    """Write the extract to ``dest`` unless a verified copy is already there.

    Like download.fetch, the new file only replaces ``dest`` once its SHA-256 matches.
    """
    if is_verified(dest, sha256):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    write_buildings(source, bbox, part)
    return promote(part, dest, sha256, f"Overture {config.OVERTURE_RELEASE} extract")

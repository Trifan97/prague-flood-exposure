"""Write results as GeoParquet for CARTO.

The outputs are small: 57 districts, about 10,000 buildings and 2,229 flood zones. Each
is collected to the driver and written with GeoPandas, which is the right tool for small
tables and behaves the same locally and on Wherobots. (Local Spark cannot write files
on Windows; see the README.)

CARTO expects WGS84 longitude/latitude, so geometries go back from UTM 33N to EPSG:4326.
Areas were computed in metres before this step and travel as columns.
"""

from pathlib import Path
from typing import Any

import geopandas as gpd
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from prague_flood import config


def to_geodataframe(df: DataFrame, sort_by: list[str]) -> gpd.GeoDataFrame:
    """Collect ``df`` to the driver as a GeoDataFrame in WGS84, sorted by ``sort_by``.

    The sort fixes the row order, so the same results always give the same file.
    """
    wgs84 = df.withColumn(
        "geometry", expr(f"ST_Transform(geometry, '{config.TARGET_CRS}', '{config.SOURCE_CRS}')")
    )
    # PySpark types toPandas() as a minimal protocol; at run time it is a pandas DataFrame.
    table: Any = wgs84.toPandas()
    table = table.sort_values(sort_by, ignore_index=True)
    return gpd.GeoDataFrame(table, geometry="geometry", crs=config.SOURCE_CRS)


def write_geoparquet(gdf: gpd.GeoDataFrame, path: Path) -> Path:
    """Write ``gdf`` to ``path``, replacing any previous file only once the write succeeds."""
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    gdf.to_parquet(part, index=False, compression="zstd")
    part.replace(path)
    return path


def exposed_buildings(exposure: DataFrame) -> DataFrame:
    """Buildings in a flood zone or behind the defences; the 'none' class is left out."""
    return exposure.where("exposure_class <> 'none'")


def zones_for_map(zones: DataFrame) -> DataFrame:
    """Whole flood zones with their area, for drawing under the buildings."""
    return zones.withColumn("area_ha", expr("ST_Area(geometry) / 1e4"))

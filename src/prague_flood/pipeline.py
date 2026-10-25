"""The whole analysis, from the files in data/raw to the GeoParquet outputs."""

from pathlib import Path

from pyspark.sql import SparkSession

from prague_flood import aggregate, config, export, hazard, join, load


def run(
    spark: SparkSession, out_dir: Path = config.PROCESSED_DIR, raw_uri: str = config.RAW_URI
) -> dict[str, Path]:
    """Load, join, aggregate and export. Returns the path of each output.

    ``raw_uri`` is the folder with the inputs (an s3:// folder on Wherobots). ``out_dir``
    is a local folder on the machine running this code (the driver).
    """
    # cache(): each of these feeds several later steps, and recomputing them (reading,
    # re-projecting, repairing, splitting) for every branch of every plan is the slow part.
    buildings = load.load_buildings(spark, raw_uri).cache()
    districts = load.load_districts(spark, raw_uri).cache()
    zones = hazard.stack_zones(
        load.load_vltava_zones(spark, raw_uri),
        load.load_small_streams(spark, raw_uri),
        load.load_vltava_categories(spark, raw_uri),
    ).cache()
    pieces = hazard.subdivide(zones).cache()

    exposure = join.exposure(buildings, districts, pieces).cache()
    districts_table = aggregate.district_summary(exposure, districts)

    tables = {
        "districts": export.to_geodataframe(districts_table, ["district_code"]),
        "buildings": export.to_geodataframe(export.exposed_buildings(exposure), ["building_id"]),
        "zones": export.to_geodataframe(export.zones_for_map(zones), ["hazard", "zone", "area_ha"]),
    }
    return {
        name: export.write_geoparquet(table, out_dir / config.OUTPUTS[name])
        for name, table in tables.items()
    }

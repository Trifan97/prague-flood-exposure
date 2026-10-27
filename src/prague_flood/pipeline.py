"""The whole analysis, from the input files to the GeoParquet outputs and the maps."""

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from prague_flood import aggregate, config, export, hazard, join, load


@dataclass
class Results:
    """What the analysis produces, as Sedona DataFrames in UTM 33N."""

    exposure: DataFrame  # one row per building inside Prague
    districts: DataFrame  # one row per district, with totals and ranks
    zones: DataFrame  # the flood zones, whole (not split into join pieces)


def compute(spark: SparkSession, raw_uri: str = config.RAW_URI) -> Results:
    """Load, join and aggregate. ``raw_uri`` is the folder with the inputs."""
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
    return Results(exposure, aggregate.district_summary(exposure, districts), zones)


def run(
    spark: SparkSession, out_dir: Path = config.PROCESSED_DIR, raw_uri: str = config.RAW_URI
) -> dict[str, Path]:
    """Compute and export the three GeoParquet files. Returns the path of each output.

    ``raw_uri`` is the folder with the inputs (an s3:// folder on Wherobots). ``out_dir``
    is a local folder on the machine running this code (the driver).
    """
    results = compute(spark, raw_uri)
    tables = {
        "districts": export.to_geodataframe(results.districts, ["district_code"]),
        "buildings": export.to_geodataframe(
            export.exposed_buildings(results.exposure), ["building_id"]
        ),
        "zones": export.to_geodataframe(
            export.zones_for_map(results.zones), ["hazard", "zone", "area_ha"]
        ),
    }
    return {
        name: export.write_geoparquet(table, out_dir / config.OUTPUTS[name])
        for name, table in tables.items()
    }


def make_maps(
    spark: SparkSession, out_dir: Path = config.PROCESSED_DIR, raw_uri: str = config.RAW_URI
) -> dict[str, Path]:
    """Compute and save the three kepler.gl maps as HTML in ``out_dir``/maps."""
    from prague_flood import maps  # needs the optional keplergl package

    results = compute(spark, raw_uri)
    specs = maps.map_specs(results.exposure, results.districts, results.zones)
    return maps.save(maps.build(specs), specs, out_dir / "maps")

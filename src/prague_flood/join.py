"""The distributed spatial joins: buildings to districts, and buildings to hazard zones.

`exposure()` returns one row per building inside Prague:

    building_id, source_dataset, building_class, num_floors
    district_code, district_name
    footprint_m2            area of the whole footprint
    vltava_return_period    smallest Vltava return period the footprint touches, or NULL
    small_stream            the footprint touches the small-stream Q100 area
    small_stream_active     ... touches the small-stream active zone
    behind_defences         ... touches the area protected by the city's flood defences
    return_period           smallest return period from either river source, or NULL
    exposure_class          'q5', 'q20', 'q50', 'q100', 'residual' or 'none'
    exposed_by_point        the building's representative point lies in a flood zone
    geometry                the footprint, EPSG:32633

A building counts as exposed when any part of its footprint touches a zone. That rule
overstates exposure for buildings that only graze a zone's edge, so `exposed_by_point`
records the stricter rule as well. Results are reported under both.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import broadcast, expr, row_number


def assign_districts(buildings: DataFrame, districts: DataFrame) -> DataFrame:
    """Add each building's district and a representative `point`; drop buildings outside Prague.

    The district is the one containing ST_PointOnSurface of the footprint. Unlike the
    centroid, that point is always on the footprint itself, even for an L-shaped building.
    """
    points = buildings.withColumn("point", expr("ST_PointOnSurface(geometry)"))
    # 57 districts are tiny next to 300,000 buildings, so broadcast them: every worker
    # gets a copy with a spatial index, and the buildings never move between workers.
    matched = points.alias("b").join(
        broadcast(districts.alias("d")), expr("ST_Intersects(d.geometry, b.point)")
    )
    # A point exactly on a shared border touches two districts. Keep the lower code, so
    # every building gets exactly one district and reruns give the same answer.
    first = Window.partitionBy("b.building_id").orderBy("d.district_code")
    return (
        matched.withColumn("rank", row_number().over(first))
        .where("rank = 1")
        .select("b.*", "d.district_code", "d.district_name")
    )


def touched_zones(
    buildings: DataFrame, zones: DataFrame, broadcast_zones: bool = True
) -> DataFrame:
    """One row per building that touches any hazard zone, with what it touches.

    `buildings` needs the `point` column from assign_districts. A building matches every
    piece it touches: several pieces of one zone, and each of the nested Vltava extents.
    The aggregation collapses those matches to one row.

    The point rule needs no join of its own. The point lies on the footprint, so any
    zone piece that contains the point also touches the footprint and is already among
    the matched pairs.
    """
    # The 7,940 zone pieces fit easily in memory, so by default they are broadcast with a
    # spatial index (Sedona's BroadcastIndexJoin) instead of both sides being
    # repartitioned across the cluster (RangeJoin). `broadcast_zones=False` keeps the
    # RangeJoin, to compare the two.
    right = broadcast(zones) if broadcast_zones else zones
    pairs = buildings.alias("b").join(
        right.alias("z"), expr("ST_Intersects(b.geometry, z.geometry)")
    )
    return pairs.groupBy("b.building_id").agg(
        expr("min(CASE WHEN z.hazard = 'vltava' THEN z.return_period END)").alias(
            "vltava_return_period"
        ),
        expr("bool_or(z.hazard = 'small_stream')").alias("small_stream"),
        expr("bool_or(z.zone = 'active_zone')").alias("small_stream_active"),
        expr("bool_or(z.hazard = 'vltava_residual')").alias("behind_defences"),
        # Residual risk is left out: it is not a flood zone while the defences hold.
        expr("bool_or(z.hazard <> 'vltava_residual' AND ST_Intersects(z.geometry, b.point))").alias(
            "exposed_by_point"
        ),
    )


def exposure(
    buildings: DataFrame, districts: DataFrame, zones: DataFrame, broadcast_zones: bool = True
) -> DataFrame:
    """The per-building exposure table described in the module docstring.

    For speed, cache `buildings` and `zones` first if they are reused: Spark otherwise
    recomputes them (re-projection, repair, subdivision) for every branch of the plan.
    """
    located = assign_districts(buildings, districts)
    touched = touched_zones(located, zones, broadcast_zones)

    return (
        located.join(touched, "building_id", "left")
        .selectExpr(
            "building_id",
            "source_dataset",
            "building_class",
            "num_floors",
            "district_code",
            "district_name",
            "ST_Area(geometry) AS footprint_m2",
            "vltava_return_period",
            "coalesce(small_stream, false) AS small_stream",
            "coalesce(small_stream_active, false) AS small_stream_active",
            "coalesce(behind_defences, false) AS behind_defences",
            # least() skips NULLs, so this is NULL only when neither source applies.
            "least(vltava_return_period, CASE WHEN small_stream THEN 100 END) AS return_period",
            "coalesce(exposed_by_point, false) AS exposed_by_point",
            "geometry",
        )
        .withColumn(
            "exposure_class",
            expr(
                "CASE WHEN return_period IS NOT NULL THEN concat('q', return_period) "
                "WHEN behind_defences THEN 'residual' ELSE 'none' END"
            ),
        )
    )

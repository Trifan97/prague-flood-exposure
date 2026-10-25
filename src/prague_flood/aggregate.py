"""Summarise the per-building exposure table by district and by flood class.

Areas are building footprint areas in hectares (1 ha = 10,000 m²). "Exposed" means the
footprint touches a flood zone with the defences holding; `_point` columns use the
stricter rule that the building's representative point lies in a zone.

Districts are ranked by exposed footprint area, not by building count: a garage and an
apartment block count as one building each, and many exposed buildings are small sheds
and cabins (see config.MIN_BUILDING_M2).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, expr, rank

from prague_flood import config

EXPOSED = "return_period IS NOT NULL"


def _hectares(condition: str) -> str:
    return f"coalesce(sum(CASE WHEN {condition} THEN footprint_m2 END), 0) / 1e4"


# Column name -> aggregate expression over the exposure table.
DISTRICT_MEASURES = {
    "buildings_n": "count(*)",
    "buildings_ha": "sum(footprint_m2) / 1e4",
    # Every return period is at most 100 years, so these also equal Q100 exposure.
    "exposed_n": f"count_if({EXPOSED})",
    "exposed_ha": _hectares(EXPOSED),
    "exposed_n_point": "count_if(exposed_by_point)",
    "exposed_ha_point": _hectares("exposed_by_point"),
    "exposed_n_min_size": f"count_if({EXPOSED} AND footprint_m2 >= {config.MIN_BUILDING_M2})",
    # Cumulative: buildings reached by a flood of at most this return period.
    "q5_n": "count_if(return_period <= 5)",
    "q20_n": "count_if(return_period <= 20)",
    "q50_n": "count_if(return_period <= 50)",
    "q5_ha": _hectares("return_period <= 5"),
    "q20_ha": _hectares("return_period <= 20"),
    "q50_ha": _hectares("return_period <= 50"),
    "small_stream_n": "count_if(small_stream)",
    "active_zone_n": "count_if(small_stream_active)",
    # Behind the defences and in no flood zone: flooded only if the defences fail.
    "residual_n": "count_if(exposure_class = 'residual')",
    "residual_ha": _hectares("exposure_class = 'residual'"),
    "exposed_ml_n": f"count_if({EXPOSED} AND source_dataset <> 'OpenStreetMap')",
}

RANKED_BY = ("exposed_ha", "exposed_n", "residual_ha")


def district_summary(exposure: DataFrame, districts: DataFrame) -> DataFrame:
    """One row per district with the measures above, shares, ranks and the district polygon."""
    measures = exposure.groupBy("district_code").agg(
        *[expr(sql).alias(name) for name, sql in DISTRICT_MEASURES.items()]
    )
    df = (
        districts.join(measures, "district_code", "left")
        # A district without buildings would have NULLs; it has zero of everything.
        .fillna(0, subset=list(DISTRICT_MEASURES))
        .withColumn(
            "exposed_share_pct",
            expr("CASE WHEN buildings_ha > 0 THEN 100 * exposed_ha / buildings_ha END"),
        )
        .withColumn(
            "exposed_ml_share_pct",
            expr("CASE WHEN exposed_n > 0 THEN 100 * exposed_ml_n / exposed_n END"),
        )
    )
    for metric in RANKED_BY:
        # A window with no partition puts all rows in one partition. Fine for 57 rows.
        df = df.withColumn(f"rank_{metric}", rank().over(Window.orderBy(col(metric).desc())))

    first = ["district_code", "district_name", *[f"rank_{m}" for m in RANKED_BY]]
    rest = [c for c in df.columns if c not in first and c != "geometry"]
    return df.select(*first, *rest, "geometry")


def class_summary(exposure: DataFrame) -> DataFrame:
    """City-wide totals for each exposure class ('q5' ... 'q100', 'residual', 'none')."""
    return exposure.groupBy("exposure_class").agg(
        expr("count(*)").alias("buildings_n"),
        expr("sum(footprint_m2) / 1e4").alias("buildings_ha"),
        expr("count_if(exposed_by_point)").alias("buildings_n_point"),
        expr(_hectares("exposed_by_point")).alias("buildings_ha_point"),
        expr(f"count_if(footprint_m2 >= {config.MIN_BUILDING_M2})").alias("buildings_n_min_size"),
        expr("count_if(source_dataset <> 'OpenStreetMap')").alias("buildings_ml_n"),
    )

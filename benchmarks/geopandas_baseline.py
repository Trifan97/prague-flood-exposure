"""The same exposure analysis with GeoPandas on one machine: the honest baseline.

It reads the same raw files, applies the same rules as the Sedona pipeline (re-projection,
repair, district by representative point, smallest return period, the point rule) and
then:

1. prints how long each stage took, for the README's laptop-vs-Sedona comparison;
2. compares every building's class with the Sedona output in data/processed. GeoPandas
   runs on GEOS/shapely and Sedona on JTS, so agreement between two independent
   implementations is strong evidence both are right.

The zones can be joined whole, or split into pieces of at most 256 vertices like the
Sedona pipeline does. A fair engine comparison uses split zones on both sides.

    python -m prague_flood run                       # first, for the Sedona output
    python benchmarks/geopandas_baseline.py --zones split
"""

import argparse
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import geopandas as gpd
import pandas as pd
import shapely

from prague_flood import config

TIMES: dict[str, float] = {}


@contextmanager
def stage(name: str) -> Iterator[None]:
    start = time.perf_counter()
    yield
    TIMES[name] = time.perf_counter() - start
    print(f"{name:<34} {TIMES[name]:7.1f} s", flush=True)


def polygonal(geom: Any) -> Any:
    """The polygon parts of a repaired geometry, like Sedona's ST_CollectionExtract(g, 3)."""
    if geom.geom_type in ("Polygon", "MultiPolygon"):
        return geom
    polygons = shapely.get_parts(geom)
    polygons = [p for p in polygons if p.geom_type == "Polygon"]
    return shapely.MultiPolygon(polygons) if polygons else shapely.Polygon()


def subdivide(geom: Any, max_vertices: int = config.MAX_VERTICES) -> list[Any]:
    """Cut ``geom`` in half through its bounding box until no piece has too many vertices.

    The same idea as Sedona's ST_SubDivideExplode (which shapely does not have): the
    pieces do not overlap, so their areas add up to the original.
    """
    if shapely.get_num_coordinates(geom) <= max_vertices:
        return [geom]
    xmin, ymin, xmax, ymax = geom.bounds
    if xmax - xmin >= ymax - ymin:
        mid = (xmin + xmax) / 2
        halves = [shapely.box(xmin, ymin, mid, ymax), shapely.box(mid, ymin, xmax, ymax)]
    else:
        mid = (ymin + ymax) / 2
        halves = [shapely.box(xmin, ymin, xmax, mid), shapely.box(xmin, mid, xmax, ymax)]
    pieces = []
    for half in halves:
        part = polygonal(shapely.intersection(geom, half))
        if not part.is_empty:
            pieces.extend(subdivide(part, max_vertices))
    return pieces


def to_target_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Re-project to UTM 33N, repair invalid polygons and drop empty ones (as load.py)."""
    gdf = gdf.to_crs(config.TARGET_CRS)
    invalid = ~gdf.is_valid
    gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].make_valid().apply(polygonal)
    return gdf[~gdf.is_empty]


def read_ipr(layer: str) -> gpd.GeoDataFrame:
    return gpd.read_file(config.RAW_DIR / config.IPR_LAYERS[layer].filename)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--zones",
        choices=["whole", "split"],
        default="split",
        help="join against whole zones, or zones split into pieces of at most 256 vertices",
    )
    args = parser.parse_args()
    print(f"zones: {args.zones}\n")

    with stage("read inputs"):
        buildings = gpd.read_parquet(config.RAW_DIR / config.BUILDINGS_FILENAME)
        districts = read_ipr("districts")
        vltava = [
            read_ipr(layer).assign(return_period=years)
            for layer, years in config.VLTAVA_RETURN_PERIODS.items()
        ]
        streams = read_ipr("small_streams")
        categories = read_ipr("vltava_categories")

    with stage("re-project and repair"):
        buildings = to_target_crs(buildings.rename(columns={"id": "building_id"}))
        districts = to_target_crs(
            districts.rename(columns={"kod_mc": "district_code", "nazev_mc": "district_name"})
        )
        zone_parts = [
            to_target_crs(v)[["return_period", "geometry"]].assign(
                hazard="vltava", zone=lambda d: "q" + d["return_period"].astype(str)
            )
            for v in vltava
        ]
        streams = to_target_crs(streams)
        zone_parts.append(
            streams.assign(
                hazard="small_stream",
                return_period=100,
                zone=streams["kateg"].astype(int).map(config.SMALL_STREAM_ZONES),
            )
        )
        categories = to_target_crs(categories)
        categories["category"] = categories["kateg"].astype(int).map(config.VLTAVA_CATEGORIES)
        zone_parts.append(
            categories[categories["category"] == "protected_by_city"].assign(
                hazard="vltava_residual", return_period=pd.NA, zone="protected_by_city"
            )
        )
        columns = ["hazard", "return_period", "zone", "geometry"]
        zones = gpd.GeoDataFrame(
            pd.concat([z[columns] for z in zone_parts], ignore_index=True),
            crs=config.TARGET_CRS,
        )

    if args.zones == "split":
        with stage("split zones (<= 256 vertices)"):
            # Lists of pieces in a plain column first, then one row per piece.
            pieces = zones.drop(columns="geometry").assign(geometry=zones.geometry.apply(subdivide))
            zones = gpd.GeoDataFrame(
                pieces.explode("geometry", ignore_index=True),
                geometry="geometry",
                crs=config.TARGET_CRS,
            )
        print(f"{'':<34} {len(zones):,} pieces")

    with stage("join: districts (representative point)"):
        points = gpd.GeoDataFrame(
            buildings[["building_id"]], geometry=buildings.representative_point()
        )
        located = gpd.sjoin(
            points, districts[["district_code", "geometry"]], predicate="intersects"
        )
        # A point on a shared border touches two districts: keep the lower code.
        located = located.sort_values(["building_id", "district_code"]).drop_duplicates(
            "building_id"
        )[["building_id", "district_code"]]
        inside = buildings.merge(located, on="building_id")
        points = points[points["building_id"].isin(located["building_id"])]

    with stage("join: hazard zones (footprints)"):
        pairs = gpd.sjoin(inside[["building_id", "geometry"]], zones, predicate="intersects")
        pairs["vltava_rp"] = pairs["return_period"].where(pairs["hazard"] == "vltava")
        pairs["small_stream"] = pairs["hazard"] == "small_stream"
        pairs["behind_defences"] = pairs["hazard"] == "vltava_residual"
        touched = pairs.groupby("building_id").agg(
            vltava_rp=("vltava_rp", "min"),
            small_stream=("small_stream", "any"),
            behind_defences=("behind_defences", "any"),
        )

    with stage("join: flood zones (points)"):
        flood = zones[zones["hazard"] != "vltava_residual"]
        by_point = set(gpd.sjoin(points, flood, predicate="intersects")["building_id"])

    with stage("classify"):
        table = inside[["building_id"]].merge(touched, on="building_id", how="left")
        # Buildings that touch no zone have NaN flags after the left merge: they are False.
        small = table["small_stream"].astype("boolean").fillna(False).astype(bool)
        small_rp = small.map({True: 100, False: None})
        rp = pd.concat([pd.to_numeric(table["vltava_rp"]), pd.to_numeric(small_rp)], axis=1)
        table["return_period"] = rp.min(axis=1)
        behind = table["behind_defences"].astype("boolean").fillna(False).astype(bool)
        table["exposure_class"] = "none"
        table.loc[behind, "exposure_class"] = "residual"
        has_rp = table["return_period"].notna()
        table.loc[has_rp, "exposure_class"] = "q" + table.loc[has_rp, "return_period"].astype(
            int
        ).astype(str)
        table["exposed_by_point"] = table["building_id"].isin(by_point)

    print(f"{'total':<34} {sum(TIMES.values()):7.1f} s")
    print(f"\nbuildings in Prague: {len(table):,}")
    print(table["exposure_class"].value_counts().sort_index().to_string())
    compare_with_sedona(table)


def compare_with_sedona(table: pd.DataFrame) -> None:
    path = config.PROCESSED_DIR / config.OUTPUTS["buildings"]
    if not path.exists():
        print(f"\n{path.name} not found; run `python -m prague_flood run` to compare.")
        return
    sedona = pd.read_parquet(path, columns=["building_id", "exposure_class", "exposed_by_point"])
    ours = table[table["exposure_class"] != "none"]
    both = ours.merge(sedona, on="building_id", how="outer", suffixes=("_gpd", "_sedona"))
    class_differs = both["exposure_class_gpd"] != both["exposure_class_sedona"]
    point_differs = both["exposed_by_point_gpd"] != both["exposed_by_point_sedona"]
    print(
        f"\nSedona output: {len(sedona):,} exposed or residual buildings; GeoPandas: {len(ours):,}"
    )
    print(f"buildings whose class differs:            {int(class_differs.sum())}")
    print(f"buildings whose point-rule result differs: {int(point_differs.sum())}")
    if class_differs.any():
        print(both[class_differs].head(10).to_string(index=False))


if __name__ == "__main__":
    main()

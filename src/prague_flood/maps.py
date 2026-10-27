"""Interactive kepler.gl maps of the results, rendered from Sedona and styled in code.

Each map's colours, layers, tooltips and starting view are defined here, so a notebook
only renders finished maps; nobody styles anything by hand on a runtime that bills by
the minute. SedonaKepler turns each Sedona DataFrame into a kepler.gl dataset, and every
map can also be saved as one standalone HTML file anyone can open.

Two display-only steps keep the maps light without touching the analysis:

- Flood-zone and district outlines are simplified to 2 m and 5 m before drawing. A
  100,000-vertex polygon is mostly detail that no screen shows at city zoom.
- Categories get labels that start with a number. kepler.gl orders categories
  alphabetically, so "1 · Q5" ... "5 · ..." makes the legend read in flood order.

The maps need the optional `keplergl` package: pip install -e .[maps]
"""

import inspect
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from prague_flood import config

# Legend labels and colours, in legend order. Reds for how often a building floods,
# purple for flooding only if the defences fail.
FLOOD_CLASSES = {
    "q5": ("1 · Q5, every 5 years", "#7f0000"),
    "q20": ("2 · Q20", "#d7301f"),
    "q50": ("3 · Q50", "#fc8d59"),
    "q100": ("4 · Q100", "#fdcc8a"),
    "residual": ("5 · Behind the defences only", "#6a51a3"),
}
# Blues for the Vltava (darker = more frequent), teals for small streams, lavender for the
# area the city's defences protect (it pairs with the purple residual-risk buildings).
ZONES = {
    "q5": ("1 · Vltava Q5", "#08306b"),
    "q20": ("2 · Vltava Q20", "#2171b5"),
    "q50": ("3 · Vltava Q50", "#4292c6"),
    "q100": ("4 · Vltava Q100", "#9ecae1"),
    "active_zone": ("5 · Small streams, active zone", "#00796b"),
    "q100_outside_active_zone": ("6 · Small streams, rest of Q100", "#80cbc4"),
    "protected_by_city": ("7 · Protected by the city's defences", "#bcbddc"),
}
# Exposed footprint per district (defences holding), the breaks used in docs/carto.md.
EXPOSURE_BANDS = (
    (0.0, "0 · No exposed buildings", "#e8e8e8"),
    (1.0, "1 · Under 1 ha", "#fde0a2"),
    (5.0, "2 · 1 to 5 ha", "#fdae61"),
    (10.0, "3 · 5 to 10 ha", "#e8603c"),
    (float("inf"), "4 · 10 ha or more", "#b2182b"),
)

COLORS = {label: color for label, color in [*FLOOD_CLASSES.values(), *ZONES.values()]}
COLORS |= {label: color for _, label, color in EXPOSURE_BANDS}


def sql_text(text: str) -> str:
    """A Spark SQL string literal. Labels such as "city's" contain quotes."""
    return "'" + text.replace("'", "''") + "'"


def _label(column: str, labels: dict[str, tuple[str, str]]) -> str:
    cases = " ".join(
        f"WHEN {sql_text(key)} THEN {sql_text(label)}" for key, (label, _) in labels.items()
    )
    return f"CASE {column} {cases} END"


def _band(column: str) -> str:
    cases = " ".join(
        f"WHEN {column} <= 0 THEN {sql_text(label)}"
        if upper == 0.0
        else f"WHEN {column} < {upper} THEN {sql_text(label)}"
        for upper, label, _ in EXPOSURE_BANDS
        if upper != float("inf")
    )
    return f"CASE {cases} ELSE {sql_text(EXPOSURE_BANDS[-1][1])} END"


def to_wgs84(df: DataFrame, simplify_m: float = 0.0) -> DataFrame:
    """Simplify `geometry` (in metres, UTM) for display, then re-project to lon/lat."""
    geometry = "geometry"
    if simplify_m:
        geometry = f"ST_SimplifyPreserveTopology(geometry, {simplify_m})"
    return df.withColumn(
        "geometry", expr(f"ST_Transform({geometry}, '{config.TARGET_CRS}', '{config.SOURCE_CRS}')")
    )


def district_table(districts: DataFrame) -> DataFrame:
    """aggregate.district_summary(), with the exposure band and rounded numbers for display."""
    return to_wgs84(
        districts.selectExpr(
            "district_name",
            f"{_band('exposed_ha')} AS exposure_band",
            "rank_exposed_ha",
            "round(exposed_ha, 2) AS exposed_ha",
            "exposed_n",
            "exposed_n_min_size",
            "round(exposed_share_pct, 1) AS exposed_share_pct",
            "round(residual_ha, 2) AS residual_ha",
            # No column that can be empty: older keplergl widgets cannot send the NaN that
            # an empty number becomes (the machine-learned share is empty for districts
            # with no exposed buildings, so it is left out).
            "geometry",
        ),
        simplify_m=5.0,
    )


def zone_table(zones: DataFrame) -> DataFrame:
    """hazard.stack_zones() output (whole zones), labelled for the legend."""
    return to_wgs84(
        zones.selectExpr(
            "hazard",
            f"{_label('zone', ZONES)} AS zone",
            "round(ST_Area(geometry) / 1e4, 1) AS area_ha",
            "geometry",
        ),
        simplify_m=2.0,
    )


def building_table(exposure: DataFrame) -> DataFrame:
    """Exposed and residual-risk buildings from join.exposure(), labelled for the legend."""
    return to_wgs84(
        exposure.where("exposure_class <> 'none'").selectExpr(
            f"{_label('exposure_class', FLOOD_CLASSES)} AS flood_class",
            "district_name",
            "round(footprint_m2, 0) AS footprint_m2",
            "building_class",
            "source_dataset",
            "exposed_by_point",
            "geometry",
        )
    )


@dataclass(frozen=True)
class View:
    """Where a map opens: centre in lon/lat degrees, and web-map zoom level."""

    longitude: float
    latitude: float
    zoom: float


@dataclass(frozen=True)
class Layer:
    """One kepler.gl layer: a dataset drawn as polygons, coloured by a category column."""

    dataset: str
    label: str
    color_by: str
    tooltip: list[str]
    opacity: float = 0.8
    outline: tuple[int, int, int] | None = None


@dataclass
class MapSpec:
    """A map: its datasets (name -> display table), layers (first drawn on top), view."""

    title: str
    datasets: dict[str, DataFrame]
    layers: list[Layer]
    view: View
    categories: dict[str, list[str]] = field(default_factory=dict)


def layer_config(layer: Layer, categories: list[str]) -> dict[str, Any]:
    """kepler.gl config for one layer.

    An ordinal colour scale hands out colours to the categories in sorted order, so the
    colours are listed for exactly the categories present, in that order.
    """
    return {
        "id": f"{layer.dataset}-{layer.color_by}",
        "type": "geojson",
        "config": {
            "dataId": layer.dataset,
            "label": layer.label,
            "columns": {"geojson": "geometry"},
            "isVisible": True,
            "visConfig": {
                "opacity": layer.opacity,
                "strokeOpacity": 0.9,
                "thickness": 0.5,
                "strokeColor": list(layer.outline) if layer.outline else None,
                "colorRange": {
                    "name": layer.label,
                    "type": "custom",
                    "category": "Custom",
                    "colors": [COLORS[c] for c in sorted(categories)],
                },
                "filled": True,
                "stroked": layer.outline is not None,
            },
        },
        "visualChannels": {
            "colorField": {"name": layer.color_by, "type": "string"},
            "colorScale": "ordinal",
        },
    }


def basemap_for(keplergl_version: str) -> str:
    """The light basemap's name in the kepler.gl that this keplergl package bundles.

    keplergl 0.3.2 bundles kepler.gl 2.5, whose basemaps are Mapbox styles ("light").
    0.3.4 and later bundle kepler.gl 3, with free basemaps such as "positron". Wherobots
    ships its own keplergl, older than the 0.4 used locally, so the name is chosen at
    run time.
    """
    numbers = []
    for part in keplergl_version.split(".")[:3]:
        if not part.isdigit():
            break
        numbers.append(int(part))
    return "positron" if tuple(numbers) >= (0, 3, 4) else "light"


def kepler_config(spec: MapSpec, basemap: str = "positron") -> dict[str, Any]:
    """The whole kepler.gl config for a map whose categories are filled in."""
    return {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [layer_config(lay, spec.categories[lay.dataset]) for lay in spec.layers],
                "interactionConfig": {
                    "tooltip": {
                        "enabled": True,
                        "fieldsToShow": {
                            lay.dataset: [{"name": name, "format": None} for name in lay.tooltip]
                            for lay in spec.layers
                        },
                    }
                },
                "layerBlending": "normal",
            },
            "mapState": {
                "longitude": spec.view.longitude,
                "latitude": spec.view.latitude,
                "zoom": spec.view.zoom,
                "bearing": 0,
                "pitch": 0,
                "dragRotate": False,
            },
            "mapStyle": {"styleType": basemap},
        },
    }


BUILDING_TOOLTIP = [
    "flood_class",
    "district_name",
    "footprint_m2",
    "building_class",
    "source_dataset",
    "exposed_by_point",
]


def map_specs(exposure: DataFrame, districts: DataFrame, zones: DataFrame) -> dict[str, MapSpec]:
    """The three maps: the district ranking, the flood zones, and the defences-fail view."""
    buildings = building_table(exposure)
    zone_layer = zone_table(zones)
    residual_only = f"flood_class = {sql_text(FLOOD_CLASSES['residual'][0])}"
    return {
        "districts": MapSpec(
            title="Exposed building footprint by district, defences holding",
            datasets={"districts": district_table(districts)},
            layers=[
                Layer(
                    "districts",
                    "Exposed footprint (ha)",
                    "exposure_band",
                    [
                        "district_name",
                        "rank_exposed_ha",
                        "exposed_ha",
                        "exposed_n",
                        "exposed_share_pct",
                        "residual_ha",
                    ],
                    opacity=0.75,
                    outline=(255, 255, 255),
                )
            ],
            view=View(longitude=14.46, latitude=50.06, zoom=10.3),
        ),
        "flood_zones": MapSpec(
            title="Buildings in the flood zones, Vltava-Berounka confluence",
            datasets={
                "buildings": buildings.where(f"NOT ({residual_only})"),
                "zones": zone_layer.where("hazard <> 'vltava_residual'"),
            },
            layers=[
                Layer("buildings", "Most frequent flood", "flood_class", BUILDING_TOOLTIP, 0.9),
                Layer("zones", "Flood zones", "zone", ["zone", "area_ha"], opacity=0.35),
            ],
            view=View(longitude=14.375, latitude=49.975, zoom=13.0),
        ),
        "defences_fail": MapSpec(
            title="If the defences fail: residual risk in central Prague",
            # The protected area, with the Q100 extent for context: the defences stop the
            # water at the edge of the blue.
            datasets={
                "buildings": buildings.where(residual_only),
                "zones": zone_layer.where(
                    f"hazard = 'vltava_residual' OR zone = {sql_text(ZONES['q100'][0])}"
                ),
            },
            layers=[
                Layer("buildings", "Behind the defences", "flood_class", BUILDING_TOOLTIP, 0.9),
                Layer("zones", "Zones", "zone", ["zone", "area_ha"], opacity=0.5),
            ],
            view=View(longitude=14.435, latitude=50.091, zoom=13.6),
        ),
    }


def installed_basemap() -> str:
    return basemap_for(metadata.version("keplergl"))


def build(specs: dict[str, MapSpec], height: int = 650) -> dict[str, Any]:
    """Render each MapSpec as a kepler.gl map. Returns name -> KeplerGl (shows in Jupyter).

    SedonaKepler.add_df turns each Sedona DataFrame into a kepler.gl dataset. The map
    itself is created directly: SedonaKepler.create_map passes arguments that only the
    newest keplergl accepts, and Wherobots ships an older one.
    """
    from keplergl import KeplerGl
    from sedona.spark.maps.SedonaKepler import SedonaKepler

    basemap = installed_basemap()
    maps = {}
    for name, spec in specs.items():
        for dataset, df in spec.datasets.items():
            column = next(lay.color_by for lay in spec.layers if lay.dataset == dataset)
            spec.categories[dataset] = [row[0] for row in df.select(column).distinct().collect()]
        kepler_map = KeplerGl(height=height)
        for dataset, df in spec.datasets.items():
            SedonaKepler.add_df(kepler_map, df, name=dataset)
        kepler_map.config = kepler_config(spec, basemap)
        maps[name] = kepler_map
    return maps


def save(maps: dict[str, Any], specs: dict[str, MapSpec], folder: Path) -> dict[str, Path]:
    """Write each map as one standalone HTML file, opening at its configured view.

    The page title (`app_name`) is only passed where the installed keplergl accepts it.
    """
    folder.mkdir(parents=True, exist_ok=True)
    basemap = installed_basemap()
    paths = {}
    for name, kepler_map in maps.items():
        path = folder / f"prague_flood_{name}.html"
        options = {
            "file_name": str(path),
            "config": kepler_config(specs[name], basemap),
            "center_map": False,
            "app_name": specs[name].title,
        }
        accepted = inspect.signature(kepler_map.save_to_html).parameters
        kepler_map.save_to_html(**{k: v for k, v in options.items() if k in accepted})
        paths[name] = path
    return paths

"""Compare two sets of pipeline outputs, for example a local run and a Wherobots run.

    python scripts/compare_outputs.py data/processed C:/Users/you/Downloads

For each of the three GeoParquet files it checks the columns and row count, then every
cell: text and flags must match exactly; numbers within a relative 1e-9; geometries
within 1e-9 degrees (about 0.1 mm), since two runtimes may differ in the last bits of
floating-point arithmetic.
"""

import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from prague_flood import config

SORT_KEYS = {
    "districts": ["district_code"],
    "buildings": ["building_id"],
    "zones": ["hazard", "zone", "area_ha"],
}


def differing_cells(left: gpd.GeoDataFrame, right: gpd.GeoDataFrame, column: str) -> int:
    a, b = left[column], right[column]
    if column == "geometry":
        same = left.geometry.geom_equals_exact(right.geometry, tolerance=1e-9)
    elif pd.api.types.is_numeric_dtype(a) and not pd.api.types.is_bool_dtype(a):
        same = pd.Series(np.isclose(a, b, rtol=1e-9, atol=0, equal_nan=True))
    else:
        same = (a == b) | (a.isna() & b.isna())
    return int((~same).sum())


def compare(name: str, first: Path, second: Path) -> bool:
    keys = SORT_KEYS[name]
    left = gpd.read_parquet(first).sort_values(keys, ignore_index=True)
    right = gpd.read_parquet(second).sort_values(keys, ignore_index=True)
    print(f"{name}: {len(left):,} vs {len(right):,} rows")
    if list(left.columns) != list(right.columns):
        print(f"  columns differ: {list(left.columns)} vs {list(right.columns)}")
        return False
    if len(left) != len(right):
        return False
    differences = {c: differing_cells(left, right, c) for c in left.columns}
    for column, n in differences.items():
        if n:
            print(f"  {column}: {n} cells differ")
    total = sum(differences.values())
    print(
        f"  {len(left.columns)} columns, {len(left) * len(left.columns):,} cells: "
        + ("all identical" if total == 0 else f"{total} differ")
    )
    return total == 0


def main() -> None:
    first, second = (Path(arg) for arg in sys.argv[1:3])
    results = [
        compare(name, first / filename, second / filename)
        for name, filename in config.OUTPUTS.items()
    ]
    print("\nIDENTICAL" if all(results) else "\nDIFFERENT")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()

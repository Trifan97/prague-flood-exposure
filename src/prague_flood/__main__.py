"""Command line: python -m prague_flood <stage>.

Stages are separate because they can run in different places: `download` runs locally
(DuckDB reads Overture from S3), and `run`, the Sedona pipeline, runs locally or on
Wherobots.
"""

import argparse

from prague_flood import config
from prague_flood.convert import convert_ipr
from prague_flood.download import download_ipr, is_verified
from prague_flood.overture import extract_buildings


def download() -> None:
    # The GeoJSON as published, then the GeoParquet copies Spark reads (see convert.py).
    for name, path in convert_ipr(download_ipr()).items():
        print(f"{name:<18} {path}")
    dest = config.RAW_DIR / config.BUILDINGS_FILENAME
    if not is_verified(dest, config.BUILDINGS_SHA256):
        print("buildings          extracting from Overture on S3 (about 6 minutes)")
    path = extract_buildings(dest, config.BUILDINGS_SHA256)
    print(f"{'buildings':<18} {path}")


def run() -> None:
    # Imported here so `download` works without starting Java.
    from prague_flood.pipeline import run as run_pipeline
    from prague_flood.session import local_session

    spark = local_session()
    try:
        for name, path in run_pipeline(spark).items():
            print(f"{name:<10} {path}")
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m prague_flood")
    stages = parser.add_subparsers(dest="stage", required=True)
    stages.add_parser("download", help="fetch the IPR layers and the Overture buildings extract")
    stages.add_parser("run", help="join, aggregate and export GeoParquet to data/processed")
    args = parser.parse_args(argv)

    if args.stage == "download":
        download()
    elif args.stage == "run":
        run()


if __name__ == "__main__":
    main()

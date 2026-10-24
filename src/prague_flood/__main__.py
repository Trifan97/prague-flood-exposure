"""Command line: python -m prague_flood <stage>.

Stages are separate because they run in different places: `download` runs locally
(DuckDB reads Overture from S3), and the Sedona pipeline will run on Wherobots.
"""

import argparse

from prague_flood import config
from prague_flood.download import download_ipr, is_verified
from prague_flood.overture import extract_buildings


def download() -> None:
    for name, path in download_ipr().items():
        print(f"{name:<18} {path}")
    dest = config.RAW_DIR / config.BUILDINGS_FILENAME
    if not is_verified(dest, config.BUILDINGS_SHA256):
        print("buildings          extracting from Overture on S3 (about 6 minutes)")
    path = extract_buildings(dest, config.BUILDINGS_SHA256)
    print(f"{'buildings':<18} {path}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m prague_flood")
    stages = parser.add_subparsers(dest="stage", required=True)
    stages.add_parser("download", help="fetch the IPR layers and the Overture buildings extract")
    args = parser.parse_args(argv)

    if args.stage == "download":
        download()


if __name__ == "__main__":
    main()

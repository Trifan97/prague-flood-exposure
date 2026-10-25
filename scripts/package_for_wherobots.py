"""Zip the committed prague_flood package for Wherobots.

Wherobots loads your own Python code from a zip in its storage:
`sedona.sparkContext.addPyFile("s3://.../prague_flood-<commit>.zip")`. The zip is built
with `git archive` from the last commit, never from the working tree, so the code that
runs in the cloud is exactly a commit you can point to.

    python scripts/package_for_wherobots.py
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def git(*args: str) -> str:
    result = subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def main() -> None:
    if git("status", "--porcelain", "--", "src"):
        sys.exit("src/ has uncommitted changes. Commit them first, so the zip matches a commit.")
    commit = git("rev-parse", "--short", "HEAD")
    out = ROOT / "dist" / f"prague_flood-{commit}.zip"
    out.parent.mkdir(exist_ok=True)
    # HEAD:src is the src folder of the last commit; the zip then holds prague_flood/...
    # core.autocrlf=false: Git for Windows defaults to true, which would make git archive
    # write Windows line endings. This keeps the zip byte-identical to the commit.
    git(
        "-c", "core.autocrlf=false",
        "archive", "--format=zip", f"--output={out}", "HEAD:src", "prague_flood",
    )  # fmt: skip
    print(out)


if __name__ == "__main__":
    main()

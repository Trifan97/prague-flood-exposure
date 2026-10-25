"""The package must import from a plain zip, the way Wherobots loads it (addPyFile)."""

import subprocess
import sys
import zipfile
from pathlib import Path

import prague_flood

SRC = Path(prague_flood.__file__).parent


def test_imports_from_a_zip_without_install_metadata(tmp_path: Path) -> None:
    archive = tmp_path / "prague_flood.zip"
    with zipfile.ZipFile(archive, "w") as z:
        for file in SRC.glob("*.py"):
            z.write(file, f"prague_flood/{file.name}")

    # -S skips site-packages, so the installed copy and its metadata are invisible, as
    # they are on Wherobots. Only modules that need no third-party packages can load.
    code = (
        f"import sys; sys.path.insert(0, {str(archive)!r}); "
        "import prague_flood, prague_flood.config; "
        "print(prague_flood.__file__); print(prague_flood.__version__)"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c", code], capture_output=True, text=True, check=True
    )
    imported_from, version = result.stdout.splitlines()
    assert imported_from.startswith(str(archive))
    assert version == "unknown (not installed)"

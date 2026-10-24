"""Download files and prove they are the exact bytes the project was built with."""

import hashlib
import shutil
import urllib.request
from pathlib import Path

from prague_flood import config
from prague_flood.config import Source


class ChecksumError(Exception):
    """A file's SHA-256 differs from the one pinned in config.py."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        # Read in 1 MiB chunks so large files never have to fit in memory.
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_verified(path: Path, sha256: str) -> bool:
    return path.exists() and sha256_of(path) == sha256


def promote(part: Path, dest: Path, sha256: str, origin: str) -> Path:
    """Rename ``part`` to ``dest`` if its SHA-256 matches; otherwise delete it and raise.

    Writing to a ``.part`` file first means an interrupted or wrong download never leaves
    a file that looks complete.
    """
    actual = sha256_of(part)
    if actual != sha256:
        part.unlink()
        raise ChecksumError(f"{origin}: expected SHA-256 {sha256}, got {actual}")
    part.replace(dest)
    return dest


def fetch(source: Source, folder: Path) -> Path:
    """Download ``source`` into ``folder`` unless a verified copy is already there."""
    dest = folder / source.filename
    if is_verified(dest, source.sha256):
        return dest

    folder.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    with urllib.request.urlopen(source.url, timeout=60) as response, part.open("wb") as out:
        shutil.copyfileobj(response, out)
    return promote(part, dest, source.sha256, source.url)


def download_ipr(folder: Path = config.RAW_DIR) -> dict[str, Path]:
    """Fetch every IPR Praha layer listed in config.IPR_LAYERS."""
    return {name: fetch(source, folder) for name, source in config.IPR_LAYERS.items()}

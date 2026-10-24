"""Download files and prove they are the exact bytes the project was built with."""

import hashlib
import shutil
import urllib.request
from pathlib import Path

from prague_flood.config import Source


class ChecksumError(Exception):
    """A downloaded file's SHA-256 differs from the one pinned in config.py."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        # Read in 1 MiB chunks so large files never have to fit in memory.
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch(source: Source, folder: Path) -> Path:
    """Download ``source`` into ``folder`` unless a verified copy is already there.

    The bytes go to a ``.part`` file that is renamed only once its checksum matches, so
    an interrupted or corrupted download never leaves a file that looks complete.
    """
    dest = folder / source.filename
    if dest.exists() and sha256_of(dest) == source.sha256:
        return dest

    folder.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    with urllib.request.urlopen(source.url, timeout=60) as response, part.open("wb") as out:
        shutil.copyfileobj(response, out)

    actual = sha256_of(part)
    if actual != source.sha256:
        part.unlink()
        raise ChecksumError(f"{source.url}: expected SHA-256 {source.sha256}, got {actual}")
    part.replace(dest)
    return dest

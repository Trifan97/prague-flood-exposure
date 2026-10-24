"""fetch() must only ever leave files whose checksum matches the pinned one.

The tests download from file:// URLs, so they need no network.
"""

import hashlib
from pathlib import Path

import pytest

from prague_flood.config import Source
from prague_flood.download import ChecksumError, fetch

CONTENT = b"flood zones"
GOOD_SHA256 = hashlib.sha256(CONTENT).hexdigest()


def make_source(tmp_path: Path, sha256: str = GOOD_SHA256) -> Source:
    origin = tmp_path / "origin.bin"
    origin.write_bytes(CONTENT)
    return Source(url=origin.as_uri(), filename="copy.bin", sha256=sha256)


def test_downloads_and_verifies(tmp_path: Path) -> None:
    dest = fetch(make_source(tmp_path), tmp_path / "raw")
    assert dest.read_bytes() == CONTENT


def test_bad_checksum_raises_and_leaves_nothing(tmp_path: Path) -> None:
    folder = tmp_path / "raw"
    with pytest.raises(ChecksumError):
        fetch(make_source(tmp_path, sha256="0" * 64), folder)
    assert list(folder.iterdir()) == []


def test_verified_copy_is_not_downloaded_again(tmp_path: Path) -> None:
    source = make_source(tmp_path)
    folder = tmp_path / "raw"
    fetch(source, folder)
    # With the origin gone, a second download would fail; fetch must reuse the copy.
    (tmp_path / "origin.bin").unlink()
    assert fetch(source, folder).read_bytes() == CONTENT

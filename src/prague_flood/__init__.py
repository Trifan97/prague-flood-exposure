"""Prague buildings exposed to flood zones, computed with Apache Sedona."""

from importlib.metadata import PackageNotFoundError, version

# Read the version from the installed package metadata, so pyproject.toml is the only
# place it is written down.
try:
    __version__ = version("prague-flood-exposure")
except PackageNotFoundError:
    # Loaded from a zip with sparkContext.addPyFile on Wherobots: no install, no metadata.
    __version__ = "unknown (not installed)"

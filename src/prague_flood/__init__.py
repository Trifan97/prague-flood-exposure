"""Prague buildings exposed to flood zones, computed with Apache Sedona."""

from importlib.metadata import version

# Read the version from the installed package metadata, so pyproject.toml is the only
# place it is written down.
__version__ = version("prague-flood-exposure")

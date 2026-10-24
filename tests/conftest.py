"""Shared test fixtures."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from prague_flood import config
from prague_flood.session import local_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # One session for the whole test run: starting the JVM takes seconds, so starting
    # one per test would dominate the run time. Two cores keep the tests light.
    session = local_session(master="local[2]", app_name="prague-flood-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw_data() -> None:
    """Skip, with the reason printed, unless every input is in data/raw."""
    paths = [config.RAW_DIR / s.filename for s in config.IPR_LAYERS.values()]
    paths.append(config.RAW_DIR / config.BUILDINGS_FILENAME)
    missing = [p.name for p in paths if not p.exists()]
    if missing:
        pytest.skip(f"not downloaded: {missing}; run: python -m prague_flood download")

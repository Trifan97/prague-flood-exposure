"""Shared test fixtures."""

from collections.abc import Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession

from prague_flood import config, hazard, load
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


@pytest.fixture(scope="session")
def layers(spark: SparkSession, raw_data: None) -> Iterator[dict[str, DataFrame]]:
    """Every loaded layer plus the hazard-zone table, cached and registered as SQL views.

    Shared by all data tests, so each layer is read, re-projected and subdivided once
    per test run rather than once per test file.
    """
    loaded = {
        "districts": load.load_districts(spark).cache(),
        "vltava_zones": load.load_vltava_zones(spark).cache(),
        "small_streams": load.load_small_streams(spark).cache(),
        "vltava_categories": load.load_vltava_categories(spark).cache(),
        "buildings": load.load_buildings(spark).cache(),
    }
    loaded["hazard_zones"] = hazard.hazard_zones(
        loaded["vltava_zones"], loaded["small_streams"], loaded["vltava_categories"]
    ).cache()
    for name, df in loaded.items():
        df.createOrReplaceTempView(name)
        # cache() is lazy. count() fills the cache now, once, rather than in the middle of
        # a later query whose plan reads the same layer from several branches.
        df.count()
    yield loaded
    for df in loaded.values():
        df.unpersist()

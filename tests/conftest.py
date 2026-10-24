"""Shared test fixtures."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from prague_flood.session import local_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # One session for the whole test run: starting the JVM takes seconds, so starting
    # one per test would dominate the run time. Two cores keep the tests light.
    session = local_session(master="local[2]", app_name="prague-flood-tests")
    yield session
    session.stop()

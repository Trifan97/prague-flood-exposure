"""Step-0 checks: the package is installed and the environment matches Wherobots."""

from importlib.metadata import version

from prague_flood import __version__, config


def test_package_is_installed() -> None:
    # The src/ layout means this import only works through `pip install -e .`, so the tests
    # exercise the installed package, not stray files next to them.
    assert __version__ == "0.1.0"


def test_project_root_is_the_repository() -> None:
    assert (config.PROJECT_ROOT / "pyproject.toml").is_file()


def test_data_dirs_are_separate_and_inside_the_repo() -> None:
    assert config.RAW_DIR.is_relative_to(config.PROJECT_ROOT)
    assert config.PROCESSED_DIR.is_relative_to(config.PROJECT_ROOT)
    assert config.RAW_DIR != config.PROCESSED_DIR


def test_pyspark_matches_wherobots_runtime() -> None:
    # Wherobots v2 runs Spark 4.0. On another minor version, code can pass here and
    # fail on the cluster.
    assert version("pyspark").startswith("4.0.")


def test_sedona_python_matches_sedona_jars() -> None:
    assert version("apache-sedona") == config.SEDONA_VERSION

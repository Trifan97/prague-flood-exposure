"""DuckDB with spatial support: the local tool that prepares the GeoParquet inputs."""

import duckdb

from prague_flood import config


def quoted(text: str) -> str:
    """A SQL string literal (single quotes doubled)."""
    return "'" + text.replace("'", "''") + "'"


def connect() -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with spatial support and anonymous access to Overture's bucket."""
    con = duckdb.connect()
    con.execute(f"SET extension_directory = {quoted(config.DUCKDB_EXTENSIONS_DIR.as_posix())}")
    for extension in ("spatial", "httpfs"):
        con.install_extension(extension)
        con.load_extension(extension)
    con.execute("SET s3_region = 'us-west-2'")
    return con
